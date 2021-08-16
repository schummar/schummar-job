import assert from 'assert';
import { Collection } from 'mongodb';
import { nanoid } from 'nanoid';
import { Queue } from 'schummar-queue';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import { DistributedJobOptions, JobDbEntry, JobExecuteOptions, JobImplementation, json } from './types';

export class DistributedJob<Data extends json, Result extends json | void> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  private subscribedExecutionIds = new Array<{
    executionId: string | number;
    listener: (result: Result | null, error?: unknown) => void;
  }>();
  public readonly options: DistributedJobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly collection: MaybePromise<Collection<JobDbEntry<any, any>>>,
    public readonly jobId: string,
    public readonly implementation: JobImplementation<Data, Result> | null,
    options: Partial<DistributedJobOptions<Data>> = {}
  ) {
    this.options = {
      maxParallel: DistributedJob.DEFAULT_MAX_PARALLEL,
      retryCount: scheduler.options.retryCount,
      retryDelay: scheduler.options.retryDelay,
      log: scheduler.options.log,
      lockDuration: scheduler.options.lockDuration,
      lockCheckInterval: scheduler.options.lockCheckInterval,
      ...options,
    };
    this.q = new Queue({ parallel: this.options.maxParallel });

    if (implementation) {
      this.schedule();
      this.checkLocks();
      this.next();
    }
  }

  async execute(
    ...[data, { delay = 0, executionId = nanoid() } = {}]: null extends Data
      ? [data?: null, options?: JobExecuteOptions]
      : [data: Data, options?: JobExecuteOptions]
  ): Promise<string> {
    const clonedData = data && JSON.parse(JSON.stringify(data));
    const col = await this.collection;
    await col.updateOne(
      {
        jobId: this.jobId,
        executionId,
      },
      {
        $setOnInsert: {
          jobId: this.jobId,
          schedule: null,
          data: clonedData ?? null,
          nextRun: new Date(Date.now() + delay),
          completedOn: null,
          lock: null,
          attempt: 0,
          history: [],
        },
      },
      { upsert: true }
    );

    return executionId;
  }

  async getResult(executionId: string): Promise<Result> {
    return new Promise<Result>(async (resolve, reject) => {
      const listener = (result: Result | null, error?: unknown) => {
        this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener);
        if (error) reject(error);
        else resolve(result as Result);
      };

      this.subscribedExecutionIds.push({ executionId, listener });

      const col = await this.collection;
      const existing = await col.findOne({ jobId: this.jobId, executionId });
      if (existing?.completedOn) {
        const { result = null, error } = existing.history?.[existing.history.length - 1] ?? {};
        listener(result, error);
      }
    });
  }

  async executeAndAwait(...args: Parameters<DistributedJob<Data, Result>['execute']>): Promise<Result> {
    const id = await this.execute(...args);
    return this.getResult(id);
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }

    await this.q.untilEmpty;
  }

  private async schedule() {
    const { schedule } = this.options;
    const data = schedule && (schedule as { data?: Data }).data;
    const clonedData = data && JSON.parse(JSON.stringify(data));

    if (!schedule) return;

    const col = await this.collection;
    await col.updateOne(
      { jobId: this.jobId, executionId: null },
      {
        $setOnInsert: {
          jobId: this.jobId,
          executionId: null,
          data: clonedData ?? null,
          nextRun: calcNextRun(schedule),
          completedOn: null,
          lock: null,
          attempt: 0,
          history: [],
        },
        $set: {
          schedule: schedule,
        },
      },
      { upsert: true }
    );
  }

  private async checkLocks() {
    const col = await this.collection;

    while (!this.hasShutDown) {
      try {
        const threshold = new Date(Date.now() - this.options.lockDuration);
        const res = await col.updateMany({ jobId: this.jobId, lock: { $lt: threshold } }, { $set: { lock: null } });
        if (res.modifiedCount) this.options.log('info', 'Unlocked jobs:', res.modifiedCount);
      } catch (e) {
        this.options.log('warn', 'Failed to check locks:', e);
      }

      await sleep(this.options.lockCheckInterval);
    }
  }

  private next() {
    if (this.hasShutDown) return;

    this.q.clear(true);
    this.q.schedule(async () => {
      try {
        if (this.timeout) {
          clearTimeout(this.timeout.handle);
          delete this.timeout;
        }

        const col = await this.collection;

        const now = new Date();

        const { value: job } = await col.findOneAndUpdate(
          {
            jobId: this.jobId,
            nextRun: { $lte: now },
            lock: null,
            completedOn: null,
            attempt: { $lte: this.options.retryCount },
          },
          {
            $set: { lock: now },
          }
        );

        if (!job) {
          this.checkForNextRun();
          return;
        }
        this.next();

        assert(this.implementation);
        const history = job.history ?? [];

        try {
          const history = job.history ?? [];
          const { result = null, error } = history[history.length - 1] ?? {};
          const newResult = await this.implementation(job.data, { result, error, attempt: job.attempt }, job);

          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                nextRun: job.schedule ? calcNextRun(job.schedule) : undefined,
                lock: null,
                completedOn: job.schedule ? null : new Date(),
                attempt: job.attempt + 1,
                history: history.concat({
                  t: new Date(),
                  result: newResult,
                  error: null,
                }),
              },
            }
          );
        } catch (e) {
          const msg = e instanceof Error ? e.message : e instanceof Object ? JSON.stringify(e) : String(e);
          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                nextRun: new Date(Date.now() + this.options.retryDelay),
                lock: null,
                attempt: job.attempt + 1,
                history: history.concat({
                  t: new Date(),
                  result: null,
                  error: msg,
                }),
              },
              $push: {},
            }
          );
        }
      } catch (e) {
        this.options.log('error', 'next failed', e);
      }
    });
  }

  async checkForNextRun(): Promise<void> {
    if (this.hasShutDown) return;

    const col = await this.collection;

    const [next] = await col
      .find({
        jobId: this.jobId,
        lock: null,
        completedOn: null,
        attempt: { $lte: this.options.retryCount },
      })
      .sort({ nextRun: 1 })
      .limit(1)
      .toArray();

    if (next) this.planNextRun(next);
  }

  async receiveUpdate(job: JobDbEntry<Data, Result>): Promise<void> {
    if (job.completedOn) {
      const history = job.history ?? [];
      const { result = null, error } = history[history.length - 1] ?? {};
      for (const { executionId, listener } of this.subscribedExecutionIds) {
        if (executionId === job.executionId) listener(result, error);
      }
    } else {
      return this.planNextRun(job);
    }
  }

  async planNextRun(job: JobDbEntry<Data, Result>): Promise<void> {
    if (this.hasShutDown) return;

    const now = Date.now();
    const date = new Date(Math.min(job.nextRun.getTime(), now + 60 * 60 * 1000));

    if (!this.timeout || date.getTime() < this.timeout.date.getTime()) {
      if (this.timeout) clearTimeout(this.timeout.handle);
      this.timeout = {
        handle: setTimeout(() => this.next(), date.getTime() - now),
        date,
      };
    }
  }
}
