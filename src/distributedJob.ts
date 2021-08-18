import assert from 'assert';
import { Collection } from 'mongodb';
import { nanoid } from 'nanoid';
import { Queue } from 'schummar-queue';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import { DistributedJobImplementation, DistributedJobOptions, JobDbEntry, JobExecuteOptions, json } from './types';

export class DistributedJob<Data extends json, Result extends json | void, Progress extends json> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  private subscribedExecutionIds = new Array<{
    executionId: string;
    listener: (job: JobDbEntry<Data, Result, Progress>) => void;
  }>();
  public readonly options: DistributedJobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly collection: MaybePromise<Collection<JobDbEntry<any, any, any>>>,
    public readonly jobId: string,
    public readonly implementation: DistributedJobImplementation<Data, Result, Progress> | null,
    {
      maxParallel = DistributedJob.DEFAULT_MAX_PARALLEL,
      retryCount = scheduler.options.retryCount,
      retryDelay = scheduler.options.retryDelay,
      log = scheduler.options.log,
      lockDuration = scheduler.options.lockDuration,
      lockCheckInterval = scheduler.options.lockCheckInterval,
      ...otherOptions
    }: Partial<DistributedJobOptions<Data>> = {}
  ) {
    this.options = { maxParallel, retryCount, retryDelay, log, lockDuration, lockCheckInterval, ...otherOptions };
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
          executionId,

          schedule: null,
          nextRun: new Date(Date.now() + delay),
          lock: null,
          finished: null,
          attempt: 0,

          data: clonedData ?? null,
          progress: 0,
          state: 'planned',
        },
      },
      { upsert: true }
    );

    return executionId;
  }

  async await(executionId: string): Promise<Result> {
    return new Promise<Result>(async (resolve, reject) => {
      const listener = (job: JobDbEntry<Data, Result, Progress>) => {
        if (job.state === 'completed') resolve(job.result);
        else if (job.state === 'error') reject(Error(job.error));
        else return;

        this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener);
      };

      this.subscribedExecutionIds.push({ executionId, listener });

      const col = await this.collection;
      const existing = await col.findOne({ jobId: this.jobId, executionId });
      if (existing) listener(existing);
    });
  }

  async executeAndAwait(...args: Parameters<DistributedJob<Data, Result, Progress>['execute']>): Promise<Result> {
    const id = await this.execute(...args);
    return this.await(id);
  }

  onProgress(executionId: string, callback: (progress: Progress) => void): () => void {
    let lastValue: unknown;
    const listener = (job: JobDbEntry<Data, Result, Progress>) => {
      if (job.progress !== lastValue) callback(job.progress);
      lastValue = job.progress;

      if (job.state === 'completed' || job.state === 'error') cancel();
    };

    const cancel = () => (this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener));

    this.subscribedExecutionIds.push({ executionId, listener });

    return cancel;
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }

    await this.q.untilEmpty;
  }

  private async schedule(lastJob?: JobDbEntry<Data, Result, Progress>) {
    const { schedule } = this.options;
    const data = schedule && (schedule as { data?: Data }).data;
    const clonedData = data && JSON.parse(JSON.stringify(data));

    if (!schedule) return;

    const col = await this.collection;
    await col.updateOne(
      {
        jobId: this.jobId,
        schedule: { $ne: null },
        state: 'planned',
        lock: lastJob ? null : undefined,
      },
      {
        $setOnInsert: {
          jobId: this.jobId,
          executionId: nanoid(),

          schedule: schedule,
          nextRun: calcNextRun(schedule, lastJob?.nextRun),
          lock: null,
          finishedOn: null,
          attempt: 0,

          data: clonedData ?? null,
          progress: 0,
          state: 'planned',
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
    if (this.hasShutDown || !this.implementation) return;

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
            state: 'planned',
            nextRun: { $lte: now },
            lock: null,
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
        assert(job.state === 'planned');

        try {
          const result = await this.implementation(job.data, {
            job,
            setProgress: (progress) => this.setProgress(job, progress),
          });

          if (job.schedule) {
            await this.schedule(job);
          }

          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                lock: null,
                finishedOn: new Date(),
                progress: 1,
                state: 'completed',
                result,
              },
            }
          );
        } catch (e) {
          const msg = e instanceof Error ? e.message : e instanceof Object ? JSON.stringify(e) : String(e);
          const retry = job.attempt < this.options.retryCount;
          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                nextRun: retry ? new Date(Date.now() + this.options.retryDelay) : job.nextRun,
                lock: null,
                attempt: retry ? job.attempt + 1 : job.attempt,

                progress: 0,
                state: retry ? 'planned' : 'error',
                error: msg,
              },
            }
          );
        }
      } catch (e) {
        this.options.log('error', 'next failed', e);
      }
    });
  }

  private async setProgress(job: JobDbEntry<Data, Result, Progress>, progress: Progress) {
    const col = await this.collection;
    await col.updateOne(
      { _id: job._id },
      {
        $set: {
          lock: new Date(),
          progress,
        },
      }
    );
  }

  async checkForNextRun(): Promise<void> {
    if (this.hasShutDown || !this.implementation) return;

    const col = await this.collection;

    const [next] = await col
      .find({
        jobId: this.jobId,
        lock: null,
        state: 'planned',
      })
      .sort({ nextRun: 1 })
      .limit(1)
      .toArray();

    if (next) this.planNextRun(next);
  }

  async receiveUpdate(job: JobDbEntry<Data, Result, Progress>): Promise<void> {
    for (const { executionId, listener } of this.subscribedExecutionIds) {
      if (executionId === job.executionId) listener(job);
    }

    if (job.state === 'planned') {
      return this.planNextRun(job);
    }
  }

  async changeStreamReconnected(): Promise<void> {
    this.checkForNextRun();

    const executionIds = new Set(this.subscribedExecutionIds.map((x) => x.executionId));
    const col = await this.collection;
    const cursor = col.find<JobDbEntry<Data, Result, Progress>>({ jobId: this.jobId, executionId: { $in: [...executionIds] } });
    for await (const job of cursor) {
      await this.receiveUpdate(job);
    }
  }

  async planNextRun(job: JobDbEntry<Data, Result, Progress>): Promise<void> {
    if (this.hasShutDown || !this.implementation) return;

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

  async getPlanned(): Promise<JobDbEntry<Data, Result, Progress>[]> {
    const col = await this.collection;
    return await col
      .find({
        jobId: this.jobId,
        state: 'planned',
      })
      .toArray();
  }
}
