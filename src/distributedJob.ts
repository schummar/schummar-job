import assert from 'assert';
import { Collection } from 'mongodb';
import { Queue } from 'schummar-queue';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import { Job, JobDbEntry, JobImplementation, JobOptions } from './types';

export class DistributedJob<Data> implements Job<Data> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  public readonly options: JobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly collection: MaybePromise<Collection<JobDbEntry<any>>>,
    public readonly jobId: string,
    public readonly implementation: JobImplementation<Data> | null,
    options: Partial<JobOptions<Data>> = {}
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
    this.q = new Queue({ parallel: options.maxParallel });

    if (implementation) {
      this.schedule();
      this.checkLocks();
      this.next();
    }
  }

  async execute(...[data, { delay = 0 } = {}]: Parameters<Job<Data>['execute']>): Promise<void> {
    const col = await this.collection;
    await col.insertOne({
      jobId: this.jobId,
      schedule: null,
      data,
      nextRun: new Date(Date.now() + delay),
      lock: null,
      error: null,
      attempt: 0,
    });
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }

    await this.q.last;
  }

  private async schedule() {
    const { schedule } = this.options;
    if (!schedule) return;

    const col = await this.collection;
    await col.updateOne(
      { jobId: this.jobId },
      {
        $setOnInsert: {
          jobId: this.jobId,
          data: (schedule as { data?: Data }).data ?? null,
          nextRun: calcNextRun(schedule),
          lock: null,
          error: null,
          attempt: 0,
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

        try {
          assert(this.implementation);
          await this.implementation(job.data, { attempt: job.attempt, error: job.error });

          if (job.schedule) {
            await col.updateOne(
              { _id: job._id },
              {
                $set: {
                  nextRun: calcNextRun(job.schedule),
                  lock: null,
                  error: null,
                  attempt: 0,
                },
              }
            );
          } else {
            await col.deleteOne({ _id: job._id });
          }
        } catch (e) {
          const msg = e instanceof Error ? e.message : e instanceof Object ? JSON.stringify(e) : String(e);
          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                nextRun: new Date(now.getTime() + this.options.retryDelay),
                lock: null,
                error: msg,
                attempt: job.attempt + 1,
              },
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
        attempt: { $lte: this.options.retryCount },
      })
      .sort({ nextRun: 1 })
      .limit(1)
      .toArray();

    if (next) this.planNextRun(next);
  }

  async planNextRun(job: JobDbEntry<Data>): Promise<void> {
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
