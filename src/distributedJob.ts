import assert from 'assert';
import { Collection } from 'mongodb';
import { Queue } from 'schummar-queue';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import { Job, JobDbEntry, JobImplementation, JobOptions } from './types';

export class DistributedJob<Data> implements Job<Data> {
  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly collection: MaybePromise<Collection<JobDbEntry<any>>>,
    public readonly jobId: string,
    public readonly implementation: JobImplementation<Data> | null,
    public readonly options: JobOptions<Data> = {}
  ) {
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
    const duration = this.options.lockDuration ?? this.scheduler.options.lockDuration ?? Scheduler.DEFAULT_LOCK_DURATION;
    const interval = this.options.lockCheckInterval ?? this.scheduler.options.lockCheckInterval ?? Scheduler.DEFAULT_LOCK_CHECK_INTERVAL;
    const log = this.options.log ?? this.scheduler.options.log ?? console;

    while (!this.hasShutDown) {
      try {
        const threshold = new Date(Date.now() - duration);
        const res = await col.updateMany({ jobId: this.jobId, lock: { $lt: threshold } }, { $set: { lock: null } });
        if (res.modifiedCount) log.info('Unlocked jobs:', res.modifiedCount);
      } catch (e) {
        log.warn('Failed to check locks:', e);
      }

      await sleep(interval);
    }
  }

  private next() {
    if (this.hasShutDown) return;

    const retryCount = this.options.retryCount ?? this.scheduler.options.retryCount ?? Scheduler.DEFAULT_RETRY_COUNT;
    const retryDelay = this.options.retryDelay ?? this.scheduler.options.retryDelay ?? Scheduler.DEFAULT_RETRY_DELAY;
    const log = this.options.log ?? this.scheduler.options.log ?? console;

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
            attempt: { $lte: retryCount },
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
                nextRun: new Date(now.getTime() + retryDelay),
                lock: null,
                error: msg,
                attempt: job.attempt + 1,
              },
            }
          );
        }
      } catch (e) {
        log.error('next failed', e);
      }
    });
  }

  async checkForNextRun(): Promise<void> {
    if (this.hasShutDown) return;

    const col = await this.collection;
    const retryCount = this.options.retryCount ?? this.scheduler.options.retryCount ?? 10;

    const [next] = await col
      .find({
        jobId: this.jobId,
        lock: null,
        attempt: { $lte: retryCount },
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
