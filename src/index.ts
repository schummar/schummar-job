import { ChangeEvent, ChangeStream, Collection, Cursor, MongoClient, ObjectID } from 'mongodb';
import { Queue } from 'schummar-queue';
import { sleep } from './helpers';

type MaybePromise<T> = T | Promise<T>;

type JobDbEntry<Data> = {
  _id: ObjectID;
  jobId: string;
  interval: number | null;
  data: Data;
  nextRun: Date;
  lock: Date | null;
  error: string | null;
  tryCount: number;
};

type CollectionInfo = MaybePromise<Collection<JobDbEntry<any>> | { uri: string; db: string; collection: string }>;

type SchedulerOptions = {
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

type JobOptions<Data> = {
  schedule?: { interval: number; data: undefined extends Data ? never : Data };
  maxParallel?: number;
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

type JobExecuteOptions = {
  delay?: number;
};

export class Scheduler {
  static DEFAULT_LOCK_DURATION = 5 * 60 * 1000; // 5 minutes
  static DEFAULT_LOCK_CHECK_INTERVAL = 60 * 1000; // 1 minute
  static DEFAULT_RETRY_COUNT = 10;
  static DEFAULT_RETRY_DELAY = 60 * 1000; // 1 minute

  readonly collection: MaybePromise<Collection<JobDbEntry<any>>>;
  private listeners = new Set<(job?: JobDbEntry<any>) => void>();
  private stream?: ChangeStream<JobDbEntry<any>>;
  private hasShutDown = false;

  constructor(collection: CollectionInfo, public readonly options: SchedulerOptions = {}) {
    if ('uri' in collection) {
      this.collection = MongoClient.connect(collection.uri).then((client) => client.db(collection.db).collection(collection.collection));
    } else {
      this.collection = collection as Collection;
    }
  }

  private async watch() {
    const col = await this.collection;
    if (this.stream) return;

    try {
      this.stream = col.watch(
        [
          {
            $match: { operationType: { $in: ['insert', 'replace', 'update'] } },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      // When starting watching or after connection loss, force refresh
      for (const listener of this.listeners) listener();

      const cursor = this.stream.stream() as Cursor<ChangeEvent<JobDbEntry<any>>>;
      for await (const change of cursor) {
        if ('fullDocument' in change && change.fullDocument) {
          for (const listener of this.listeners) listener(change.fullDocument);
        }
      }
    } catch (e) {
      if (this.hasShutDown) return;
      console.warn('Change stream error:', e);

      await sleep(10);
      delete this.stream;
      this.watch();
    }
  }

  subscribeChangeStream(listener: (job?: JobDbEntry<any>) => void): () => void {
    this.watch();
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  async reset(): Promise<void> {
    const col = await this.collection;
    await col.deleteMany({});
  }

  shutdown(): void {
    this.hasShutDown = true;
    this.stream?.close();
  }
}

export class Job<Data> {
  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;

  constructor(
    private scheduler: Scheduler,
    private jobId: string,
    private implementation: (data: Data, job: JobDbEntry<Data>) => MaybePromise<void>,
    private options: JobOptions<Data> = {}
  ) {
    this.q = new Queue({ parallel: options.maxParallel });

    this.schedule();
    this.checkLocks();
    this.subscribeChanges();
  }

  async execute(data: Data, { delay = 0 }: JobExecuteOptions = {}): Promise<void> {
    const col = await this.scheduler.collection;
    await col.insertOne({
      jobId: this.jobId,
      interval: null,
      data,
      nextRun: new Date(Date.now() + delay),
      lock: null,
      error: null,
      tryCount: 0,
    });
  }

  shutdown(): void {
    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }
  }

  private async schedule() {
    const { schedule } = this.options;
    if (!schedule) return;

    const col = await this.scheduler.collection;
    const res = await col.updateOne(
      { jobId: this.jobId, type: 'schedule' },
      {
        $setOnInsert: {
          jobId: this.jobId,
          data: schedule.data,
          nextRun: new Date(Date.now() + schedule.interval),
          lock: null,
          error: null,
          tryCount: 0,
        },
        $set: {
          interval: schedule.interval,
        },
      },
      { upsert: true }
    );
    console.log(res.modifiedCount);
  }

  private async checkLocks() {
    const col = await this.scheduler.collection;
    const duration = this.options.lockDuration ?? this.scheduler.options.lockDuration ?? Scheduler.DEFAULT_LOCK_DURATION;
    const interval = this.options.lockCheckInterval ?? this.scheduler.options.lockCheckInterval ?? Scheduler.DEFAULT_LOCK_CHECK_INTERVAL;

    while (!this.hasShutDown) {
      try {
        const threshold = new Date(Date.now() - duration);
        const res = await col.updateMany({ jobId: this.jobId, lock: { $lt: threshold } }, { $set: { lock: null } });
        if (res.modifiedCount) console.debug('Unlocked jobs:', res.modifiedCount);
      } catch (e) {
        console.warn('Failed to check locks:', e);
      }

      await sleep(interval);
    }
  }

  private subscribeChanges() {
    this.scheduler.subscribeChangeStream((job) => (job ? this.planNextRun(job) : this.checkForNextRun()));
  }

  private next() {
    this.q.clear(true);
    this.q.schedule(async () => {
      try {
        if (this.hasShutDown) return;
        if (this.timeout) {
          clearTimeout(this.timeout.handle);
          delete this.timeout;
        }

        const col = await this.scheduler.collection;
        const retryCount = this.options.retryCount ?? this.scheduler.options.retryCount ?? Scheduler.DEFAULT_RETRY_COUNT;
        const retryDelay = this.options.retryDelay ?? this.scheduler.options.retryDelay ?? Scheduler.DEFAULT_RETRY_DELAY;
        const now = new Date();

        const { value: job } = await col.findOneAndUpdate(
          {
            jobId: this.jobId,
            nextRun: { $lte: now },
            lock: null,
            tryCount: { $lte: retryCount },
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
          await this.implementation(job.data, job);

          if (job.interval) {
            await col.updateOne(
              { _id: job._id },
              {
                $set: {
                  nextRun: new Date(now.getTime() + job.interval),
                  lock: null,
                  error: null,
                  tryCount: 0,
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
                tryCount: job.tryCount + 1,
              },
            }
          );
        }
      } catch (e) {
        console.error('next failed', e);
      }
    });
  }

  private async checkForNextRun() {
    const col = await this.scheduler.collection;
    const retryCount = this.options.retryCount ?? this.scheduler.options.retryCount ?? 10;

    const [next] = await col
      .find({
        jobId: this.jobId,
        lock: null,
        tryCount: { $lte: retryCount },
      })
      .sort({ nextRun: 1 })
      .limit(1)
      .toArray();

    if (next) this.planNextRun(next);
  }

  private async planNextRun(job: JobDbEntry<Data>) {
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
