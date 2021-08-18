import assert from 'assert';
import { ChangeStream, Collection, MongoClient } from 'mongodb';
import { DistributedJob } from './distributedJob';
import { MaybePromise, sleep } from './helpers';
import { LocalJob } from './localJob';
import {
  DbConnection,
  DistributedJobImplementation,
  DistributedJobOptions,
  JobDbEntry,
  json,
  LocalJobImplementation,
  LocalJobOptions,
  SchedulerOptions,
} from './types';

export class Scheduler {
  static DEFAULT_LOCK_DURATION = 5 * 60 * 1000; // 5 minutes
  static DEFAULT_LOCK_CHECK_INTERVAL = 60 * 1000; // 1 minute
  static DEFAULT_RETRY_COUNT = 10;
  static DEFAULT_RETRY_DELAY = 60 * 1000; // 1 minute

  readonly collection?: MaybePromise<Collection<JobDbEntry<any, any, any>>>;
  private distributedJobs = new Set<DistributedJob<any, any, any>>();
  private localJobs = new Set<LocalJob<any, any>>();
  private stream?: ChangeStream<JobDbEntry<any, any, any>>;
  private hasShutDown = false;
  public readonly options: SchedulerOptions;

  constructor(
    collection?: DbConnection,
    {
      retryCount = Scheduler.DEFAULT_RETRY_COUNT,
      retryDelay = Scheduler.DEFAULT_RETRY_DELAY,
      lockDuration = Scheduler.DEFAULT_LOCK_DURATION,
      lockCheckInterval = Scheduler.DEFAULT_LOCK_CHECK_INTERVAL,
      log = (level, ...args) => console[level](...args),
      ...otherOptions
    }: Partial<SchedulerOptions> = {}
  ) {
    this.options = { retryCount, retryDelay, lockDuration, lockCheckInterval, log, ...otherOptions };

    if (collection && 'uri' in collection) {
      this.collection = MongoClient.connect(collection.uri).then((client) => client.db(collection.db).collection(collection.collection));
    } else {
      this.collection = collection as MaybePromise<Collection<JobDbEntry<any, any, any>>> | undefined;
    }
  }

  private async watch() {
    if (this.hasShutDown || this.stream) return;

    try {
      const col = await this.collection;
      assert(col);
      this.stream = col.watch(
        [
          {
            $match: { operationType: { $in: ['insert', 'replace', 'update'] } },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      // When starting watching or after connection loss, force refresh
      for (const job of this.distributedJobs) job.checkForNextRun();

      const cursor = this.stream.stream();
      for await (const change of cursor) {
        if ('fullDocument' in change && change.fullDocument) {
          for (const job of this.distributedJobs) {
            if (job.jobId === change.fullDocument.jobId) {
              job.receiveUpdate(change.fullDocument);
            }
          }
        }
      }
    } catch (e) {
      delete this.stream;
      if (this.hasShutDown) return;
      this.options.log('warn', 'Change stream error:', e);

      await sleep(10);
      this.watch();
    }
  }

  addJob<Data extends json, Result extends json | void, Progress extends json = number>(
    jobId: string,
    implementation: DistributedJobImplementation<Data, Result, Progress> | null = null,
    options?: Partial<DistributedJobOptions<Data>>
  ): DistributedJob<Data, Result, Progress> {
    if (!this.collection) throw Error('No db set up!');

    const job = new DistributedJob(this, this.collection, jobId, implementation, options);
    this.distributedJobs.add(job);

    this.hasShutDown = false;
    this.watch();

    return job;
  }

  addLocalJob<Data, Result>(
    implementation: LocalJobImplementation<Data, Result>,
    options?: Partial<LocalJobOptions<Data>>
  ): LocalJob<Data, Result> {
    const job = new LocalJob(this, implementation, options);
    this.localJobs.add(job);

    return job;
  }

  async clearDB(): Promise<void> {
    if (!this.collection) throw Error('No db set up!');

    const col = await this.collection;
    await col.deleteMany({});
  }

  async clearJobs(): Promise<void> {
    await Promise.all([...this.distributedJobs, ...this.localJobs].map((job) => job.shutdown()));
    this.distributedJobs.clear();
    this.localJobs.clear();
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    this.stream?.close();

    await this.clearJobs();
  }
}
