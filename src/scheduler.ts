import assert from 'assert';
import { ChangeEvent, ChangeStream, Collection, Cursor, MongoClient } from 'mongodb';
import { DistributedJob } from './distributedJob';
import { MaybePromise, sleep } from './helpers';
import { LocalJob } from './localJob';
import { DbConnection, Job, JobDbEntry, JobImplementation, JobOptions, LocalJobOptions, SchedulerOptions } from './types';

export class Scheduler {
  static DEFAULT_LOCK_DURATION = 5 * 60 * 1000; // 5 minutes
  static DEFAULT_LOCK_CHECK_INTERVAL = 60 * 1000; // 1 minute
  static DEFAULT_RETRY_COUNT = 10;
  static DEFAULT_RETRY_DELAY = 60 * 1000; // 1 minute

  readonly collection?: MaybePromise<Collection<JobDbEntry<any>>>;
  private distributedJobs = new Set<DistributedJob<any>>();
  private localJobs = new Set<LocalJob<any>>();
  private stream?: ChangeStream<JobDbEntry<any>>;
  private hasShutDown = false;

  constructor(collection?: DbConnection, public readonly options: SchedulerOptions = {}) {
    if (collection && 'uri' in collection) {
      this.collection = MongoClient.connect(collection.uri).then((client) => client.db(collection.db).collection(collection.collection));
    } else {
      this.collection = collection as MaybePromise<Collection<JobDbEntry<any>>> | undefined;
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

      const cursor = this.stream.stream() as Cursor<ChangeEvent<JobDbEntry<any>>>;
      for await (const change of cursor) {
        if ('fullDocument' in change && change.fullDocument) {
          for (const job of this.distributedJobs) {
            if (job.jobId === change.fullDocument.jobId) {
              job.planNextRun(change.fullDocument);
            }
          }
        }
      }
    } catch (e) {
      delete this.stream;
      if (this.hasShutDown) return;
      console.warn('Change stream error:', e);

      await sleep(10);
      this.watch();
    }
  }

  addJob<Data>(jobId: string, implementation: JobImplementation<Data> | null, options?: JobOptions<Data>): Job<Data> {
    if (!this.collection) throw Error('No db set up!');

    const job = new DistributedJob(this, this.collection, jobId, implementation, options);
    this.distributedJobs.add(job);

    this.hasShutDown = false;
    this.watch();

    return job;
  }

  addLocalJob<Data>(implementation: JobImplementation<Data>, options?: LocalJobOptions<Data>): Job<Data> {
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
