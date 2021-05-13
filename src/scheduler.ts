import { ChangeEvent, ChangeStream, Collection, Cursor, MongoClient } from 'mongodb';
import { MaybePromise, sleep } from './helpers';
import { Job } from './job';
import { DbConnection, JobDbEntry, JobImplementation, JobOptions, SchedulerOptions } from './types';

export class Scheduler {
  static DEFAULT_LOCK_DURATION = 5 * 60 * 1000; // 5 minutes
  static DEFAULT_LOCK_CHECK_INTERVAL = 60 * 1000; // 1 minute
  static DEFAULT_RETRY_COUNT = 10;
  static DEFAULT_RETRY_DELAY = 60 * 1000; // 1 minute

  readonly collection: MaybePromise<Collection<JobDbEntry<any>>>;
  private jobs = new Set<Job<any>>();
  private stream?: ChangeStream<JobDbEntry<any>>;
  private hasShutDown = false;

  constructor(collection: DbConnection, public readonly options: SchedulerOptions = {}) {
    if ('uri' in collection) {
      this.collection = MongoClient.connect(collection.uri).then((client) => client.db(collection.db).collection(collection.collection));
    } else {
      this.collection = collection as Collection;
    }
  }

  private async watch() {
    if (this.hasShutDown || this.stream) return;

    try {
      const col = await this.collection;
      this.stream = col.watch(
        [
          {
            $match: { operationType: { $in: ['insert', 'replace', 'update'] } },
          },
        ],
        { fullDocument: 'updateLookup' }
      );

      // When starting watching or after connection loss, force refresh
      for (const job of this.jobs) job.checkForNextRun();

      const cursor = this.stream.stream() as Cursor<ChangeEvent<JobDbEntry<any>>>;
      for await (const change of cursor) {
        if ('fullDocument' in change && change.fullDocument) {
          for (const job of this.jobs) {
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
    const job = new Job(this, jobId, implementation, options);
    this.jobs.add(job);

    this.hasShutDown = false;
    this.watch();

    return job;
  }

  async clearDB(): Promise<void> {
    const col = await this.collection;
    await col.deleteMany({});
  }

  async clearJobs(): Promise<void> {
    await Promise.all([...this.jobs].map((job) => job.shutdown()));
    this.jobs.clear();
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    this.stream?.close();

    await this.clearJobs();
  }
}
