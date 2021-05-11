import { ChangeEvent, ChangeStream, Collection, Cursor, MongoClient } from 'mongodb';
import { MaybePromise, sleep } from './helpers';
import { Job } from './job';
import { CollectionInfo, JobDbEntry, SchedulerOptions } from './types';

export class Scheduler {
  static DEFAULT_LOCK_DURATION = 5 * 60 * 1000; // 5 minutes
  static DEFAULT_LOCK_CHECK_INTERVAL = 60 * 1000; // 1 minute
  static DEFAULT_RETRY_COUNT = 10;
  static DEFAULT_RETRY_DELAY = 60 * 1000; // 1 minute

  readonly collection: MaybePromise<Collection<JobDbEntry<any>>>;
  private jobs = new Set<Job<any>>();
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
      if (this.hasShutDown) return;
      console.warn('Change stream error:', e);

      await sleep(10);
      delete this.stream;
      this.watch();
    }
  }

  addJob(job: Job<any>): () => void {
    this.watch();
    this.jobs.add(job);
    return () => {
      this.jobs.delete(job);
    };
  }

  async clearDB(): Promise<void> {
    const col = await this.collection;
    await col.deleteMany({});
  }

  clearJobs(): void {
    for (const job of this.jobs) {
      job.shutdown();
    }
    this.jobs.clear();
  }

  shutdown(): void {
    this.clearJobs();
    this.hasShutDown = true;
    this.stream?.close();
  }
}
