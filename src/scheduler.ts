import { ChangeStream, Collection, MongoClient, type Filter } from 'mongodb';
import { DistributedJob } from './distributedJob';
import { MaybePromise, sleep } from './helpers';
import { LocalJob } from './localJob';
import {
  DbConnection,
  DistributedJobImplementation,
  DistributedJobOptions,
  JobDbEntry,
  LocalJobImplementation,
  LocalJobOptions,
  SchedulerOptions,
} from './types';

const defaultLogger: SchedulerOptions['log'] = (level, ...args) => {
  if (level === 'error' || level === 'warn') {
    console[level](...args);
  }
};

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
  private label = `[schummar-job]`;
  private executionListeners = new Set<(execution: JobDbEntry<any, any, any>) => void>();
  private reconnectListeners = new Set<() => void>();
  public readonly options: SchedulerOptions;

  constructor(
    collection?: DbConnection,
    {
      retryCount = Scheduler.DEFAULT_RETRY_COUNT,
      retryDelay = Scheduler.DEFAULT_RETRY_DELAY,
      lockDuration = Scheduler.DEFAULT_LOCK_DURATION,
      lockCheckInterval = Scheduler.DEFAULT_LOCK_CHECK_INTERVAL,
      log = defaultLogger,
      ...otherOptions
    }: Partial<SchedulerOptions> = {},
  ) {
    this.options = { retryCount, retryDelay, lockDuration, lockCheckInterval, log, ...otherOptions };

    if (collection && 'uri' in collection) {
      this.collection = MongoClient.connect(collection.uri).then((client) => client.db(collection.db).collection(collection.collection));
    } else {
      this.collection = collection as MaybePromise<Collection<JobDbEntry<any, any, any>>> | undefined;
    }
  }

  private async watch() {
    if (this.hasShutDown || this.stream) {
      return;
    }

    try {
      const col = await this.collection;
      if (!col) {
        throw new Error('No db set up!');
      }

      if (this.hasShutDown || this.stream) {
        return;
      }

      this.options.log('debug', this.label, 'start db watcher');
      this.stream = col.watch(
        [
          {
            $match: { operationType: { $in: ['insert', 'replace', 'update'] } },
          },
        ],
        { fullDocument: 'updateLookup' },
      );

      this.stream.once('resumeTokenChanged', () => {
        this.options.log('debug', this.label, 'db watcher first token');
        for (const job of this.distributedJobs) job.changeStreamReconnected();
        for (const listener of this.reconnectListeners) listener();
      });

      // When starting watching or after connection loss, force refresh
      const cursor = this.stream.stream();
      for await (const change of cursor) {
        this.options.log(
          'debug',
          this.label,
          'db watcher change received',
          'fullDocument' in change && change.fullDocument ? `${change.fullDocument.jobId} ${change.fullDocument._id}` : undefined,
        );
        if ('fullDocument' in change && change.fullDocument) {
          for (const job of this.distributedJobs) {
            if (job.jobId === change.fullDocument.jobId) {
              job.receiveUpdate(change.fullDocument);
            }
          }
          for (const listener of this.executionListeners) {
            listener(change.fullDocument);
          }
        }
      }
    } catch (e) {
      this.options.log('warn', this.label, 'Change stream error:', e);

      if (!this.hasShutDown) {
        await sleep(10);
      }
    }

    delete this.stream;

    if (!this.hasShutDown) {
      this.watch();
    }
  }

  addJob<Data = undefined, Result = undefined, Progress = number>(
    jobId: string,
    implementation?: DistributedJobImplementation<Data, Result, Progress>,
    options?: Partial<DistributedJobOptions<Data>>,
  ): DistributedJob<Data, Result, Progress> {
    if (!this.collection) throw Error('No db set up!');

    const job = new DistributedJob(this, this.collection, jobId, implementation, options);
    this.distributedJobs.add(job);

    this.hasShutDown = false;
    this.watch();

    return job;
  }

  addLocalJob<Data = undefined, Result = void>(
    implementation: LocalJobImplementation<Data, Result>,
    options?: Partial<LocalJobOptions<Data>>,
  ): LocalJob<Data, Result> {
    const job = new LocalJob(this, implementation, options);
    this.localJobs.add(job);

    return job;
  }

  getJobs(): DistributedJob<any, any, any>[] {
    return [...this.distributedJobs];
  }

  getLocalJobs(): LocalJob<any, any>[] {
    return [...this.localJobs];
  }

  onExecutionUpdate(listener: (execution: JobDbEntry<any, any, any>) => void): () => void {
    this.executionListeners.add(listener);
    return () => {
      this.executionListeners.delete(listener);
    };
  }

  onReconnect(listener: () => void): () => void {
    this.reconnectListeners.add(listener);
    return () => {
      this.reconnectListeners.delete(listener);
    };
  }

  async getExecutions(filter: Filter<JobDbEntry<any, any, any>>): Promise<JobDbEntry<any, any, any>[]> {
    if (!this.collection) throw Error('No db set up!');

    const col = await this.collection;
    return await col.find(filter).toArray();
  }

  async clearDB(): Promise<void> {
    if (!this.collection) throw Error('No db set up!');

    const col = await this.collection;
    await col.deleteMany({});

    this.options.log('info', this.label, 'cleared db');
  }

  async clearJobs(): Promise<void> {
    await Promise.all([...this.distributedJobs, ...this.localJobs].map((job) => job.shutdown()));
    this.distributedJobs.clear();
    this.localJobs.clear();

    this.options.log('info', this.label, 'cleared jobs');
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    this.stream?.close();

    await this.clearJobs();

    this.options.log('info', this.label, 'shut down');
  }
}
