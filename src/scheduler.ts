import { DistributedJob } from './distributedJob';
import { sleep } from './helpers';
import { LocalJob } from './localJob';
import {
  DistributedJobImplementation,
  DistributedJobOptions,
  JobDbEntry,
  LocalJobImplementation,
  LocalJobOptions,
  SchedulerOptions,
} from './types';
import {
  ChangeStream,
  Collection,
  MongoClient,
  MongoServerError,
  type ChangeStreamDocument,
  type Filter,
  type IndexDescriptionInfo,
} from 'mongodb';

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

  readonly client?: MongoClient;
  readonly collection?: Collection<JobDbEntry<any, any, any>>;
  readonly lockCollection?: Collection<{ _id: string; n: number }>;
  readonly indexesReady: Promise<void>;
  private distributedJobs = new Set<DistributedJob<any, any, any>>();
  private localJobs = new Set<LocalJob<any, any>>();
  private stream?: ChangeStream<JobDbEntry<any, any, any>>;
  private hasShutDown = false;
  private label = `[schummar-job]`;
  private executionListeners = new Set<(execution: JobDbEntry<any, any, any>) => void>();
  private reconnectListeners = new Set<() => void>();
  public readonly options: SchedulerOptions;

  constructor({
    client,
    collection,
    retryCount = Scheduler.DEFAULT_RETRY_COUNT,
    retryDelay = Scheduler.DEFAULT_RETRY_DELAY,
    lockDuration = Scheduler.DEFAULT_LOCK_DURATION,
    lockCheckInterval = Scheduler.DEFAULT_LOCK_CHECK_INTERVAL,
    log = defaultLogger,
    forwardJobLogs = false,
    createIndexes = true,
    ...otherOptions
  }: Partial<SchedulerOptions> = {}) {
    this.options = { retryCount, retryDelay, lockDuration, lockCheckInterval, log, forwardJobLogs, createIndexes, ...otherOptions };

    if (typeof client === 'string') {
      this.client = new MongoClient(client);
    } else {
      this.client = client;
    }

    if (collection && 'collection' in collection) {
      this.collection = this.client?.db(collection.db).collection(collection.collection);
    } else {
      this.collection = collection;
    }

    this.lockCollection = this.collection?.db.collection(`${this.collection.collectionName}_locks`);
    this.indexesReady = this.collection ? this.ensureIndexes(this.collection) : Promise.resolve();
  }

  private async ensureIndexes(coll: Collection<JobDbEntry<any, any, any>>): Promise<void> {
    if (!this.options.createIndexes) {
      return;
    }

    try {
      await coll.createIndexes(this.getIndexSpecs());

      // Creating it implicitly inside concurrent transactions can fail
      await coll.db.createCollection(`${coll.collectionName}_locks`).catch((error) => {
        if (!(error instanceof MongoServerError && error.codeName === 'NamespaceExists')) throw error;
      });
    } catch (error) {
      this.options.log('error', this.label, 'Error ensuring indexes:', error);
    }
  }

  getIndexSpecs(): IndexDescriptionInfo[] {
    return [
      {
        key: {
          jobId: 1,
          nextRun: 1,
        },
        partialFilterExpression: {
          $or: [{ state: 'planned' }, { lock: { $type: 'date' } }],
        },
      },

      {
        key: {
          jobId: 1,
        },
        partialFilterExpression: {
          isScheduled: true,
          state: 'planned',
        },
        unique: true,
      },
    ];
  }

  private async watch() {
    if (!this.client || !this.collection) {
      throw new Error('No db set up!');
    }

    if (this.hasShutDown || this.stream) {
      return;
    }

    try {
      this.options.log('debug', this.label, 'start db watcher');

      this.stream = this.client.watch(
        [
          {
            $match: {
              $or: [
                { 'ns.db': this.collection.db.databaseName, 'ns.coll': this.collection.collectionName },
                { operationType: 'rename', 'to.db': this.collection.db.databaseName, 'to.coll': this.collection.collectionName },
              ],
            },
          },
        ],
        {
          fullDocument: 'updateLookup',
        },
      );

      // When starting watching or after connection loss, force refresh
      this.stream.once('resumeTokenChanged', () => {
        this.options.log('debug', this.label, 'db watcher first token');
        this.notifyReconnect();
      });

      const cursor = this.stream.stream() as AsyncIterable<ChangeStreamDocument<JobDbEntry<any, any, any>>>;

      for await (const change of cursor) {
        this.options.log(
          'debug',
          this.label,
          'db watcher change received',
          'fullDocument' in change && change.fullDocument ? `${change.fullDocument.jobId} ${change.fullDocument._id}` : undefined,
        );

        switch (change.operationType) {
          case 'insert':
          case 'replace':
          case 'update': {
            if (change.fullDocument) {
              this.notifyUpdate(change.fullDocument);
            }
            break;
          }

          case 'rename': {
            this.options.log('debug', this.label, 'db watcher rename detected, refreshing jobs');
            this.notifyReconnect();
          }
        }
      }
    } catch (e) {
      if (this.hasShutDown) return;

      this.options.log('warn', this.label, 'Change stream error:', e);
      await sleep(1000);
    }

    delete this.stream;

    if (!this.hasShutDown) {
      void this.watch();
    }
  }

  private notifyReconnect() {
    for (const job of this.distributedJobs) {
      void job.changeStreamReconnected();
    }

    for (const listener of this.reconnectListeners) {
      listener();
    }
  }

  private notifyUpdate(execution: JobDbEntry<any, any, any>) {
    for (const job of this.distributedJobs) {
      if (job.options.jobId === execution.jobId) {
        void job.receiveUpdate(execution);
      }
    }

    for (const listener of this.executionListeners) {
      listener(execution);
    }
  }

  addJob<Data = undefined, Result = undefined, Progress = number>(
    jobId: string,
    run?: DistributedJobImplementation<Data, Result, Progress>,
    options?: Omit<DistributedJobOptions<Data, Result, Progress>, 'jobId' | 'run' | 'scheduler'>,
  ): DistributedJob<Data, Result, Progress>;

  addJob<Data = undefined, Result = undefined, Progress = number>(
    options: Omit<DistributedJobOptions<Data, Result, Progress>, 'scheduler'>,
  ): DistributedJob<Data, Result, Progress>;

  addJob<Data = undefined, Result = undefined, Progress = number>(
    job: DistributedJob<Data, Result, Progress>,
  ): DistributedJob<Data, Result, Progress>;

  addJob<Data = undefined, Result = undefined, Progress = number>(
    ...args:
      | [
          jobId: string,
          run?: DistributedJobImplementation<Data, Result, Progress>,
          options?: Omit<DistributedJobOptions<Data, Result, Progress>, 'jobId' | 'run' | 'scheduler'>,
        ]
      | [job: DistributedJob<Data, Result, Progress>]
      | [options: Omit<DistributedJobOptions<Data, Result, Progress>, 'scheduler'>]
  ): DistributedJob<Data, Result, Progress> {
    let job: DistributedJob<Data, Result, Progress>;

    if (typeof args[0] === 'string') {
      job = new DistributedJob({
        jobId: args[0],
        run: args[1],
        ...args[2],
        scheduler: this,
      });
    } else if (args[0] instanceof DistributedJob) {
      job = args[0];
      job.updateOptions({ scheduler: this });
    } else {
      job = new DistributedJob({
        ...args[0],
        scheduler: this,
      });
    }

    this.distributedJobs.add(job);
    this.hasShutDown = false;
    if (this.client && this.collection) {
      void this.watch();
    }

    return job;
  }

  addLocalJob<Data = undefined, Result = void>(
    run: LocalJobImplementation<Data, Result>,
    options?: Omit<LocalJobOptions<Data, Result>, 'run' | 'scheduler'>,
  ): LocalJob<Data, Result>;

  addLocalJob<Data = undefined, Result = void>(options: Omit<LocalJobOptions<Data, Result>, 'scheduler'>): LocalJob<Data, Result>;

  addLocalJob<Data = undefined, Result = void>(job: LocalJob<Data, Result>): LocalJob<Data, Result>;

  addLocalJob<Data = undefined, Result = void>(
    ...args:
      | [run: LocalJobImplementation<Data, Result>, options?: Omit<LocalJobOptions<Data, Result>, 'run' | 'scheduler'>]
      | [job: LocalJob<Data, Result>]
      | [options: Omit<LocalJobOptions<Data, Result>, 'scheduler'>]
  ): LocalJob<Data, Result> {
    let job: LocalJob<Data, Result>;

    if (typeof args[0] === 'function') {
      job = new LocalJob({
        run: args[0],
        ...args[1],
        scheduler: this,
      });
    } else if (args[0] instanceof LocalJob) {
      job = args[0];
      job.updateOptions({ scheduler: this });
    } else {
      job = new LocalJob({
        ...args[0],
        scheduler: this,
      });
    }

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

    return await this.collection.find(filter).toArray();
  }

  async clearDB(): Promise<void> {
    if (!this.collection) throw Error('No db set up!');

    await this.collection.deleteMany({});

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
    await this.stream?.close();
    await this.clearJobs();

    this.options.log('info', this.label, 'shut down');
  }
}
