import { Collection, MongoError, type Filter } from 'mongodb';
import { nanoid } from 'nanoid';
import assert from 'node:assert';
import { isPromise } from 'node:util/types';
import { createQueue, type Queue } from 'schummar-queue';
import { createCancelable, type Cancelable } from './cancelable';
import errorToString from './errorToString';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import {
  DistributedJobImplementation,
  DistributedJobOptions,
  JobDbEntry,
  type ExecuteArgs,
  type HistoryItem,
  type JobListener,
  type Logger,
  type LogLevel,
} from './types';

export class DistributedJob<Data, Result, Progress> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  private subscribedExecutionIds = new Map<JobListener<Data, Result, Progress>, string>();
  private label;
  private _options: DistributedJobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly collection: MaybePromise<Collection<JobDbEntry<any, any, any>>>,
    public readonly jobId: string,
    public readonly implementation: DistributedJobImplementation<Data, Result, Progress> | undefined,
    options: Partial<DistributedJobOptions<Data>> = {},
  ) {
    this.label = `[schummar-job/${this.jobId}]`;
    this._options = this.normalizeOptions(options);
    this.q = createQueue({ parallel: this.options.maxParallel });

    if (implementation) {
      this.ensureSchedule();
      this.checkLocks();
      this.next();
    }
  }

  get options() {
    return { ...this._options };
  }

  set options(options: Partial<DistributedJobOptions<Data>>) {
    this._options = this.normalizeOptions(options);

    if (this.implementation) {
      this.schedule();
      this.checkLocks();
      this.next();
    }
  }

  private normalizeOptions({
    maxParallel = DistributedJob.DEFAULT_MAX_PARALLEL,
    retryCount = this.scheduler.options.retryCount,
    retryDelay = this.scheduler.options.retryDelay,
    log = this.scheduler.options.log,
    lockDuration = this.scheduler.options.lockDuration,
    lockCheckInterval = this.scheduler.options.lockCheckInterval,
    forwardJobLogs = this.scheduler.options.forwardJobLogs,
    ...otherOptions
  }: Partial<DistributedJobOptions<Data>>): DistributedJobOptions<Data> {
    return { maxParallel, retryCount, retryDelay, log, lockDuration, lockCheckInterval, forwardJobLogs, ...otherOptions };
  }

  async execute(...args: ExecuteArgs<Data>): Promise<string> {
    const [data, { at, delay = 0, executionId, replacePlanned = false } = {}] = args;
    const t = at ? new Date(at) : new Date();
    t.setMilliseconds(t.getMilliseconds() + delay);

    const _id = executionId ?? this.options.getExecutionId?.(data as Data) ?? nanoid();

    let filter: Filter<JobDbEntry<Data, Result, Progress>> = {
      _id,
    };

    let $setOnInsert: Partial<JobDbEntry<Data, Result, Progress>> = {
      _id,
      jobId: this.jobId,
      isScheduled: false,
      state: 'planned',
      lock: null,
      nextRun: t,
      finishedOn: null,
      attempt: 0,
      data: data,
      history: [],
    };

    let $set: Partial<JobDbEntry<Data, Result, Progress>> = {};

    if (!executionId && replacePlanned) {
      filter = {
        jobId: this.jobId,
        isScheduled: false,
        state: 'planned',
        lock: null,
      };

      delete $setOnInsert.nextRun;
      delete $setOnInsert.data;

      $set = {
        nextRun: t,
        data: data,
      };
    }

    const col = await this.collection;
    const result = await col.findOneAndUpdate(
      filter,
      { $setOnInsert, $set },
      {
        upsert: true,
        returnDocument: 'after',
      },
    );

    this._options.log('debug', this.label, 'scheduled for execution', result?._id, !at && !delay ? 'immediately' : `at ${t.toISOString()}`);

    return result!._id;
  }

  async executeAndAwait(...args: Parameters<DistributedJob<Data, Result, Progress>['execute']>): Promise<Result> {
    const id = await this.execute(...args);
    return this.await(id);
  }

  watch(executionId: string, callback: (job: JobDbEntry<Data, Result, Progress>) => void): Cancelable {
    const check = (job: JobDbEntry<Data, Result, Progress>) => {
      callback(job);
      if (job.state === 'completed' || job.state === 'error') {
        cancel();
      }
    };

    const q = createQueue();
    const listener = (job: JobDbEntry<Data, Result, Progress>) => {
      q.schedule(() => check(job));
    };

    const cancel = () => {
      this.subscribedExecutionIds.delete(listener);
    };

    this.subscribedExecutionIds.set(listener, executionId);

    q.schedule(async () => {
      const col = await this.collection;
      const existing = await col.findOne({ _id: executionId });
      if (existing) check(existing);
    });

    return createCancelable(cancel);
  }

  await(executionId: string): Promise<Result> {
    return new Promise<Result>((resolve, reject) => {
      this.watch(executionId, (job) => {
        if (job.state === 'completed') {
          resolve(job.result);
        } else if (job.state === 'error') {
          reject(Error(job.error));
        }
      });
    });
  }

  onProgress(executionId: string, callback: (progress: Progress) => void): Cancelable {
    let lastValue: unknown;

    return this.watch(executionId, (job) => {
      if (job.progress && job.progress !== lastValue) {
        callback(job.progress);
      }

      lastValue = job.progress;
    });
  }

  async getExecution(executionId: string): Promise<JobDbEntry<Data, Result, Progress> | null> {
    const col = await this.collection;
    return await col.findOne({ _id: executionId });
  }

  async shutdown(): Promise<void> {
    this._options.log('info', this.label, 'shutting down');

    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }

    await this.q.whenEmpty();
  }

  async schedule(lastRun?: Date): Promise<void | JobDbEntry<Data, Result, Progress>> {
    const { schedule } = this._options;
    if (this.hasShutDown || !schedule) return;

    try {
      const data = (schedule as { data?: Data }).data;
      const _id = this.options.getExecutionId?.(data as Data) ?? nanoid();
      const col = isPromise(this.collection) ? await this.collection : this.collection;

      const state = await col.findOneAndUpdate(
        {
          jobId: this.jobId,
          isScheduled: true,
          state: 'planned',
        },
        {
          $setOnInsert: {
            _id,
            jobId: this.jobId,
            isScheduled: true,
            state: 'planned',
            lock: null,
            finishedOn: null,
            attempt: 0,

            data: data ?? null,
            progress: 0,
          },
          $min: {
            nextRun: calcNextRun(schedule, lastRun),
          },
        },
        {
          upsert: true,
          returnDocument: 'after',
        },
      );

      return state ?? undefined;
    } catch (error) {
      if (error instanceof MongoError && error.code === 11000) {
        // Duplicate key => another instance scheduled it simultaneously
        // repeating the call should return the existing one
        return this.schedule(lastRun);
      }

      this._options.log('warn', this.label, 'Failed to schedule next run:', error);
      setTimeout(() => this.schedule(), 10_000);
    }
  }

  private async ensureSchedule() {
    while (!this.hasShutDown) {
      try {
        await this.schedule();
      } catch (error) {
        this._options.log('warn', this.label, 'Failed to ensure schedule:', error);
      }
      await sleep(600_000);
    }
  }

  private async checkLocks() {
    const col = await this.collection;

    while (!this.hasShutDown) {
      try {
        const threshold = new Date(Date.now() - this._options.lockDuration);
        const res = await col.updateMany({ jobId: this.jobId, lock: { $lt: threshold } }, { $set: { lock: null } });
        if (res.modifiedCount) this._options.log('info', this.label, 'Unlocked jobs:', res.modifiedCount);
      } catch (e) {
        this._options.log('warn', this.label, 'Failed to check locks:', e);
      }

      await sleep(this._options.lockCheckInterval);
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

        const job = await col.findOneAndUpdate(
          {
            jobId: this.jobId,
            state: 'planned',
            nextRun: { $lte: now },
            lock: null,
          },
          {
            $set: { lock: now },
          },
        );

        if (!job) {
          this.checkForNextRun();
          return;
        }
        this.next();

        assert(this.implementation);
        assert(job.state === 'planned');

        // Setup updater that will batch logs, progress updates, etc. and flush them periodically
        const q = createQueue();

        let $set: Partial<JobDbEntry<Data, Result, Progress>> = {};
        let history: HistoryItem[] = [];

        const addHistory = (event: HistoryItem['event'], level?: string, message?: string) => {
          history.push({ t: Date.now(), attempt: job.attempt, event, level, message });
        };

        const logger: Logger = new Proxy({} as Logger, {
          get: (logger, level: string) => {
            return (logger[level as LogLevel] ??= (...args: unknown[]) => {
              const message = args.map(errorToString).join(' ');

              addHistory('log', level, message);

              if (this._options.forwardJobLogs) {
                this.options.log?.(level as LogLevel, this.label, ...args);
              }
            });
          },
        });

        const flush = async () => {
          if (Object.keys($set).length === 0 && history.length === 0) {
            return;
          }

          const update = {
            ...(Object.keys($set).length > 0 && { $set }),
            ...(history.length > 0 && { $push: { history: { $each: history } } }),
          };

          const historyLength = history.length;
          await q.schedule(() => col.updateOne({ _id: job._id }, update));
          history = history.slice(historyLength);
        };

        const flushInterval = setInterval(() => {
          flush().catch((e) => {
            this._options.log('warn', this.label, 'Failed to flush job updates:', e);
          });
        }, 1000);

        try {
          this._options.log('debug', this.label, 'run', job?._id);

          addHistory('start', 'info');

          const result = await this.implementation(job.data, {
            job,
            setProgress(progress) {
              $set.progress = progress;
            },
            logger,
            flush,
          });

          Object.assign($set, {
            lock: null,
            finishedOn: new Date(),
            state: 'completed',
            result,
            error: null,
          });

          addHistory('complete', 'info');
          clearInterval(flushInterval);
          await flush();

          this._options.log('debug', this.label, 'done', job?._id);
        } catch (error) {
          const errorString = errorToString(error);
          const shouldRetry = job.attempt < this._options.retryCount;

          Object.assign($set, {
            nextRun: shouldRetry ? new Date(Date.now() + this._options.retryDelay) : job.nextRun,
            lock: null,
            attempt: shouldRetry ? job.attempt + 1 : job.attempt,
            progress: 0,
            state: shouldRetry ? 'planned' : 'error',
            error: errorString,
          });

          addHistory('error', 'error', errorString);
          clearInterval(flushInterval);

          await flush().catch((e) => {
            this._options.log('warn', this.label, 'Failed to flush job updates after error:', e);
          });

          throw error;
        } finally {
          await this.schedule(job.nextRun);
        }
      } catch (e) {
        if (this.hasShutDown) return;

        this._options.log('error', this.label, 'job failed:', e);
      }
    });
  }

  private async checkForNextRun(): Promise<void> {
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
    for (const [listener, executionId] of this.subscribedExecutionIds) {
      if (executionId === job._id) listener(job);
    }

    if (job.state === 'planned') {
      return this.planNextRun(job);
    }
  }

  async changeStreamReconnected(): Promise<void> {
    this.checkForNextRun();

    const executionIds = new Set(this.subscribedExecutionIds.values());
    const col = await this.collection;
    const cursor = col.find<JobDbEntry<Data, Result, Progress>>({ _id: { $in: [...executionIds] } });
    for await (const job of cursor) {
      await this.receiveUpdate(job);
    }
  }

  private async planNextRun(job: JobDbEntry<Data, Result, Progress>): Promise<void> {
    if (this.hasShutDown || !this.implementation) return;

    const now = Date.now();
    const date = new Date(Math.min(job.nextRun.getTime(), now + 60 * 60 * 1000));

    if (!this.timeout || date.getTime() < this.timeout.date.getTime()) {
      this._options.log('debug', this.label, 'plan next run', date.toISOString());
      if (this.timeout) clearTimeout(this.timeout.handle);
      this.timeout = {
        handle: setTimeout(() => this.next(), Math.max(date.getTime() - now, 0)),
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
