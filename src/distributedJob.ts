import { createCancelable, type Cancelable } from './cancelable';
import errorToString from './errorToString';
import { calcNextRun, sleep } from './helpers';
import { Scheduler } from './scheduler';
import {
  DistributedJobOptions,
  JobDbEntry,
  type DistributedJobOptionsNormalized,
  type ExecuteArgs,
  type HistoryItem,
  type JobListener,
  type Logger,
  type LogLevel,
} from './types';
import { MongoError, type Filter } from 'mongodb';
import { nanoid } from 'nanoid';
import assert from 'node:assert';
import { isPromise } from 'node:util/types';
import { createQueue, type Queue } from 'schummar-queue';

export class DistributedJob<Data, Result, Progress> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  private subscribedExecutionIds = new Map<JobListener<Data, Result, Progress>, string>();
  private label: string;
  private _options: DistributedJobOptionsNormalized<Data, Result, Progress>;

  constructor(options: DistributedJobOptions<Data, Result, Progress>) {
    this._options = this.normalizeOptions(options);
    this.label = `[schummar-job/${this.options.jobId}]`;
    this.q = createQueue({ parallel: this.options.maxParallel });

    void this.schedule();
    void this.checkLocks();
    this.next();
    void this.watchSchedule();
  }

  get options() {
    return this._options;
  }

  updateOptions(options: Partial<Omit<DistributedJobOptions<Data, Result, Progress>, 'jobId'>> = {}): void {
    this._options = this.normalizeOptions({ ...this.options, ...options });

    if (this.options.run && this.options.scheduler?.collection) {
      void this.schedule();
      void this.checkLocks();
      this.next();
    }
  }

  private normalizeOptions(
    options: DistributedJobOptions<Data, Result, Progress>,
  ): DistributedJobOptionsNormalized<Data, Result, Progress> {
    return {
      jobId: options.jobId,
      run: options.run,
      scheduler: options.scheduler,
      lockDuration: options.lockDuration ?? options.scheduler?.options.lockDuration ?? Scheduler.DEFAULT_LOCK_DURATION,
      lockCheckInterval: options.lockCheckInterval ?? options.scheduler?.options.lockCheckInterval ?? Scheduler.DEFAULT_LOCK_CHECK_INTERVAL,
      forwardJobLogs: options.forwardJobLogs ?? options.scheduler?.options.forwardJobLogs ?? false,
      getExecutionId: options.getExecutionId,
      schedule: options.schedule,
      maxParallel: options.maxParallel ?? DistributedJob.DEFAULT_MAX_PARALLEL,
      retryCount: options.retryCount ?? options.scheduler?.options.retryCount ?? Scheduler.DEFAULT_RETRY_COUNT,
      retryDelay: options.retryDelay ?? options.scheduler?.options.retryDelay ?? Scheduler.DEFAULT_RETRY_DELAY,
      log: options.log ?? options.scheduler?.options.log,
    };
  }

  private get collection() {
    if (!this.options.scheduler?.collection) {
      throw new Error('Distributed job has no scheduler or collection defined');
    }

    return this.options.scheduler.collection;
  }

  async execute(...args: ExecuteArgs<Data, Result, Progress>): Promise<string> {
    const [data, { at, delay = 0, executionId, replacePlanned = false } = {}] = args;
    const t = at ? new Date(at) : new Date();
    t.setMilliseconds(t.getMilliseconds() + delay);

    const _id = executionId ?? this.options.getExecutionId?.(data as Data) ?? nanoid();

    let filter: Filter<JobDbEntry<Data, Result, Progress>> = {
      _id,
    };

    let $setOnInsert: Partial<JobDbEntry<Data, Result, Progress>> = {
      _id,
      jobId: this.options.jobId,
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
        jobId: this.options.jobId,
        isScheduled: false,
        state: 'planned',
        lock: null,
      };

      if (typeof replacePlanned === 'object' && replacePlanned.match) {
        filter.$and = [replacePlanned.match];
      }

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

    this.options.log?.(
      'debug',
      this.label,
      'scheduled for execution',
      result?._id,
      !at && !delay ? 'immediately' : `at ${t.toISOString()}`,
    );

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
      void q.schedule(() => check(job));
    };

    const cancel = () => {
      this.subscribedExecutionIds.delete(listener);
    };

    this.subscribedExecutionIds.set(listener, executionId);

    void q.schedule(async () => {
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
    this.options.log?.('info', this.label, 'shutting down');

    this.hasShutDown = true;
    if (this.timeout) {
      clearTimeout(this.timeout.handle);
      delete this.timeout;
    }

    await this.q.whenEmpty();
  }

  async schedule(lastRun?: Date): Promise<void | JobDbEntry<Data, Result, Progress>> {
    const { schedule } = this.options;
    if (this.hasShutDown || !schedule || !this.options.scheduler?.collection) return;

    try {
      const data = (schedule as { data?: Data }).data;
      const _id = this.options.getExecutionId?.(data as Data) ?? nanoid();
      const col = isPromise(this.collection) ? await this.collection : this.collection;

      const state = await col.findOneAndUpdate(
        {
          jobId: this.options.jobId,
          isScheduled: true,
          state: 'planned',
        },
        {
          $setOnInsert: {
            _id,
            jobId: this.options.jobId,
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

      this.options.log?.('warn', this.label, 'Failed to schedule next run:', error);
      setTimeout(() => this.schedule(), 10_000);
    }
  }

  private async watchSchedule() {
    while (!this.hasShutDown) {
      try {
        await sleep(600_000);
        await this.schedule();
      } catch (error) {
        this.options.log?.('warn', this.label, 'Failed to ensure schedule:', error);
      }
    }
  }

  private async checkLocks() {
    while (!this.hasShutDown) {
      if (this.options.scheduler?.collection) {
        try {
          const col = await this.collection;
          const threshold = new Date(Date.now() - this.options.lockDuration);
          const res = await col.updateMany({ jobId: this.options.jobId, lock: { $lt: threshold } }, { $set: { lock: null } });
          if (res.modifiedCount) this.options.log?.('info', this.label, 'Unlocked jobs:', res.modifiedCount);
        } catch (e) {
          this.options.log?.('warn', this.label, 'Failed to check locks:', e);
        }
      }

      await sleep(this.options.lockCheckInterval);
    }
  }

  private next() {
    if (this.hasShutDown || !this.options.run) return;

    this.q.clear(true);
    void this.q.schedule(async () => {
      try {
        if (this.timeout) {
          clearTimeout(this.timeout.handle);
          delete this.timeout;
        }

        const col = await this.collection;
        const now = new Date();

        const job = await col.findOneAndUpdate(
          {
            jobId: this.options.jobId,
            state: 'planned',
            nextRun: { $lte: now },
            lock: null,
          },
          {
            $set: { lock: now },
          },
        );

        if (!job) {
          void this.checkForNextRun();
          return;
        }
        this.next();

        assert(this.options.run);
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

              if (this.options.forwardJobLogs) {
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
            this.options.log?.('warn', this.label, 'Failed to flush job updates:', e);
          });
        }, 1000);

        try {
          this.options.log?.('debug', this.label, 'run', job?._id);

          addHistory('start', 'info');

          const result = await this.options.run(job.data, {
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

          this.options.log?.('debug', this.label, 'done', job?._id);
        } catch (error) {
          const errorString = errorToString(error);
          const shouldRetry = job.attempt < this.options.retryCount;

          Object.assign($set, {
            nextRun: shouldRetry ? new Date(Date.now() + this.options.retryDelay) : job.nextRun,
            lock: null,
            attempt: shouldRetry ? job.attempt + 1 : job.attempt,
            progress: 0,
            state: shouldRetry ? 'planned' : 'error',
            error: errorString,
          });

          addHistory('error', 'error', errorString);
          clearInterval(flushInterval);

          await flush().catch((e) => {
            this.options.log?.('warn', this.label, 'Failed to flush job updates after error:', e);
          });

          throw error;
        } finally {
          await this.schedule(job.nextRun);
        }
      } catch (e) {
        if (this.hasShutDown) return;

        this.options.log?.('error', this.label, 'job failed:', e);
      }
    });
  }

  private async checkForNextRun(): Promise<void> {
    if (this.hasShutDown || !this.options.run) return;

    const col = await this.collection;

    const [next] = await col
      .find({
        jobId: this.options.jobId,
        lock: null,
        state: 'planned',
      })
      .sort({ nextRun: 1 })
      .limit(1)
      .toArray();

    if (next) {
      void this.planNextRun(next);
    }
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
    void this.checkForNextRun();

    const executionIds = new Set(this.subscribedExecutionIds.values());
    const col = await this.collection;
    const cursor = col.find<JobDbEntry<Data, Result, Progress>>({ _id: { $in: [...executionIds] } });
    for await (const job of cursor) {
      await this.receiveUpdate(job);
    }
  }

  private async planNextRun(job: JobDbEntry<Data, Result, Progress>): Promise<void> {
    if (this.hasShutDown || !this.options.run) return;

    const now = Date.now();
    const date = new Date(Math.min(job.nextRun.getTime(), now + 60 * 60 * 1000));

    if (!this.timeout || date.getTime() < this.timeout.date.getTime()) {
      this.options.log?.('debug', this.label, 'plan next run', date.toISOString());
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
        jobId: this.options.jobId,
        state: 'planned',
      })
      .toArray();
  }
}
