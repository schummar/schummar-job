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
import { type Filter, type UpdateFilter } from 'mongodb';
import { nanoid } from 'nanoid';
import assert from 'node:assert';
import { createQueue, type Queue } from 'schummar-queue';

const PICK_UP_RETRY_DELAY = 1000;

export class DistributedJob<Data = undefined, Result = undefined, Progress = number> {
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
      timeout: options.timeout,
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

    const replace = !executionId && !!replacePlanned;

    if (replace) {
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

    // A filter on _id is already atomic thanks to its unique index
    const result = replace
      ? await this.safeUpsert(filter, { $setOnInsert, $set })
      : await this.collection.findOneAndUpdate(filter, { $setOnInsert, $set }, { upsert: true, returnDocument: 'after' });

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
      try {
        callback(job);
      } catch (error) {
        this.options.log?.('error', this.label, 'Error in watch callback:', error);
      }

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
      // If the execution already finished, no change event will come, so this lookup must succeed eventually
      while (this.subscribedExecutionIds.has(listener)) {
        let existing;
        try {
          existing = await this.collection.findOne({ _id: executionId });
        } catch (error) {
          this.options.log?.('warn', this.label, 'Failed to look up execution:', error);
          await sleep(PICK_UP_RETRY_DELAY);
          continue;
        }

        if (existing) check(existing);
        return;
      }
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
    return await this.collection.findOne({ _id: executionId });
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

      const state = await this.safeUpsert(
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
      );

      return state ?? undefined;
    } catch (error) {
      this.options.log?.('warn', this.label, 'Failed to schedule next run:', error);
      setTimeout(() => this.schedule(), 10_000);
    }
  }

  /**
   * Upsert for filters without a unique index behind them. Concurrent calls would each see no match and
   * each insert. Writing a shared per-job document makes them conflict, so withTransaction retries the
   * loser, which then finds the winner's document.
   */
  private async safeUpsert(
    filter: Filter<JobDbEntry<any, any, any>>,
    update: UpdateFilter<JobDbEntry<any, any, any>>,
  ): Promise<JobDbEntry<Data, Result, Progress> | null> {
    const scheduler = this.options.scheduler!;
    await scheduler.indexesReady;

    return await this.collection.db.client.withSession((session) =>
      session.withTransaction(async () => {
        await scheduler.lockCollection!.updateOne({ _id: this.options.jobId }, { $inc: { n: 1 } }, { upsert: true, session });
        return await this.collection.findOneAndUpdate(filter, update, { upsert: true, returnDocument: 'after', session });
      }),
    );
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
          // Server time, so clock differences between instances don't release locks early
          const expired = {
            jobId: this.options.jobId,
            state: 'planned' as const,
            lock: { $type: 'date' as const },
            $expr: { $lt: ['$lock', { $subtract: ['$$NOW', this.options.lockDuration] }] },
          };

          // The worker died or hung, so this counts as a failed attempt. Otherwise a run that crashes
          // the process would be retried forever.
          const historyWithError = {
            $concatArrays: [
              { $ifNull: ['$history', []] },
              [{ t: { $toLong: '$$NOW' }, attempt: '$attempt', event: 'error', level: 'error', message: 'Lock expired' }],
            ],
          };

          const failed = await this.collection.updateMany({ ...expired, attempt: { $gte: this.options.retryCount } }, [
            {
              $set: {
                state: 'error',
                error: 'Lock expired',
                lock: null,
                lockId: null,
                finishedOn: '$$NOW',
                history: historyWithError,
              },
            },
          ]);

          const released = await this.collection.updateMany(expired, [
            { $set: { lock: null, lockId: null, attempt: { $add: ['$attempt', 1] }, history: historyWithError } },
          ]);

          if (failed.modifiedCount || released.modifiedCount) {
            this.options.log?.('info', this.label, 'Expired locks:', { failed: failed.modifiedCount, retried: released.modifiedCount });
          }

          if (failed.modifiedCount) {
            await this.schedule();
          }
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

        const now = new Date();
        const lockId = nanoid();

        let job;
        try {
          job = await this.collection.findOneAndUpdate(
            {
              jobId: this.options.jobId,
              state: 'planned',
              nextRun: { $lte: now },
              lock: null,
            },
            { $currentDate: { lock: true }, $set: { lockId } },
            { returnDocument: 'after' },
          );
        } catch (error) {
          // Without a retry nothing would wake this worker up again until the next change event
          this.options.log?.('warn', this.label, 'Failed to pick up next job:', error);
          this.planAt(new Date(Date.now() + PICK_UP_RETRY_DELAY));
          return;
        }

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

        let lockLost = false;
        const abortController = new AbortController();
        const aborted = new Promise<never>((_resolve, reject) => {
          abortController.signal.addEventListener('abort', () => reject(abortController.signal.reason as Error));
        });
        aborted.catch(() => undefined);

        // While running, every flush also refreshes the lock so checkLocks doesn't release it
        const flush = async ({ heartbeat = true } = {}) => {
          if (lockLost) {
            return;
          }

          // Take the buffers now: entries added while this write is in flight belong to the next flush
          const set = $set;
          const batch = history;
          $set = {};
          history = [];

          const update: UpdateFilter<JobDbEntry<any, any, any>> = {
            ...(Object.keys(set).length > 0 && { $set: set }),
            ...(batch.length > 0 && { $push: { history: { $each: batch } } }),
            ...(heartbeat && { $currentDate: { lock: true } }),
          };

          if (Object.keys(update).length === 0) {
            return;
          }

          let res;
          try {
            // Only while we still hold the lock. Otherwise another worker may have taken over the run.
            res = await q.schedule(() => this.collection.updateOne({ _id: job._id, lockId }, update));
          } catch (error) {
            $set = { ...set, ...$set };
            history = [...batch, ...history];
            throw error;
          }

          if (res.matchedCount === 0) {
            lockLost = true;
            this.options.log?.('warn', this.label, 'Lost lock, discarding updates for', job._id);
            abortController.abort(new Error('Lost lock'));
          }
        };

        const flushInterval = setInterval(
          () => {
            flush().catch((e) => {
              this.options.log?.('warn', this.label, 'Failed to flush job updates:', e);
            });
          },
          Math.min(1000, this.options.lockDuration / 3),
        );

        const { timeout } = this.options;
        const timeoutHandle =
          timeout !== undefined ? setTimeout(() => abortController.abort(new Error(`Timed out after ${timeout}ms`)), timeout) : undefined;

        try {
          this.options.log?.('debug', this.label, 'run', job?._id);

          addHistory('start', 'info');

          const result = await Promise.race([
            this.options.run(job.data, {
              job,
              setProgress(progress) {
                $set.progress = progress;
              },
              logger,
              flush: () => flush(),
              signal: abortController.signal,
            }),
            aborted,
          ]);
          clearTimeout(timeoutHandle);

          Object.assign($set, {
            lock: null,
            lockId: null,
            finishedOn: new Date(),
            state: 'completed',
            result,
            error: null,
          });

          addHistory('complete', 'info');
          clearInterval(flushInterval);
          await flush({ heartbeat: false });

          this.options.log?.('debug', this.label, 'done', job?._id);
        } catch (error) {
          clearTimeout(timeoutHandle);
          const errorString = errorToString(error);
          const shouldRetry = job.attempt < this.options.retryCount;

          Object.assign($set, {
            nextRun: shouldRetry ? new Date(Date.now() + this.options.retryDelay) : job.nextRun,
            lock: null,
            lockId: null,
            attempt: shouldRetry ? job.attempt + 1 : job.attempt,
            progress: 0,
            state: shouldRetry ? 'planned' : 'error',
            error: errorString,
          });

          addHistory('error', 'error', errorString);
          clearInterval(flushInterval);

          await flush({ heartbeat: false }).catch((e) => {
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

    let next;
    try {
      [next] = await this.collection
        .find({
          jobId: this.options.jobId,
          lock: null,
          state: 'planned',
        })
        .sort({ nextRun: 1 })
        .limit(1)
        .toArray();
    } catch (error) {
      this.options.log?.('warn', this.label, 'Failed to look up next run:', error);
      this.planAt(new Date(Date.now() + PICK_UP_RETRY_DELAY));
      return;
    }

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

    try {
      const executionIds = new Set(this.subscribedExecutionIds.values());
      const cursor = this.collection.find<JobDbEntry<Data, Result, Progress>>({ _id: { $in: [...executionIds] } });
      for await (const job of cursor) {
        await this.receiveUpdate(job);
      }
    } catch (error) {
      this.options.log?.('warn', this.label, 'Failed to refresh watched executions:', error);
    }
  }

  private async planNextRun(job: JobDbEntry<Data, Result, Progress>): Promise<void> {
    this.planAt(job.nextRun);
  }

  private planAt(nextRun: Date): void {
    if (this.hasShutDown || !this.options.run) return;

    const now = Date.now();
    const date = new Date(Math.min(nextRun.getTime(), now + 60 * 60 * 1000));

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
    return await this.collection
      .find({
        jobId: this.options.jobId,
        state: 'planned',
      })
      .toArray();
  }
}
