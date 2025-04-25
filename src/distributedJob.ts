import { Collection } from 'mongodb';
import { nanoid } from 'nanoid';
import assert from 'node:assert';
import { createQueue, type Queue } from 'schummar-queue';
import { calcNextRun, MaybePromise, sleep } from './helpers';
import { Scheduler } from './scheduler';
import { DistributedJobImplementation, DistributedJobOptions, JobDbEntry, type ExecuteArgs, type HistoryItem } from './types';

export class DistributedJob<Data, Result, Progress> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private timeout?: { handle: NodeJS.Timeout; date: Date };
  private hasShutDown = false;
  private subscribedExecutionIds = new Array<{
    executionId: string;
    listener: (job: JobDbEntry<Data, Result, Progress>) => void;
  }>();
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
      this.schedule();
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
    ...otherOptions
  }: Partial<DistributedJobOptions<Data>>): DistributedJobOptions<Data> {
    return { maxParallel, retryCount, retryDelay, log, lockDuration, lockCheckInterval, ...otherOptions };
  }

  async execute(...[data, { at, delay = 0, executionId = nanoid() } = {}]: ExecuteArgs<Data>): Promise<string> {
    const t = at ? new Date(at) : new Date();
    t.setMilliseconds(t.getMilliseconds() + delay);

    this._options.log('debug', this.label, 'schedule for execution', this.jobId, !at && !delay ? 'immediately' : `at ${t.toISOString()}`);

    const col = await this.collection;
    await col.updateOne(
      {
        _id: executionId,
      },
      {
        $setOnInsert: {
          jobId: this.jobId,

          schedule: null,
          nextRun: t,
          lock: null,
          finishedOn: null,
          attempt: 0,

          data: data,
          progress: 0,
          history: [],
          state: 'planned',
        },
      },
      { upsert: true },
    );

    this._options.log('debug', this.label, 'successfully scheduled for execution', executionId);

    return executionId;
  }

  async await(executionId: string): Promise<Result> {
    return new Promise<Result>((resolve, reject) => {
      const listener = (job: JobDbEntry<Data, Result, Progress>) => {
        if (job.state === 'completed') resolve(job.result);
        else if (job.state === 'error') reject(Error(job.error));
        else return;

        this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener);
      };

      this.subscribedExecutionIds.push({ executionId, listener });

      (async () => {
        const col = await this.collection;
        const existing = await col.findOne({ _id: executionId });
        if (existing) listener(existing);
      })();
    });
  }

  async executeAndAwait(...args: Parameters<DistributedJob<Data, Result, Progress>['execute']>): Promise<Result> {
    const id = await this.execute(...args);
    return this.await(id);
  }

  onProgress(executionId: string, callback: (progress: Progress) => void): () => void {
    let lastValue: unknown;
    const listener = (job: JobDbEntry<Data, Result, Progress>) => {
      if (job.progress && job.progress !== lastValue) callback(job.progress);
      lastValue = job.progress;

      if (job.state === 'completed' || job.state === 'error') cancel();
    };

    const cancel = () => (this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener));

    this.subscribedExecutionIds.push({ executionId, listener });

    (async () => {
      const col = await this.collection;
      const existing = await col.findOne({ _id: executionId });
      if (existing) listener(existing);
    })();

    return cancel;
  }

  watch(executionId: string, callback: (job: JobDbEntry<Data, Result, Progress>) => void): () => void {
    const listener = (job: JobDbEntry<Data, Result, Progress>) => {
      callback(job);
      if (job.state === 'completed' || job.state === 'error') cancel();
    };

    const cancel = () => (this.subscribedExecutionIds = this.subscribedExecutionIds.filter((x) => x.listener !== listener));

    this.subscribedExecutionIds.push({ executionId, listener });

    (async () => {
      const col = await this.collection;
      const existing = await col.findOne({ _id: executionId });
      if (existing) listener(existing);
    })();

    return cancel;
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

  private async schedule(lastJob?: JobDbEntry<Data, Result, Progress>) {
    const { schedule } = this._options;
    const data = schedule && (schedule as { data?: Data }).data;

    if (!schedule) return;

    const col = await this.collection;
    await col.updateOne(
      {
        jobId: this.jobId,
        schedule: { $ne: null },
        state: 'planned',
        lock: lastJob ? null : undefined,
      },
      {
        $setOnInsert: {
          _id: nanoid(),

          schedule: schedule,
          nextRun: calcNextRun(schedule, lastJob?.nextRun),
          lock: null,
          finishedOn: null,
          attempt: 0,

          data: data ?? null,
          progress: 0,
          state: 'planned',
        },
      },
      { upsert: true },
    );
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
        this._options.log('debug', this.label, 'find next', job?._id);

        if (!job) {
          this.checkForNextRun();
          return;
        }
        this.next();

        assert(this.implementation);
        assert(job.state === 'planned');

        try {
          const q = createQueue();
          this._options.log('debug', this.label, 'execute next', job?._id);

          let $set: Partial<JobDbEntry<Data, Result, Progress>> = {};
          let history: HistoryItem[] = [];

          const addHistory = (event: HistoryItem['event'], message?: string) => {
            history.push({ t: Date.now(), attempt: job.attempt, event, message });
          };

          const flush = () => {
            if (Object.keys($set).length === 0 && history.length === 0) {
              return;
            }

            const update = {
              ...(Object.keys($set).length > 0 && { $set }),
              ...(history.length > 0 && { $push: { history: { $each: history } } }),
            };

            q.schedule(() => col.updateOne({ _id: job._id }, update));
            $set = {};
            history = [];
          };

          const flushInterval = setInterval(flush, 1000);

          addHistory('start');

          const result = await this.implementation(job.data, {
            job,
            setProgress(progress) {
              $set.progress = progress;
            },
            log(message) {
              addHistory('log', message);
            },
          });

          if (job.schedule) {
            await this.schedule(job);
          }

          Object.assign($set, {
            lock: null,
            finishedOn: new Date(),
            state: 'completed',
            result,
            error: null,
          });

          addHistory('complete');
          clearInterval(flushInterval);
          flush();
          await q.whenEmpty();

          this._options.log('debug', this.label, 'execute next done', job?._id);
        } catch (e) {
          const msg = e instanceof Error ? e.message : e instanceof Object ? JSON.stringify(e) : String(e);
          const retry = job.attempt < this._options.retryCount;

          await col.updateOne(
            { _id: job._id },
            {
              $set: {
                nextRun: retry ? new Date(Date.now() + this._options.retryDelay) : job.nextRun,
                lock: null,
                attempt: retry ? job.attempt + 1 : job.attempt,

                progress: 0,
                state: retry ? 'planned' : 'error',
                error: msg,
              },
            },
          );

          throw e;
        }
      } catch (e) {
        this._options.log('error', this.label, 'next failed', e);
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
    for (const { executionId, listener } of this.subscribedExecutionIds) {
      if (executionId === job._id) listener(job);
    }

    if (job.state === 'planned') {
      return this.planNextRun(job);
    }
  }

  async changeStreamReconnected(): Promise<void> {
    this.checkForNextRun();

    const executionIds = new Set(this.subscribedExecutionIds.map((x) => x.executionId));
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
