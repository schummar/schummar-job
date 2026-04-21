import { Scheduler, type LocalJobOptionsNormalized } from '.';
import { calcNextRun } from './helpers';
import { LocalJobOptions, type ExecuteArgs } from './types';
import { nanoid } from 'nanoid';
import { createQueue, Queue } from 'schummar-queue';

const CANCELED = Symbol('canceled');

export class LocalJob<Data = undefined, Result = void> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private handles = new Set<() => void>();
  private hasShutDown = false;
  private executionIds = new Map<string, Promise<Result>>();
  private _options: LocalJobOptionsNormalized<Data, Result>;

  constructor(options: LocalJobOptions<Data, Result>) {
    this._options = this.normalizeOptions(options);
    this.q = createQueue({ parallel: this.options.maxParallel });

    void this.schedule();
  }

  get options() {
    return this._options;
  }

  updateOptions(options: Partial<LocalJobOptions<Data, Result>>) {
    this._options = this.normalizeOptions({ ...this._options, ...options });
  }

  private normalizeOptions(options: LocalJobOptions<Data, Result>): LocalJobOptionsNormalized<Data, Result> {
    return {
      run: options.run,
      scheduler: options.scheduler,
      schedule: options.schedule,
      maxParallel: options.maxParallel ?? LocalJob.DEFAULT_MAX_PARALLEL,
      retryCount: options.retryCount ?? options.scheduler?.options.retryCount ?? Scheduler.DEFAULT_RETRY_COUNT,
      retryDelay: options.retryDelay ?? options.scheduler?.options.retryDelay ?? Scheduler.DEFAULT_RETRY_DELAY,
      log: options.log ?? options.scheduler?.options.log,
    };
  }

  private async schedule() {
    try {
      const { schedule } = this.options;
      if (!schedule) return;

      while (!this.hasShutDown) {
        const nextRun = calcNextRun(schedule);
        await this.sleep(nextRun.getTime() - Date.now());
        await this.execute(...([schedule.data] as ExecuteArgs<Data, Result, never>));
      }
    } catch (e) {
      if (e !== CANCELED) throw e;
    }
  }

  async execute(...[data, { delay = 0, executionId = nanoid() } = {}]: ExecuteArgs<Data, Result, never>): Promise<Result> {
    try {
      const existing = this.executionIds.get(executionId);
      if (existing) return existing;

      const promise = (async () => {
        if (delay > 0) {
          await this.sleep(delay);
        }

        let attempt = 0,
          error: unknown;
        while (!this.hasShutDown) {
          try {
            return await this.q.schedule(() => this.options.run(data as Data, { error, attempt }));
          } catch (e) {
            error = e;
            if (!this.hasShutDown && attempt < this.options.retryCount) {
              attempt++;
              await this.sleep(this.options.retryDelay);
            } else {
              throw error;
            }
          }
        }
        throw CANCELED;
      })();

      this.executionIds.set(executionId, promise);

      return await promise;
    } catch (e) {
      if (e !== CANCELED) {
        this.options.log?.('error', 'Error in job execution:', e);
      }
      throw e;
    } finally {
      this.executionIds.delete(executionId);
    }
  }

  async shutdown(): Promise<void> {
    this.hasShutDown = true;
    for (const handle of this.handles) {
      handle();
    }
  }

  private sleep(ms: number) {
    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.handles.delete(handle);
        resolve();
      }, ms);
      const handle = () => {
        clearTimeout(timeout);
        reject(CANCELED);
      };
      this.handles.add(handle);
    });
  }
}
