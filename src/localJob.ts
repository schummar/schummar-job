import { nanoid } from 'nanoid';
import { createQueue, Queue } from 'schummar-queue';
import { Scheduler } from '.';
import { calcNextRun } from './helpers';
import { LocalJobImplementation, LocalJobOptions, type ExecuteArgs } from './types';

const CANCELED = Symbol('canceled');

export class LocalJob<Data, Result> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private handles = new Set<() => void>();
  private hasShutDown = false;
  private executionIds = new Map<string, Promise<Result>>();
  public readonly options: LocalJobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly implementation: LocalJobImplementation<Data, Result>,
    {
      maxParallel = LocalJob.DEFAULT_MAX_PARALLEL,
      retryCount = scheduler.options.retryCount,
      retryDelay = scheduler.options.retryDelay,
      log = scheduler.options.log,
      ...otherOptions
    }: Partial<LocalJobOptions<Data>> = {},
  ) {
    this.options = { maxParallel, retryCount, retryDelay, log, ...otherOptions };
    this.q = createQueue({ parallel: this.options.maxParallel });

    this.schedule();
  }

  private async schedule() {
    try {
      const { schedule } = this.options;
      if (!schedule) return;

      while (!this.hasShutDown) {
        const nextRun = calcNextRun(schedule);
        await this.sleep(nextRun.getTime() - Date.now());
        await this.execute(...([schedule.data] as ExecuteArgs<Data>));
      }
    } catch (e) {
      if (e !== CANCELED) throw e;
    }
  }

  async execute(...[data, { delay = 0, executionId = nanoid() } = {}]: ExecuteArgs<Data>): Promise<Result> {
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
            return await this.q.schedule(() => this.implementation(data as Data, { error, attempt }));
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
        this.options.log('error', 'Error in job execution:', e);
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
