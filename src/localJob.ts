import { nanoid } from 'nanoid';
import { Queue } from 'schummar-queue';
import { Scheduler } from '.';
import { calcNextRun } from './helpers';
import { Job, JobImplementation, LocalJobOptions } from './types';

const CANCELED = Symbol('canceled');

export class LocalJob<Data> implements Job<Data> {
  static DEFAULT_MAX_PARALLEL = 1;

  private q: Queue;
  private handles = new Set<() => void>();
  private hasShutDown = false;
  private executionIds = new Set<string | number>();
  public readonly options: LocalJobOptions<Data>;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly implementation: JobImplementation<Data>,
    options: Partial<LocalJobOptions<Data>> = {}
  ) {
    this.options = {
      maxParallel: LocalJob.DEFAULT_MAX_PARALLEL,
      retryCount: scheduler.options.retryCount,
      retryDelay: scheduler.options.retryDelay,
      log: scheduler.options.log,
      ...options,
    };
    this.q = new Queue({ parallel: options.maxParallel });

    this.schedule();
  }

  private async schedule() {
    try {
      const { schedule } = this.options;
      if (!schedule) return;

      const data = (schedule as any).data as Data;

      while (!this.hasShutDown) {
        const nextRun = calcNextRun(schedule);
        await this.sleep(nextRun.getTime() - Date.now());
        await this.execute(...([data] as any));
      }
    } catch (e) {
      if (e !== CANCELED) throw e;
    }
  }

  async execute(...[data, { delay = 0, executionId = nanoid() } = {}]: Parameters<Job<Data>['execute']>): Promise<void> {
    (async () => {
      try {
        if (this.executionIds.has(executionId)) return;
        this.executionIds.add(executionId);
        await this.sleep(delay);

        let attempt = 0,
          error: unknown = undefined;
        while (!this.hasShutDown) {
          error = await this.q.schedule(async () => {
            try {
              await this.implementation(data as Data, { attempt, error });
            } catch (e) {
              return e;
            }
          });

          if (!error || this.hasShutDown) return;
          if (attempt < this.options.retryCount) {
            attempt++;
            await this.sleep(this.options.retryDelay);
          } else {
            throw error;
          }
        }
      } catch (e) {
        if (e !== CANCELED) {
          this.options.log('error', 'Error in job execution:', e);
        }
      } finally {
        this.executionIds.delete(executionId);
      }
    })();
  }

  async shutdown(): Promise<void> {
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
