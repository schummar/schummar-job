import { Queue } from 'schummar-queue';
import { Scheduler } from '.';
import { calcNextRun } from './helpers';
import { Job, JobImplementation, LocalJobOptions } from './types';

const CANCELED = Symbol('canceled');

export class LocalJob<Data> implements Job<Data> {
  private q: Queue;
  private handles = new Set<() => void>();
  private hasShutDown = false;

  constructor(
    public readonly scheduler: Scheduler,
    public readonly implementation: JobImplementation<Data>,
    public readonly options: LocalJobOptions<Data> = {}
  ) {
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

  async execute(...[data, { delay = 0 } = {}]: Parameters<Job<Data>['execute']>): Promise<void> {
    (async () => {
      const retryCount = this.options.retryCount ?? this.scheduler.options.retryCount ?? Scheduler.DEFAULT_RETRY_COUNT;
      const retryDelay = this.options.retryDelay ?? this.scheduler.options.retryDelay ?? Scheduler.DEFAULT_RETRY_DELAY;
      const log = this.options.log ?? this.scheduler.options.log ?? console;

      try {
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
          if (attempt < retryCount) {
            attempt++;
            await this.sleep(retryDelay);
          } else {
            throw error;
          }
        }
      } catch (e) {
        if (e !== CANCELED) {
          log.error('Error in job execution:', e);
        }
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
