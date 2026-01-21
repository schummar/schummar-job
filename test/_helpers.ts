import type { DistributedJob, JobDbEntry } from '../src';
import { sleep } from '../src/helpers';

export const poll = async (condition: () => unknown, timeout = 10000, interval = 10): Promise<void> => {
  for (let t = 0; t < timeout; t += interval) {
    await sleep(interval);
    if (condition()) return;
  }
  throw Error('Timeout');
};

export const noopLogger = (): void => {
  // ignore
};

export function waitUntilJob<D, R, P>(
  job: DistributedJob<D, R, P>,
  id: string,
  predicate: (job: JobDbEntry<D, R, P>) => boolean,
  timeout = 1000,
) {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cancel();
      reject(new Error('Timeout waiting for job condition'));
    }, timeout);

    const unwatch = job.watch(id, (state) => {
      if (predicate(state)) {
        cancel();
        resolve();
      }
    });

    function cancel() {
      clearTimeout(timer);
      unwatch();
    }
  });
}
