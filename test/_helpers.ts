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
