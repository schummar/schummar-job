import { Schedule } from './types';
import { parseCronExpression } from 'cron-schedule';

export type MaybePromise<T> = T | Promise<T>;

/** Resolves early when `signal` aborts. */
export const sleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted) return resolve();

    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', done, { once: true });
  });

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;
const ONE_HOUR = 60 * ONE_MINUTE;
const ONE_DAY = 24 * ONE_HOUR;

export const calcNextRun = (schedule: Schedule, lastRun = new Date()): Date => {
  let t = Math.max(lastRun.getTime(), Date.now());

  if ('cron' in schedule) {
    return parseCronExpression(schedule.cron).getNextDate(new Date(t));
  }

  if ('milliseconds' in schedule) {
    t += schedule.milliseconds;
  }
  if ('seconds' in schedule) {
    t += schedule.seconds * ONE_SECOND;
  }
  if ('minutes' in schedule) {
    t += schedule.minutes * ONE_MINUTE;
  }
  if ('hours' in schedule) {
    t += schedule.hours * ONE_HOUR;
  }
  if ('days' in schedule) {
    t += schedule.days * ONE_DAY;
  }

  return new Date(t);
};
