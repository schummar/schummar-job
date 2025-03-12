import { parseCronExpression } from 'cron-schedule';
import { Schedule } from './types';

export type MaybePromise<T> = T | Promise<T>;

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

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
