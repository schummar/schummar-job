import { parseCronExpression } from 'cron-schedule';
import { Schedule } from './types';

export type MaybePromise<T> = T | Promise<T>;

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export const calcNextRun = (schedule: Schedule, lastRun = new Date()): Date => {
  const t = Math.max(lastRun.getTime(), Date.now());

  if ('milliseconds' in schedule) {
    return new Date(t + schedule.milliseconds);
  }
  if ('seconds' in schedule) {
    return new Date(t + schedule.seconds * 1000);
  }
  if ('minutes' in schedule) {
    return new Date(t + schedule.minutes * 60 * 1000);
  }
  if ('hours' in schedule) {
    return new Date(t + schedule.hours * 60 * 60 * 1000);
  }
  if ('days' in schedule) {
    return new Date(t + schedule.days * 24 * 60 * 60 * 1000);
  }

  return parseCronExpression(schedule.cron).getNextDate(new Date(t));
};
