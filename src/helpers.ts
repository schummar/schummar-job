import { parseCronExpression } from 'cron-schedule';
import { Schedule } from './types';

export type MaybePromise<T> = T | Promise<T>;

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export const calcNextRun = (schedule: Schedule): Date => {
  if (typeof schedule === 'string') {
    return parseCronExpression(schedule).getNextDate();
  } else {
    return new Date(Date.now() + schedule);
  }
};
