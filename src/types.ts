import { Collection, ObjectId } from 'mongodb';
import { MaybePromise } from './helpers';

export type Schedule =
  | { milliseconds: number }
  | { seconds: number }
  | { minutes: number }
  | { hours: number }
  | { days: number }
  | { cron: string };

export interface LogLine {
  t: number;
  log: string;
}

export type JobDbEntry<Data, Result, Progress> = {
  _id: ObjectId;
  jobId: string;
  executionId: string;

  schedule: Schedule | null;
  nextRun: Date;
  lock: Date | null;
  finishedOn: Date | null;
  attempt: number;

  data: Data;
  progress?: Progress;
  logs: LogLine[];
} & ({ state: 'planned' } | { state: 'completed'; result: Result } | { state: 'error'; error: string });

export type DbConnection = MaybePromise<Collection<JobDbEntry<any, any, any>> | { uri: string; db: string; collection: string }>;

export interface SchedulerOptions {
  retryCount: number;
  retryDelay: number;
  lockDuration: number;
  lockCheckInterval: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<(typeof console)['log']>) => void;
}

export interface LocalJobImplementation<Data, Result> {
  (
    data: Data,
    helpers: {
      attempt: number;
      error: unknown;
    },
  ): MaybePromise<Result>;
}

export interface DistributedJobImplementation<Data, Result, Progress> {
  (
    data: Data,
    helpers: {
      job: JobDbEntry<Data, never, Progress>;
      setProgress(progress: Progress): Promise<void>;
      log(log: string): Promise<void>;
    },
  ): MaybePromise<Result>;
}

export interface LocalJobOptions<Data> {
  schedule?: Schedule & (undefined extends Data ? { data?: Data } : { data: Data });
  maxParallel: number;
  retryCount: number;
  retryDelay: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<(typeof console)['log']>) => void;
}

export interface DistributedJobOptions<Data> extends LocalJobOptions<Data> {
  lockDuration: number;
  lockCheckInterval: number;
}

export interface JobExecuteOptions {
  delay?: number;
  executionId?: string;
}

export type ExecuteArgs<Data> = undefined extends Data
  ? [data?: Data, options?: JobExecuteOptions]
  : [data: Data, options?: JobExecuteOptions];
