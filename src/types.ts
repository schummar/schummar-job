import { Collection, ObjectId } from 'mongodb';
import { MaybePromise } from './helpers';

export type Schedule =
  | { milliseconds: number }
  | { seconds: number }
  | { minutes: number }
  | { hours: number }
  | { days: number }
  | { cron: string };

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
  progress: Progress;
} & ({ state: 'planned' } | { state: 'completed'; result: Result } | { state: 'error'; error: string });

export type DbConnection = MaybePromise<Collection<JobDbEntry<any, any, any>> | { uri: string; db: string; collection: string }>;

export type SchedulerOptions = {
  retryCount: number;
  retryDelay: number;
  lockDuration: number;
  lockCheckInterval: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<typeof console['log']>) => void;
};

export type LocalJobImplementation<Data, Result> = (
  data: Data,
  helpers: {
    attempt: number;
    error: unknown;
  }
) => MaybePromise<Result>;

export type DistributedJobImplementation<Data, Result, Progress> = (
  data: Data,
  helpers: {
    job: JobDbEntry<Data, never, Progress>;
    setProgress: (progress: Progress) => Promise<void>;
  }
) => MaybePromise<Result>;

export type LocalJobOptions<Data> = {
  schedule?: null extends Data ? Schedule : Schedule & { data: Data };
  maxParallel: number;
  retryCount: number;
  retryDelay: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<typeof console['log']>) => void;
};

export type DistributedJobOptions<Data> = LocalJobOptions<Data> & {
  lockDuration: number;
  lockCheckInterval: number;
};

export type JobExecuteOptions = {
  delay?: number;
  executionId?: string;
};
