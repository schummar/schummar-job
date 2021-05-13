import { Collection, ObjectID } from 'mongodb';
import { MaybePromise } from './helpers';

export type Schedule =
  | { milliseconds: number }
  | { seconds: number }
  | { minutes: number }
  | { hours: number }
  | { days: number }
  | { cron: string };

export type JobDbEntry<Data> = {
  _id: ObjectID;
  jobId: string;
  schedule: Schedule | null;
  data: Data;
  nextRun: Date;
  lock: Date | null;
  error: string | null;
  attempt: number;
};

export type DbConnection = MaybePromise<Collection<JobDbEntry<any>> | { uri: string; db: string; collection: string }>;

export type SchedulerOptions = {
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
  log?: { [K in 'error' | 'warn' | 'info']: typeof console[K] };
};

export type JobImplementation<Data> = (data: Data, jobInfo: { attempt: number; error?: unknown }) => MaybePromise<void>;

export type LocalJobOptions<Data = undefined> = {
  schedule?: undefined extends Data ? Schedule : Schedule & { data: Data };
  maxParallel?: number;
  retryCount?: number;
  retryDelay?: number;
  log?: { [K in 'error' | 'warn' | 'info']: typeof console[K] };
};

export type JobOptions<Data = undefined> = LocalJobOptions<Data> & {
  lockDuration?: number;
  lockCheckInterval?: number;
};

export type JobExecuteOptions = {
  delay?: number;
};

export type Job<Data> = {
  execute(
    ...args: undefined extends Data
      ? [] | [data: undefined] | [data: undefined, options: JobExecuteOptions]
      : [data: Data] | [data: Data, options: JobExecuteOptions]
  ): Promise<void>;
  shutdown(): Promise<void>;
};
