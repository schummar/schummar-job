import { Collection, ObjectId } from 'mongodb';
import { MaybePromise } from './helpers';

export type json = string | number | boolean | Date | null | json[] | { [id: string]: json };

export type Schedule =
  | { milliseconds: number }
  | { seconds: number }
  | { minutes: number }
  | { hours: number }
  | { days: number }
  | { cron: string };

export type JobDbEntry<Data, Result> = {
  _id: ObjectId;
  jobId: string;
  executionId: string | null;
  schedule: Schedule | null;
  data: Data;
  nextRun: Date;
  completedOn: Date | null;
  lock: Date | null;
  attempt: number;
  history:
    | {
        t: Date;
        error: string | null;
        result: Result | null;
      }[]
    | null;
};

export type DbConnection = MaybePromise<Collection<JobDbEntry<any, any>> | { uri: string; db: string; collection: string }>;

export type SchedulerOptions = {
  retryCount: number;
  retryDelay: number;
  lockDuration: number;
  lockCheckInterval: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<typeof console['log']>) => void;
};

export type JobImplementation<Data, Result> = (
  data: Data,
  lastRun: { result: Result | null; error?: unknown; attempt: number },
  jobInfo?: JobDbEntry<Data, Result>
) => MaybePromise<Result>;

export type LocalJobOptions<Data> = {
  schedule?: null extends Data ? Schedule : Schedule & { data: Data };
  maxParallel: number;
  retryCount: number;
  retryDelay: number;
  log: (level: 'error' | 'warn' | 'info' | 'debug', ...args: Parameters<typeof console['log']>) => void;
};

export type DistributedJobOptions<Data extends json> = LocalJobOptions<Data> & {
  lockDuration: number;
  lockCheckInterval: number;
};

export type JobExecuteOptions = {
  delay?: number;
  executionId?: string;
};
