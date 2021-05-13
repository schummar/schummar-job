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
  tryCount: number;
};

export type DbConnection = MaybePromise<Collection<JobDbEntry<any>> | { uri: string; db: string; collection: string }>;

export type SchedulerOptions = {
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

export type JobImplementation<Data> = (data: Data, job: JobDbEntry<Data>) => MaybePromise<void>;

export type JobOptions<Data = undefined> = {
  schedule?: undefined extends Data ? Schedule : Schedule & { data: Data };
  maxParallel?: number;
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

export type JobExecuteOptions = {
  delay?: number;
};
