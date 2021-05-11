import { Collection, ObjectID } from 'mongodb';
import { MaybePromise } from './helpers';

export type JobDbEntry<Data> = {
  _id: ObjectID;
  jobId: string;
  interval: number | null;
  data: Data;
  nextRun: Date;
  lock: Date | null;
  error: string | null;
  tryCount: number;
};

export type CollectionInfo = MaybePromise<Collection<JobDbEntry<any>> | { uri: string; db: string; collection: string }>;

export type SchedulerOptions = {
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

export type JobOptions<Data> = {
  schedule?: { interval: number; data: undefined extends Data ? never : Data };
  maxParallel?: number;
  retryCount?: number;
  retryDelay?: number;
  lockDuration?: number;
  lockCheckInterval?: number;
};

export type JobExecuteOptions = {
  delay?: number;
};
