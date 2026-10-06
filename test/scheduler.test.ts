import { DistributedJob, JobDbEntry, LocalJob, Scheduler } from '../src';
import errorToString from '../src/errorToString';
import { calcNextRun, sleep } from '../src/helpers';
import { poll } from './_helpers';
import { MongoClient } from 'mongodb';
import { afterAll, afterEach, beforeEach, expect, inject, test, vi } from 'vite-plus/test';

declare module 'vite-plus/test' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

const connectionString = inject('mongo').connectionString;
const client = new MongoClient(connectionString, { directConnection: true });
const db = client.db('schummar-job-scheduler-tests');

beforeEach(async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.deleteMany({});
  t.scheduler = new Scheduler({ client, collection, lockDuration: 100, log: () => undefined });
  await t.scheduler.indexesReady;
});

afterEach(async (t) => {
  await t.scheduler?.shutdown();
});

afterAll(async () => {
  await client.close();
});

async function killChangeStreams() {
  const ops = await client
    .db('admin')
    .aggregate([
      { $currentOp: { allUsers: true, idleCursors: false } },
      { $match: { 'cursor.originatingCommand.pipeline.0.$changeStream': { $exists: true } } },
    ])
    .toArray();

  for (const op of ops) {
    await client.db('admin').command({ killOp: 1, op: op.opid });
  }

  return ops.length;
}

test('connection string and collection by name', async () => {
  const scheduler = new Scheduler({
    client: `${connectionString}?directConnection=true`,
    collection: { db: db.databaseName, collection: 'by name' },
    log: () => undefined,
  });

  try {
    const job = scheduler.addJob('job0', () => 42);
    await expect(job.executeAndAwait()).resolves.toBe(42);
    expect(scheduler.collection?.collectionName).toBe('by name');
  } finally {
    await scheduler.shutdown();
    await scheduler.client?.close();
  }
});

test('creates indexes and lock collection', async (t) => {
  const indexes = await t.scheduler.collection!.indexes();
  expect(indexes.map((x) => x.key)).toEqual([{ _id: 1 }, { jobId: 1, nextRun: 1 }, { jobId: 1 }]);

  const collections = await db.listCollections({ name: `${t.scheduler.collection!.collectionName}_locks` }).toArray();
  expect(collections).toHaveLength(1);
});

test('createIndexes: false', async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.drop();

  const scheduler = new Scheduler({ client, collection, createIndexes: false, log: () => undefined });
  await scheduler.indexesReady;

  const exists = await db.listCollections({ name: collection.collectionName }).toArray();
  expect(exists).toHaveLength(0);
});

test('logs index errors', async (t) => {
  const log = vi.fn();
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.dropIndexes();
  await collection.createIndex({ jobId: 1 }, { name: 'jobId_1', unique: false });

  const scheduler = new Scheduler({ client, collection, log });
  await scheduler.indexesReady;

  expect(log).toHaveBeenCalledWith('error', expect.any(String), 'Error ensuring indexes:', expect.anything());
});

test('default logger writes warnings and errors to the console', async (t) => {
  const error = vi.spyOn(console, 'error').mockImplementation(() => undefined);
  const debug = vi.spyOn(console, 'debug').mockImplementation(() => undefined);

  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.dropIndexes();
  await collection.createIndex({ jobId: 1 }, { name: 'jobId_1', unique: false });

  try {
    const scheduler = new Scheduler({ client, collection });
    await scheduler.indexesReady;
    scheduler.options.log('debug', 'ignored');

    expect(error).toHaveBeenCalled();
    expect(debug).not.toHaveBeenCalled();
  } finally {
    error.mockRestore();
    debug.mockRestore();
  }
});

test('addJob overloads', async (t) => {
  const fromOptions = t.scheduler.addJob({ jobId: 'job0', run: () => 'options' });
  const instance = new DistributedJob({ jobId: 'job1', run: () => 'instance' });
  const fromInstance = t.scheduler.addJob(instance);

  expect(fromInstance).toBe(instance);
  expect(t.scheduler.getJobs()).toEqual([fromOptions, instance]);

  await expect(fromOptions.executeAndAwait()).resolves.toBe('options');
  await expect(fromInstance.executeAndAwait()).resolves.toBe('instance');
});

test('addLocalJob overloads', async (t) => {
  const fromFunction = t.scheduler.addLocalJob(() => 'function');
  const fromOptions = t.scheduler.addLocalJob({ run: () => 'options' });
  const instance = new LocalJob({ run: () => 'instance' });
  const fromInstance = t.scheduler.addLocalJob(instance);

  expect(fromInstance).toBe(instance);
  expect(t.scheduler.getLocalJobs()).toEqual([fromFunction, fromOptions, instance]);
  await expect(fromOptions.execute()).resolves.toBe('options');
  await expect(fromInstance.execute()).resolves.toBe('instance');
});

test('clearJobs', async (t) => {
  t.scheduler.addJob('job0');
  t.scheduler.addLocalJob(() => undefined);

  await t.scheduler.clearJobs();

  expect(t.scheduler.getJobs()).toEqual([]);
  expect(t.scheduler.getLocalJobs()).toEqual([]);
});

test('onExecutionUpdate unsubscribe', async (t) => {
  const listener = vi.fn();
  const unsubscribe = t.scheduler.onExecutionUpdate(listener);
  const job = t.scheduler.addJob('job0', () => undefined);

  await job.executeAndAwait();
  await poll(() => listener.mock.calls.length > 0);

  unsubscribe();
  listener.mockClear();

  await job.executeAndAwait();
  await sleep(100);
  expect(listener).not.toHaveBeenCalled();
});

test('onReconnect fires when the change stream starts and after it is interrupted', async (t) => {
  const listener = vi.fn();
  const unsubscribe = t.scheduler.onReconnect(listener);
  const fn = vi.fn();
  const job = t.scheduler.addJob('job0', fn);

  await poll(() => listener.mock.calls.length === 1);
  await poll(async () => (await killChangeStreams()) > 0);
  await poll(() => listener.mock.calls.length === 2);

  await job.executeAndAwait();
  expect(fn).toHaveBeenCalledTimes(1);

  unsubscribe();
  await killChangeStreams();
  await sleep(500);
  expect(listener).toHaveBeenCalledTimes(2);
});

test('collection renamed onto the jobs collection triggers a refresh', async (t) => {
  const listener = vi.fn();
  t.scheduler.onReconnect(listener);
  const fn = vi.fn();
  const job = t.scheduler.addJob('job0', fn);
  await poll(() => listener.mock.calls.length === 1);

  const other = db.collection<JobDbEntry<any, any, any>>(`${t.task.name}_other`);
  await other.deleteMany({});
  await other.insertOne({
    _id: 'renamed',
    jobId: 'job0',
    isScheduled: false,
    state: 'planned',
    nextRun: new Date(),
    lock: null,
    finishedOn: null,
    attempt: 0,
    data: undefined,
    history: [],
  });
  await other.rename(t.scheduler.collection!.collectionName, { dropTarget: true });

  await poll(() => listener.mock.calls.length === 2);
  await expect(job.await('renamed')).resolves.toBeNull();
  expect(fn).toHaveBeenCalledTimes(1);
});

test('getExecutions and clearDB', async (t) => {
  const job = t.scheduler.addJob('job0');
  await job.execute(undefined, { executionId: 'a' });
  await job.execute(undefined, { executionId: 'b' });

  expect((await t.scheduler.getExecutions({ _id: 'a' })).map((x) => x._id)).toEqual(['a']);

  await t.scheduler.clearDB();
  expect(await t.scheduler.getExecutions({})).toEqual([]);
});

test('without a db', async () => {
  const scheduler = new Scheduler({ log: () => undefined });

  await expect(scheduler.getExecutions({})).rejects.toThrow('No db set up!');
  await expect(scheduler.clearDB()).rejects.toThrow('No db set up!');
  await expect(scheduler.indexesReady).resolves.toBeUndefined();
});

test('errorToString', () => {
  expect(errorToString(new Error('message'))).toBe('message');
  expect(errorToString('string')).toBe('string');
  expect(errorToString({ a: 1 })).toBe('{"a":1}');

  const circular: Record<string, unknown> = {};
  circular.self = circular;
  expect(errorToString(circular)).toBe('[object Object]');
});

test('calcNextRun with cron', () => {
  const lastRun = new Date(Date.now() + 60_000);
  lastRun.setSeconds(30, 0);

  const next = calcNextRun({ cron: '* * * * *' }, lastRun);
  expect(next.getTime() - lastRun.getTime()).toBe(30_000);
});
