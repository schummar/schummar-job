import { DistributedJob, JobDbEntry, Scheduler } from '../src';
import { sleep } from '../src/helpers';
import { poll, trackUnhandledRejections, waitUntilJob } from './_helpers';
import { deepEqual } from 'fast-equals';
import { MongoClient } from 'mongodb';
import { afterEach, assert, beforeEach, expect, inject, test, vi, vitest } from 'vite-plus/test';

declare module 'vite-plus/test' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

const client = new MongoClient(inject('mongo').connectionString, { directConnection: true });
const db = client.db('schummar-job-tests');

beforeEach(async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.deleteMany({});
  t.scheduler = new Scheduler({
    client,
    collection,
    lockDuration: 100,
    log: () => undefined,
  });
  await t.scheduler.indexesReady;
});

afterEach(async (t) => {
  await t.scheduler?.shutdown();
});

test('simple', async (t) => {
  expect.assertions(1);

  const job = t.scheduler.addJob('job0', ({ x }: { x: number }) => {
    expect(x).toBe(42);
  });

  await job.executeAndAwait({ x: 42 });
});

test('return value', async (t) => {
  const job = t.scheduler.addJob('job0', ({ x }: { x: number }) => {
    expect(x).toBe(42);
    return x + 1;
  });

  await expect(job.executeAndAwait({ x: 42 })).resolves.toBe(43);
});

test('error once', async (t) => {
  expect.assertions(2);

  const job = t.scheduler.addJob(
    'job0',
    (_data, { job }) => {
      if (job.attempt === 0) {
        expect(true).toBe(true); // TODO make nicer
        throw Error('testerror');
      }
      expect(job.attempt).toBe(1);
    },
    { retryDelay: 0 },
  );

  await job.executeAndAwait();
});

test('error multiple', async (t) => {
  const fn = vitest.fn(() => {
    throw Error('testerror');
  });

  const job = t.scheduler.addJob('job0', fn, { retryDelay: 0, retryCount: 2 });

  const id = await job.execute();
  await expect(job.await(id)).rejects.toThrow('testerror');

  const state = await job.getExecution(id);
  expect(fn).toHaveBeenCalledTimes(3);
  expect(state).toMatchObject({ attempt: 2, state: 'error' });
});

test('repeated error in scheduled job', { retry: 3 }, async (t) => {
  const job = t.scheduler.addJob(
    'job0',
    () => {
      throw Error('testerror');
    },
    { schedule: { milliseconds: 100 }, retryDelay: 100, retryCount: 2 },
  );

  job.updateOptions({ schedule: { hours: 1 } });

  const id = (await job.schedule())?._id;
  assert(id);

  await expect(waitUntilJob(job, id, (j) => j.state === 'error' && j.attempt === 2, 5000)).resolves.toBeUndefined();

  let plannedJob: JobDbEntry<undefined, undefined, number> | undefined;
  await poll(async () => ([plannedJob] = await job.getPlanned()).length > 0);
  expect(plannedJob).toMatchObject({
    _id: expect.not.stringMatching(id),
    jobId: 'job0',
    attempt: 0,
    state: 'planned',
  });
});

test('scheduling in parallel creates only one job', async (t) => {
  const job = t.scheduler.addJob(
    'job0',
    () => {
      console.log('run');
      // noop
    },
    { schedule: { hours: 1 } },
  );

  await Promise.all(
    Array(5)
      .fill(0)
      .map(() => job.schedule()),
  );

  const planned = await job.getPlanned();
  expect(planned.length).toBe(1);
});

test('schedule does not retry forever on _id collision', async (t) => {
  await t.scheduler.collection!.insertOne({
    _id: 'fixed',
    jobId: 'job0',
    isScheduled: true,
    state: 'completed',
    result: undefined,
    nextRun: new Date(),
    lock: null,
    finishedOn: new Date(),
    attempt: 0,
    data: undefined,
    history: [],
  });

  const job = t.scheduler.addJob('job0', () => undefined, {
    schedule: { hours: 1 },
    getExecutionId: () => 'fixed',
  });

  await expect(job.schedule()).resolves.toBeUndefined();
});

test('multiple workers', async (t) => {
  const fn = vi.fn();
  const props = ['job0', fn] as const;

  const job = t.scheduler.addJob(...props);
  t.scheduler.addJob(...props);
  t.scheduler.addJob(...props);

  await Promise.all(
    Array(5)
      .fill(0)
      .map(() => job.executeAndAwait()),
  );

  expect(fn).toHaveBeenCalledTimes(5);
});

test('schedule', async (t) => {
  let count = 0;

  t.scheduler.addJob(
    'job0',
    () => {
      count++;
    },
    { schedule: { milliseconds: 10 } },
  );

  await poll(() => count >= 2);
  expect(true).toBe(true); // TODO make nicer
});

test('schedule with data', async (t) => {
  let count = 0;

  t.scheduler.addJob(
    'job0',
    (x: number) => {
      expect(x).toBe(42);
      count++;
    },
    { schedule: { milliseconds: 10, data: 42 } },
  );

  await poll(() => count >= 2);
  expect(true).toBe(true); // TODO make nicer
});

test('restart', async (t) => {
  expect.assertions(1);

  const job = t.scheduler.addJob('job0', () => {
    expect.fail();
  });

  await t.scheduler.shutdown();
  const id = await job.execute();

  const newScheduler = new Scheduler({ client, collection: t.scheduler.collection, lockDuration: 100 });
  const newJob = newScheduler.addJob('job0', () => {
    expect(true).toBe(true); // TODO make nicer
  });

  await newJob.await(id);
});

test('null implementation', async (t) => {
  expect.assertions(1);

  const job = t.scheduler.addJob('job0');
  const id = await job.execute();

  t.scheduler.addJob('job0', () => {
    expect(true).toBe(true); // TODO make nicer
  });
  await job.await(id);
});

test('executionId', async (t) => {
  const fn = vi.fn(() => {
    return 42;
  });
  const job = t.scheduler.addJob('job0', fn);

  await job.execute(undefined, { executionId: 'foo' });
  await job.execute(undefined, { executionId: 'foo' });

  await expect(job.executeAndAwait(undefined, { executionId: 'foo' })).resolves.toBe(42);
  expect(fn).toHaveBeenCalledTimes(1);
});

test('getExecutionId', async (t) => {
  const fn = vi.fn((x: number) => {
    return x;
  });
  const job = t.scheduler.addJob('job0', fn, {
    getExecutionId(x) {
      return `value-${x}`;
    },
  });

  await job.execute(42);
  await job.execute(42);

  await expect(job.executeAndAwait(42)).resolves.toBe(42);
  expect(fn).toHaveBeenCalledTimes(1);
});

test('replacePlanned', async (t) => {
  const fn = vi.fn((x: number) => {
    return x;
  });
  const job = t.scheduler.addJob('job0', fn);

  await job.execute(0, { delay: 200 });
  await job.execute(1, { delay: 200 });
  const result = await job.executeAndAwait(2, { replacePlanned: true });

  expect(result).toBe(2);
  expect(fn.mock.calls.length).toBeLessThanOrEqual(2);
});

test('replacePlanned sameData', async (t) => {
  const fn = vi.fn((x: number) => {
    return x;
  });
  const job = t.scheduler.addJob('job0', fn);

  // Schedule two jobs with data=1 and one with data=2
  const id1a = await job.execute(1, { delay: 100 });
  const id1b = await job.execute(1, { delay: 100 });
  const id2a = await job.execute(2, { delay: 100 });

  const id1c = await job.execute(1, { replacePlanned: { match: { data: 1 } } });
  const id2b = await job.execute(2);
  const id3 = await job.execute(3, { replacePlanned: { match: { data: 3 } } });
  await Promise.all([id1a, id1b, id1c, id2a, id2b, id3].map((id) => job.await(id)));

  expect(id1c).toBeOneOf([id1a, id1b]);
  expect(id2b).not.toBe(id2a);
  expect(fn.mock.calls.filter(([x]) => x === 1).length).toBe(2);
  expect(fn.mock.calls.filter(([x]) => x === 2).length).toBe(2);
  expect(fn.mock.calls.filter(([x]) => x === 3).length).toBe(1);
});

test('replacePlanned in parallel creates only one job', async (t) => {
  const job = t.scheduler.addJob<number>('job0');

  const ids = await Promise.all(
    Array(10)
      .fill(0)
      .map((_, i) => job.execute(i, { delay: 10_000, replacePlanned: true })),
  );

  const planned = await job.getPlanned();
  expect(planned.length).toBe(1);
  expect(new Set(ids)).toEqual(new Set([planned[0]!._id]));
});

test('replacePlanned with match in parallel creates one job per match', async (t) => {
  const job = t.scheduler.addJob<number>('job0');

  await Promise.all(
    Array(20)
      .fill(0)
      .map((_, i) => job.execute(i % 2, { delay: 10_000, replacePlanned: { match: { data: i % 2 } } })),
  );

  const planned = await job.getPlanned();
  expect(planned.map((x) => x.data).sort((a, b) => a - b)).toEqual([0, 1]);
});

test('progress', async (t) => {
  let progress = 0;

  const job = t.scheduler.addJob('job0', async (_data, { setProgress, flush }) => {
    setProgress(0.3);
    await flush();
    await poll(() => progress === 0.3);

    setProgress(0.6);
    await flush();
    await poll(() => progress === 0.6);

    setProgress(1);
    await flush();
    await poll(() => progress === 1);
  });

  const id = await job.execute();
  job.onProgress(id, (p) => {
    progress = p;
  });
  await poll(() => progress === 1);
  expect(true).toBe(true); // TODO make nicer
});

test('logs', async (t) => {
  const job = t.scheduler.addJob('job0', async (_data, { logger }) => {
    logger.info('foo');
    logger.debug('bar', { baz: 42 }, new Error('something went wrong'));
  });

  const id = await job.execute();
  await job.await(id);
  const entry = await job.getExecution(id);

  expect(entry?.history.map((x) => [x.event, x.level, x.message])).toMatchInlineSnapshot(`
    [
      [
        "start",
        "info",
        null,
      ],
      [
        "log",
        "info",
        "foo",
      ],
      [
        "log",
        "debug",
        "bar {"baz":42} something went wrong",
      ],
      [
        "complete",
        "info",
        null,
      ],
    ]
  `);
});

test('forward logs', async (t) => {
  const log = vi.fn();
  const scheduler = new Scheduler({
    client,
    collection: t.scheduler.collection,
    forwardJobLogs: true,
    log: (level, ...args) => (level === 'debug' ? undefined : log(level, ...args)),
  });

  const job = scheduler.addJob('job0', async (_data, { logger }) => {
    logger.info('info log');
    logger.debug('debug log');
    logger.error('bar', { baz: 42 }, new Error('something went wrong'));
  });

  await job.executeAndAwait();

  expect(log.mock.calls).toMatchInlineSnapshot(`
    [
      [
        "info",
        "[schummar-job/job0]",
        "info log",
      ],
      [
        "error",
        "[schummar-job/job0]",
        "bar",
        {
          "baz": 42,
        },
        [Error: something went wrong],
      ],
    ]
  `);
});

test('watch', async (t) => {
  const invocations = new Array<string>();

  let resolve: (() => void) | undefined,
    firstWatch = false;

  const job = t.scheduler.addJob('job0', async () => {
    if (firstWatch) return;
    return new Promise<void>((r) => {
      resolve = r;
    });
  });

  const id = await job.execute();
  let last: any;
  job.watch(id, (j) => {
    if (j.state !== last) {
      invocations.push(j.state);
      last = j.state;
      firstWatch = true;
      resolve?.();
    }
  });

  await poll(() => deepEqual(invocations, ['planned', 'completed']));
  expect(true).toBe(true); // TODO make nicer
});

test('getPlanned', async (t) => {
  const job = t.scheduler.addJob('job0');
  await job.execute();

  const planned = await job.getPlanned();
  expect(planned.length).toBe(1);
});

test('subscribe to executions', async (t) => {
  const listener = vi.fn();
  t.scheduler.onExecutionUpdate(listener);

  const job = t.scheduler.addJob('job', (_, { setProgress, logger }) => {
    setProgress(0.5);
    logger.info('foo');
  });

  await job.executeAndAwait();
  await sleep(100);

  expect(listener).toHaveBeenCalledWith(
    expect.objectContaining({
      state: 'planned',
    }),
  );

  expect(listener).toHaveBeenLastCalledWith(
    expect.objectContaining({
      state: 'completed',
      progress: 0.5,
      history: expect.toSatisfy((x) => x.length === 3),
    }),
  );
});

test('get executions', async (t) => {
  const job = t.scheduler.addJob('job0', () => {
    return 42;
  });

  await job.executeAndAwait();
  await job.execute(undefined, { delay: 10_000 });

  const executions = await t.scheduler.getExecutions({ jobId: 'job0' });

  expect(executions.length).toBe(2);
  expect(executions[0]).toMatchObject({ state: 'completed', result: 42 });
  expect(executions[1]).toMatchObject({
    state: 'planned',
    nextRun: expect.toSatisfy((x) => new Date(x).getTime() > Date.now()),
  });
});

test('add scheduler later', async (t) => {
  const job = new DistributedJob({
    jobId: 'job0',
    async run() {
      return 42;
    },
  });

  await expect(() => job.executeAndAwait()).rejects.toThrowErrorMatchingInlineSnapshot(
    `[Error: Distributed job has no scheduler or collection defined]`,
  );

  t.scheduler.addJob(job);

  expect(await job.executeAndAwait()).toBe(42);
});

test('a job running longer than lockDuration is not started twice', async (t) => {
  const collection = t.scheduler.collection!;
  const other = new Scheduler({ client, collection, lockDuration: 400, lockCheckInterval: 50, log: () => undefined });
  const scheduler = new Scheduler({ client, collection, lockDuration: 400, lockCheckInterval: 50, log: () => undefined });

  try {
    let starts = 0;
    const run = async () => {
      starts++;
      await sleep(1200);
    };

    const job = scheduler.addJob('job0', run);
    other.addJob('job0', run);

    await job.executeAndAwait();
    expect(starts).toBe(1);
  } finally {
    await scheduler.shutdown();
    await other.shutdown();
  }
});

test('a worker that lost its lock does not overwrite the newer attempt', async (t) => {
  const collection = t.scheduler.collection!;
  let finishStale!: () => void;
  const stale = new Promise<void>((resolve) => (finishStale = resolve));
  let started = false;
  let finished = false;

  const job = t.scheduler.addJob('job0', async () => {
    started = true;
    await stale;
    finished = true;
    return 'stale';
  });

  const id = await job.execute();
  await poll(() => started);

  // Another worker took over after the lock was released and finished the run
  await collection.updateOne({ _id: id }, { $set: { state: 'completed', result: 'fresh', lock: null, lockId: 'other' } });

  finishStale();
  await poll(() => finished);
  await sleep(200);

  expect(await collection.findOne({ _id: id })).toMatchObject({ state: 'completed', result: 'fresh' });
});

test('a failed pick-up is retried', async (t) => {
  const fn = vi.fn();
  const job = t.scheduler.addJob('job0');
  await job.execute();
  // Let the insert's change event pass, so only the failing pick-up below can start the run
  await sleep(300);

  vi.spyOn(t.scheduler.collection!, 'findOneAndUpdate').mockRejectedValueOnce(new Error('network error'));
  job.updateOptions({ run: fn });

  await poll(() => fn.mock.calls.length > 0, 3000);
});

test('a failing lookup for the next run does not cause an unhandled rejection', async (t) => {
  using unhandled = trackUnhandledRejections();
  const fn = vi.fn();

  vi.spyOn(t.scheduler.collection!, 'find').mockImplementationOnce(() => {
    throw new Error('network error');
  });
  const job = t.scheduler.addJob('job0', fn);
  await sleep(200);

  await job.executeAndAwait();
  expect(fn).toHaveBeenCalledTimes(1);
  expect(unhandled.errors).toEqual([]);
});

test('await resolves even if the first lookup fails', async (t) => {
  using unhandled = trackUnhandledRejections();
  const job = t.scheduler.addJob('job0', () => 42);
  const id = await job.execute();
  await waitUntilJob(job, id, (x) => x.state === 'completed');

  vi.spyOn(t.scheduler.collection!, 'findOne').mockRejectedValueOnce(new Error('network error'));

  await expect(job.await(id)).resolves.toBe(42);
  expect(unhandled.errors).toEqual([]);
});

test('a throwing watch callback does not cause an unhandled rejection', async (t) => {
  using unhandled = trackUnhandledRejections();
  const job = t.scheduler.addJob('job0', () => 42);
  const id = await job.execute();

  job.watch(id, () => {
    throw new Error('callback error');
  });

  await job.await(id);
  await sleep(100);
  expect(unhandled.errors).toEqual([]);
});

test('timeout fails a hung run and frees the worker', async (t) => {
  let calls = 0;
  let firstSignal: AbortSignal | undefined;

  const job = t.scheduler.addJob(
    'job0',
    (_data, { signal }) => {
      calls++;
      if (calls === 1) {
        firstSignal = signal;
        return new Promise<number>(() => undefined);
      }
      return 42;
    },
    { timeout: 100, retryCount: 0 },
  );

  await expect(job.executeAndAwait()).rejects.toThrow('Timed out after 100ms');
  expect(firstSignal?.aborted).toBe(true);
  await expect(job.executeAndAwait()).resolves.toBe(42);
});

test('an expired lock counts as a failed attempt', async (t) => {
  const collection = t.scheduler.collection!;
  const expired = {
    jobId: 'job0',
    isScheduled: false,
    state: 'planned' as const,
    nextRun: new Date(0),
    lock: new Date(0),
    lockId: 'dead worker',
    finishedOn: null,
    data: undefined,
    history: [],
  };
  await collection.insertMany([
    { ...expired, _id: 'exhausted', attempt: 1 },
    { ...expired, _id: 'retry', attempt: 0 },
  ]);

  const fn = vi.fn();
  const job = t.scheduler.addJob('job0', fn, { retryCount: 1 });

  await expect(job.await('exhausted')).rejects.toThrow('Lock expired');
  await expect(job.await('retry')).resolves.toBeNull();
  expect(fn).toHaveBeenCalledTimes(1);
  expect(await collection.findOne({ _id: 'retry' })).toMatchObject({ attempt: 1 });
});

test('concurrent flushes do not duplicate history', async (t) => {
  const job = t.scheduler.addJob('job0', async (_data, { logger, flush }) => {
    const flushes = [];
    for (let i = 0; i < 10; i++) {
      logger.info(`${i}`);
      flushes.push(flush());
    }
    await Promise.all(flushes);
  });

  const id = await job.execute();
  await job.await(id);

  const execution = await job.getExecution(id);
  expect(execution?.history.filter((x) => x.event === 'log').map((x) => x.message)).toEqual([
    '0',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
  ]);
});

test('a failed completion write is retried instead of failing the run', async (t) => {
  const collection = t.scheduler.collection!;
  const fn = vi.fn(() => {
    vi.spyOn(collection, 'updateOne').mockRejectedValueOnce(new Error('network error'));
    return 42;
  });
  const job = t.scheduler.addJob('job0', fn, { retryDelay: 0 });

  await expect(job.executeAndAwait()).resolves.toBe(42);
  expect(fn).toHaveBeenCalledTimes(1);
});

test('updates to a running job do not make other instances poll', async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  const observer = new Scheduler({ client, collection, lockDuration: 100, log: () => undefined });

  const run = async (_data: undefined, { setProgress }: { setProgress: (progress: number) => void }) => {
    for (let i = 0; i < 10; i++) {
      setProgress(i);
      await sleep(100);
    }
  };

  try {
    // Whichever instance runs the job, the other one is idle
    observer.addJob('job0', run);
    const job = t.scheduler.addJob('job0', run);
    await sleep(200);

    const pickUps = [vi.spyOn(collection, 'findOneAndUpdate'), vi.spyOn(t.scheduler.collection!, 'findOneAndUpdate')];
    await job.executeAndAwait();

    expect(pickUps[0]!.mock.calls.length + pickUps[1]!.mock.calls.length).toBeLessThan(6);
  } finally {
    await observer.shutdown();
  }
});

test('updateOptions does not start additional background loops', async (t) => {
  const job = t.scheduler.addJob('job0', () => undefined, { lockCheckInterval: 50 });
  for (let i = 0; i < 5; i++) {
    job.updateOptions({ run: () => undefined });
  }

  await sleep(50);
  const lockChecks = vi.spyOn(t.scheduler.collection!, 'updateMany');
  await sleep(500);

  // One loop runs about 10 checks of 2 updates each in that time
  expect(lockChecks.mock.calls.length).toBeLessThan(40);
});

test('failing schedule() calls share one retry timer', async (t) => {
  vi.spyOn(t.scheduler.lockCollection!, 'updateOne').mockRejectedValue(new Error('network error'));
  const timers = vi.spyOn(globalThis, 'setTimeout');

  const job = t.scheduler.addJob('job0', () => undefined, { schedule: { hours: 1 } });
  for (let i = 0; i < 4; i++) {
    await job.schedule();
  }

  expect(timers.mock.calls.filter(([, ms]) => ms === 10_000)).toHaveLength(1);
  timers.mockRestore();
});

test('replacePlanned works in parallel without createIndexes', async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.drop().catch(() => undefined);
  await db
    .collection(`${t.task.name}_locks`)
    .drop()
    .catch(() => undefined);

  const scheduler = new Scheduler({ client, collection, createIndexes: false, log: () => undefined });
  try {
    const job = scheduler.addJob<number>('job0');
    await Promise.all(
      Array(10)
        .fill(0)
        .map((_, i) => job.execute(i, { delay: 10_000, replacePlanned: true })),
    );

    expect(await job.getPlanned()).toHaveLength(1);
  } finally {
    await scheduler.shutdown();
  }
});

test('a failed flush is carried by the next one', async (t) => {
  const collection = t.scheduler.collection!;
  const updateOne = collection.updateOne.bind(collection);

  const job = t.scheduler.addJob('job0', (_data, { logger, flush }) => {
    logger.info('a');
    vi.spyOn(collection, 'updateOne').mockImplementationOnce(async () => {
      await sleep(50);
      throw new Error('network error');
    });
    flush().catch(() => undefined);
  });

  const id = await job.execute();
  await job.await(id);
  vi.mocked(collection.updateOne).mockImplementation(updateOne);

  const execution = await job.getExecution(id);
  expect(execution?.history.map((x) => x.message ?? x.event)).toEqual(['start', 'a', 'complete']);
});

test('an idle run only writes heartbeats at the lockDuration pace', async (t) => {
  const scheduler = new Scheduler({ client, collection: t.scheduler.collection!, lockDuration: 60_000, log: () => undefined });

  try {
    let writes = -1;
    const job = scheduler.addJob('job0', async () => {
      const updateOne = vi.spyOn(scheduler.collection!, 'updateOne');
      await sleep(2500);
      writes = updateOne.mock.calls.length;
    });

    await job.executeAndAwait();
    // Only the buffered 'start' history entry, no heartbeat-only writes
    expect(writes).toBe(1);
  } finally {
    await scheduler.shutdown();
  }
});

test('a scheduled run ended by another instance is rescheduled by the owner', async (t) => {
  const collection = t.scheduler.collection!;
  const owner = new Scheduler({ client, collection, lockDuration: 100, lockCheckInterval: 60_000, log: () => undefined });

  try {
    const ownerJob = owner.addJob('job0', undefined, { schedule: { hours: 1 } });
    const scheduled = await ownerJob.schedule();
    assert(scheduled);

    // The worker has no schedule of its own
    t.scheduler.addJob('job0', () => undefined, { retryCount: 0, lockCheckInterval: 50 });
    await collection.updateOne({ _id: scheduled._id }, { $set: { lock: new Date(0), lockId: 'dead worker' } });

    await poll(async () => {
      const [planned] = await ownerJob.getPlanned();
      return planned && planned._id !== scheduled._id;
    }, 3000);
  } finally {
    await owner.shutdown();
  }
});
