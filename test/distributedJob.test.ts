import { deepEqual } from 'fast-equals';
import { MongoClient } from 'mongodb';
import { afterEach, assert, beforeEach, expect, test, vi, vitest } from 'vitest';
import { JobDbEntry, Scheduler } from '../src';
import { sleep } from '../src/helpers';
import { poll, waitUntilJob } from './_helpers';

declare module 'vitest' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

const client = new MongoClient(import.meta.env.VITE_MONGODB_CONNECTION || 'mongodb://localhost/?directConnection=true');
const db = client.db('schummar-job-tests');

beforeEach(async (t) => {
  const collection = db.collection<JobDbEntry<any, any, any>>(t.task.name);
  await collection.deleteMany({});
  t.scheduler = new Scheduler(collection, { lockDuration: 100, log: () => undefined });
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

test('repeated error in scheduled job', async (t) => {
  const job = t.scheduler.addJob(
    'job0',
    () => {
      throw Error('testerror');
    },
    { schedule: { milliseconds: 100 }, retryDelay: 100, retryCount: 2 },
  );
  job.options = { ...job.options, schedule: { hours: 1 } }; // prevent further scheduling

  const id = (await job.schedule())?._id;
  assert(id);

  await expect(waitUntilJob(job, id, (j) => j.state === 'error' && j.attempt === 2, 5000)).resolves.toBeUndefined();

  const [plannedJob] = await job.getPlanned();
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

  Array(5)
    .fill(0)
    .map(() => job.schedule());

  const planned = await job.getPlanned();
  expect(planned.length).toBe(1);
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

  const newScheduler = new Scheduler(t.scheduler.collection, { lockDuration: 100 });
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

  await job.execute(0);
  await job.execute(1);
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

test('progress', async (t) => {
  let progress = 0;

  const job = t.scheduler.addJob('job0', async (_data, { setProgress, flush }) => {
    setProgress(0.3);
    flush();
    await poll(() => progress === 0.3);

    setProgress(0.6);
    flush();
    await poll(() => progress === 0.6);

    setProgress(1);
    flush();
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
  const scheduler = new Scheduler(t.scheduler.collection, {
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
  expect(executions[1]).toMatchObject({ state: 'planned', nextRun: expect.toSatisfy((x) => new Date(x).getTime() > Date.now()) });
});
