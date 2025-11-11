import { deepEqual } from 'fast-equals';
import { MongoClient } from 'mongodb';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';
import { Scheduler } from '../src';
import { sleep } from '../src/helpers';
import { poll } from './_helpers';

declare module 'vitest' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

const client = MongoClient.connect(import.meta.env.VITE_MONGODB_CONNECTION || 'mongodb://localhost', { directConnection: true });
const db = client.then((client) => client.db('schummar-job-tests'));

beforeEach(async (t) => {
  t.scheduler = new Scheduler((await db).collection(t.task.name), { lockDuration: 100, log: () => undefined });
  await t.scheduler.clearDB();
});

afterEach(async (t) => {
  await t.scheduler.shutdown();
  await t.scheduler.clearDB();
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
  expect.assertions(4);

  const job = t.scheduler.addJob(
    'job0',
    () => {
      expect(true).toBe(true); // TODO make nicer
      throw Error('testerror');
    },
    { retryDelay: 0, retryCount: 2 },
  );

  await expect(job.executeAndAwait()).rejects.toThrow('testerror');
});

test('multiple workers', async (t) => {
  expect.assertions(5);

  const props = [
    'job0',
    () => {
      expect(true).toBe(true); // TODO make nicer
    },
  ] as const;

  const job = t.scheduler.addJob(...props);
  t.scheduler.addJob(...props);
  t.scheduler.addJob(...props);

  await Promise.all(
    Array(5)
      .fill(0)
      .map(() => job.executeAndAwait()),
  );
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

test('replacePlanned', async (t) => {
  const fn = vi.fn(() => {
    return 42;
  });
  const job = t.scheduler.addJob('job0', fn);

  await job.execute();
  await job.execute();
  const result = await job.executeAndAwait(undefined, { replacePlanned: true });

  expect(result).toBe(42);
  expect(fn.mock.calls.length).toBeLessThanOrEqual(2);
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
  console.log(id);
  await job.await(id);
  console.log('done');
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
