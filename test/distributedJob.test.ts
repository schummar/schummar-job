import { deepEqual } from 'fast-equals';
import { MongoClient } from 'mongodb';
import { afterEach, beforeEach, expect, test } from 'vitest';
import { Scheduler } from '../src';
import { poll } from './_helpers';

declare module 'vitest' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

const client = MongoClient.connect('mongodb://localhost', { directConnection: true });
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
  const job = t.scheduler.addJob('job0', () => {
    return 42;
  });

  await job.execute(null, { executionId: 'foo' });
  await job.execute(null, { executionId: 'foo' });

  await expect(job.executeAndAwait(null, { executionId: 'foo' })).resolves.toBe(42);
});

test('progress', async (t) => {
  let progress = 0;

  const job = t.scheduler.addJob('job0', async (_data, { setProgress }) => {
    await setProgress(0.3);
    await poll(() => progress === 0.3);
    await setProgress(0.6);
    await poll(() => progress === 0.6);
    await setProgress(1);
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
  const job = t.scheduler.addJob('job0', async (_data, { log }) => {
    await log('foo');
    await log('bar');
  });

  const id = await job.execute();
  await job.await(id);
  const entry = await job.getExecution(id);
  expect(entry?.logs.length).toBe(2);
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
