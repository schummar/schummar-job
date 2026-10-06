import { Scheduler } from '../src';
import { sleep } from '../src/helpers';
import { noopLogger, poll, trackUnhandledRejections } from './_helpers';
import { afterEach, beforeEach, expect, test, vi } from 'vite-plus/test';

declare module 'vite-plus/test' {
  export interface TestContext {
    scheduler: Scheduler;
  }
}

beforeEach((t) => {
  t.scheduler = new Scheduler({ log: noopLogger });
});

afterEach(async (t) => {
  await t.scheduler.clearJobs();
  vi.useRealTimers();
});

test('simple', async (t) => {
  expect.assertions(1);

  const job = t.scheduler.addLocalJob(({ x }: { x: number }) => {
    expect(x).toBe(42);
  });

  await job.execute({ x: 42 });
});

test('return value', async (t) => {
  expect.assertions(2);

  const job = t.scheduler.addLocalJob(({ x }: { x: number }) => {
    expect(x).toBe(42);
    return x + 1;
  });

  await expect(job.execute({ x: 42 })).resolves.toBe(43);
});

test('error once', async (t) => {
  const job = t.scheduler.addLocalJob(
    (_data, { attempt, error }) => {
      if (attempt === 0) throw 'testerror';
      expect(error).toBe('testerror');
      expect(attempt).toBe(1);
    },
    { retryDelay: 0 },
  );

  await job.execute();
});

test('error multiple', async (t) => {
  expect.assertions(4);

  const job = t.scheduler.addLocalJob(
    () => {
      expect(true).toBe(true); // TODO make nicer
      throw Error('testerror');
    },
    { retryDelay: 0, retryCount: 2 },
  );

  await expect(job.execute()).rejects.toThrow('testerror');
});

test('schedule', async (t) => {
  vi.useFakeTimers();
  const fn = vi.fn();

  t.scheduler.addLocalJob(fn, { schedule: { milliseconds: 1, seconds: 1, minutes: 1, hours: 1, days: 1 } });
  await vi.advanceTimersByTimeAsync(2 * (24 * 60 * 60 * 1000 + 60 * 60 * 1000 + 60 * 1000 + 1000) + 2);
  expect(fn).toHaveBeenCalledTimes(2);
});

test('schedule with data', async (t) => {
  vi.useFakeTimers();
  const fn = vi.fn((x: number) => expect(x).toBe(42));

  t.scheduler.addLocalJob(fn, { schedule: { milliseconds: 10, data: 42 } });

  await vi.advanceTimersByTimeAsync(20 + 2);
  expect(fn).toHaveBeenCalledTimes(2);
});

test('executionId', async (t) => {
  expect.assertions(2);

  const job = t.scheduler.addLocalJob(() => {
    expect(true).toBe(true); // TODO make nicer
  });

  const j0 = job.execute(undefined, { executionId: 'foo' });
  const j1 = job.execute(undefined, { executionId: 'foo' });

  await Promise.all([j0, j1]);
  await job.execute(undefined, { executionId: 'foo' });
});

test('delay', async (t) => {
  const job = t.scheduler.addLocalJob(() => Date.now());

  const start = Date.now();
  const end = await job.execute(undefined, { delay: 100 });

  expect(end - start).toBeGreaterThanOrEqual(95);
});

test('updateOptions', async (t) => {
  const job = t.scheduler.addLocalJob(() => 'before');
  job.updateOptions({ run: () => 'after' });

  await expect(job.execute()).resolves.toBe('after');
});

test('shutdown cancels pending executions', async (t) => {
  const fn = vi.fn();
  const job = t.scheduler.addLocalJob(fn);

  const promise = job.execute(undefined, { delay: 1000 });
  await job.shutdown();

  await expect(promise).rejects.toBeTypeOf('symbol');
  expect(fn).not.toHaveBeenCalled();
});

test('a failing scheduled job keeps its schedule', async (t) => {
  using unhandled = trackUnhandledRejections();
  const fn = vi.fn(() => {
    throw new Error('job error');
  });

  t.scheduler.addLocalJob(fn, { schedule: { milliseconds: 10 }, retryCount: 0 });

  await poll(() => fn.mock.calls.length >= 3, 1000);
  expect(unhandled.errors).toEqual([]);
});

test('an invalid schedule is logged, not thrown', async () => {
  using unhandled = trackUnhandledRejections();
  const log = vi.fn();
  const scheduler = new Scheduler({ log });

  scheduler.addLocalJob(() => undefined, { schedule: { cron: 'invalid' } });
  await sleep(50);

  expect(log).toHaveBeenCalledWith('error', 'Error in job schedule:', expect.anything());
  expect(unhandled.errors).toEqual([]);
});

test('executionId deduplicates while running', async (t) => {
  const fn = vi.fn(() => sleep(100));
  const job = t.scheduler.addLocalJob(fn);

  const first = job.execute(undefined, { executionId: 'x' });
  const second = job.execute(undefined, { executionId: 'x' });
  await sleep(10);
  const third = job.execute(undefined, { executionId: 'x' });
  await Promise.all([first, second, third]);

  expect(fn).toHaveBeenCalledTimes(1);
});
