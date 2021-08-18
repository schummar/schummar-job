import anyTest, { TestInterface } from 'ava';
import { Scheduler } from '../src';
import { noopLogger, poll } from './_helpers';

const test = anyTest as TestInterface<Scheduler>;

test.beforeEach((t) => {
  t.context = new Scheduler(undefined, { log: noopLogger });
});

test.afterEach.always(async (t) => {
  await t.context.clearJobs();
});

test('simple', async (t) => {
  t.plan(1);

  const job = t.context.addLocalJob(({ x }: { x: number }) => {
    t.is(x, 42);
  });

  await job.execute({ x: 42 });
});

test('return value', async (t) => {
  t.plan(2);

  const job = t.context.addLocalJob(({ x }: { x: number }) => {
    t.is(x, 42);
    return x + 1;
  });

  t.is(await job.execute({ x: 42 }), 43);
});

test('error once', async (t) => {
  const job = t.context.addLocalJob(
    (_data, { attempt, error }) => {
      if (attempt === 0) throw 'testerror';
      t.is(error, 'testerror');
      t.is(attempt, 1);
    },
    { retryDelay: 0 }
  );

  await job.execute();
});

test('error multiple', async (t) => {
  t.plan(4);

  const job = t.context.addLocalJob(
    () => {
      t.pass();
      throw Error('testerror');
    },
    { retryDelay: 0, retryCount: 2 }
  );

  await t.throwsAsync(() => job.execute());
});

test('schedule', async (t) => {
  let count = 0;

  t.context.addLocalJob(
    () => {
      count++;
    },
    { schedule: { milliseconds: 10 } }
  );

  await poll(() => count >= 2);
  t.pass();
});

test('schedule with data', async (t) => {
  let count = 0;

  t.context.addLocalJob(
    (x: number) => {
      t.is(x, 42);
      count++;
    },
    { schedule: { milliseconds: 10, data: 42 } }
  );

  await poll(() => count >= 2);
  t.pass();
});

test.serial('executionId', async (t) => {
  t.plan(2);

  const job = t.context.addLocalJob(() => {
    t.pass();
  });

  const j0 = job.execute(null, { executionId: 'foo' });
  const j1 = job.execute(null, { executionId: 'foo' });

  await Promise.all([j0, j1]);
  await job.execute(null, { executionId: 'foo' });
});
