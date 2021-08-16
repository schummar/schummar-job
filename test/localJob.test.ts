import test from 'ava';
import { Scheduler } from '../src';
import { countdown, noopLogger } from './_helpers';

const scheduler = new Scheduler(undefined, { log: noopLogger });

test('simple', async (t) => {
  await countdown(1, async (count) => {
    const job = scheduler.addLocalJob(({ x }: { x: number }) => {
      t.is(x, 42);
      count();
    });

    await job.execute({ x: 42 });
  });
});

test('error once', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addLocalJob(
      (_data, { attempt, error }) => {
        count();

        if (attempt === 0) throw 'testerror';
        t.is(error, 'testerror');
        t.is(attempt, 1);
      },
      { retryDelay: 0 }
    );

    await job.execute();
  });
});

test('error multiple', async (t) => {
  await countdown(3, async (count) => {
    const job = scheduler.addLocalJob(
      () => {
        count();
        throw Error('testerror');
      },
      { retryDelay: 0, retryCount: 2 }
    );

    await job.execute().catch(() => {
      // ignore
    });
  });

  t.pass();
});

test('schedule', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addLocalJob(
      () => {
        count();
      },
      { schedule: { milliseconds: 10 } }
    );

    await job.execute();
  });

  t.pass();
});

test('schedule with data', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addLocalJob(
      (x: number) => {
        t.is(x, 42);
        count();
      },
      { schedule: { milliseconds: 10, data: 42 } }
    );

    await job.execute(42);
  });

  t.pass();
});

test.serial('executionId', async (t) => {
  await countdown(
    2,
    async (count) => {
      const job = scheduler.addLocalJob(count);

      const j0 = job.execute(null, { executionId: 'foo' });
      const j1 = job.execute(null, { executionId: 'foo' });

      await Promise.all([j0, j1]);
      await job.execute(null, { executionId: 'foo' });
    },
    1000,
    true
  );

  t.pass();
});
