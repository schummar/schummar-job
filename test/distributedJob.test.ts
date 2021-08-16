import test from 'ava';
import { MongoClient } from 'mongodb';
import { Scheduler } from '../src';
import { sleep } from '../src/helpers';
import { countdown } from './_helpers';

const client = MongoClient.connect('mongodb://localhost', { useUnifiedTopology: true });
const collection = client.then((client) => client.db('dev').collection('jobs'));
let scheduler = new Scheduler(collection, { lockDuration: 100 });

test.before(async () => {
  await scheduler.clearDB();
});

test.afterEach.always(async () => {
  await scheduler.clearDB();
  await scheduler.clearJobs();
});

test.serial('simple', async (t) => {
  await countdown(1, async (count) => {
    const job = scheduler.addJob('job0', ({ x }: { x: number }) => {
      t.is(x, 42);
      count();
    });

    await job.execute({ x: 42 });
  });
});

test.serial('error once', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addJob(
      'job0',
      (_data, { attempt, error }) => {
        count();
        if (attempt === 0) throw Error('testerror');
        t.is(error, 'testerror');
        t.is(attempt, 1);
      },
      { retryDelay: 0 }
    );

    await job.execute();
  });
});

test.serial('error multiple', async (t) => {
  await countdown(3, async (count) => {
    const job = scheduler.addJob(
      'job0',
      () => {
        count();
        throw Error('testerror');
      },
      { retryDelay: 0, retryCount: 2 }
    );

    await job.execute();
  });

  t.pass();
});

test.serial('multiple workers', async (t) => {
  await countdown(5, async (count) => {
    const props = [
      'job0',
      () => {
        count();
      },
    ] as const;

    const job = scheduler.addJob(...props);
    scheduler.addJob(...props);
    scheduler.addJob(...props);

    await job.execute();
    await job.execute();
    await job.execute();
    await job.execute();
    await job.execute();
  });

  t.pass();
});

test.serial('schedule', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addJob(
      'job0',
      () => {
        count();
      },
      { schedule: { milliseconds: 10 } }
    );

    await job.execute();
  });

  t.pass();
});

test.serial('schedule with data', async (t) => {
  await countdown(2, async (count) => {
    const job = scheduler.addJob(
      'job0',
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

test.serial('restart', async (t) => {
  await countdown(1, async (count) => {
    const job = scheduler.addJob('job0', () => {
      t.fail();
    });

    await scheduler.shutdown();
    await job.execute(undefined);
    scheduler = new Scheduler(collection, { lockDuration: 100 });

    scheduler.addJob('job0', () => {
      count();
    });
  });

  t.pass();
});

test.serial('null implementation', async (t) => {
  await countdown(1, async (count) => {
    const job = scheduler.addJob('job0', null);
    await job.execute(undefined);

    scheduler.addJob('job0', () => {
      count();
    });
  });

  t.pass();
});

test.serial('executionId', async (t) => {
  await countdown(
    2,
    async (count) => {
      const job = scheduler.addJob('job0', count);

      await job.execute(undefined, { executionId: 'foo' });
      await job.execute(undefined, { executionId: 'foo' });

      await sleep(500);
      await job.execute(undefined, { executionId: 'foo' });
    },
    1000,
    true
  );

  t.pass();
});
