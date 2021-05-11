import test from 'ava';
import { MongoClient } from 'mongodb';
import { Job, Scheduler } from '../src';
import { countdown } from './_helpers';

const client = MongoClient.connect('mongodb://localhost', { useUnifiedTopology: true });
const collection = client.then((client) => client.db('dev').collection('jobs'));
let scheduler = new Scheduler(collection, { lockDuration: 100 });

test.before(async () => {
  await scheduler.clearDB();
});

test.afterEach.always(async () => {
  await scheduler.clearDB();
  scheduler.clearJobs();
});

test.serial('simple', async (t) => {
  await countdown(1, async (count) => {
    const job = new Job(scheduler, 'job0', ({ x }: { x: number }) => {
      t.is(x, 42);
      count();
    });

    await job.execute({ x: 42 });
  });
});

test.serial('error once', async (t) => {
  await countdown(2, async (count) => {
    const job = new Job(
      scheduler,
      'job0',
      (_data, { tryCount, error }) => {
        count();
        if (tryCount === 0) throw Error('testerror');
        t.is(error, 'testerror');
        t.is(tryCount, 1);
      },
      { retryDelay: 0 }
    );

    await job.execute(undefined);
  });
});

test.serial('error multiple', async (t) => {
  await countdown(3, async (count) => {
    const job = new Job(
      scheduler,
      'job0',
      () => {
        count();
        throw Error('testerror');
      },
      { retryDelay: 0, retryCount: 2 }
    );

    await job.execute(undefined);
  });

  t.pass();
});

test.serial('multiple workers', async (t) => {
  await countdown(5, async (count) => {
    const props = [
      scheduler,
      'job0',
      () => {
        count();
      },
    ] as const;

    const job = new Job(...props);
    new Job(...props);
    new Job(...props);

    await job.execute(undefined);
    await job.execute(undefined);
    await job.execute(undefined);
    await job.execute(undefined);
    await job.execute(undefined);
  });

  t.pass();
});

test.serial('schedule', async (t) => {
  await countdown(2, async (count) => {
    const job = new Job(
      scheduler,
      'job0',
      () => {
        count();
      },
      { schedule: { data: null, interval: 10 } }
    );

    await job.execute(null);
  });

  t.pass();
});

test.serial('restart', async (t) => {
  await countdown(1, async (count) => {
    let job = new Job(scheduler, 'job0', () => {
      t.fail();
    });

    await job.execute(undefined);
    scheduler.shutdown();
    scheduler = new Scheduler(collection, { lockDuration: 100 });

    job = new Job(scheduler, 'job0', () => {
      count();
    });
  });

  t.pass();
});
