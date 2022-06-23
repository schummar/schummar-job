import anyTest, { TestInterface } from 'ava';
import { MongoClient } from 'mongodb';
import { Scheduler } from '../src';
import { poll } from './_helpers';

const test = anyTest as TestInterface<Scheduler>;

const client = MongoClient.connect('mongodb://localhost', { directConnection: true });
const db = client.then((client) => client.db('schummar-job-tests'));

test.beforeEach(async (t) => {
  t.context = new Scheduler((await db).collection(t.title), { lockDuration: 100, log: () => undefined });
  await t.context.clearDB();
});

test.afterEach.always(async (t) => {
  await t.context.shutdown();
  await t.context.clearDB();
});

test('simple', async (t) => {
  t.plan(1);

  const job = t.context.addJob('job0', ({ x }: { x: number }) => {
    t.is(x, 42);
  });

  await job.executeAndAwait({ x: 42 });
});

test('return value', async (t) => {
  const job = t.context.addJob('job0', ({ x }: { x: number }) => {
    t.is(x, 42);
    return x + 1;
  });

  t.is(await job.executeAndAwait({ x: 42 }), 43);
});

test('error once', async (t) => {
  t.plan(2);

  const job = t.context.addJob(
    'job0',
    (_data, { job }) => {
      if (job.attempt === 0) {
        t.pass();
        throw Error('testerror');
      }
      t.is(job.attempt, 1);
    },
    { retryDelay: 0 }
  );

  await job.executeAndAwait();
});

test('error multiple', async (t) => {
  t.plan(4);

  const job = t.context.addJob(
    'job0',
    () => {
      t.pass();
      throw Error('testerror');
    },
    { retryDelay: 0, retryCount: 2 }
  );

  await t.throwsAsync(() => job.executeAndAwait());
});

test('multiple workers', async (t) => {
  t.plan(5);

  const props = [
    'job0',
    () => {
      t.pass();
    },
  ] as const;

  const job = t.context.addJob(...props);
  t.context.addJob(...props);
  t.context.addJob(...props);

  await Promise.all(
    Array(5)
      .fill(0)
      .map(() => job.executeAndAwait())
  );
});

test('schedule', async (t) => {
  let count = 0;

  t.context.addJob(
    'job0',
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

  t.context.addJob(
    'job0',
    (x: number) => {
      t.is(x, 42);
      count++;
    },
    { schedule: { milliseconds: 10, data: 42 } }
  );

  await poll(() => count >= 2);
  t.pass();
});

test('restart', async (t) => {
  t.plan(1);

  const job = t.context.addJob('job0', () => {
    t.fail();
  });

  await t.context.shutdown();
  const id = await job.execute();

  const newScheduler = new Scheduler(t.context.collection, { lockDuration: 100 });
  const newJob = newScheduler.addJob('job0', () => {
    t.pass();
  });

  await newJob.await(id);
});

test('null implementation', async (t) => {
  t.plan(1);

  const job = t.context.addJob('job0');
  const id = await job.execute();

  t.context.addJob('job0', () => {
    t.pass();
  });
  await job.await(id);
});

test('executionId', async (t) => {
  const job = t.context.addJob('job0', () => {
    return 42;
  });

  await job.execute(null, { executionId: 'foo' });
  await job.execute(null, { executionId: 'foo' });

  t.is(await job.executeAndAwait(null, { executionId: 'foo' }), 42);
});

test('progress', async (t) => {
  let progress = 0;

  const job = t.context.addJob('job0', async (_data, { setProgress }) => {
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
  t.pass();
});

test('watch', async (t) => {
  const invocations: any[] = [];

  const job = t.context.addJob('job0', async () => {
    return 1;
  });

  const id = await job.execute();
  let last: any;
  job.watch(id, (j) => {
    if (j.state !== last) {
      invocations.push(j.state);
      last = j.state;
    }
  });
  await job.await(id);
  t.deepEqual(invocations, ['planned', 'completed']);
});

test('getPlanned', async (t) => {
  const job = t.context.addJob('job0');
  await job.execute();

  const planned = await job.getPlanned();
  t.is(planned.length, 1);
});
