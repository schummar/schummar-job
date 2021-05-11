import test from 'ava';
import { MongoClient } from 'mongodb';
import { Job, Scheduler } from '../src';

test('foo', async (t) => {
  t.plan(6);

  const client = await MongoClient.connect('mongodb://localhost', { useUnifiedTopology: true });
  const scheduler = new Scheduler(client.db('dev').collection('jobs'), { lockDuration: 100 });
  await scheduler.reset();

  const props = (name: string) =>
    [
      scheduler,
      'job0',
      ({ foo }: { foo: number }) => {
        console.log(name, foo);
        t.pass();
      },
      { schedule: { data: { foo: 43 }, interval: 1000 } },
    ] as const;

  const job = new Job(...props('worker0'));
  new Job(...props('worker1'));
  new Job(...props('worker2'));

  await job.execute({ foo: 42 }, {});
  await job.execute({ foo: 42 }, {});
  await job.execute({ foo: 42 }, {});

  await new Promise((r) => setTimeout(r, 3500));
});
