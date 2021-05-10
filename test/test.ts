import test from 'ava';
import { MongoClient } from 'mongodb';
import { Job } from '../src';

test('foo', async (t) => {
  t.plan(1);

  const client = await MongoClient.connect('mongodb://localhost', { useUnifiedTopology: true });

  Job.defaultCollection = client.db('dev').collection('jobs');

  const job = new Job<{ foo: number }>('job0', ({ data }) => {
    console.log('job!!!', data.foo);
    t.pass();
  });

  await job.execute({ foo: 42 }, {});
  await new Promise((r) => setTimeout(r, 1000));
});
