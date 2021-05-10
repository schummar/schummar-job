import { ChangeEvent, Collection, Cursor, ObjectID } from 'mongodb';
import { Queue } from 'schummar-queue';
type MaybePromise<T> = T | Promise<T>;

type JobProps<Data> = { data: Data };

type JobDbEntry = {
  _id: ObjectID;
  id: string;
  type: 'schedule' | 'once';
  data: unknown;
  nextRun: Date;
  lock: Date | null;
};

type JobOptions = {
  collection?: MaybePromise<Collection<JobDbEntry>>;
  schedule?: number;
};

type JobExecuteProps = {
  delay?: number;
};

export class Job<Data> {
  static defaultCollection?: MaybePromise<Collection<JobDbEntry>>;

  private col: MaybePromise<Collection<JobDbEntry>>;
  private q = new Queue();

  constructor(
    private id: string,
    private implementation: (props: JobProps<Data>) => void | Promise<void>,
    { collection = Job.defaultCollection, schedule }: JobOptions = {}
  ) {
    if (!collection) throw Error('Missing collection!');
    this.col = collection;
    this.next();
  }

  async execute(data: Data, { delay = 0 }: JobExecuteProps) {
    const col = await this.col;
    await col.insertOne({ id: this.id, type: 'once', data, nextRun: new Date(Date.now() + delay), lock: null });
  }

  private async watch() {
    const col = await this.col;
    for await (const _x of col.watch().stream() as Cursor<ChangeEvent<JobDbEntry>>) {
      this.next();
    }
  }

  private next() {
    this.q.clear();
    this.q.schedule(async () => {
      try {
        const col = await this.col;
        const now = new Date();

        const { value } = await col.findOneAndUpdate(
          {
            id: this.id,
            nextRun: { $lte: now },
            lock: null,
          },
          {
            $set: { lock: now },
          }
        );

        if (value) {
          await this.implementation({ data: value.data as Data });
          await col.deleteOne({ _id: value._id });
        } else {
          console.warn('none');

          const [next] = await col.find({ id: this.id }).sort({ nextRun: 1 }).limit(1).toArray();
          if (next) setTimeout(() => this.next(), next.nextRun.getTime() - Date.now());
        }
      } catch (e) {
        console.error('next failed', e);
      }
    });
  }
}
