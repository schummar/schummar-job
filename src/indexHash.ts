import type { IndexDescription } from 'mongodb';
import { hasher } from 'node-object-hash';

export function indexHash(index: IndexDescription): string {
  if (index.name === '_id_') {
    return '_id_';
  }

  const { key, ...rest } = index;
  return JSON.stringify(key) + objectHash(rest);
}

const hash = hasher({ coerce: false });

export default function objectHash(obj: unknown): string {
  return hash.sort(obj);
}
