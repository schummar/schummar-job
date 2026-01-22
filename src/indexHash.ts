import type { IndexDescriptionInfo } from 'mongodb';
import { hasher } from 'node-object-hash';

export function indexHash(index: IndexDescriptionInfo): string {
  if (index.name === '_id_') {
    return '_id_';
  }

  const { key, name: _name, v: _v, ...rest } = index;

  return (
    JSON.stringify(key) +
    objectHash({
      partialFilterExpression: rest.partialFilterExpression,
    })
  );
}

const hash = hasher({ coerce: false });

export default function objectHash(obj: unknown): string {
  return hash.sort(obj);
}
