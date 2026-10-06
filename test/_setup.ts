import { MongoDBAtlasLocalContainer } from '@testcontainers/mongodb';
import type { TestProject } from 'vite-plus/test/node';

declare module 'vite-plus/test' {
  export interface ProvidedContext {
    mongo: {
      connectionString: string;
    };
  }
}

export default async function setup({ provide }: TestProject) {
  const mongo = await new MongoDBAtlasLocalContainer('mongodb/mongodb-atlas-local:8.3.4').start();

  provide('mongo', {
    connectionString: mongo.getConnectionString(),
  });

  return async function teardown() {
    await mongo.stop();
  };
}
