import { loadEnvFile } from 'node:process';
import { defineConfig } from 'vitest/config';

loadEnvFile('.env');

export default defineConfig({
  test: {
    reporters: process.env.CI ? ['dot', 'github-actions', ['junit', { outputFile: 'test-results.xml' }]] : ['default'],
    coverage: {
      reporter: ['text', 'json-summary', 'json'],
    },
  },
});
