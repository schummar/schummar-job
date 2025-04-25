import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    reporters: process.env.CI ? ['dot', 'github-actions', ['junit', { outputFile: 'test-results.xml' }]] : ['default'],
    coverage: {
      reporter: ['text', 'json-summary', 'json'],
    },
  },
});
