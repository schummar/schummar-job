import { loadEnvFile } from 'node:process';
import { defineConfig } from 'vite-plus';

try {
  loadEnvFile('.env');
} catch {
  // ignore
}

export default defineConfig({
  staged: {
    '*': 'vp check --fix',
  },

  test: {
    reporters: process.env.CI ? ['dot', 'github-actions', ['junit', { outputFile: 'test-results.xml' }]] : ['default'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json-summary', 'json'],
    },
    globalSetup: './test/_setup.ts',
  },

  pack: {
    entry: 'src/index.ts',
    dts: true,
    format: ['cjs', 'esm'],
    sourcemap: true,
    exports: true,
    attw: true,
    publint: true,
    deps: {
      onlyBundle: false,
    },
  },

  fmt: {
    printWidth: 140,
    singleQuote: true,
    sortImports: {
      groups: [],
    },
    sortPackageJson: true,
  },

  lint: {
    options: {
      typeAware: true,
    },
    rules: {
      '@typescript-eslint/unbound-method': 'off',
    },
  },

  run: {
    cache: {
      scripts: true,
    },
  },
});
