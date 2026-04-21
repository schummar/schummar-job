import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: 'src/index.ts',
  dts: true,
  format: ['cjs', 'esm'],
  sourcemap: true,
  exports: true,
  attw: true,
  publint: true,
});
