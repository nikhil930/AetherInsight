import { defineConfig } from 'vitest/config';

// Serialize test files. Multiple files touch a shared Redis DB and call
// flushdb() in beforeEach — running them in parallel causes cross-file
// races (one file's flush wipes another file's setup). This makes tests
// deterministic at the cost of ~1s wall time on the current suite.
export default defineConfig({
  test: {
    fileParallelism: false,
    setupFiles: ['./test/setup.js'],
  },
});
