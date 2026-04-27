import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    coverage: {
      provider: "v8",
      thresholds: {
        statements: 95,
        functions: 95,
        lines: 95,
        branches: 80,
      },
    },
  },
});
