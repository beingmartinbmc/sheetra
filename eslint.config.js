import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ["dist/**", "coverage/**", "src/workers/worker-runner.ts"],
  },
  {
    files: ["src/**/*.ts", "test/**/*.ts", "benchmark/**/*.ts"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    ignores: ["src/xls/index.ts"],
    rules: {
      "@typescript-eslint/consistent-type-imports": "error",
    },
  },
);
