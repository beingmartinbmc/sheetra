import { performance } from "node:perf_hooks";
import { read, write, formatBytes } from "../src/index.js";

const rowCount = Number(process.env.PRAVAAH_BENCH_ROWS ?? 100_000);
const rows = Array.from({ length: rowCount }, (_, index) => ({
  id: index + 1,
  a: index,
  b: index * 2,
  email: `user${index}@example.com`,
}));

const started = performance.now();
const stats = await write(read(rows).map((row) => ({ ...row, total: Number(row.a) + Number(row.b) })), "/tmp/pravaah-bench.csv", {
  format: "csv",
});
const ended = performance.now();

console.log({
  rowsProcessed: stats.rowsProcessed,
  rowsWritten: stats.rowsWritten,
  timeMs: Math.round(ended - started),
  peakMemory: formatBytes(stats.peakRssBytes),
  destination: "/tmp/pravaah-bench.csv",
});
