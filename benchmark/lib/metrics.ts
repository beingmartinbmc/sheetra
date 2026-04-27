import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { PerformanceObserver, performance } from "node:perf_hooks";
import { formatBytes } from "../../src/index.js";

export interface MemorySample {
  elapsedMs: number;
  rssBytes: number;
  heapUsedBytes: number;
  externalBytes: number;
}

export interface BenchmarkResult {
  suite: string;
  name: string;
  mode: string;
  format: "csv" | "xlsx" | "mixed" | "memory";
  rows: number;
  columns: number;
  fileSizeBytes?: number;
  timeMs: number;
  rowsPerSecond: number;
  startRssBytes: number;
  endRssBytes: number;
  peakRssBytes: number;
  peakHeapBytes: number;
  gcCount: number;
  gcTimeMs: number;
  success: boolean;
  notes?: string;
  samples: MemorySample[];
}

export interface BenchmarkCase {
  suite: string;
  name: string;
  mode: string;
  format: BenchmarkResult["format"];
  rows: number;
  columns: number;
  fileSizeBytes?: number;
  notes?: string;
  run: () => Promise<number | void>;
}

export interface MeasureOptions {
  sampleEveryMs?: number;
}

export async function measureCase(testCase: BenchmarkCase, options: MeasureOptions = {}): Promise<BenchmarkResult> {
  const sampleEveryMs = options.sampleEveryMs ?? 100;
  const started = performance.now();
  const startMemory = process.memoryUsage();
  const samples: MemorySample[] = [];
  let peakRssBytes = startMemory.rss;
  let peakHeapBytes = startMemory.heapUsed;
  let gcCount = 0;
  let gcTimeMs = 0;
  let success = true;
  let notes = testCase.notes;

  const observer = new PerformanceObserver((items) => {
    for (const item of items.getEntries()) {
      gcCount += 1;
      gcTimeMs += item.duration;
    }
  });

  try {
    observer.observe({ entryTypes: ["gc"], buffered: false });
  } catch {
    notes = appendNote(notes, "GC observer unavailable on this Node runtime");
  }

  const sampler = setInterval(() => {
    const memory = process.memoryUsage();
    peakRssBytes = Math.max(peakRssBytes, memory.rss);
    peakHeapBytes = Math.max(peakHeapBytes, memory.heapUsed);
    samples.push({
      elapsedMs: Math.round(performance.now() - started),
      rssBytes: memory.rss,
      heapUsedBytes: memory.heapUsed,
      externalBytes: memory.external,
    });
  }, sampleEveryMs);
  sampler.unref();

  let processedRows = testCase.rows;
  try {
    if (global.gc !== undefined) global.gc();
    const reportedRows = await testCase.run();
    if (typeof reportedRows === "number") processedRows = reportedRows;
  } catch (error) {
    success = false;
    notes = appendNote(notes, error instanceof Error ? error.message : "unknown benchmark failure");
  } finally {
    clearInterval(sampler);
    observer.disconnect();
  }

  const ended = performance.now();
  const endMemory = process.memoryUsage();
  peakRssBytes = Math.max(peakRssBytes, endMemory.rss);
  peakHeapBytes = Math.max(peakHeapBytes, endMemory.heapUsed);

  const timeMs = Math.round(ended - started);
  return {
    suite: testCase.suite,
    name: testCase.name,
    mode: testCase.mode,
    format: testCase.format,
    rows: processedRows,
    columns: testCase.columns,
    fileSizeBytes: testCase.fileSizeBytes,
    timeMs,
    rowsPerSecond: timeMs === 0 ? processedRows : Math.round((processedRows / timeMs) * 1000),
    startRssBytes: startMemory.rss,
    endRssBytes: endMemory.rss,
    peakRssBytes,
    peakHeapBytes,
    gcCount,
    gcTimeMs: Math.round(gcTimeMs),
    success,
    notes,
    samples,
  };
}

export function resultForConsole(result: BenchmarkResult): Record<string, string | number | boolean> {
  return {
    suite: result.suite,
    name: result.name,
    mode: result.mode,
    format: result.format,
    rows: result.rows,
    columns: result.columns,
    timeMs: result.timeMs,
    rowsPerSecond: result.rowsPerSecond,
    peakMemory: formatBytes(result.peakRssBytes),
    heap: formatBytes(result.peakHeapBytes),
    gcMs: result.gcTimeMs,
    success: result.success,
    notes: result.notes ?? "",
  };
}

export async function writeJson(path: string, results: BenchmarkResult[]): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(results, null, 2)}\n`);
}

export async function writeMarkdown(path: string, results: BenchmarkResult[]): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const rows = results
    .map(
      (result) =>
        `| ${result.suite} | ${result.name} | ${result.mode} | ${result.format} | ${result.rows.toLocaleString()} | ${result.columns} | ${result.timeMs.toLocaleString()}ms | ${result.rowsPerSecond.toLocaleString()} | ${formatBytes(result.peakRssBytes)} | ${result.gcTimeMs}ms | ${result.success ? "yes" : "no"} | ${result.notes ?? ""} |`,
    )
    .join("\n");

  await writeFile(
    path,
    `# Strong Benchmark Results\n\n| Suite | Case | Mode | Format | Rows | Columns | Time | Rows/sec | Peak RSS | GC time | Success | Notes |\n| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n${rows}\n`,
  );
}

function appendNote(current: string | undefined, next: string): string {
  return current === undefined || current.length === 0 ? next : `${current}; ${next}`;
}
