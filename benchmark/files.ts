import { createReadStream } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import { basename, extname, join } from "node:path";
import { performance } from "node:perf_hooks";
import { parse as parseCsv } from "@fast-csv/parse";
import { formatBytes, read } from "../src/index.js";

interface BenchmarkRow {
  file: string;
  size: string;
  engine: string;
  rows: number | string;
  timeMs: number | string;
  peakMemory: string;
  notes: string;
}

const fixtureDir = join(process.cwd(), "benchmark", "files");
const selectedFile = process.env.SHEETRA_BENCH_FILE;
const rowLimit = Number(process.env.SHEETRA_BENCH_LIMIT ?? 0);
const includeMemoryHeavy = process.env.SHEETRA_BENCH_INCLUDE_MEMORY === "1";
const memoryHeavyLimitBytes = Number(process.env.SHEETRA_BENCH_MEMORY_LIMIT_MB ?? 50) * 1024 * 1024;

const files = await discoverFiles();
const results: BenchmarkRow[] = [];

for (const file of files) {
  const fileStat = await stat(file);
  const label = basename(file);
  const extension = extname(file).toLowerCase();

  if (extension === ".csv") {
    results.push(await measure(label, fileStat.size, "sheetra:csv:stream", () => drainSheetra(file)));
    results.push(await measure(label, fileStat.size, "fast-csv:stream", () => drainFastCsv(file)));
  }

  if (extension === ".xlsx") {
    results.push(await measure(label, fileStat.size, "sheetra:xlsx", () => drainSheetra(file)));
  }

  if ([".csv", ".xlsx"].includes(extension)) {
    results.push(
      await memoryHeavy(label, fileStat.size, "sheetjs:xlsx:readFile", () =>
        loadSheetJs().then((xlsx) => {
          const workbook = xlsx.readFile(file, { dense: true });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          return xlsx.utils.sheet_to_json(firstSheet, { header: 1 }).length - 1;
        }),
      ),
    );
  }
}

if (results.length === 0) {
  console.log(`No benchmark fixtures found in ${fixtureDir}. Add CSV or XLSX files and rerun.`);
} else {
  console.table(results);
}

async function discoverFiles(): Promise<string[]> {
  const entries = await readdir(fixtureDir, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => join(fixtureDir, entry.name))
    .filter((file) => [".csv", ".xlsx"].includes(extname(file).toLowerCase()))
    .filter((file) => selectedFile === undefined || basename(file) === selectedFile);
}

async function drainSheetra(file: string): Promise<number> {
  const pipeline = read(file);
  const stats = rowLimit > 0 ? await pipeline.take(rowLimit).drain() : await pipeline.drain();
  return stats.rowsProcessed;
}

async function drainFastCsv(file: string): Promise<number> {
  return new Promise((resolve, reject) => {
    let rows = 0;
    let settled = false;
    const input = createReadStream(file);
    const parser = parseCsv({ headers: true, ignoreEmpty: true });
    const finish = () => {
      if (!settled) {
        settled = true;
        resolve(rows);
      }
    };

    parser.on("data", () => {
      if (rowLimit > 0 && rows >= rowLimit) return;
      rows += 1;
      if (rowLimit > 0 && rows >= rowLimit) input.destroy();
    });
    parser.on("error", reject);
    parser.on("end", finish);
    input.on("error", reject);
    input.on("close", () => {
      if (rowLimit > 0) finish();
    });
    input.pipe(parser);
  });
}

async function measure(
  file: string,
  sizeBytes: number,
  engine: string,
  runner: () => Promise<number>,
): Promise<BenchmarkRow> {
  const started = performance.now();
  const before = process.memoryUsage().rss;
  const rows = await runner();
  const after = process.memoryUsage().rss;
  const ended = performance.now();

  return {
    file,
    size: formatBytes(sizeBytes),
    engine,
    rows,
    timeMs: Math.round(ended - started),
    peakMemory: formatBytes(Math.max(before, after)),
    notes: rowLimit > 0 ? `limited to ${rowLimit} rows` : "full file",
  };
}

async function memoryHeavy(
  file: string,
  sizeBytes: number,
  engine: string,
  runner: () => Promise<number>,
): Promise<BenchmarkRow> {
  if (!includeMemoryHeavy && sizeBytes > memoryHeavyLimitBytes) {
    return {
      file,
      size: formatBytes(sizeBytes),
      engine,
      rows: "skipped",
      timeMs: "skipped",
      peakMemory: "n/a",
      notes: `set SHEETRA_BENCH_INCLUDE_MEMORY=1 to run files over ${formatBytes(memoryHeavyLimitBytes)}`,
    };
  }

  try {
    return await measure(file, sizeBytes, engine, runner);
  } catch (error) {
    return {
      file,
      size: formatBytes(sizeBytes),
      engine,
      rows: "failed",
      timeMs: "failed",
      peakMemory: "n/a",
      notes: error instanceof Error ? error.message : "unknown error",
    };
  }
}

async function loadSheetJs(): Promise<typeof import("xlsx")> {
  const module = await import("xlsx");
  return (module.default ?? module) as typeof import("xlsx");
}
