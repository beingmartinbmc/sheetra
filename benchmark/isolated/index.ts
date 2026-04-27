import { spawn } from "node:child_process";
import { readdir, stat } from "node:fs/promises";
import { basename, dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { formatBytes } from "../../src/index.js";

interface IsolatedResult {
  workload: string;
  rows: number | string;
  size: string;
  engine: string;
  timeMs: number | string;
  peakRss: string;
  notes: string;
}

const here = dirname(fileURLToPath(import.meta.url));
const fixtureDir = join(process.cwd(), "benchmark", "files");
const runs = Number(process.env.SHEETRA_BENCH_RUNS ?? 2);
const includeMemoryHeavyDefault = process.env.SHEETRA_BENCH_INCLUDE_MEMORY === "1";
const memoryHeavyLimitBytes = Number(process.env.SHEETRA_BENCH_MEMORY_LIMIT_MB ?? 100) * 1024 * 1024;
const writeRows = Number(process.env.SHEETRA_BENCH_WRITE_ROWS ?? 100000);
const skipWrite = process.env.SHEETRA_BENCH_SKIP_WRITE === "1";
const skipRead = process.env.SHEETRA_BENCH_SKIP_READ === "1";

const aggregated: IsolatedResult[] = [];

if (!skipRead) {
  const files = await discoverFiles();
  for (const file of files) {
    const fileStat = await stat(file);
    const extension = extname(file).toLowerCase();
    const engines = readEnginesFor(extension);

    for (const engine of engines) {
      const heavy = engine.heavy === true && fileStat.size > memoryHeavyLimitBytes;
      if (heavy && !includeMemoryHeavyDefault) {
        aggregated.push({
          workload: readWorkloadLabel(extension),
          rows: "skipped",
          size: formatBytes(fileStat.size),
          engine: engine.name,
          timeMs: "skipped",
          peakRss: "skipped",
          notes: `set SHEETRA_BENCH_INCLUDE_MEMORY=1 to run files over ${formatBytes(memoryHeavyLimitBytes)}`,
        });
        continue;
      }

      const samples = await runMultiple(file, engine.name, runs);
      if (samples.length === 0) {
        aggregated.push({
          workload: readWorkloadLabel(extension),
          rows: "failed",
          size: formatBytes(fileStat.size),
          engine: engine.name,
          timeMs: "failed",
          peakRss: "failed",
          notes: "runner failed",
        });
        continue;
      }

      const best = samples.reduce((a, b) => (a.timeMs <= b.timeMs ? a : b));
      const minRss = samples.reduce((a, b) => (a.peakRssBytes <= b.peakRssBytes ? a : b));
      aggregated.push({
        workload: readWorkloadLabel(extension),
        rows: best.rows,
        size: formatBytes(fileStat.size),
        engine: engine.name,
        timeMs: best.timeMs,
        peakRss: formatBytes(minRss.peakRssBytes),
        notes: `best of ${samples.length}`,
      });
    }
  }
}

if (!skipWrite) {
  const writeEngines = writeEngineSpecs();
  for (const engine of writeEngines) {
    const samples = await runMultipleWrite(engine.name, writeRows, runs);
    if (samples.length === 0) {
      aggregated.push({
        workload: engine.workload,
        rows: "failed",
        size: "-",
        engine: engine.name,
        timeMs: "failed",
        peakRss: "failed",
        notes: "runner failed",
      });
      continue;
    }

    const best = samples.reduce((a, b) => (a.timeMs <= b.timeMs ? a : b));
    const minRss = samples.reduce((a, b) => (a.peakRssBytes <= b.peakRssBytes ? a : b));
    aggregated.push({
      workload: engine.workload,
      rows: writeRows,
      size: "-",
      engine: engine.name,
      timeMs: best.timeMs,
      peakRss: formatBytes(minRss.peakRssBytes),
      notes: `best of ${samples.length}`,
    });
  }
}

if (aggregated.length === 0) {
  console.log(`No benchmark fixtures found in ${fixtureDir}. Add CSV or XLSX files and rerun.`);
} else {
  console.table(aggregated);
}

function readWorkloadLabel(extension: string): string {
  return extension === ".csv" ? "CSV Read" : "XLSX Read";
}

async function discoverFiles(): Promise<string[]> {
  const selectedFile = process.env.SHEETRA_BENCH_FILE;
  let entries: string[];
  try {
    entries = await readdir(fixtureDir);
  } catch {
    return [];
  }
  return entries
    .map((entry) => join(fixtureDir, entry))
    .filter((path) => [".csv", ".xlsx"].includes(extname(path).toLowerCase()))
    .filter((path) => selectedFile === undefined || basename(path) === selectedFile);
}

interface EngineSpec {
  name: string;
  heavy?: boolean;
}

interface WriteEngineSpec {
  name: string;
  workload: string;
}

function readEnginesFor(extension: string): EngineSpec[] {
  if (extension === ".csv") {
    return [
      { name: "sheetra-raw-drain" },
      { name: "sheetra-row-parse" },
      { name: "fastcsv" },
      { name: "sheetjs", heavy: true },
    ];
  }
  return [{ name: "sheetra" }, { name: "sheetjs" }, { name: "exceljs" }];
}

function writeEngineSpecs(): WriteEngineSpec[] {
  return [
    { name: "sheetra-write-csv", workload: "CSV Write" },
    { name: "fastcsv-write", workload: "CSV Write" },
    { name: "sheetjs-write-csv", workload: "CSV Write" },
    { name: "sheetra-write-xlsx", workload: "XLSX Write" },
    { name: "sheetjs-write-xlsx", workload: "XLSX Write" },
    { name: "exceljs-write-xlsx", workload: "XLSX Write" },
  ];
}

async function runMultiple(
  file: string,
  engine: string,
  count: number,
): Promise<Array<{ timeMs: number; peakRssBytes: number; rows: number }>> {
  const samples: Array<{ timeMs: number; peakRssBytes: number; rows: number }> = [];
  for (let i = 0; i < count; i++) {
    try {
      samples.push(await runIsolated(file, engine));
    } catch {
      break;
    }
  }
  return samples;
}

async function runMultipleWrite(
  engine: string,
  rowCount: number,
  count: number,
): Promise<Array<{ timeMs: number; peakRssBytes: number; rows: number }>> {
  const samples: Array<{ timeMs: number; peakRssBytes: number; rows: number }> = [];
  for (let i = 0; i < count; i++) {
    try {
      samples.push(await runIsolated("__write__", engine, { SHEETRA_BENCH_WRITE_ROWS: String(rowCount) }));
    } catch {
      break;
    }
  }
  return samples;
}

function runIsolated(
  file: string,
  engine: string,
  extraEnv: Record<string, string> = {},
): Promise<{ timeMs: number; peakRssBytes: number; rows: number }> {
  return new Promise((resolve, reject) => {
    const tsx = join(process.cwd(), "node_modules", ".bin", "tsx");
    const script = join(here, "runner.ts");
    const child = spawn(tsx, [script, file, engine], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, ...extraEnv },
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString("utf8"); });
    child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString("utf8"); });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`runner exited with code ${code}: ${stderr.trim()}`));
        return;
      }
      const lastLine = stdout.trim().split(/\r?\n/).pop() ?? "";
      try {
        resolve(JSON.parse(lastLine) as { timeMs: number; peakRssBytes: number; rows: number });
      } catch (error) {
        reject(new Error(`invalid runner output: ${lastLine} (${(error as Error).message})`));
      }
    });
  });
}
