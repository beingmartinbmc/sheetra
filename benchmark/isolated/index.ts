import { spawn } from "node:child_process";
import { readdir, stat } from "node:fs/promises";
import { basename, dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { formatBytes } from "../../src/index.js";

interface IsolatedResult {
  file: string;
  size: string;
  engine: string;
  rows: number | string;
  timeMs: number | string;
  peakRss: string;
  notes: string;
}

const here = dirname(fileURLToPath(import.meta.url));
const fixtureDir = join(process.cwd(), "benchmark", "files");
const runs = Number(process.env.SHEETRA_BENCH_RUNS ?? 2);
const includeMemoryHeavyDefault = process.env.SHEETRA_BENCH_INCLUDE_MEMORY === "1";
const memoryHeavyLimitBytes = Number(process.env.SHEETRA_BENCH_MEMORY_LIMIT_MB ?? 100) * 1024 * 1024;

const files = await discoverFiles();
const aggregated: IsolatedResult[] = [];

for (const file of files) {
  const fileStat = await stat(file);
  const extension = extname(file).toLowerCase();
  const engines = enginesFor(extension);

  for (const engine of engines) {
    const heavy = engine.heavy === true && fileStat.size > memoryHeavyLimitBytes;
    if (heavy && !includeMemoryHeavyDefault) {
      aggregated.push({
        file: basename(file),
        size: formatBytes(fileStat.size),
        engine: engine.name,
        rows: "skipped",
        timeMs: "skipped",
        peakRss: "skipped",
        notes: `set SHEETRA_BENCH_INCLUDE_MEMORY=1 to run files over ${formatBytes(memoryHeavyLimitBytes)}`,
      });
      continue;
    }

    const samples: { timeMs: number; peakRssBytes: number; rows: number }[] = [];
    let lastError: string | undefined;
    for (let run = 0; run < runs; run += 1) {
      try {
        samples.push(await runIsolated(file, engine.name));
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
        break;
      }
    }

    if (samples.length === 0) {
      aggregated.push({
        file: basename(file),
        size: formatBytes(fileStat.size),
        engine: engine.name,
        rows: "failed",
        timeMs: "failed",
        peakRss: "failed",
        notes: lastError ?? "unknown error",
      });
      continue;
    }

    const best = samples.reduce((a, b) => (a.timeMs <= b.timeMs ? a : b));
    const minRss = samples.reduce((a, b) => (a.peakRssBytes <= b.peakRssBytes ? a : b));
    aggregated.push({
      file: basename(file),
      size: formatBytes(fileStat.size),
      engine: engine.name,
      rows: best.rows,
      timeMs: best.timeMs,
      peakRss: formatBytes(minRss.peakRssBytes),
      notes: `best of ${samples.length} fresh-process runs`,
    });
  }
}

if (aggregated.length === 0) {
  console.log(`No benchmark fixtures found in ${fixtureDir}. Add CSV or XLSX files and rerun.`);
} else {
  console.table(aggregated);
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

function enginesFor(extension: string): EngineSpec[] {
  if (extension === ".csv") {
    return [{ name: "sheetra" }, { name: "fastcsv" }, { name: "sheetjs", heavy: true }];
  }
  return [{ name: "sheetra" }, { name: "sheetjs" }, { name: "exceljs" }];
}

function runIsolated(file: string, engine: string): Promise<{ timeMs: number; peakRssBytes: number; rows: number }> {
  return new Promise((resolve, reject) => {
    const tsx = join(process.cwd(), "node_modules", ".bin", "tsx");
    const script = join(here, "runner.ts");
    const child = spawn(tsx, [script, file, engine], { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });
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
