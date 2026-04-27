import { createReadStream, createWriteStream } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { format as formatCsv } from "@fast-csv/format";
import { parse as parseCsv } from "@fast-csv/parse";
import { finished } from "node:stream/promises";
import { read, write } from "../../src/index.js";

interface RunnerResult {
  engine: string;
  file: string;
  rows: number;
  timeMs: number;
  peakRssBytes: number;
}

const file = process.argv[2]!;
const engine = process.argv[3]!;

if (!file || !engine) {
  console.error("usage: tsx benchmark/isolated/runner.ts <file> <engine>");
  process.exit(1);
}

let peakRss = process.memoryUsage().rss;
const sampler = setInterval(() => {
  const rss = process.memoryUsage().rss;
  if (rss > peakRss) peakRss = rss;
}, 25);
sampler.unref();

type Runner = () => Promise<number>;

function generateRows(count: number): Array<Record<string, unknown>> {
  const rows: Array<Record<string, unknown>> = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      name: `User_${i}`,
      email: `user${i}@example.com`,
      score: Math.round(Math.random() * 1000),
      active: i % 3 !== 0,
      joined: `2026-01-${String((i % 28) + 1).padStart(2, "0")}`,
    });
  }
  return rows;
}

const readRunners: Record<string, Runner> = {
  "sheetra-raw-drain": async () => (await read(file).drain()).rowsProcessed,
  "sheetra-row-parse": async () => (await read(file).map((row) => row).drain()).rowsProcessed,
  sheetra: async () => (await read(file).drain()).rowsProcessed,
  fastcsv: () =>
    new Promise<number>((resolve, reject) => {
      let rows = 0;
      const input = createReadStream(file);
      const parser = parseCsv({ headers: true, ignoreEmpty: true });
      parser.on("data", () => { rows += 1; });
      parser.on("error", reject);
      parser.on("end", () => resolve(rows));
      input.on("error", reject);
      input.pipe(parser);
    }),
  sheetjs: async () => {
    const xlsxModule = await import("xlsx");
    const xlsx = (xlsxModule as { default?: typeof xlsxModule }).default ?? xlsxModule;
    const workbook = xlsx.readFile(file, { dense: true });
    const firstName = workbook.SheetNames[0];
    if (!firstName) return 0;
    const firstSheet = workbook.Sheets[firstName];
    if (!firstSheet) return 0;
    return (xlsx.utils.sheet_to_json(firstSheet, { header: 1 }) as unknown[]).length - 1;
  },
  exceljs: async () => {
    const ExcelJSModule = await import("exceljs");
    const ExcelJS = (ExcelJSModule as { default?: typeof ExcelJSModule }).default ?? ExcelJSModule;
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(file);
    return workbook.worksheets.reduce((total, sheet) => total + Math.max(0, sheet.rowCount - 1), 0);
  },
};

const writeRowCount = Number(process.env.SHEETRA_BENCH_WRITE_ROWS ?? file);

const writeRunners: Record<string, Runner> = {
  "sheetra-write-csv": async () => {
    const rows = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.csv");
    await write(rows, dest, { format: "csv" });
    await rm(dir, { recursive: true, force: true });
    return rows.length;
  },
  "sheetra-write-xlsx": async () => {
    const rows = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.xlsx");
    await write(rows, dest, { format: "xlsx" });
    await rm(dir, { recursive: true, force: true });
    return rows.length;
  },
  "fastcsv-write": async () => {
    const rows = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.csv");
    const csv = formatCsv({ headers: true });
    csv.pipe(createWriteStream(dest));
    for (const row of rows) csv.write(row);
    csv.end();
    await finished(csv);
    await rm(dir, { recursive: true, force: true });
    return rows.length;
  },
  "sheetjs-write-csv": async () => {
    const xlsxModule = await import("xlsx");
    const xlsx = (xlsxModule as { default?: typeof xlsxModule }).default ?? xlsxModule;
    const rows = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.csv");
    const ws = xlsx.utils.json_to_sheet(rows);
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, "Sheet1");
    xlsx.writeFile(wb, dest, { bookType: "csv" });
    await rm(dir, { recursive: true, force: true });
    return rows.length;
  },
  "sheetjs-write-xlsx": async () => {
    const xlsxModule = await import("xlsx");
    const xlsx = (xlsxModule as { default?: typeof xlsxModule }).default ?? xlsxModule;
    const rows = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.xlsx");
    const ws = xlsx.utils.json_to_sheet(rows);
    const wb = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(wb, ws, "Sheet1");
    xlsx.writeFile(wb, dest);
    await rm(dir, { recursive: true, force: true });
    return rows.length;
  },
  "exceljs-write-xlsx": async () => {
    const ExcelJSModule = await import("exceljs");
    const ExcelJS = (ExcelJSModule as { default?: typeof ExcelJSModule }).default ?? ExcelJSModule;
    const data = generateRows(writeRowCount);
    const dir = await mkdtemp(join(tmpdir(), "sheetra-bench-"));
    const dest = join(dir, "out.xlsx");
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet("Sheet1");
    if (data.length > 0) {
      sheet.columns = Object.keys(data[0]!).map((key) => ({ header: key, key }));
      for (const row of data) sheet.addRow(row);
    }
    await workbook.xlsx.writeFile(dest);
    await rm(dir, { recursive: true, force: true });
    return data.length;
  },
};

const allRunners: Record<string, Runner> = { ...readRunners, ...writeRunners };
const runner = allRunners[engine];
if (!runner) {
  console.error(`unknown engine: ${engine}`);
  process.exit(1);
}

const started = performance.now();
const rows = await runner();
const ended = performance.now();
clearInterval(sampler);
peakRss = Math.max(peakRss, process.memoryUsage().rss);

const result: RunnerResult = {
  engine,
  file,
  rows,
  timeMs: Math.round(ended - started),
  peakRssBytes: peakRss,
};

process.stdout.write(`${JSON.stringify(result)}\n`);
