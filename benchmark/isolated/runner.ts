import { createReadStream } from "node:fs";
import { performance } from "node:perf_hooks";
import { parse as parseCsv } from "@fast-csv/parse";
import { read } from "../../src/index.js";

interface Runners {
  [engine: string]: () => Promise<number>;
}

const file = process.argv[2];
const engine = process.argv[3];

if (file === undefined || engine === undefined) {
  console.error("usage: tsx benchmark/isolated/runner.ts <file> <engine>");
  process.exit(1);
}

let peakRss = process.memoryUsage().rss;
const sampler = setInterval(() => {
  const rss = process.memoryUsage().rss;
  if (rss > peakRss) peakRss = rss;
}, 25);
sampler.unref();

const runners: Runners = {
  sheetra: async () => (await read(file).drain()).rowsProcessed,
  "sheetra-raw-drain": async () => (await read(file).drain()).rowsProcessed,
  "sheetra-row-parse": async () => (await read(file).map((row) => row).drain()).rowsProcessed,
  fastcsv: () =>
    new Promise<number>((resolve, reject) => {
      let rows = 0;
      const input = createReadStream(file);
      const parser = parseCsv({ headers: true, ignoreEmpty: true });
      parser.on("data", () => {
        rows += 1;
      });
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
    if (firstName === undefined) return 0;
    const firstSheet = workbook.Sheets[firstName];
    if (firstSheet === undefined) return 0;
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

const runner = runners[engine];
if (runner === undefined) {
  console.error(`unknown engine: ${engine}`);
  process.exit(1);
}

const started = performance.now();
const rows = await runner();
const ended = performance.now();
clearInterval(sampler);
peakRss = Math.max(peakRss, process.memoryUsage().rss);

process.stdout.write(
  `${JSON.stringify({
    engine,
    file,
    rows,
    timeMs: Math.round(ended - started),
    peakRssBytes: peakRss,
  })}\n`,
);
