import { performance } from "node:perf_hooks";
import { read, write, formatBytes } from "../src/index.js";

const rowCount = Number(process.env.PRAVAAH_BENCH_ROWS ?? 100_000);
const rows = Array.from({ length: rowCount }, (_, index) => ({
  id: index + 1,
  name: `User ${index}`,
  amount: index * 3,
}));

const results = [
  await measure("pravaah:csv:pipeline", async () => {
    const stats = await write(read(rows).map((row) => ({ ...row, total: Number(row.amount) + 1 })), "/tmp/pravaah-compare.csv", {
      format: "csv",
    });
    return { peakRssBytes: stats.peakRssBytes };
  }),
  await optionalMeasure("xlsx", "sheetjs:xlsx:json_to_sheet", async (xlsx) => {
    const workbook = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(rows), "Sheet1");
    xlsx.writeFile(workbook, "/tmp/pravaah-sheetjs.xlsx");
    return {};
  }),
  await optionalMeasure("exceljs", "exceljs:workbook:csv", async (ExcelJS) => {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet("Sheet1");
    sheet.addRows(rows.map((row) => Object.values(row)));
    await workbook.csv.writeFile("/tmp/pravaah-exceljs.csv");
    return {};
  }),
].filter(Boolean);

console.table(results);

async function measure(
  name: string,
  runner: () => Promise<{ peakRssBytes?: number }>,
): Promise<Record<string, string | number>> {
  const started = performance.now();
  const before = process.memoryUsage().rss;
  const result = await runner();
  const ended = performance.now();
  const after = process.memoryUsage().rss;

  return {
    name,
    rows: rowCount,
    timeMs: Math.round(ended - started),
    peakMemory: formatBytes(Math.max(before, after, result.peakRssBytes ?? 0)),
  };
}

async function optionalMeasure(
  packageName: string,
  name: string,
  runner: (module: any) => Promise<{ peakRssBytes?: number }>,
): Promise<Record<string, string | number> | undefined> {
  try {
    const module = await import(packageName);
    return measure(name, () => runner(module.default ?? module));
  } catch {
    return {
      name,
      rows: rowCount,
      timeMs: "install dependency to compare",
      peakMemory: "n/a",
    };
  }
}
