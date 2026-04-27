import { mkdir, readdir, stat } from "node:fs/promises";
import { basename, extname, join } from "node:path";
import {
  parseDetailed,
  read,
  schema,
  workerMap,
  writeWorkbook,
  workbook,
  worksheet,
  formula,
} from "../src/index.js";
import { ensureCsvFixture, ensureXlsxFixture, limitFromEnv, scaleFromEnv, syntheticRows, transformRow } from "./lib/datasets.js";
import {
  drainExcelJsStreaming,
  drainFastCsv,
  drainPravaahCsv,
  readExcelJsInMemory,
  readSheetJs,
  pravaahCsvEndToEnd,
} from "./lib/engines.js";
import { type BenchmarkCase, measureCase, resultForConsole, writeJson, writeMarkdown } from "./lib/metrics.js";

const profile = process.env.PRAVAAH_BENCH_PROFILE ?? "quick";
const outputDir = join(process.cwd(), "benchmark", "results");
const workDir = join(outputDir, "fixtures");
const scales =
  profile === "full" ? scaleFromEnv([100_000, 500_000, 1_000_000, 2_000_000]) : scaleFromEnv([100_000, 500_000]);
const xlsxRows = limitFromEnv("PRAVAAH_BENCH_XLSX_ROWS", profile === "full" ? 50_000 : 10_000);
const wideRows = limitFromEnv("PRAVAAH_BENCH_WIDE_ROWS", profile === "full" ? 50_000 : 10_000);
const memoryHeavyLimitBytes = Number(process.env.PRAVAAH_BENCH_MEMORY_LIMIT_MB ?? 100) * 1024 * 1024;

await mkdir(workDir, { recursive: true });

const cases: BenchmarkCase[] = [
  ...(await scaleCases()),
  ...(await modeCases()),
  ...(await xlsxCases()),
  ...(await shapeCases()),
  ...(await transformCases()),
  ...(await faultToleranceCases()),
  ...(await workerCases()),
  ...(await coldWarmCases()),
  ...(await scenarioCases()),
  ...(await fileSizeCases()),
];

const results = [];
for (const testCase of cases) {
  const result = await measureCase(testCase);
  results.push(result);
  console.table([resultForConsole(result)]);
}

await writeJson(join(outputDir, "strong-results.json"), results);
await writeMarkdown(join(outputDir, "strong-results.md"), results);

console.log(`Wrote benchmark artifacts to ${outputDir}`);

async function scaleCases(): Promise<BenchmarkCase[]> {
  const cases: BenchmarkCase[] = [];
  for (const rows of scales) {
    const path = await ensureCsvFixture(join(workDir, `scale-${rows}-x10.csv`), { rows, columns: 10 });
    const size = (await stat(path)).size;
    cases.push({
      suite: "scale",
      name: `${rows.toLocaleString()} rows x 10 columns`,
      mode: "pravaah raw streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainPravaahCsv(path),
    });
    cases.push({
      suite: "scale",
      name: `${rows.toLocaleString()} rows x 10 columns`,
      mode: "fast-csv streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainFastCsv(path),
    });
  }
  return cases;
}

async function modeCases(): Promise<BenchmarkCase[]> {
  const rows = Math.min(scales[0] ?? 100_000, 100_000);
  const path = await ensureCsvFixture(join(workDir, `modes-${rows}.csv`), { rows, columns: 10 });
  const size = (await stat(path)).size;
  return [
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "pravaah raw streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainPravaahCsv(path),
    },
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "pravaah inferTypes",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainPravaahCsv(path, undefined, { inferTypes: true }),
    },
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "pravaah schema+cleaning",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: async () => {
        const result = await parseDetailed(
          path,
          {
            id: schema.number(),
            email: schema.email(),
            amount: schema.number(),
            quantity: schema.number(),
          },
          {
            format: "csv",
            validation: "collect",
            cleaning: { trim: true, normalizeWhitespace: true },
          },
        );
        return result.rows.length + result.issues.length;
      },
    },
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "fast-csv streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainFastCsv(path),
    },
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "exceljs in-memory",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => readExcelJsInMemory(path),
    },
    {
      suite: "streaming-vs-memory",
      name: "CSV read",
      mode: "sheetjs in-memory",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => readSheetJs(path),
    },
  ];
}

async function xlsxCases(): Promise<BenchmarkCase[]> {
  const single = await ensureXlsxFixture(join(workDir, `xlsx-${xlsxRows}-x10.xlsx`), { rows: xlsxRows, columns: 10 });
  const multi = join(workDir, `xlsx-multi-${xlsxRows}.xlsx`);
  await writeWorkbook(
    workbook([
      worksheet("Leads", [...syntheticRows({ rows: xlsxRows, columns: 10 })]),
      worksheet("Finance", [
        { label: "Gross", amount: 1200 },
        { label: "Net", amount: formula("SUM(B2:B2)", 1200) },
      ]),
      worksheet("Logs", [...syntheticRows({ rows: Math.max(100, Math.floor(xlsxRows / 10)), columns: 20 })]),
    ]),
    multi,
  );
  const singleSize = (await stat(single)).size;
  const multiSize = (await stat(multi)).size;

  return [
    {
      suite: "xlsx",
      name: `${xlsxRows.toLocaleString()} rows x 10 columns`,
      mode: "pravaah read",
      format: "xlsx",
      rows: xlsxRows,
      columns: 10,
      fileSizeBytes: singleSize,
      run: () => read(single).drain().then((stats) => stats.rowsProcessed),
    },
    {
      suite: "xlsx",
      name: `${xlsxRows.toLocaleString()} rows x 10 columns`,
      mode: "sheetjs in-memory",
      format: "xlsx",
      rows: xlsxRows,
      columns: 10,
      fileSizeBytes: singleSize,
      run: () => readSheetJs(single),
    },
    {
      suite: "xlsx",
      name: `${xlsxRows.toLocaleString()} rows x 10 columns`,
      mode: "exceljs streaming",
      format: "xlsx",
      rows: xlsxRows,
      columns: 10,
      fileSizeBytes: singleSize,
      run: () => drainExcelJsStreaming(single),
    },
    {
      suite: "xlsx",
      name: "multi-sheet workbook",
      mode: "pravaah read first sheet",
      format: "xlsx",
      rows: xlsxRows,
      columns: 10,
      fileSizeBytes: multiSize,
      run: () => read(multi).drain().then((stats) => stats.rowsProcessed),
    },
  ];
}

async function shapeCases(): Promise<BenchmarkCase[]> {
  const tallRows = Math.min(scales[0] ?? 100_000, 100_000);
  const tall = await ensureCsvFixture(join(workDir, `shape-tall-${tallRows}-x10.csv`), { rows: tallRows, columns: 10 });
  const wide = await ensureCsvFixture(join(workDir, `shape-wide-${wideRows}-x200.csv`), { rows: wideRows, columns: 200 });
  return [
    {
      suite: "shape",
      name: "tall data",
      mode: "pravaah raw streaming",
      format: "csv",
      rows: tallRows,
      columns: 10,
      fileSizeBytes: (await stat(tall)).size,
      run: () => drainPravaahCsv(tall),
    },
    {
      suite: "shape",
      name: "wide data",
      mode: "pravaah raw streaming",
      format: "csv",
      rows: wideRows,
      columns: 200,
      fileSizeBytes: (await stat(wide)).size,
      run: () => drainPravaahCsv(wide),
    },
  ];
}

async function transformCases(): Promise<BenchmarkCase[]> {
  const rows = Math.min(scales[0] ?? 100_000, 100_000);
  const path = await ensureCsvFixture(join(workDir, `transform-${rows}.csv`), { rows, columns: 20 });
  const size = (await stat(path)).size;
  return (["none", "light", "heavy"] as const).map((level) => ({
    suite: "transform",
    name: `${level} transform`,
    mode: "pravaah raw streaming",
    format: "csv",
    rows,
    columns: 20,
    fileSizeBytes: size,
    run: () => drainPravaahCsv(path, (row) => transformRow(row, level)),
  }));
}

async function faultToleranceCases(): Promise<BenchmarkCase[]> {
  const rows = Math.min(scales[0] ?? 100_000, 100_000);
  const path = await ensureCsvFixture(join(workDir, `fault-${rows}.csv`), { rows, columns: 12, messy: true });
  return [
    {
      suite: "fault-tolerance",
      name: "messy CRM import",
      mode: "collect issues",
      format: "csv",
      rows,
      columns: 12,
      fileSizeBytes: (await stat(path)).size,
      notes: "invalid emails and blank values are reported without crashing",
      run: async () => {
        const result = await parseDetailed(
          path,
          {
            id: schema.number(),
            email: schema.email(),
            amount: schema.number(),
            quantity: schema.number(),
          },
          { format: "csv", validation: "collect", cleaning: { trim: true } },
        );
        return result.rows.length + result.issues.length;
      },
    },
  ];
}

async function workerCases(): Promise<BenchmarkCase[]> {
  const rows = limitFromEnv("PRAVAAH_BENCH_WORKER_ROWS", profile === "full" ? 100_000 : 25_000);
  const data = [...syntheticRows({ rows, columns: 10 })];
  const mapper = `(row) => {
    const amount = Number(row.amount || 0);
    let score = 0;
    for (let index = 0; index < 75; index += 1) score += Math.sqrt(amount + index);
    return { ...row, score };
  }`;
  return [1, Math.min(4, Math.max(2, Number(process.env.PRAVAAH_BENCH_WORKERS ?? 4)))].map((workers) => ({
    suite: "parallel",
    name: "heavy row mapper",
    mode: `${workers} worker${workers === 1 ? "" : "s"}`,
    format: "memory",
    rows,
    columns: 10,
    run: () => workerMap(data, mapper, { concurrency: workers }).then((result) => result.length),
  }));
}

async function coldWarmCases(): Promise<BenchmarkCase[]> {
  const rows = Math.min(scales[0] ?? 100_000, 100_000);
  const path = await ensureCsvFixture(join(workDir, `cold-warm-${rows}.csv`), { rows, columns: 10 });
  const size = (await stat(path)).size;
  return [
    {
      suite: "cold-warm",
      name: "first run",
      mode: "pravaah raw streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainPravaahCsv(path),
    },
    {
      suite: "cold-warm",
      name: "second run",
      mode: "pravaah raw streaming",
      format: "csv",
      rows,
      columns: 10,
      fileSizeBytes: size,
      run: () => drainPravaahCsv(path),
    },
  ];
}

async function scenarioCases(): Promise<BenchmarkCase[]> {
  const rows = Math.min(scales[0] ?? 100_000, 100_000);
  const crm = await ensureCsvFixture(join(workDir, `scenario-crm-${rows}.csv`), { rows, columns: 15, messy: true });
  const logs = await ensureCsvFixture(join(workDir, `scenario-logs-${rows}.csv`), { rows, columns: 30 });
  const finance = join(workDir, "scenario-finance.xlsx");
  await writeWorkbook(
    workbook([
      worksheet("Report", [
        { account: "Revenue", value: 1000, projected: formula("SUM(B2:B2)", 1000) },
        { account: "Costs", value: 250, projected: formula("SUM(B3:B3)", 250) },
      ]),
    ]),
    finance,
  );

  return [
    {
      suite: "scenario",
      name: "Import CRM leads",
      mode: "validate messy headers",
      format: "csv",
      rows,
      columns: 15,
      fileSizeBytes: (await stat(crm)).size,
      run: async () => {
        const result = await parseDetailed(
          crm,
          { id: schema.number(), email: schema.email(), amount: schema.number() },
          { format: "csv", cleaning: { trim: true, fuzzyHeaders: { email: ["E-mail", "email id", "mail"] } } },
        );
        return result.rows.length + result.issues.length;
      },
    },
    {
      suite: "scenario",
      name: "Log ingestion export",
      mode: "read-transform-write",
      format: "csv",
      rows,
      columns: 30,
      fileSizeBytes: (await stat(logs)).size,
      run: () => pravaahCsvEndToEnd(logs, join(workDir, "scenario-logs-out.csv"), (row) => transformRow(row, "heavy")),
    },
    {
      suite: "scenario",
      name: "Financial formulas",
      mode: "xlsx preserve formulas",
      format: "xlsx",
      rows: 2,
      columns: 3,
      fileSizeBytes: (await stat(finance)).size,
      run: () => read(finance, { formulas: "preserve" }).drain().then((stats) => stats.rowsProcessed),
    },
  ];
}

async function fileSizeCases(): Promise<BenchmarkCase[]> {
  const fixtureDir = join(process.cwd(), "benchmark", "files");
  let files: string[];
  try {
    files = (await readdir(fixtureDir)).map((entry) => join(fixtureDir, entry));
  } catch {
    return [];
  }

  const cases: BenchmarkCase[] = [];
  for (const file of files) {
    const size = (await stat(file)).size;
    const extension = extname(file);
    if (![".csv", ".xlsx"].includes(extension)) continue;

    cases.push({
      suite: "file-size",
      name: basename(file),
      mode: "pravaah",
      format: extension === ".csv" ? "csv" : "xlsx",
      rows: 0,
      columns: 0,
      fileSizeBytes: size,
      run: () => read(file).drain().then((stats) => stats.rowsProcessed),
    });

    if (size <= memoryHeavyLimitBytes) {
      cases.push({
        suite: "file-size",
        name: basename(file),
        mode: "sheetjs in-memory",
        format: extension === ".csv" ? "csv" : "xlsx",
        rows: 0,
        columns: 0,
        fileSizeBytes: size,
        run: () => readSheetJs(file),
      });
    }
  }
  return cases;
}
