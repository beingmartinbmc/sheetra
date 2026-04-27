import { createReadStream } from "node:fs";
import { parse as parseCsv } from "@fast-csv/parse";
import ExcelJS from "exceljs";
import { read, write } from "../../src/index.js";
import type { ReadOptions, Row } from "../../src/index.js";

export async function drainSheetraCsv(
  path: string,
  transform?: (row: Row) => Row,
  options: ReadOptions = {},
): Promise<number> {
  const pipeline = transform === undefined ? read(path, options) : read(path, options).map((row) => transform(row as Row));
  return (await pipeline.drain()).rowsProcessed;
}

export async function drainFastCsv(path: string, transform?: (row: Row) => Row): Promise<number> {
  return new Promise((resolve, reject) => {
    let rows = 0;
    const parser = parseCsv({ headers: true, ignoreEmpty: true });
    parser.on("data", (row: Row) => {
      if (transform !== undefined) transform(row);
      rows += 1;
    });
    parser.on("error", reject);
    parser.on("end", () => resolve(rows));
    createReadStream(path).on("error", reject).pipe(parser);
  });
}

export async function readSheetJs(path: string): Promise<number> {
  const xlsx = await import("xlsx");
  const module = xlsx.default ?? xlsx;
  const workbook = module.readFile(path, { dense: true });
  const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
  return module.utils.sheet_to_json(firstSheet, { header: 1 }).length - 1;
}

export async function readExcelJsInMemory(path: string): Promise<number> {
  const workbook = new ExcelJS.Workbook();
  if (path.endsWith(".csv")) {
    const worksheet = await workbook.csv.readFile(path);
    return worksheet.rowCount - 1;
  }

  await workbook.xlsx.readFile(path);
  return workbook.worksheets.reduce((total, sheet) => total + Math.max(0, sheet.rowCount - 1), 0);
}

export async function drainExcelJsStreaming(path: string): Promise<number> {
  if (path.endsWith(".csv")) return drainFastCsv(path);

  const workbook = new ExcelJS.stream.xlsx.WorkbookReader(path, {
    worksheets: "emit",
    sharedStrings: "cache",
    hyperlinks: "ignore",
    styles: "ignore",
  });
  let rows = 0;

  for await (const worksheet of workbook) {
    for await (const row of worksheet) {
      void row;
      rows += 1;
    }
  }

  return Math.max(0, rows - 1);
}

export async function sheetraCsvEndToEnd(input: string, output: string, transform: (row: Row) => Row): Promise<number> {
  const pipeline = read(input).map((row) => transform(row as Row));
  const stats = await write(pipeline, output, { format: "csv" });
  return stats.rowsProcessed;
}
