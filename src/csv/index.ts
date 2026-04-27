import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { format } from "@fast-csv/format";
import { parse } from "@fast-csv/parse";
import { finishStats, createStats, observeMemory } from "../perf/index.js";
import type { CellValue, ProcessStats, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

const MEMORY_SAMPLE_INTERVAL_ROWS = 4096;

export async function* readCsv(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const headers = options.headers ?? true;
  const inferTypes = options.inferTypes ?? false;
  const input = createCsvParser(source, options);

  if (headers !== false && !inferTypes) {
    yield* input as AsyncIterable<Row>;
    return;
  }

  for await (const value of input) {
    const row = value as unknown;
    if (Array.isArray(row)) {
      yield arrayRowToObject(row, inferTypes);
    } else if (inferTypes) {
      yield inferObjectValues(row as Record<string, string>);
    } else {
      yield row as Row;
    }
  }
}

export function drainCsvViaEvents(source: string | Buffer, options: ReadOptions = {}): Promise<ProcessStats> {
  if (canScanCsvRecords(options)) return drainCsvByScanningRecords(source, options);

  return new Promise((resolve, reject) => {
    const stats = createStats();
    const input = createCsvParser(source, options);
    input.on("data", () => {
      stats.rowsProcessed += 1;
      if (stats.rowsProcessed % MEMORY_SAMPLE_INTERVAL_ROWS === 0) observeMemory(stats);
    });
    input.on("error", reject);
    input.on("end", () => resolve(finishStats(stats)));
  });
}

function drainCsvByScanningRecords(source: string | Buffer, options: ReadOptions): Promise<ProcessStats> {
  return new Promise((resolve, reject) => {
    const stats = createStats();
    const stream = typeof source === "string" ? createReadStream(source) : Readable.from(source);
    const scanner = createRecordScanner((options.headers ?? true) === true, (options.delimiter ?? ",").charCodeAt(0));
    let failed = false;

    stream.on("data", (chunk: Buffer | string) => {
      try {
        scanner.scan(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
        stats.rowsProcessed = scanner.rows;
        observeMemory(stats);
      } catch (error) {
        failed = true;
        stream.destroy(error instanceof Error ? error : new Error(String(error)));
      }
    });
    stream.on("error", reject);
    stream.on("end", () => {
      if (failed) return;
      try {
        scanner.finish();
        stats.rowsProcessed = scanner.rows;
        resolve(finishStats(stats));
      } catch (error) {
        reject(error);
      }
    });
  });
}

function canScanCsvRecords(options: ReadOptions): boolean {
  return (options.delimiter ?? ",").length === 1;
}

function createRecordScanner(skipFirstRecord: boolean, delimiter: number): {
  readonly rows: number;
  scan(chunk: Buffer): void;
  finish(): void;
} {
  let inQuotes = false;
  let quotePending = false;
  let atFieldStart = true;
  let recordHasContent = false;
  let lastWasCarriageReturn = false;
  let records = 0;
  let rows = 0;

  const endRecord = () => {
    if (recordHasContent) {
      records += 1;
      if (!(skipFirstRecord && records === 1)) rows += 1;
    }
    recordHasContent = false;
    atFieldStart = true;
    quotePending = false;
  };

  return {
    get rows() {
      return rows;
    },
    scan(chunk: Buffer) {
      for (let index = 0; index < chunk.length; index += 1) {
        const byte = chunk[index]!;

        if (lastWasCarriageReturn) {
          lastWasCarriageReturn = false;
          if (byte === 10) continue;
        }

        if (inQuotes && byte === 34) {
          if (inQuotes && quotePending) {
            quotePending = false;
            recordHasContent = true;
          } else if (inQuotes) {
            quotePending = true;
          }
          continue;
        }

        if (quotePending && byte === delimiter) {
          inQuotes = false;
          quotePending = false;
          atFieldStart = true;
          continue;
        }

        if (quotePending && (byte === 10 || byte === 13)) {
          inQuotes = false;
          quotePending = false;
          endRecord();
          lastWasCarriageReturn = byte === 13;
          continue;
        }

        if (quotePending) throw new Error("Invalid quoted CSV field");

        if (!inQuotes && (byte === 10 || byte === 13)) {
          endRecord();
          lastWasCarriageReturn = byte === 13;
          continue;
        }

        if (!inQuotes && byte === delimiter) {
          atFieldStart = true;
          continue;
        }

        if (!inQuotes && byte === 34 && atFieldStart) {
          inQuotes = true;
          atFieldStart = false;
          continue;
        }

        recordHasContent = true;
        atFieldStart = false;
      }
    },
    finish() {
      if (inQuotes && !quotePending) throw new Error("Unclosed quoted CSV field");
      if (quotePending) {
        inQuotes = false;
        quotePending = false;
      }
      if (recordHasContent) endRecord();
    },
  };
}

export function collectCsvViaEvents(source: string | Buffer, options: ReadOptions = {}): Promise<Row[]> {
  return new Promise((resolve, reject) => {
    const headers = options.headers ?? true;
    const inferTypes = options.inferTypes ?? false;
    const fastPath = headers !== false && !inferTypes;
    const rows: Row[] = [];
    const input = createCsvParser(source, options);
    input.on("data", (row: unknown) => {
      if (fastPath) {
        rows.push(row as Row);
        return;
      }
      if (Array.isArray(row)) rows.push(arrayRowToObject(row, inferTypes));
      else if (inferTypes) rows.push(inferObjectValues(row as Record<string, string>));
      else rows.push(row as Row);
    });
    input.on("error", reject);
    input.on("end", () => resolve(rows));
  });
}

function createCsvParser(source: string | Buffer, options: ReadOptions): NodeJS.ReadableStream {
  const headers = options.headers ?? true;
  const stream = typeof source === "string" ? createReadStream(source) : Readable.from(source);
  const parser = parse({
    headers,
    delimiter: options.delimiter ?? ",",
    ignoreEmpty: true,
    trim: false,
  });
  stream.on("error", (error) => parser.destroy(error));
  return stream.pipe(parser);
}

export async function writeCsv(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<void> {
  const csv = format({ headers: options.headers ?? true, delimiter: options.delimiter ?? "," });
  csv.pipe(createWriteStream(destination));

  for await (const row of toAsync(rows)) {
    csv.write(row as never);
  }

  csv.end();
  await finished(csv);
}

export function inferCsv(value: string): CellValue {
  return normalizeCsvValue(value);
}

async function* toAsync<T>(rows: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
  yield* rows;
}

function normalizeCsvValue(value: string): CellValue {
  if (value === "") return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && value.trim() !== "") return numeric;
  if (/^(true|false)$/i.test(value)) return value.toLowerCase() === "true";
  return value;
}

function inferObjectValues(row: Record<string, string>): Row {
  const output: Row = {};
  for (const key in row) output[key] = normalizeCsvValue(row[key] ?? "");
  return output;
}

function arrayRowToObject(row: string[], inferTypes: boolean): Row {
  const output: Row = {};
  for (let index = 0; index < row.length; index += 1) {
    const value = row[index] ?? "";
    output[`_${index + 1}`] = inferTypes ? normalizeCsvValue(value) : value;
  }
  return output;
}

