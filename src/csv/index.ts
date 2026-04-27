import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { format } from "@fast-csv/format";
import { finishStats, createStats, observeMemory } from "../perf/index.js";
import type { CellValue, ProcessStats, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

export async function* readCsv(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const headers = options.headers ?? true;
  const inferTypes = options.inferTypes ?? false;
  const delimiter = options.delimiter ?? ",";

  if (headers !== false && !inferTypes && delimiter.length === 1) {
    yield* nativeReadCsv(source, delimiter, headers === true ? undefined : (headers as string[]));
    return;
  }

  if (delimiter.length > 1) {
    throw new Error("delimiter must be a single character");
  }

  for await (const row of nativeReadCsv(source, delimiter, headers === true ? undefined : headers === false ? null : (headers as string[]))) {
    if (inferTypes) {
      yield inferObjectValues(row as Record<string, string>);
    } else {
      yield row;
    }
  }
}

export function drainCsvViaEvents(source: string | Buffer, options: ReadOptions = {}): Promise<ProcessStats> {
  if ((options.delimiter ?? ",").length > 1) {
    return Promise.reject(new Error("delimiter must be a single character"));
  }
  return drainCsvByScanningRecords(source, options);
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

export function collectCsvViaEvents(source: string | Buffer, options: ReadOptions = {}): Promise<Row[]> {
  if ((options.delimiter ?? ",").length > 1) {
    return Promise.reject(new Error("delimiter must be a single character"));
  }
  return collectCsvNative(source, options);
}

async function collectCsvNative(source: string | Buffer, options: ReadOptions): Promise<Row[]> {
  const rows: Row[] = [];
  const headers = options.headers ?? true;
  const inferTypes = options.inferTypes ?? false;
  const delimiter = options.delimiter ?? ",";
  const explicitHeaders = headers === false ? null : headers === true ? undefined : (headers as string[]);

  for await (const row of nativeReadCsv(source, delimiter, explicitHeaders)) {
    if (inferTypes) {
      rows.push(inferObjectValues(row as Record<string, string>));
    } else {
      rows.push(row);
    }
  }
  return rows;
}

// --- Custom streaming CSV parser (no @fast-csv/parse on hot path) ---

const QUOTE = 34;
const CR = 13;
const LF = 10;

async function* nativeReadCsv(
  source: string | Buffer,
  delimiter: string,
  explicitHeaders?: string[] | null,
): AsyncIterable<Row> {
  const delimCode = delimiter.charCodeAt(0);
  const stream = typeof source === "string" ? createReadStream(source) : Readable.from(source);

  let headers: string[] | null = explicitHeaders ?? null;
  const autoHeaders = explicitHeaders === undefined;
  let tail = "";

  for await (const chunk of stream as AsyncIterable<Buffer | string>) {
    const text = tail + (typeof chunk === "string" ? chunk : chunk.toString("utf8"));
    tail = "";

    let cursor = 0;
    while (cursor < text.length) {
      const result = parseNextRecord(text, cursor, delimCode);
      if (result === null) {
        tail = text.slice(cursor);
        break;
      }

      const [fields, nextCursor] = result;
      cursor = nextCursor;

      if (isEmptyRecord(fields)) continue;

      if (autoHeaders && headers === null) {
        headers = fields;
        continue;
      }

      const resolved = headers ?? fields.map((_, i) => `_${i + 1}`);
      const row: Row = {};
      for (let i = 0; i < resolved.length; i++) {
        row[resolved[i]!] = fields[i] ?? null;
      }
      yield row;
    }
  }

  if (tail.length > 0) {
    const fields = parseLastRecord(tail, delimCode);
    if (fields !== null && !isEmptyRecord(fields)) {
      if (autoHeaders && headers === null) {
        return;
      }
      const resolved = headers ?? fields.map((_, i) => `_${i + 1}`);
      const row: Row = {};
      for (let i = 0; i < resolved.length; i++) {
        row[resolved[i]!] = fields[i] ?? null;
      }
      yield row;
    }
  }
}

function isEmptyRecord(fields: string[]): boolean {
  return fields.every((f) => f === "");
}

function parseNextRecord(
  text: string,
  start: number,
  delim: number,
): [string[], number] | null {
  const fields: string[] = [];
  let cursor = start;
  let fieldStart = cursor;
  let inQuotes = false;

  while (cursor < text.length) {
    const code = text.charCodeAt(cursor);

    if (inQuotes) {
      if (code === QUOTE) {
        if (cursor + 1 < text.length && text.charCodeAt(cursor + 1) === QUOTE) {
          cursor += 2;
          continue;
        }
        inQuotes = false;
        cursor += 1;
        continue;
      }
      cursor += 1;
      continue;
    }

    if (code === QUOTE && cursor === fieldStart) {
      inQuotes = true;
      cursor += 1;
      continue;
    }

    if (code === delim) {
      fields.push(extractField(text, fieldStart, cursor));
      cursor += 1;
      fieldStart = cursor;
      continue;
    }

    if (code === CR || code === LF) {
      fields.push(extractField(text, fieldStart, cursor));
      cursor += 1;
      if (code === CR && cursor < text.length && text.charCodeAt(cursor) === LF) {
        cursor += 1;
      }
      return [fields, cursor];
    }

    cursor += 1;
  }

  if (inQuotes) return null;

  return null;
}

function parseLastRecord(text: string, delim: number): string[] | null {
  const fields: string[] = [];
  let cursor = 0;
  let fieldStart = 0;
  let inQuotes = false;

  while (cursor < text.length) {
    const code = text.charCodeAt(cursor);

    if (inQuotes) {
      if (code === QUOTE) {
        if (cursor + 1 < text.length && text.charCodeAt(cursor + 1) === QUOTE) {
          cursor += 2;
          continue;
        }
        inQuotes = false;
        cursor += 1;
        continue;
      }
      cursor += 1;
      continue;
    }

    if (code === QUOTE && cursor === fieldStart) {
      inQuotes = true;
      cursor += 1;
      continue;
    }

    if (code === delim) {
      fields.push(extractField(text, fieldStart, cursor));
      cursor += 1;
      fieldStart = cursor;
      continue;
    }

    if (code === CR || code === LF) {
      fields.push(extractField(text, fieldStart, cursor));
      cursor += 1;
      if (code === CR && cursor < text.length && text.charCodeAt(cursor) === LF) {
        cursor += 1;
      }
      fieldStart = cursor;
      continue;
    }

    cursor += 1;
  }

  if (inQuotes) return null;
  fields.push(extractField(text, fieldStart, cursor));
  return fields;
}

function extractField(text: string, start: number, end: number): string {
  if (start >= end) return "";
  if (text.charCodeAt(start) === QUOTE && end > start + 1 && text.charCodeAt(end - 1) === QUOTE) {
    return text.slice(start + 1, end - 1).replace(/""/g, "\"");
  }
  return text.slice(start, end);
}

// --- Record scanner for drain (count-only) ---

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

export async function writeCsv(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<void> {
  const csv = format({ headers: options.headers ?? true, delimiter: options.delimiter ?? "," });
  csv.pipe(createWriteStream(destination));

  for await (const row of toAsync(rows)) {
    const ok = csv.write(row as never);
    if (!ok) await new Promise<void>((resolve) => csv.once("drain", resolve));
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
