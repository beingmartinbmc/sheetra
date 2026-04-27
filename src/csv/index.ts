import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { format } from "@fast-csv/format";
import { parse } from "@fast-csv/parse";
import type { CellValue, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

export async function* readCsv(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const headers = options.headers ?? true;
  const inferTypes = options.inferTypes ?? false;
  const stream =
    typeof source === "string"
      ? createReadStream(source)
      : Readable.from(source);

  const parser = parse({
    headers,
    delimiter: options.delimiter ?? ",",
    ignoreEmpty: true,
    trim: false,
  });

  const input = stream.pipe(parser);
  for await (const row of input) {
    if (Array.isArray(row)) {
      yield arrayRowToObject(row, inferTypes);
    } else if (inferTypes) {
      yield inferObjectValues(row as Record<string, string>);
    } else {
      yield row as Row;
    }
  }
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

