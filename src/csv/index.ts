import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { format } from "@fast-csv/format";
import { parse } from "@fast-csv/parse";
import type { CellValue, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

export async function* readCsv(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const headers = options.headers ?? true;
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
      yield Object.fromEntries(row.map((value, index) => [`_${index + 1}`, normalizeCsvValue(value)]));
    } else {
      yield Object.fromEntries(
        Object.entries(row as Record<string, string>).map(([key, value]) => [key, normalizeCsvValue(value)]),
      );
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

