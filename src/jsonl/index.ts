import { createReadStream, createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import { createGunzip, createGzip } from "node:zlib";
import type { Row, RowLike, WriteOptions } from "../types.js";

export async function* readJsonl(source: string | Buffer): AsyncIterable<Row> {
  const base = typeof source === "string" ? createReadStream(source) : Readable.from(source);
  const stream = shouldGunzip(source) ? base.pipe(createGunzip()) : base;
  const decoder = new StringDecoder("utf8");
  let buffer = "";

  for await (const chunk of stream as AsyncIterable<Buffer | string>) {
    buffer += typeof chunk === "string" ? chunk : decoder.write(chunk);
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line.length > 0) yield parseLine(line);
      newlineIndex = buffer.indexOf("\n");
    }
  }

  const tail = (buffer + decoder.end()).trim();
  if (tail.length > 0) yield parseLine(tail);
}

export async function writeJsonl(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<void> {
  const useGzip = options.gzip ?? destination.toLowerCase().endsWith(".gz");
  const fileStream = createWriteStream(destination);
  const writeTarget: NodeJS.WritableStream = useGzip
    ? (() => {
        const gzip = createGzip();
        gzip.pipe(fileStream);
        return gzip;
      })()
    : fileStream;

  for await (const row of toAsync(rows)) {
    const line = `${JSON.stringify(row, dateReplacer)}\n`;
    if (!writeTarget.write(line)) await new Promise<void>((resolve) => writeTarget.once("drain", resolve));
  }

  writeTarget.end();
  await finished(fileStream);
}

function parseLine(line: string): Row {
  const parsed = JSON.parse(line);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`JSONL lines must be objects, got: ${line.slice(0, 60)}`);
  }
  return parsed as Row;
}

function shouldGunzip(source: string | Buffer): boolean {
  if (typeof source === "string") return source.toLowerCase().endsWith(".gz");
  if (Buffer.isBuffer(source) && source.length >= 2 && source[0] === 0x1f && source[1] === 0x8b) return true;
  return false;
}

function dateReplacer(_key: string, value: unknown): unknown {
  return value instanceof Date ? value.toISOString() : value;
}

async function* toAsync<T>(rows: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
  yield* rows;
}
