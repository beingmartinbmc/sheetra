import { Buffer } from "node:buffer";
import { parseDetailed } from "../pipeline/index.js";
import type { InferSchema, SchemaDefinition } from "../schema/index.js";
import type { PravaahFormat, ProcessResult, ReadOptions } from "../types.js";

export interface UploadMeta {
  /** Original filename, used to infer the format when `format` is omitted. */
  filename?: string;
  /** Explicit format override; takes precedence over `filename`. */
  format?: PravaahFormat;
}

type UploadSource =
  | NodeJS.ReadableStream
  | AsyncIterable<Buffer | Uint8Array | string>
  | Buffer
  | Uint8Array;

/**
 * Stream a file upload (multer/busboy/raw request stream) straight into
 * {@link parseDetailed}, returning clean typed rows plus an issue report and
 * stats. The format is taken from `meta.format`, else inferred from the
 * filename extension.
 */
export async function parseUpload<S extends SchemaDefinition>(
  source: UploadSource,
  meta: UploadMeta,
  definition: S,
  options: Omit<ReadOptions, "format"> = {},
): Promise<ProcessResult<InferSchema<S>>> {
  const format = meta.format ?? formatFromFilename(meta.filename);
  if (format === undefined) {
    throw new Error(
      `Unsupported upload: could not infer format from "${meta.filename ?? "<unknown>"}"; pass meta.format explicitly`,
    );
  }

  const buffer = await collect(source);
  return parseDetailed(buffer, definition, { ...options, format });
}

function formatFromFilename(filename: string | undefined): PravaahFormat | undefined {
  if (filename === undefined) return undefined;
  const lower = filename.toLowerCase();
  if (lower.endsWith(".csv") || lower.endsWith(".csv.gz")) return "csv";
  if (lower.endsWith(".jsonl") || lower.endsWith(".jsonl.gz") || lower.endsWith(".ndjson")) return "jsonl";
  if (lower.endsWith(".json")) return "json";
  if (lower.endsWith(".xlsx")) return "xlsx";
  if (lower.endsWith(".xls")) return "xls";
  return undefined;
}

async function collect(source: UploadSource): Promise<Buffer> {
  if (Buffer.isBuffer(source)) return source;
  if (source instanceof Uint8Array) return Buffer.from(source);

  const chunks: Buffer[] = [];
  for await (const chunk of source as AsyncIterable<Buffer | Uint8Array | string>) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
