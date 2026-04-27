import { collectCsvViaEvents, drainCsvViaEvents, readCsv, writeCsv } from "../csv/index.js";
import { finishStats, createStats, observeMemory } from "../perf/index.js";
import { cleanRow, type InferSchema, type SchemaDefinition, PravaahValidationError, validateRow } from "../schema/index.js";
import type { ProcessResult, ProcessStats, ReadOptions, Row, RowLike, PravaahIssue, WriteOptions } from "../types.js";
import { readXlsx, writeXlsx } from "../xlsx/index.js";

const MEMORY_SAMPLE_INTERVAL_ROWS = 4096;

export interface PipelineFastPaths<T> {
  drain?: () => Promise<ProcessStats>;
  collect?: () => Promise<T[]>;
}

type FusedOp<T, U> =
  | { kind: "map"; fn: (row: T, index: number) => U | Promise<U> }
  | { kind: "filter"; fn: (row: T, index: number) => boolean | Promise<boolean> };

export class PravaahPipeline<T = Row> implements AsyncIterable<T> {
  private readonly pendingOps: FusedOp<unknown, unknown>[] = [];

  constructor(
    private readonly source: () => AsyncIterable<T>,
    private readonly fastPaths: PipelineFastPaths<T> = {},
  ) {}

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.buildIterator()[Symbol.asyncIterator]();
  }

  private buildIterator(): AsyncIterable<T> {
    if (this.pendingOps.length === 0) return this.source();

    const ops = [...this.pendingOps];
    const src = this.source;

    return {
      async *[Symbol.asyncIterator]() {
        let index = 0;
        outer:
        for await (const raw of src()) {
          let current: unknown = raw;
          for (const op of ops) {
            if (op.kind === "map") {
              current = await (op.fn as (row: unknown, i: number) => unknown)(current, index);
            } else {
              const keep = await (op.fn as (row: unknown, i: number) => boolean | Promise<boolean>)(current, index);
              if (!keep) {
                index += 1;
                continue outer;
              }
            }
          }
          yield current as T;
          index += 1;
        }
      },
    };
  }

  map<U>(mapper: (row: T, index: number) => U | Promise<U>): PravaahPipeline<U> {
    const next = new PravaahPipeline<U>(this.source as unknown as () => AsyncIterable<U>);
    next.pendingOps.push(...this.pendingOps);
    next.pendingOps.push({ kind: "map", fn: mapper as unknown as (row: unknown, index: number) => unknown });
    return next;
  }

  filter(predicate: (row: T, index: number) => boolean | Promise<boolean>): PravaahPipeline<T> {
    const next = new PravaahPipeline<T>(this.source);
    next.pendingOps.push(...this.pendingOps);
    next.pendingOps.push({ kind: "filter", fn: predicate as unknown as (row: unknown, index: number) => boolean | Promise<boolean> });
    return next;
  }

  clean(options: NonNullable<ReadOptions["cleaning"]>): PravaahPipeline<T> {
    return this.map((row) => (isRow(row) ? cleanRow(row, options) : row) as T);
  }

  schema<S extends SchemaDefinition>(
    definition: S,
    options: Pick<ReadOptions, "validation" | "cleaning"> = {},
  ): PravaahPipeline<InferSchema<S>> {
    const iterate = () => this.buildIterator();
    return new PravaahPipeline(async function* () {
      let rowNumber = 1;
      for await (const row of iterate()) {
        if (!isRow(row)) {
          throw new PravaahValidationError([
            {
              code: "array_row",
              message: "Schema validation expects object rows with named columns",
              rowNumber,
              severity: "error",
            },
          ]);
        }

        const cleaned = cleanRow(row, options.cleaning);
        const result = validateRow(cleaned, definition, { rowNumber });
        if (result.value !== undefined) {
          yield result.value;
        } else if (options.validation === "fail-fast") {
          throw new PravaahValidationError(result.issues);
        } else if (options.validation !== "skip") {
          for (const issue of result.issues) process.emitWarning(issue.message, { code: issue.code });
        }
        rowNumber += 1;
      }
    });
  }

  take(limit: number): PravaahPipeline<T> {
    const iterate = () => this.buildIterator();
    return new PravaahPipeline(async function* () {
      let count = 0;
      for await (const row of iterate()) {
        if (count >= limit) break;
        yield row;
        count += 1;
      }
    });
  }

  async collect(): Promise<T[]> {
    if (this.pendingOps.length === 0 && this.fastPaths.collect !== undefined) return this.fastPaths.collect();
    const rows: T[] = [];
    for await (const row of this) rows.push(row);
    return rows;
  }

  async process(): Promise<ProcessResult<T>> {
    const stats = createStats();
    const rows: T[] = [];
    const issues: PravaahIssue[] = [];

    try {
      for await (const row of this) {
        rows.push(row);
        stats.rowsProcessed += 1;
        observeMemoryPeriodically(stats);
      }
    } catch (error) {
      if (error instanceof PravaahValidationError) {
        issues.push(...error.issues);
        stats.errors += error.issues.length;
      } else {
        throw error;
      }
    }

    return { rows, issues, stats: finishStats(stats) };
  }

  async drain(): Promise<ProcessStats> {
    if (this.pendingOps.length === 0 && this.fastPaths.drain !== undefined) return this.fastPaths.drain();
    const stats = createStats();

    try {
      for await (const row of this) {
        void row;
        stats.rowsProcessed += 1;
        observeMemoryPeriodically(stats);
      }
    } catch (error) {
      if (error instanceof PravaahValidationError) {
        stats.errors += error.issues.length;
      } else {
        throw error;
      }
    }

    return finishStats(stats);
  }

  async write(destination: string, options: WriteOptions = {}): Promise<ProcessStats> {
    return write(this as AsyncIterable<RowLike>, destination, options);
  }
}

export function read(
  source: string | Buffer | AsyncIterable<RowLike> | Iterable<RowLike>,
  options: ReadOptions = {},
): PravaahPipeline<RowLike> {
  if (isIterableSource(source)) {
    return new PravaahPipeline(async function* () {
      yield* source;
    });
  }

  const format = options.format ?? inferFormat(typeof source === "string" ? source : undefined);
  if (format === "csv") {
    return new PravaahPipeline(() => readCsv(source, options), {
      drain: () => drainCsvViaEvents(source, options),
      collect: () => collectCsvViaEvents(source, options) as Promise<RowLike[]>,
    });
  }
  if (format === "xlsx") return new PravaahPipeline(() => readXlsx(source, options));
  if (format === "json") {
    return new PravaahPipeline(async function* () {
      const text = Buffer.isBuffer(source)
        ? source.toString("utf8")
        : await import("node:fs/promises").then((fs) => fs.readFile(source, "utf8"));
      const data = JSON.parse(text) as Row[];
      yield* data;
    });
  }

  throw new Error(`Unsupported read format: ${format}`);
}

export async function write(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<ProcessStats> {
  const stats = createStats();
  const counted = countRows(rows, stats);
  const format = options.format ?? inferFormat(destination);

  if (format === "csv") await writeCsv(counted, destination, options);
  else if (format === "xlsx") await writeXlsx(counted, destination, options);
  else if (format === "json") await writeJson(counted, destination);
  else throw new Error(`Unsupported write format: ${format}`);

  return finishStats(stats);
}

export async function parse<S extends SchemaDefinition>(
  source: string | Buffer,
  definition: S,
  options: ReadOptions = {},
): Promise<InferSchema<S>[]> {
  const schemaOptions: Pick<ReadOptions, "validation" | "cleaning"> = {};
  if (options.validation !== undefined) schemaOptions.validation = options.validation;
  if (options.cleaning !== undefined) schemaOptions.cleaning = options.cleaning;
  return read(source, options).schema(definition, schemaOptions).collect();
}

export async function parseDetailed<S extends SchemaDefinition>(
  source: string | Buffer,
  definition: S,
  options: ReadOptions = {},
): Promise<ProcessResult<InferSchema<S>>> {
  const stats = createStats();
  const rows: InferSchema<S>[] = [];
  const issues: PravaahIssue[] = [];
  let rowNumber = 1;

  for await (const row of read(source, options)) {
    if (!isRow(row)) {
      const issue: PravaahIssue = {
        code: "array_row",
        message: "Schema validation expects object rows with named columns",
        rowNumber,
        severity: "error",
      };
      issues.push(issue);
      stats.errors += 1;
      if (options.validation === "fail-fast") throw new PravaahValidationError([issue]);
      rowNumber += 1;
      continue;
    }

    const cleaned = cleanRow(row, options.cleaning);
    const result = validateRow(cleaned, definition, { rowNumber });
    stats.rowsProcessed += 1;
    observeMemoryPeriodically(stats);

    if (result.value !== undefined) {
      rows.push(result.value);
    } else {
      issues.push(...result.issues);
      stats.errors += result.issues.length;
      if (options.validation === "fail-fast") throw new PravaahValidationError(result.issues);
    }

    rowNumber += 1;
  }

  return { rows, issues, stats: finishStats(stats) };
}

async function writeJson(rows: AsyncIterable<RowLike>, destination: string): Promise<void> {
  const { writeFile } = await import("node:fs/promises");
  const data: RowLike[] = [];
  for await (const row of rows) data.push(row);
  await writeFile(destination, `${JSON.stringify(data, null, 2)}\n`);
}

async function* countRows<T extends RowLike>(rows: AsyncIterable<T> | Iterable<T>, stats: ProcessStats): AsyncIterable<T> {
  for await (const row of rows) {
    stats.rowsProcessed += 1;
    stats.rowsWritten += 1;
    observeMemoryPeriodically(stats);
    yield row;
  }
}

function observeMemoryPeriodically(stats: ProcessStats): void {
  if (stats.rowsProcessed % MEMORY_SAMPLE_INTERVAL_ROWS === 0) observeMemory(stats);
}

function inferFormat(path?: string): "xlsx" | "csv" | "json" {
  if (path?.endsWith(".csv")) return "csv";
  if (path?.endsWith(".json")) return "json";
  return "xlsx";
}

function isIterableSource(value: unknown): value is AsyncIterable<RowLike> | Iterable<RowLike> {
  if (typeof value === "string" || Buffer.isBuffer(value)) return false;
  return (
    typeof (value as AsyncIterable<RowLike>)[Symbol.asyncIterator] === "function" ||
    typeof (value as Iterable<RowLike>)[Symbol.iterator] === "function"
  );
}

function isRow(value: unknown): value is Row {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
