import { readCsv, writeCsv } from "../csv/index.js";
import { finishStats, createStats, observeMemory } from "../perf/index.js";
import { cleanRow, type InferSchema, type SchemaDefinition, SheetraValidationError, validateRow } from "../schema/index.js";
import type { ProcessResult, ProcessStats, ReadOptions, Row, RowLike, SheetraIssue, WriteOptions } from "../types.js";
import { readXlsx, writeXlsx } from "../xlsx/index.js";

export class SheetraPipeline<T = Row> implements AsyncIterable<T> {
  constructor(private readonly source: () => AsyncIterable<T>) {}

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.source()[Symbol.asyncIterator]();
  }

  map<U>(mapper: (row: T, index: number) => U | Promise<U>): SheetraPipeline<U> {
    return new SheetraPipeline(async function* (this: SheetraPipeline<T>) {
      let index = 0;
      for await (const row of this) {
        yield mapper(row, index);
        index += 1;
      }
    }.bind(this));
  }

  filter(predicate: (row: T, index: number) => boolean | Promise<boolean>): SheetraPipeline<T> {
    return new SheetraPipeline(async function* (this: SheetraPipeline<T>) {
      let index = 0;
      for await (const row of this) {
        if (await predicate(row, index)) yield row;
        index += 1;
      }
    }.bind(this));
  }

  clean(options: NonNullable<ReadOptions["cleaning"]>): SheetraPipeline<T> {
    return this.map((row) => (isRow(row) ? cleanRow(row, options) : row) as T);
  }

  schema<S extends SchemaDefinition>(
    definition: S,
    options: Pick<ReadOptions, "validation" | "cleaning"> = {},
  ): SheetraPipeline<InferSchema<S>> {
    return new SheetraPipeline(async function* (this: SheetraPipeline<T>) {
      let rowNumber = 1;
      for await (const row of this) {
        if (!isRow(row)) {
          throw new SheetraValidationError([
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
          throw new SheetraValidationError(result.issues);
        } else if (options.validation !== "skip") {
          for (const issue of result.issues) process.emitWarning(issue.message, { code: issue.code });
        }
        rowNumber += 1;
      }
    }.bind(this));
  }

  take(limit: number): SheetraPipeline<T> {
    return new SheetraPipeline(async function* (this: SheetraPipeline<T>) {
      let count = 0;
      for await (const row of this) {
        if (count >= limit) break;
        yield row;
        count += 1;
      }
    }.bind(this));
  }

  async collect(): Promise<T[]> {
    const rows: T[] = [];
    for await (const row of this) rows.push(row);
    return rows;
  }

  async process(): Promise<ProcessResult<T>> {
    const stats = createStats();
    const rows: T[] = [];
    const issues: SheetraIssue[] = [];

    try {
      for await (const row of this) {
        rows.push(row);
        stats.rowsProcessed += 1;
        observeMemory(stats);
      }
    } catch (error) {
      if (error instanceof SheetraValidationError) {
        issues.push(...error.issues);
        stats.errors += error.issues.length;
      } else {
        throw error;
      }
    }

    return { rows, issues, stats: finishStats(stats) };
  }

  async drain(): Promise<ProcessStats> {
    const stats = createStats();

    try {
      for await (const row of this) {
        void row;
        stats.rowsProcessed += 1;
        observeMemory(stats);
      }
    } catch (error) {
      if (error instanceof SheetraValidationError) {
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
): SheetraPipeline<RowLike> {
  if (isIterableSource(source)) {
    return new SheetraPipeline(async function* () {
      yield* source;
    });
  }

  const format = options.format ?? inferFormat(typeof source === "string" ? source : undefined);
  if (format === "csv") return new SheetraPipeline(() => readCsv(source, options));
  if (format === "xlsx") return new SheetraPipeline(() => readXlsx(source, options));
  if (format === "json") {
    return new SheetraPipeline(async function* () {
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
    observeMemory(stats);
    yield row;
  }
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
