import { readFile as readTextFile } from "node:fs/promises";
import { collectCsvViaEvents, drainCsvViaEvents, readCsv, writeCsv } from "../csv/index.js";
import { rowIdentity } from "../key.js";
import { finishStats, createStats, observeMemory } from "../perf/index.js";
import {
  applyRefinements,
  cleanRow,
  type InferSchema,
  type RowRefine,
  type SchemaDefinition,
  PravaahValidationError,
  validateRow,
} from "../schema/index.js";
import type { ProcessResult, ProcessStats, ReadOptions, Row, RowLike, PravaahIssue, WriteOptions } from "../types.js";
import { readXls } from "../xls/index.js";
import { readXlsx, writeXlsx } from "../xlsx/index.js";
import { readJsonl, writeJsonl } from "../jsonl/index.js";

const MEMORY_SAMPLE_INTERVAL_ROWS = 4096;

export interface PipelineFastPaths<T> {
  drain?: () => Promise<ProcessStats>;
  collect?: () => Promise<T[]>;
}

export interface ProgressEvent {
  rowsProcessed: number;
  peakRssBytes?: number;
}

interface SchemaOpOptions {
  validation?: ReadOptions["validation"];
  cleaning?: ReadOptions["cleaning"];
  refine?: RowRefine<Row> | RowRefine<Row>[];
}

type OpPlan =
  | { kind: "map"; fn: (row: unknown, index: number) => unknown | Promise<unknown> }
  | { kind: "filter"; fn: (row: unknown, index: number) => boolean | Promise<boolean> }
  | { kind: "clean"; options: NonNullable<ReadOptions["cleaning"]> }
  | { kind: "schema"; definition: SchemaDefinition; options: SchemaOpOptions }
  | { kind: "refine"; refiners: RowRefine<Row>[]; validation?: ReadOptions["validation"] }
  | { kind: "take"; limit: number };

type OpRuntime =
  | { kind: "map"; fn: (row: unknown, index: number) => unknown | Promise<unknown> }
  | { kind: "filter"; fn: (row: unknown, index: number) => boolean | Promise<boolean> }
  | { kind: "clean"; options: NonNullable<ReadOptions["cleaning"]>; seen: Set<string> }
  | {
      kind: "schema";
      definition: SchemaDefinition;
      options: SchemaOpOptions;
      rowNumber: number;
      seen: Set<string>;
    }
  | { kind: "refine"; refiners: RowRefine<Row>[]; validation?: ReadOptions["validation"]; rowNumber: number }
  | { kind: "take"; limit: number; count: number };

const SKIP = Symbol("pravaah.skip");
const STOP = Symbol("pravaah.stop");

function instantiate(plan: OpPlan): OpRuntime {
  if (plan.kind === "map") return { kind: "map", fn: plan.fn };
  if (plan.kind === "filter") return { kind: "filter", fn: plan.fn };
  if (plan.kind === "clean") return { kind: "clean", options: plan.options, seen: new Set() };
  if (plan.kind === "schema") {
    return { kind: "schema", definition: plan.definition, options: plan.options, rowNumber: 1, seen: new Set() };
  }
  if (plan.kind === "refine") {
    const runtime: OpRuntime & { kind: "refine" } = { kind: "refine", refiners: plan.refiners, rowNumber: 1 };
    if (plan.validation !== undefined) runtime.validation = plan.validation;
    return runtime;
  }
  return { kind: "take", limit: plan.limit, count: 0 };
}

export class PravaahPipeline<T = Row> implements AsyncIterable<T> {
  private readonly plans: OpPlan[] = [];
  private readonly progressHandlers: ((event: ProgressEvent) => void)[] = [];

  constructor(
    private readonly source: () => AsyncIterable<unknown>,
    private readonly fastPaths: PipelineFastPaths<T> = {},
  ) {}

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.buildIterator()[Symbol.asyncIterator]();
  }

  onProgress(handler: (event: ProgressEvent) => void): this {
    this.progressHandlers.push(handler);
    return this;
  }

  private emitProgress(stats: ProcessStats): void {
    if (this.progressHandlers.length === 0) return;
    const event: ProgressEvent = stats.peakRssBytes === undefined
      ? { rowsProcessed: stats.rowsProcessed }
      : { rowsProcessed: stats.rowsProcessed, peakRssBytes: stats.peakRssBytes };
    for (const handler of this.progressHandlers) handler(event);
  }

  private buildIterator(issueSink?: PravaahIssue[]): AsyncIterable<T> {
    if (this.plans.length === 0) return this.source() as AsyncIterable<T>;

    const runtime = this.plans.map(instantiate);
    const sink = issueSink ?? [];
    const src = this.source;

    return {
      async *[Symbol.asyncIterator]() {
        let index = 0;
        for await (const raw of src()) {
          const result = await runOps(raw, index, runtime, sink);
          index += 1;
          if (result === SKIP) continue;
          if (result === STOP) return;
          yield result as T;
        }
      },
    };
  }

  private clone<U>(fastPaths: PipelineFastPaths<U> = {}): PravaahPipeline<U> {
    const next = new PravaahPipeline<U>(this.source, fastPaths);
    next.plans.push(...this.plans);
    next.progressHandlers.push(...this.progressHandlers);
    return next;
  }

  map<U>(mapper: (row: T, index: number) => U | Promise<U>): PravaahPipeline<U> {
    const next = this.clone<U>();
    next.plans.push({ kind: "map", fn: mapper as unknown as (row: unknown, index: number) => unknown | Promise<unknown> });
    return next;
  }

  filter(predicate: (row: T, index: number) => boolean | Promise<boolean>): PravaahPipeline<T> {
    const next = this.clone<T>();
    next.plans.push({ kind: "filter", fn: predicate as unknown as (row: unknown, index: number) => boolean | Promise<boolean> });
    return next;
  }

  clean(options: NonNullable<ReadOptions["cleaning"]>): PravaahPipeline<T> {
    const next = this.clone<T>();
    next.plans.push({ kind: "clean", options });
    return next;
  }

  schema<S extends SchemaDefinition>(
    definition: S,
    options: Pick<ReadOptions, "validation" | "cleaning"> & { refine?: RowRefine<InferSchema<S>> | RowRefine<InferSchema<S>>[] } = {},
  ): PravaahPipeline<InferSchema<S>> {
    const next = this.clone<InferSchema<S>>();
    next.plans.push({
      kind: "schema",
      definition,
      options: options as SchemaOpOptions,
    });
    return next;
  }

  refine(refiner: RowRefine<T> | RowRefine<T>[]): PravaahPipeline<T> {
    const next = this.clone<T>();
    const refiners = (Array.isArray(refiner) ? refiner : [refiner]) as RowRefine<Row>[];
    const lastSchemaPlan = [...next.plans].reverse().find((plan) => plan.kind === "schema") as
      | Extract<OpPlan, { kind: "schema" }>
      | undefined;
    const validation = lastSchemaPlan?.options.validation;
    const plan: Extract<OpPlan, { kind: "refine" }> = { kind: "refine", refiners };
    if (validation !== undefined) plan.validation = validation;
    next.plans.push(plan);
    return next;
  }

  take(limit: number): PravaahPipeline<T> {
    const next = this.clone<T>();
    next.plans.push({ kind: "take", limit });
    return next;
  }

  async collect(): Promise<T[]> {
    if (this.plans.length === 0 && this.fastPaths.collect !== undefined) return this.fastPaths.collect();
    const rows: T[] = [];
    for await (const row of this.buildIterator()) rows.push(row);
    return rows;
  }

  async process(): Promise<ProcessResult<T>> {
    const stats = createStats();
    const rows: T[] = [];
    const issues: PravaahIssue[] = [];

    try {
      for await (const row of this.buildIterator(issues)) {
        rows.push(row);
        stats.rowsProcessed += 1;
        observeMemoryPeriodically(stats);
        if (stats.rowsProcessed % MEMORY_SAMPLE_INTERVAL_ROWS === 0) this.emitProgress(stats);
      }
    } catch (error) {
      if (error instanceof PravaahValidationError) {
        issues.push(...error.issues);
      } else {
        throw error;
      }
    }

    stats.errors = issues.length;
    const final = finishStats(stats);
    this.emitProgress(final);
    return { rows, issues, stats: final };
  }

  async drain(): Promise<ProcessStats> {
    if (this.plans.length === 0 && this.fastPaths.drain !== undefined) return this.fastPaths.drain();
    const stats = createStats();
    const issues: PravaahIssue[] = [];

    try {
      for await (const row of this.buildIterator(issues)) {
        void row;
        stats.rowsProcessed += 1;
        observeMemoryPeriodically(stats);
        if (stats.rowsProcessed % MEMORY_SAMPLE_INTERVAL_ROWS === 0) this.emitProgress(stats);
      }
    } catch (error) {
      if (error instanceof PravaahValidationError) {
        issues.push(...error.issues);
      } else {
        throw error;
      }
    }

    stats.errors = issues.length;
    const final = finishStats(stats);
    this.emitProgress(final);
    return final;
  }

  async write(destination: string, options: WriteOptions = {}): Promise<ProcessStats> {
    return write(this.buildIterator() as AsyncIterable<RowLike>, destination, options);
  }
}

async function runOps(
  value: unknown,
  index: number,
  ops: OpRuntime[],
  issueSink: PravaahIssue[],
): Promise<unknown | typeof SKIP | typeof STOP> {
  let current: unknown = value;
  for (const op of ops) {
    if (op.kind === "map") {
      current = await op.fn(current, index);
    } else if (op.kind === "filter") {
      const keep = await op.fn(current, index);
      if (!keep) return SKIP;
    } else if (op.kind === "clean") {
      if (!isRow(current)) continue;
      current = cleanRow(current, op.options);
      if (isDuplicate(current as Row, op.options.dedupeKey, op.seen)) return SKIP;
    } else if (op.kind === "schema") {
      if (!isRow(current)) {
        throw new PravaahValidationError([
          {
            code: "array_row",
            message: "Schema validation expects object rows with named columns",
            rowNumber: op.rowNumber,
            severity: "error",
          },
        ]);
      }
      const cleaned = cleanRow(current, op.options.cleaning);
      if (isDuplicate(cleaned, op.options.cleaning?.dedupeKey, op.seen)) {
        op.rowNumber += 1;
        return SKIP;
      }
      const context = { rowNumber: op.rowNumber };
      op.rowNumber += 1;
      const result = validateRow(cleaned, op.definition, context);
      if (result.value === undefined) {
        if (op.options.validation === "fail-fast") throw new PravaahValidationError(result.issues);
        if (op.options.validation !== "skip") issueSink.push(...result.issues);
        return SKIP;
      }
      if (op.options.refine !== undefined) {
        const refineIssues = applyRefinements(result.value as unknown as Row, cleaned, context, op.options.refine);
        if (refineIssues.length > 0) {
          if (op.options.validation === "fail-fast") throw new PravaahValidationError(refineIssues);
          if (op.options.validation !== "skip") issueSink.push(...refineIssues);
          return SKIP;
        }
      }
      current = result.value;
    } else if (op.kind === "refine") {
      const context = { rowNumber: op.rowNumber };
      op.rowNumber += 1;
      const rowForContext: Row = isRow(current) ? current : {};
      const issues = applyRefinements<unknown>(current, rowForContext, context, op.refiners as RowRefine<unknown>[]);
      if (issues.length > 0) {
        if (op.validation === "fail-fast") throw new PravaahValidationError(issues);
        if (op.validation !== "skip") issueSink.push(...issues);
        return SKIP;
      }
    } else if (op.kind === "take") {
      if (op.count >= op.limit) return STOP;
      op.count += 1;
    }
  }
  return current;
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
  if (format === "xls") return new PravaahPipeline(() => readXls(source, options));
  if (format === "jsonl") return new PravaahPipeline(() => readJsonl(source));
  if (format === "json") {
    return new PravaahPipeline(async function* () {
      const text = Buffer.isBuffer(source)
        ? source.toString("utf8")
        : await readTextFile(source, "utf8");
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
  else if (format === "jsonl") await writeJsonl(counted, destination, options);
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
  const schemaOptions: Pick<ReadOptions, "validation" | "cleaning"> = {};
  if (options.validation !== undefined) schemaOptions.validation = options.validation;
  if (options.cleaning !== undefined) schemaOptions.cleaning = options.cleaning;
  const pipeline = read(source, options).schema(definition, schemaOptions);
  if (options.validation === "fail-fast") {
    const rows: InferSchema<S>[] = [];
    const stats = createStats();
    for await (const row of pipeline) {
      rows.push(row);
      stats.rowsProcessed += 1;
      observeMemoryPeriodically(stats);
    }
    return { rows, issues: [], stats: finishStats(stats) };
  }
  return pipeline.process();
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

function inferFormat(path?: string): "xlsx" | "xls" | "csv" | "json" | "jsonl" {
  if (path === undefined) throw new Error("Unable to infer format");
  const lower = path.toLowerCase();
  const stripped = lower.endsWith(".gz") ? lower.slice(0, -3) : lower;
  if (stripped.endsWith(".csv")) return "csv";
  if (stripped.endsWith(".jsonl") || stripped.endsWith(".ndjson")) return "jsonl";
  if (stripped.endsWith(".json")) return "json";
  if (stripped.endsWith(".xlsx")) return "xlsx";
  if (stripped.endsWith(".xls")) return "xls";
  throw new Error(`Unable to infer format from path: ${path}`);
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

function isDuplicate(row: Row, dedupeKey: string | string[] | undefined, seen: Set<string>): boolean {
  if (dedupeKey === undefined) return false;
  const identity = rowIdentity(row, dedupeKey);
  if (seen.has(identity)) return true;
  seen.add(identity);
  return false;
}
