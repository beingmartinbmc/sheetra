export type PrimitiveCell = string | number | boolean | Date | null;
export type CellValue = unknown;
export type Row = Record<string, unknown>;
export type RowLike = Row | unknown[];

export type SheetraFormat = "xlsx" | "csv" | "json";
export type ValidationMode = "fail-fast" | "collect" | "skip";

export interface RowContext {
  rowNumber: number;
  sheetName?: string;
  source?: string;
}

export interface SheetraIssue {
  code: string;
  message: string;
  rowNumber?: number;
  column?: string;
  rawValue?: unknown;
  expected?: string;
  severity: "warning" | "error";
}

export interface ProcessStats {
  rowsProcessed: number;
  rowsWritten: number;
  errors: number;
  warnings: number;
  startedAt: number;
  endedAt?: number;
  durationMs?: number;
  peakRssBytes?: number;
  sheets: string[];
}

export interface CleaningOptions {
  trim?: boolean;
  normalizeWhitespace?: boolean;
  dedupeKey?: string | string[];
  fuzzyHeaders?: Record<string, string[]>;
}

export interface ReadOptions {
  format?: SheetraFormat;
  sheet?: string | number;
  headers?: boolean | string[];
  delimiter?: string;
  formulas?: "values" | "preserve";
  validation?: ValidationMode;
  cleaning?: CleaningOptions;
}

export interface WriteOptions {
  format?: SheetraFormat;
  sheetName?: string;
  headers?: string[];
  delimiter?: string;
}

export interface ProcessResult<T = Row> {
  rows: T[];
  issues: SheetraIssue[];
  stats: ProcessStats;
}

export interface Writer<T = RowLike> {
  write(rows: AsyncIterable<T> | Iterable<T>, destination: string, options?: WriteOptions): Promise<ProcessStats>;
}

export interface Reader<T = Row> {
  read(source: string | Buffer, options?: ReadOptions): AsyncIterable<T>;
}
