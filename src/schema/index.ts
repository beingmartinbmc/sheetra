import { writeFile } from "node:fs/promises";
import { rowIdentity } from "../key.js";
import type { CellValue, CleaningOptions, Row, RowContext, PravaahIssue, ValidationMode } from "../types.js";

export type FieldKind =
  | "string"
  | "number"
  | "integer"
  | "boolean"
  | "date"
  | "email"
  | "phone"
  | "url"
  | "uuid"
  | "enum"
  | "regex"
  | "literal"
  | "array"
  | "any";

export interface FieldConstraints {
  min?: number;
  max?: number;
  length?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: RegExp;
}

export interface FieldDefinition<T = unknown> extends FieldConstraints {
  kind: FieldKind;
  optional?: boolean;
  defaultValue?: T;
  coerce?: boolean;
  transform?: (raw: unknown, row: Row, context: RowContext) => unknown;
  values?: readonly unknown[];
  of?: FieldKind | FieldDefinition;
  validate?: { bivarianceHack(value: T, row: Row, context: RowContext): string | void | Promise<string | void> }["bivarianceHack"];
}

export type SchemaDefinition = Record<string, FieldKind | FieldDefinition>;

export type InferField<T> = T extends "string"
  ? string
  : T extends "number" | "integer"
    ? number
    : T extends "boolean"
      ? boolean
      : T extends "date"
        ? Date
        : T extends "email" | "phone" | "url" | "uuid"
          ? string
          : T extends FieldDefinition<infer U>
            ? U
            : CellValue;

export type InferSchema<T extends SchemaDefinition> = {
  [K in keyof T]: InferField<T[K]>;
};

export interface ValidationOptions {
  mode?: ValidationMode;
  cleaning?: CleaningOptions;
  refine?: RowRefine<Row> | RowRefine<Row>[];
}

export type RowRefine<T> = (
  row: T,
  context: RowContext,
) => string | { column?: string; message: string; code?: string } | void;

export class PravaahValidationError extends Error {
  constructor(public readonly issues: PravaahIssue[]) {
    super(`Pravaah validation failed with ${issues.length} issue${issues.length === 1 ? "" : "s"}`);
  }
}

type SchemaBuilders = {
  string(options?: Omit<FieldDefinition<string>, "kind">): FieldDefinition<string>;
  number(options?: Omit<FieldDefinition<number>, "kind">): FieldDefinition<number>;
  integer(options?: Omit<FieldDefinition<number>, "kind">): FieldDefinition<number>;
  boolean(options?: Omit<FieldDefinition<boolean>, "kind">): FieldDefinition<boolean>;
  date(options?: Omit<FieldDefinition<Date>, "kind">): FieldDefinition<Date>;
  email(options?: Omit<FieldDefinition<string>, "kind">): FieldDefinition<string>;
  phone(options?: Omit<FieldDefinition<string>, "kind">): FieldDefinition<string>;
  url(options?: Omit<FieldDefinition<string>, "kind">): FieldDefinition<string>;
  uuid(options?: Omit<FieldDefinition<string>, "kind">): FieldDefinition<string>;
  any(options?: Omit<FieldDefinition<CellValue>, "kind">): FieldDefinition<CellValue>;
  enum<V extends string | number>(values: readonly V[], options?: Omit<FieldDefinition<V>, "kind" | "values">): FieldDefinition<V>;
  literal<V extends string | number | boolean>(value: V, options?: Omit<FieldDefinition<V>, "kind" | "values">): FieldDefinition<V>;
  regex(pattern: RegExp, options?: Omit<FieldDefinition<string>, "kind" | "pattern">): FieldDefinition<string>;
  array<V>(of: FieldKind | FieldDefinition, options?: Omit<FieldDefinition<V[]>, "kind" | "of">): FieldDefinition<V[]>;
};

export const schema: SchemaBuilders = {
  string(options = {}) {
    return { kind: "string", coerce: true, ...options };
  },
  number(options = {}) {
    return { kind: "number", coerce: true, ...options };
  },
  integer(options = {}) {
    return { kind: "integer", coerce: true, ...options };
  },
  boolean(options = {}) {
    return { kind: "boolean", coerce: true, ...options };
  },
  date(options = {}) {
    return { kind: "date", coerce: true, ...options };
  },
  email(options = {}) {
    return { kind: "email", coerce: true, ...options };
  },
  phone(options = {}) {
    return { kind: "phone", coerce: true, ...options };
  },
  url(options = {}) {
    return { kind: "url", coerce: true, ...options };
  },
  uuid(options = {}) {
    return { kind: "uuid", coerce: true, ...options };
  },
  any(options = {}) {
    return { kind: "any", ...options };
  },
  enum(values, options = {}) {
    return { kind: "enum", values, ...options } as FieldDefinition<(typeof values)[number]>;
  },
  literal(value, options = {}) {
    return { kind: "literal", values: [value], ...options } as FieldDefinition<typeof value>;
  },
  regex(pattern, options = {}) {
    return { kind: "regex", pattern, coerce: true, ...options };
  },
  array<V>(of: FieldKind | FieldDefinition, options: Omit<FieldDefinition<V[]>, "kind" | "of"> = {}): FieldDefinition<V[]> {
    return { kind: "array", of, ...options } as unknown as FieldDefinition<V[]>;
  },
};

export function normalizeHeader(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

export function applyFuzzyHeaders(row: Row, aliases: Record<string, string[]> = {}): Row {
  if (Object.keys(aliases).length === 0) return row;
  const normalizedEntries = new Map(Object.keys(row).map((key) => [normalizeHeader(key), key]));
  let next: Row | undefined;

  for (const [canonical, names] of Object.entries(aliases)) {
    if (canonical in row || (next !== undefined && canonical in next)) continue;

    for (const name of names) {
      const existing = normalizedEntries.get(normalizeHeader(name));
      if (existing !== undefined) {
        next ??= { ...row };
        next[canonical] = next[existing] ?? null;
        break;
      }
    }
  }

  return next ?? row;
}

export function cleanRow(row: Row, options: CleaningOptions = {}): Row {
  const withHeaders = applyFuzzyHeaders(row, options.fuzzyHeaders);
  if (!options.trim && !options.normalizeWhitespace) return withHeaders;
  const cleaned: Row = {};
  let changed = withHeaders !== row;

  for (const [key, value] of Object.entries(withHeaders)) {
    if (typeof value === "string") {
      let next = value;
      if (options.trim) next = next.trim();
      if (options.normalizeWhitespace) next = next.replace(/\s+/g, " ");
      cleaned[key] = next;
      if (next !== value) changed = true;
    } else {
      cleaned[key] = value;
    }
  }

  return changed ? cleaned : row;
}

export function cleanRows(rows: Iterable<Row>, options: CleaningOptions = {}): Row[] {
  const seen = new Set<string>();
  const output: Row[] = [];

  for (const row of rows) {
    const cleaned = cleanRow(row, options);
    if (options.dedupeKey === undefined) {
      output.push(cleaned);
      continue;
    }

    const identity = rowIdentity(cleaned, options.dedupeKey);
    if (!seen.has(identity)) {
      seen.add(identity);
      output.push(cleaned);
    }
  }

  return output;
}

export function validateRow<T extends SchemaDefinition>(
  row: Row,
  definition: T,
  context: RowContext,
): { value?: InferSchema<T>; issues: PravaahIssue[] } {
  const issues: PravaahIssue[] = [];
  const parsed: Record<string, unknown> = {};

  for (const [key, rawDefinition] of Object.entries(definition)) {
    const field = typeof rawDefinition === "string" ? { kind: rawDefinition, coerce: true } : rawDefinition;
    let raw = row[key];

    if (field.transform !== undefined && raw !== undefined && raw !== null && raw !== "") {
      try {
        raw = field.transform(raw, row, context);
      } catch (error) {
        issues.push(issue("transform_failed", (error as Error).message ?? "transform failed", context, key, row[key], field.kind));
        continue;
      }
    }

    if (raw === undefined || raw === null || raw === "") {
      if (field.defaultValue !== undefined) {
        parsed[key] = field.defaultValue;
      } else if (field.optional) {
        parsed[key] = undefined;
      } else {
        issues.push(issue("missing_column", `${key} is required`, context, key, raw, field.kind));
      }
      continue;
    }

    const coerced = coerceValue(raw, field);
    if (!coerced.ok) {
      issues.push(issue("invalid_type", `${key} must be ${describeKind(field)}`, context, key, raw, describeKind(field)));
      continue;
    }

    const constraintIssue = checkConstraints(coerced.value, field);
    if (constraintIssue !== undefined) {
      issues.push(issue("constraint_failed", `${key} ${constraintIssue}`, context, key, raw, describeKind(field)));
      continue;
    }

    const customIssue = field.validate?.(coerced.value as never, row, context);
    if (typeof customIssue === "string") {
      issues.push(issue("invalid_value", customIssue, context, key, raw, describeKind(field)));
      continue;
    }

    parsed[key] = coerced.value;
  }

  return issues.length > 0 ? { issues } : { value: parsed as InferSchema<T>, issues };
}

export async function validateRowAsync<T extends SchemaDefinition>(
  row: Row,
  definition: T,
  context: RowContext,
): Promise<{ value?: InferSchema<T>; issues: PravaahIssue[] }> {
  const sync = validateRow(row, definition, context);
  if (sync.issues.length > 0 || sync.value === undefined) return sync;

  for (const [key, rawDefinition] of Object.entries(definition)) {
    const field = typeof rawDefinition === "string" ? { kind: rawDefinition, coerce: true } : rawDefinition;
    if (field.validate === undefined) continue;
    const outcome = field.validate((sync.value as Record<string, unknown>)[key] as never, row, context);
    if (outcome instanceof Promise) {
      const awaited = await outcome;
      if (typeof awaited === "string") {
        return {
          issues: [issue("invalid_value", awaited, context, key, row[key], describeKind(field))],
        };
      }
    }
  }

  return sync;
}

export function applyRefinements<T>(
  value: T,
  row: Row,
  context: RowContext,
  refine: RowRefine<T> | RowRefine<T>[] | undefined,
): PravaahIssue[] {
  if (refine === undefined) return [];
  const refiners = Array.isArray(refine) ? refine : [refine];
  const out: PravaahIssue[] = [];
  for (const refiner of refiners) {
    const result = refiner(value, context);
    if (result === undefined) continue;
    if (typeof result === "string") {
      out.push({
        code: "refine_failed",
        message: result,
        rowNumber: context.rowNumber,
        severity: "error",
      });
      continue;
    }
    out.push({
      code: result.code ?? "refine_failed",
      message: result.message,
      rowNumber: context.rowNumber,
      severity: "error",
      ...(result.column === undefined ? {} : { column: result.column }),
    });
  }
  return out;
}

export function validateRows<T extends SchemaDefinition>(
  rows: Iterable<Row>,
  definition: T,
  options: ValidationOptions = {},
): { rows: InferSchema<T>[]; issues: PravaahIssue[] } {
  const output: InferSchema<T>[] = [];
  const issues: PravaahIssue[] = [];
  let rowNumber = 1;

  for (const row of rows) {
    const cleaned = cleanRow(row, options.cleaning);
    const result = validateRow(cleaned, definition, { rowNumber });
    if (result.value !== undefined) {
      const refineIssues = applyRefinements(result.value as unknown as Row, cleaned, { rowNumber }, options.refine as RowRefine<Row>);
      if (refineIssues.length > 0) {
        if (options.mode !== "skip") issues.push(...refineIssues);
        if (options.mode === "fail-fast") throw new PravaahValidationError(refineIssues);
      } else {
        output.push(result.value);
      }
    } else if (options.mode !== "skip") {
      issues.push(...result.issues);
      if (options.mode === "fail-fast") throw new PravaahValidationError(result.issues);
    }
    rowNumber += 1;
  }

  return { rows: output, issues };
}

export async function writeIssueReport(issues: Iterable<PravaahIssue>, destination: string): Promise<void> {
  const rows = [
    ["severity", "code", "message", "rowNumber", "column", "expected", "rawValue"],
    ...[...issues].map((issue) => [
      issue.severity,
      issue.code,
      issue.message,
      issue.rowNumber ?? "",
      issue.column ?? "",
      issue.expected ?? "",
      stringifyIssueValue(issue.rawValue),
    ]),
  ];

  await writeFile(destination, rows.map((row) => row.map(csvEscape).join(",")).join("\n") + "\n");
}

function describeKind(field: FieldDefinition): string {
  if (field.kind === "enum") return `enum(${(field.values ?? []).join("|")})`;
  if (field.kind === "literal") return `literal(${String(field.values?.[0])})`;
  if (field.kind === "regex" && field.pattern !== undefined) return `regex(${field.pattern.source})`;
  return field.kind;
}

function coerceValue(value: unknown, field: FieldDefinition): { ok: true; value: unknown } | { ok: false } {
  if (field.kind === "any") return { ok: true, value };
  if (field.kind === "string") return { ok: true, value: String(value) };
  if (field.kind === "email") {
    const email = String(value).trim();
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? { ok: true, value: email } : { ok: false };
  }
  if (field.kind === "phone") {
    const phone = String(value).replace(/[^\d+]/g, "");
    return phone.length >= 7 ? { ok: true, value: phone } : { ok: false };
  }
  if (field.kind === "url") {
    const text = String(value).trim();
    try {
      new URL(text);
      return { ok: true, value: text };
    } catch {
      return { ok: false };
    }
  }
  if (field.kind === "uuid") {
    const text = String(value).trim();
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(text) ? { ok: true, value: text } : { ok: false };
  }
  if (field.kind === "number") {
    const number = typeof value === "number" ? value : field.coerce ? Number(String(value).trim()) : NaN;
    return Number.isFinite(number) ? { ok: true, value: number } : { ok: false };
  }
  if (field.kind === "integer") {
    const number = typeof value === "number" ? value : field.coerce ? Number(String(value).trim()) : NaN;
    return Number.isFinite(number) && Number.isInteger(number) ? { ok: true, value: number } : { ok: false };
  }
  if (field.kind === "boolean") {
    if (typeof value === "boolean") return { ok: true, value };
    if (!field.coerce) return { ok: false };
    const normalized = String(value).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(normalized)) return { ok: true, value: true };
    if (["false", "0", "no", "n"].includes(normalized)) return { ok: true, value: false };
    return { ok: false };
  }
  if (field.kind === "date") {
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? { ok: false } : { ok: true, value };
    if (!field.coerce) return { ok: false };
    const asNumber = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(asNumber) && String(value).trim() === String(asNumber)) {
      const ms = asNumber > 10_000_000_000 ? asNumber : asNumber * 1000;
      const date = new Date(ms);
      return Number.isNaN(date.getTime()) ? { ok: false } : { ok: true, value: date };
    }
    const date = new Date(String(value));
    return Number.isNaN(date.getTime()) ? { ok: false } : { ok: true, value: date };
  }
  if (field.kind === "enum") {
    const values = field.values ?? [];
    if (values.includes(value)) return { ok: true, value };
    const coerced = typeof value === "string" ? value : String(value);
    const match = values.find((v) => String(v) === coerced);
    return match === undefined ? { ok: false } : { ok: true, value: match };
  }
  if (field.kind === "literal") {
    const expected = field.values?.[0];
    if (value === expected) return { ok: true, value };
    if (String(value) === String(expected)) return { ok: true, value: expected };
    return { ok: false };
  }
  if (field.kind === "regex") {
    const text = String(value);
    return field.pattern !== undefined && field.pattern.test(text) ? { ok: true, value: text } : { ok: false };
  }
  if (field.kind === "array") {
    const entries = Array.isArray(value) ? value : String(value).split(/[,;|]/).map((entry) => entry.trim());
    const of = field.of;
    if (of === undefined) return { ok: true, value: entries };
    const subField = typeof of === "string" ? { kind: of, coerce: true } : of;
    const out: unknown[] = [];
    for (const entry of entries) {
      const coerced = coerceValue(entry, subField);
      if (!coerced.ok) return { ok: false };
      out.push(coerced.value);
    }
    return { ok: true, value: out };
  }

  return { ok: false };
}

function checkConstraints(value: unknown, field: FieldDefinition): string | undefined {
  if (typeof value === "number") {
    if (field.min !== undefined && value < field.min) return `must be ≥ ${field.min}`;
    if (field.max !== undefined && value > field.max) return `must be ≤ ${field.max}`;
  }
  if (typeof value === "string") {
    if (field.length !== undefined && value.length !== field.length) return `must be exactly ${field.length} characters`;
    if (field.minLength !== undefined && value.length < field.minLength) return `must be at least ${field.minLength} characters`;
    if (field.maxLength !== undefined && value.length > field.maxLength) return `must be at most ${field.maxLength} characters`;
    if (field.pattern !== undefined && field.kind !== "regex" && !field.pattern.test(value)) return `must match ${field.pattern}`;
  }
  if (Array.isArray(value)) {
    if (field.minLength !== undefined && value.length < field.minLength) return `must have at least ${field.minLength} items`;
    if (field.maxLength !== undefined && value.length > field.maxLength) return `must have at most ${field.maxLength} items`;
  }
  if (value instanceof Date) {
    if (field.min !== undefined && value.getTime() < field.min) return `must be on or after ${new Date(field.min).toISOString()}`;
    if (field.max !== undefined && value.getTime() > field.max) return `must be on or before ${new Date(field.max).toISOString()}`;
  }
  return undefined;
}

function issue(
  code: string,
  message: string,
  context: RowContext,
  column: string,
  rawValue: unknown,
  expected: string,
): PravaahIssue {
  return {
    code,
    message,
    rowNumber: context.rowNumber,
    column,
    rawValue,
    expected,
    severity: "error",
  };
}

function stringifyIssueValue(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value ?? "");
}

function csvEscape(value: unknown): string {
  const text = String(value);
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
}
