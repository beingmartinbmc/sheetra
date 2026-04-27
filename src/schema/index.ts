import { writeFile } from "node:fs/promises";
import type { CellValue, CleaningOptions, Row, RowContext, PravaahIssue, ValidationMode } from "../types.js";

export type FieldKind = "string" | "number" | "boolean" | "date" | "email" | "phone" | "any";

export interface FieldDefinition<T = unknown> {
  kind: FieldKind;
  optional?: boolean;
  defaultValue?: T;
  coerce?: boolean;
  validate?: (value: T, row: Row, context: RowContext) => string | void;
}

export type SchemaDefinition = Record<string, FieldKind | FieldDefinition>;

export type InferField<T> = T extends "string"
  ? string
  : T extends "number"
    ? number
    : T extends "boolean"
      ? boolean
      : T extends "date"
        ? Date
        : T extends "email"
          ? string
          : T extends "phone"
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
}

export class PravaahValidationError extends Error {
  constructor(public readonly issues: PravaahIssue[]) {
    super(`Pravaah validation failed with ${issues.length} issue${issues.length === 1 ? "" : "s"}`);
  }
}

export const schema = {
  string(options: Omit<FieldDefinition<string>, "kind"> = {}): FieldDefinition<string> {
    return { kind: "string", coerce: true, ...options };
  },
  number(options: Omit<FieldDefinition<number>, "kind"> = {}): FieldDefinition<number> {
    return { kind: "number", coerce: true, ...options };
  },
  boolean(options: Omit<FieldDefinition<boolean>, "kind"> = {}): FieldDefinition<boolean> {
    return { kind: "boolean", coerce: true, ...options };
  },
  date(options: Omit<FieldDefinition<Date>, "kind"> = {}): FieldDefinition<Date> {
    return { kind: "date", coerce: true, ...options };
  },
  email(options: Omit<FieldDefinition<string>, "kind"> = {}): FieldDefinition<string> {
    return { kind: "email", coerce: true, ...options };
  },
  phone(options: Omit<FieldDefinition<string>, "kind"> = {}): FieldDefinition<string> {
    return { kind: "phone", coerce: true, ...options };
  },
  any(options: Omit<FieldDefinition<CellValue>, "kind"> = {}): FieldDefinition<CellValue> {
    return { kind: "any", ...options };
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
  const normalizedEntries = new Map(Object.keys(row).map((key) => [normalizeHeader(key), key]));
  const next: Row = { ...row };

  for (const [canonical, names] of Object.entries(aliases)) {
    if (canonical in next) continue;

    for (const name of names) {
      const existing = normalizedEntries.get(normalizeHeader(name));
      if (existing !== undefined) {
        next[canonical] = next[existing] ?? null;
        break;
      }
    }
  }

  return next;
}

export function cleanRow(row: Row, options: CleaningOptions = {}): Row {
  const withHeaders = applyFuzzyHeaders(row, options.fuzzyHeaders);
  const cleaned: Row = {};

  for (const [key, value] of Object.entries(withHeaders)) {
    if (typeof value === "string") {
      let next = value;
      if (options.trim) next = next.trim();
      if (options.normalizeWhitespace) next = next.replace(/\s+/g, " ");
      cleaned[key] = next;
    } else {
      cleaned[key] = value;
    }
  }

  return cleaned;
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

    const keys = Array.isArray(options.dedupeKey) ? options.dedupeKey : [options.dedupeKey];
    const identity = keys.map((key) => String(cleaned[key] ?? "")).join("\u0000");
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
    const raw = row[key];

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
      issues.push(issue("invalid_type", `${key} must be ${field.kind}`, context, key, raw, field.kind));
      continue;
    }

    const customIssue = field.validate?.(coerced.value as never, row, context);
    if (customIssue !== undefined) {
      issues.push(issue("invalid_value", customIssue, context, key, raw, field.kind));
      continue;
    }

    parsed[key] = coerced.value;
  }

  return issues.length > 0 ? { issues } : { value: parsed as InferSchema<T>, issues };
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
      output.push(result.value);
    } else {
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
  if (field.kind === "number") {
    const number = typeof value === "number" ? value : field.coerce ? Number(value) : NaN;
    return Number.isFinite(number) ? { ok: true, value: number } : { ok: false };
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
    const date = value instanceof Date ? value : field.coerce ? new Date(String(value)) : null;
    return date instanceof Date && !Number.isNaN(date.getTime()) ? { ok: true, value: date } : { ok: false };
  }

  return { ok: false };
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
