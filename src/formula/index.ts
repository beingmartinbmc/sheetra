import type { CellValue, Row } from "../types.js";

export type FormulaFunction = (args: CellValue[], row?: Row) => CellValue;

export interface FormulaEngineOptions {
  functions?: Record<string, FormulaFunction>;
  preserveUnknown?: boolean;
}

export class FormulaEngine {
  private readonly functions = new Map<string, FormulaFunction>();

  constructor(options: FormulaEngineOptions = {}) {
    for (const [name, fn] of Object.entries(defaultFunctions)) this.register(name, fn);
    for (const [name, fn] of Object.entries(options.functions ?? {})) this.register(name, fn);
  }

  register(name: string, fn: FormulaFunction): this {
    this.functions.set(name.toUpperCase(), fn);
    return this;
  }

  evaluate(formula: string, row: Row = {}): CellValue {
    const source = formula.trim().replace(/^=/, "");
    const call = /^([A-Z][A-Z0-9.]*)\((.*)\)$/i.exec(source);
    if (call === null) return evaluateExpression(source, row);

    const name = call[1] ?? "";
    const argsSource = call[2] ?? "";
    const fn = this.functions.get(name.toUpperCase());
    if (fn === undefined) throw new Error(`Unsupported formula function: ${name}`);

    return fn(splitArgs(argsSource).map((arg) => resolveArg(arg, row)), row);
  }
}

export function evaluateFormula(formula: string, row: Row = {}, options: FormulaEngineOptions = {}): CellValue {
  return new FormulaEngine(options).evaluate(formula, row);
}

const defaultFunctions: Record<string, FormulaFunction> = {
  SUM: (args) => numbers(args).reduce((total, value) => total + value, 0),
  AVERAGE: (args) => {
    const values = numbers(args);
    return values.length === 0 ? 0 : values.reduce((total, value) => total + value, 0) / values.length;
  },
  MIN: (args) => Math.min(...numbers(args)),
  MAX: (args) => Math.max(...numbers(args)),
  COUNT: (args) => numbers(args).length,
  IF: (args) => (truthy(args[0]) ? args[1] ?? null : args[2] ?? null),
  CONCAT: (args) => args.map((value) => valueToString(value)).join(""),
};

function splitArgs(value: string): string[] {
  const args: string[] = [];
  let current = "";
  let quote: string | undefined;
  let depth = 0;

  for (const char of value) {
    if ((char === "\"" || char === "'") && quote === undefined) quote = char;
    else if (char === quote) quote = undefined;
    else if (char === "(" && quote === undefined) depth += 1;
    else if (char === ")" && quote === undefined) depth -= 1;

    if (char === "," && quote === undefined && depth === 0) {
      args.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }

  if (current.trim() !== "") args.push(current.trim());
  return args;
}

function resolveArg(arg: string, row: Row): CellValue {
  const trimmed = arg.trim();
  if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1);
  }
  if (trimmed in row) return row[trimmed] ?? null;
  const number = Number(trimmed);
  if (Number.isFinite(number)) return number;
  if (/^(true|false)$/i.test(trimmed)) return trimmed.toLowerCase() === "true";
  return trimmed;
}

function evaluateExpression(source: string, row: Row): CellValue {
  const replaced = source.replace(/\b[A-Za-z_][A-Za-z0-9_]*\b/g, (token) => {
    const value = row[token];
    return typeof value === "number" ? String(value) : token;
  });
  if (!/^[\d+\-*/().\s]+$/.test(replaced)) return source;
  return Function(`"use strict"; return (${replaced});`)() as number;
}

function numbers(args: CellValue[]): number[] {
  return args.flatMap((value) => {
    if (Array.isArray(value)) return numbers(value);
    const number = typeof value === "number" ? value : Number(value);
    return Number.isFinite(number) ? [number] : [];
  });
}

function truthy(value: CellValue | undefined): boolean {
  if (Array.isArray(value)) return value.length > 0;
  return Boolean(value);
}

function valueToString(value: CellValue): string {
  if (Array.isArray(value)) return value.map(valueToString).join("");
  if (value instanceof Date) return value.toISOString();
  return value === null ? "" : String(value);
}
