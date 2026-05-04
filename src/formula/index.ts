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
    const parser = new ExpressionParser(source, row, this.functions);
    const result = parser.parse();
    return result === undefined ? source : result;
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
  MIN: (args) => {
    const values = numbers(args);
    return values.length === 0 ? 0 : Math.min(...values);
  },
  MAX: (args) => {
    const values = numbers(args);
    return values.length === 0 ? 0 : Math.max(...values);
  },
  COUNT: (args) => numbers(args).length,
  IF: (args) => (truthy(args[0]) ? args[1] ?? null : args[2] ?? null),
  CONCAT: (args) => args.map((value) => valueToString(value)).join(""),
  ROUND: (args) => {
    const value = Number(args[0]);
    const digits = args[1] === undefined ? 0 : Number(args[1]);
    if (!Number.isFinite(value)) return null;
    const factor = 10 ** (Number.isFinite(digits) ? digits : 0);
    return Math.round(value * factor) / factor;
  },
  ABS: (args) => {
    const value = Number(args[0]);
    return Number.isFinite(value) ? Math.abs(value) : null;
  },
  LEN: (args) => valueToString(args[0] ?? null).length,
  UPPER: (args) => valueToString(args[0] ?? null).toUpperCase(),
  LOWER: (args) => valueToString(args[0] ?? null).toLowerCase(),
  TRIM: (args) => valueToString(args[0] ?? null).trim(),
  AND: (args) => args.every((value) => truthy(value)),
  OR: (args) => args.some((value) => truthy(value)),
  NOT: (args) => !truthy(args[0]),
  ISBLANK: (args) => args[0] === null || args[0] === undefined || args[0] === "",
  DATEDIF: (args) => {
    const start = toDate(args[0]);
    const end = toDate(args[1]);
    const unit = valueToString(args[2] ?? "D").toUpperCase();
    if (start === null || end === null) return null;
    const ms = end.getTime() - start.getTime();
    if (unit === "D") return Math.floor(ms / 86400000);
    if (unit === "H") return Math.floor(ms / 3600000);
    if (unit === "M") {
      const months = (end.getUTCFullYear() - start.getUTCFullYear()) * 12 + (end.getUTCMonth() - start.getUTCMonth());
      return months;
    }
    if (unit === "Y") return end.getUTCFullYear() - start.getUTCFullYear();
    return null;
  },
  TODAY: () => new Date(new Date().toISOString().slice(0, 10)),
  NOW: () => new Date(),
};

function toDate(value: CellValue | undefined): Date | null {
  if (value === undefined || value === null) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  const date = new Date(String(value));
  return Number.isNaN(date.getTime()) ? null : date;
}

function numbers(args: CellValue[]): number[] {
  return args.flatMap((value) => {
    if (Array.isArray(value)) return numbers(value);
    if (typeof value === "boolean") return [value ? 1 : 0];
    const number = typeof value === "number" ? value : Number(value);
    return Number.isFinite(number) ? [number] : [];
  });
}

function truthy(value: CellValue | undefined): boolean {
  if (value === undefined || value === null) return false;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (Array.isArray(value)) return value.length > 0;
  if (value instanceof Date) return !Number.isNaN(value.getTime());
  return String(value).length > 0;
}

function valueToString(value: CellValue): string {
  if (Array.isArray(value)) return value.map(valueToString).join("");
  if (value instanceof Date) return value.toISOString();
  return value === null || value === undefined ? "" : String(value);
}

// --- Recursive-descent expression parser supporting function calls anywhere ---

class ExpressionParser {
  private cursor = 0;

  constructor(
    private readonly source: string,
    private readonly row: Row,
    private readonly functions: Map<string, FormulaFunction>,
  ) {}

  parse(): CellValue | undefined {
    const value = this.expression();
    this.skipWhitespace();
    if (this.cursor !== this.source.length) return undefined;
    return value;
  }

  private expression(): CellValue | undefined {
    return this.comparison();
  }

  private comparison(): CellValue | undefined {
    let left = this.additive();
    if (left === undefined) return undefined;
    while (true) {
      this.skipWhitespace();
      const op = this.matchCompareOperator();
      if (op === undefined) return left;
      const right = this.additive();
      if (right === undefined) return undefined;
      left = compareValues(left, right, op);
    }
  }

  private matchCompareOperator(): string | undefined {
    if (this.source.startsWith(">=", this.cursor)) {
      this.cursor += 2;
      return ">=";
    }
    if (this.source.startsWith("<=", this.cursor)) {
      this.cursor += 2;
      return "<=";
    }
    if (this.source.startsWith("<>", this.cursor)) {
      this.cursor += 2;
      return "!=";
    }
    if (this.source.startsWith("!=", this.cursor)) {
      this.cursor += 2;
      return "!=";
    }
    const char = this.source[this.cursor];
    if (char === ">" || char === "<" || char === "=") {
      this.cursor += 1;
      return char === "=" ? "==" : char;
    }
    return undefined;
  }

  private additive(): CellValue | undefined {
    let value = this.multiplicative();
    if (value === undefined) return undefined;
    while (true) {
      this.skipWhitespace();
      const char = this.source[this.cursor];
      if (char === "&") {
        this.cursor += 1;
        const right = this.multiplicative();
        if (right === undefined) return undefined;
        value = `${valueToString(value)}${valueToString(right)}`;
        continue;
      }
      if (char !== "+" && char !== "-") return value;
      this.cursor += 1;
      const right = this.multiplicative();
      if (right === undefined) return undefined;
      const next = arithmetic(value, right, char);
      if (next === undefined) return undefined;
      value = next;
    }
  }

  private multiplicative(): CellValue | undefined {
    let value = this.unary();
    if (value === undefined) return undefined;
    while (true) {
      this.skipWhitespace();
      const char = this.source[this.cursor];
      if (char !== "*" && char !== "/") return value;
      this.cursor += 1;
      const right = this.unary();
      if (right === undefined) return undefined;
      const next = arithmetic(value, right, char);
      if (next === undefined) return undefined;
      value = next;
    }
  }

  private unary(): CellValue | undefined {
    this.skipWhitespace();
    const char = this.source[this.cursor];
    if (char === "+" || char === "-") {
      this.cursor += 1;
      const next = this.unary();
      if (next === undefined) return undefined;
      const number = Number(next);
      if (!Number.isFinite(number)) return undefined;
      return char === "-" ? -number : number;
    }
    return this.primary();
  }

  private primary(): CellValue | undefined {
    this.skipWhitespace();
    const char = this.source[this.cursor];
    if (char === "(") {
      this.cursor += 1;
      const value = this.expression();
      this.skipWhitespace();
      if (this.source[this.cursor] !== ")") return undefined;
      this.cursor += 1;
      return value;
    }
    if (char === "\"" || char === "'") return this.stringLiteral(char);
    const num = this.number();
    if (num !== undefined) return num;
    return this.identifierOrCall();
  }

  private stringLiteral(quote: string): CellValue {
    let end = this.cursor + 1;
    while (end < this.source.length && this.source[end] !== quote) end += 1;
    const value = this.source.slice(this.cursor + 1, end);
    this.cursor = end + 1;
    return value;
  }

  private number(): number | undefined {
    this.skipWhitespace();
    const match = /^\d+(?:\.\d+)?/.exec(this.source.slice(this.cursor));
    if (match === null) return undefined;
    this.cursor += match[0].length;
    return Number(match[0]);
  }

  private identifierOrCall(): CellValue | undefined {
    this.skipWhitespace();
    const match = /^[A-Za-z_][A-Za-z0-9_.]*/.exec(this.source.slice(this.cursor));
    if (match === null) return undefined;
    const name = match[0];
    this.cursor += name.length;
    this.skipWhitespace();

    if (this.source[this.cursor] === "(") {
      const fn = this.functions.get(name.toUpperCase());
      if (fn === undefined) throw new Error(`Unsupported formula function: ${name}`);
      this.cursor += 1;
      const args: CellValue[] = [];
      this.skipWhitespace();
      if (this.source[this.cursor] !== ")") {
        while (true) {
          const value = this.expression();
          args.push(value === undefined ? null : value);
          this.skipWhitespace();
          if (this.source[this.cursor] === ",") {
            this.cursor += 1;
            continue;
          }
          break;
        }
      }
      this.skipWhitespace();
      if (this.source[this.cursor] !== ")") return undefined;
      this.cursor += 1;
      return fn(args, this.row);
    }

    const lower = name.toLowerCase();
    if (lower === "true") return true;
    if (lower === "false") return false;
    if (lower === "null") return null;

    if (name in this.row) return this.row[name] ?? null;
    return name;
  }

  private skipWhitespace(): void {
    while (/\s/.test(this.source[this.cursor] ?? "")) this.cursor += 1;
  }
}

function arithmetic(left: CellValue, right: CellValue, op: string): CellValue | undefined {
  const a = typeof left === "number" ? left : typeof left === "string" && left.trim() !== "" ? Number(left) : NaN;
  const b = typeof right === "number" ? right : typeof right === "string" && right.trim() !== "" ? Number(right) : NaN;
  if (!Number.isFinite(a) || !Number.isFinite(b)) return undefined;
  if (op === "+") return a + b;
  if (op === "-") return a - b;
  if (op === "*") return a * b;
  if (op === "/") return b === 0 ? null : a / b;
  return undefined;
}

function compareValues(left: CellValue, right: CellValue, op: string): CellValue {
  if (op === "==") return compareEquality(left, right);
  if (op === "!=") return !compareEquality(left, right);
  const a = Number(left);
  const b = Number(right);
  if (Number.isFinite(a) && Number.isFinite(b)) {
    if (op === ">") return a > b;
    if (op === "<") return a < b;
    if (op === ">=") return a >= b;
    if (op === "<=") return a <= b;
  }
  const sa = valueToString(left);
  const sb = valueToString(right);
  if (op === ">") return sa > sb;
  if (op === "<") return sa < sb;
  if (op === ">=") return sa >= sb;
  if (op === "<=") return sa <= sb;
  return false;
}

function compareEquality(left: CellValue, right: CellValue): boolean {
  if (left === right) return true;
  if (left === null || right === null) return false;
  if (typeof left === "number" || typeof right === "number") return Number(left) === Number(right);
  return valueToString(left) === valueToString(right);
}
