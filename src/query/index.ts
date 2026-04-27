import { read } from "../pipeline/index.js";
import type { Row } from "../types.js";

export interface QueryOptions {
  source?: string | Buffer | AsyncIterable<Row> | Iterable<Row>;
}

export async function query(source: QueryOptions["source"], sql: string): Promise<Row[]> {
  if (source === undefined) throw new Error("query() requires a source");
  const plan = parseQuery(sql);
  const rows = read(source);
  const output: Row[] = [];

  for await (const row of rows) {
    if (plan.where === undefined || matchesWhere(row as Row, plan.where)) {
      output.push(project(row as Row, plan.columns));
    }
  }

  return output;
}

export function createIndex(rows: Iterable<Row>, key: string | string[]): Map<string, Row[]> {
  const keys = Array.isArray(key) ? key : [key];
  const index = new Map<string, Row[]>();

  for (const row of rows) {
    const id = keys.map((name) => String(row[name] ?? "")).join("\u0000");
    const bucket = index.get(id) ?? [];
    bucket.push(row);
    index.set(id, bucket);
  }

  return index;
}

export function joinRows(left: Iterable<Row>, right: Iterable<Row>, key: string | string[]): Row[] {
  const index = createIndex(right, key);
  const keys = Array.isArray(key) ? key : [key];
  const joined: Row[] = [];

  for (const row of left) {
    const id = keys.map((name) => String(row[name] ?? "")).join("\u0000");
    for (const match of index.get(id) ?? []) joined.push({ ...row, ...match });
  }

  return joined;
}

interface QueryPlan {
  columns: string[];
  where?: WhereClause | undefined;
}

interface WhereClause {
  column: string;
  operator: "=" | "!=" | ">" | ">=" | "<" | "<=" | "contains";
  value: string | number;
}

function parseQuery(sql: string): QueryPlan {
  const match = /^select\s+(.+?)(?:\s+where\s+(.+))?$/i.exec(sql.trim());
  if (match === null) throw new Error(`Unsupported query: ${sql}`);

  const select = match[1] ?? "*";
  const where = match[2];
  const columns = select.trim() === "*" ? ["*"] : select.split(",").map((column) => column.trim());
  return where === undefined ? { columns } : { columns, where: parseWhere(where) };
}

function parseWhere(where: string): WhereClause {
  const match = /^([A-Za-z_][\w\s.-]*?)\s*(>=|<=|!=|=|>|<|contains)\s*(.+)$/i.exec(where.trim());
  if (match === null) throw new Error(`Unsupported WHERE clause: ${where}`);

  return {
    column: (match[1] ?? "").trim(),
    operator: (match[2] ?? "=").toLowerCase() as WhereClause["operator"],
    value: parseLiteral((match[3] ?? "").trim()),
  };
}

function parseLiteral(value: string): string | number {
  const unquoted = value.replace(/^['"]|['"]$/g, "");
  const number = Number(unquoted);
  return Number.isFinite(number) && unquoted.trim() !== "" ? number : unquoted;
}

function matchesWhere(row: Row, where: WhereClause): boolean {
  const raw = row[where.column];
  const value = raw instanceof Date ? raw.getTime() : raw;
  const expected = where.value;

  if (where.operator === "contains") return String(value ?? "").includes(String(expected));
  if (where.operator === "=") return value === expected;
  if (where.operator === "!=") return value !== expected;
  if (typeof value !== "number" || typeof expected !== "number") return false;
  if (where.operator === ">") return value > expected;
  if (where.operator === ">=") return value >= expected;
  if (where.operator === "<") return value < expected;
  return value <= expected;
}

function project(row: Row, columns: string[]): Row {
  if (columns.length === 1 && columns[0] === "*") return row;
  return Object.fromEntries(columns.map((column) => [column, row[column] ?? null]));
}
