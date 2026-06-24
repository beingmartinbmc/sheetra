import { read } from "../pipeline/index.js";
import { rowIdentity } from "../key.js";
import type { Row } from "../types.js";

export interface QueryOptions {
  source?: string | Buffer | AsyncIterable<Row> | Iterable<Row>;
}

export type JoinSource = string | Buffer | AsyncIterable<Row> | Iterable<Row>;

export interface QueryExecutionOptions {
  /** Named join sources keyed by the table name used after JOIN. */
  join?: Record<string, JoinSource>;
}

type ComparisonOperator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "contains" | "like" | "in";

interface Comparison {
  type: "cmp";
  column: string;
  operator: ComparisonOperator;
  value: QueryValue | QueryValue[];
}

interface LogicalClause {
  type: "logical";
  operator: "and" | "or";
  left: WhereNode;
  right: WhereNode;
}

interface NotClause {
  type: "not";
  expr: WhereNode;
}

type WhereNode = Comparison | LogicalClause | NotClause;

type QueryValue = string | number | boolean | null;

interface Aggregate {
  kind: "count" | "sum" | "avg" | "min" | "max";
  column: string;
  alias: string;
}

interface SelectColumn {
  kind: "column";
  column: string;
  alias: string;
}

type SelectField = SelectColumn | Aggregate;

interface OrderByClause {
  column: string;
  direction: "asc" | "desc";
}

interface JoinClause {
  table: string;
  on: string;
}

interface QueryPlan {
  fields: SelectField[];
  join?: JoinClause;
  where?: WhereNode;
  groupBy: string[];
  having?: WhereNode;
  orderBy: OrderByClause[];
  limit?: number;
}

export async function query(
  source: QueryOptions["source"],
  sql: string,
  options: QueryExecutionOptions = {},
): Promise<Row[]> {
  if (source === undefined) throw new Error("query() requires a source");
  const plan = parseQuery(sql);
  const rows: Row[] = [];

  const joiner = plan.join !== undefined ? await buildJoiner(plan.join, options) : undefined;

  for await (const row of read(source)) {
    const base = row as Row;
    if (joiner === undefined) {
      if (plan.where === undefined || evaluateWhere(base, plan.where)) rows.push(base);
      continue;
    }
    for (const merged of joiner(base)) {
      if (plan.where === undefined || evaluateWhere(merged, plan.where)) rows.push(merged);
    }
  }

  return finalizeRows(rows, plan);
}

/**
 * Streaming query for the WHERE + projection + LIMIT case. Sorting, grouping,
 * aggregates, and joins require buffering and fall back to {@link query}.
 */
export async function* queryStream(source: QueryOptions["source"], sql: string): AsyncIterable<Row> {
  if (source === undefined) throw new Error("queryStream() requires a source");
  const plan = parseQuery(sql);
  const needsBuffering =
    plan.join !== undefined ||
    plan.groupBy.length > 0 ||
    plan.having !== undefined ||
    plan.orderBy.length > 0 ||
    plan.fields.some((field) => field.kind !== "column");

  if (needsBuffering) {
    const rows = await query(source, sql);
    yield* rows;
    return;
  }

  let emitted = 0;
  for await (const row of read(source)) {
    const base = row as Row;
    if (plan.where !== undefined && !evaluateWhere(base, plan.where)) continue;
    yield project(base, plan.fields);
    emitted += 1;
    if (plan.limit !== undefined && emitted >= plan.limit) return;
  }
}

function finalizeRows(rows: Row[], plan: QueryPlan): Row[] {
  let output = plan.groupBy.length > 0 || plan.fields.some((f) => f.kind !== "column")
    ? aggregate(rows, plan)
    : rows.map((row) => project(row, plan.fields));

  if (plan.having !== undefined) output = output.filter((row) => evaluateWhere(row, plan.having!));
  if (plan.orderBy.length > 0) output = sortRows(output, plan.orderBy);
  if (plan.limit !== undefined) output = output.slice(0, plan.limit);
  return output;
}

async function buildJoiner(
  join: JoinClause,
  options: QueryExecutionOptions,
): Promise<(left: Row) => Row[]> {
  const provided = options.join ?? {};
  const rawSource = provided[join.table];
  if (rawSource === undefined) {
    throw new Error(`Missing join source for table "${join.table}"; pass it via options.join`);
  }

  const rightRows: Row[] = [];
  for await (const row of read(rawSource)) rightRows.push(row as Row);
  const index = createIndex(rightRows, join.on);

  return (left: Row): Row[] => {
    const id = rowIdentity(left, [join.on]);
    const matches = index.get(id);
    if (matches === undefined) return [];
    return matches.map((right) => ({ ...left, ...right }));
  };
}

export function createIndex(rows: Iterable<Row>, key: string | string[]): Map<string, Row[]> {
  const keys = Array.isArray(key) ? key : [key];
  const index = new Map<string, Row[]>();

  for (const row of rows) {
    const id = rowIdentity(row, keys);
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
    const id = rowIdentity(row, keys);
    for (const match of index.get(id) ?? []) joined.push({ ...row, ...match });
  }

  return joined;
}

// --- Parser ---

function parseQuery(sql: string): QueryPlan {
  const tokens = tokenize(sql);
  const parser = new QueryParser(tokens);
  return parser.parse();
}

type Token =
  | { kind: "ident"; value: string }
  | { kind: "number"; value: number }
  | { kind: "string"; value: string }
  | { kind: "punct"; value: string }
  | { kind: "op"; value: string }
  | { kind: "keyword"; value: string };

const KEYWORDS = new Set([
  "select",
  "from",
  "join",
  "on",
  "where",
  "group",
  "having",
  "by",
  "order",
  "limit",
  "and",
  "or",
  "not",
  "in",
  "like",
  "asc",
  "desc",
  "contains",
  "as",
  "true",
  "false",
  "null",
]);

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let cursor = 0;
  const source = sql.trim();

  while (cursor < source.length) {
    const char = source[cursor]!;
    if (/\s/.test(char)) {
      cursor += 1;
      continue;
    }
    if (char === "," || char === "(" || char === ")" || char === "*") {
      tokens.push({ kind: "punct", value: char });
      cursor += 1;
      continue;
    }
    if (char === "'" || char === "\"") {
      const quote = char;
      let end = cursor + 1;
      while (end < source.length && source[end] !== quote) end += 1;
      tokens.push({ kind: "string", value: source.slice(cursor + 1, end) });
      cursor = end + 1;
      continue;
    }
    if ((char === ">" || char === "<") && source[cursor + 1] === "=") {
      tokens.push({ kind: "op", value: `${char}=` });
      cursor += 2;
      continue;
    }
    if (char === "!" && source[cursor + 1] === "=") {
      tokens.push({ kind: "op", value: "!=" });
      cursor += 2;
      continue;
    }
    if (char === "=" || char === ">" || char === "<") {
      tokens.push({ kind: "op", value: char });
      cursor += 1;
      continue;
    }
    const numMatch = /^-?\d+(?:\.\d+)?/.exec(source.slice(cursor));
    if (numMatch !== null) {
      tokens.push({ kind: "number", value: Number(numMatch[0]) });
      cursor += numMatch[0].length;
      continue;
    }
    const identMatch = /^[A-Za-z_][A-Za-z0-9_.]*/.exec(source.slice(cursor));
    if (identMatch !== null) {
      const value = identMatch[0];
      if (KEYWORDS.has(value.toLowerCase())) tokens.push({ kind: "keyword", value: value.toLowerCase() });
      else tokens.push({ kind: "ident", value });
      cursor += value.length;
      continue;
    }
    throw new Error(`Unexpected character in query: ${char}`);
  }

  return tokens;
}

class QueryParser {
  private cursor = 0;

  constructor(private readonly tokens: Token[]) {}

  parse(): QueryPlan {
    try {
      return this.doParse();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.startsWith("Expected SELECT") || message.startsWith("Unexpected end")) {
        throw new Error("Unsupported query", { cause: error });
      }
      if (message.startsWith("Expected operator")) {
        throw new Error("Unsupported WHERE clause", { cause: error });
      }
      throw error;
    }
  }

  private doParse(): QueryPlan {
    this.expectKeyword("select");
    const fields = this.parseSelectList();
    this.consumeOptionalKeyword("from");
    if (this.peek()?.kind === "ident" || this.peek()?.kind === "string") this.cursor += 1;
    const plan: QueryPlan = { fields, groupBy: [], orderBy: [] };
    if (this.consumeOptionalKeyword("join")) {
      const table = this.consumeIdent();
      this.expectKeyword("on");
      const on = this.consumeIdent();
      plan.join = { table, on };
    }
    if (this.consumeOptionalKeyword("where")) plan.where = this.parseOrExpression();
    if (this.consumeOptionalKeyword("group")) {
      this.expectKeyword("by");
      plan.groupBy = this.parseIdentifierList();
    }
    if (this.consumeOptionalKeyword("having")) plan.having = this.parseOrExpression();
    if (this.consumeOptionalKeyword("order")) {
      this.expectKeyword("by");
      plan.orderBy = this.parseOrderByList();
    }
    if (this.consumeOptionalKeyword("limit")) {
      const token = this.consume();
      if (token.kind !== "number") throw new Error("LIMIT expects a number");
      plan.limit = token.value;
    }
    return plan;
  }

  private parseOrderByList(): OrderByClause[] {
    const clauses: OrderByClause[] = [];
    while (true) {
      const column = this.consumeIdent();
      const direction = this.consumeOptionalKeyword("asc")
        ? "asc"
        : this.consumeOptionalKeyword("desc")
          ? "desc"
          : "asc";
      clauses.push({ column, direction });
      if (!this.consumeOptionalPunct(",")) break;
    }
    return clauses;
  }

  private parseSelectList(): SelectField[] {
    const fields: SelectField[] = [];
    while (true) {
      fields.push(this.parseSelectField());
      if (!this.consumeOptionalPunct(",")) break;
    }
    return fields;
  }

  private parseSelectField(): SelectField {
    const token = this.peek();
    if (token?.kind === "punct" && token.value === "*") {
      this.cursor += 1;
      return { kind: "column", column: "*", alias: "*" };
    }
    if (token?.kind === "ident" && this.isAggregateFollowing(token.value)) {
      return this.parseAggregate();
    }
    const column = this.consumeIdent();
    const alias = this.parseAlias() ?? column;
    return { kind: "column", column, alias };
  }

  private isAggregateFollowing(value: string): boolean {
    const upper = value.toUpperCase();
    if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(upper)) return false;
    const next = this.tokens[this.cursor + 1];
    return next?.kind === "punct" && next.value === "(";
  }

  private parseAggregate(): Aggregate {
    const nameToken = this.consume();
    const kind = nameToken.value.toString().toLowerCase() as Aggregate["kind"];
    this.expectPunct("(");
    const argToken = this.consume();
    let column: string;
    if (argToken.kind === "ident") column = argToken.value;
    else if (argToken.kind === "punct" && argToken.value === "*") column = "*";
    else throw new Error("Aggregate expects column or *");
    this.expectPunct(")");
    const alias = this.parseAlias() ?? `${kind}_${column}`;
    return { kind, column, alias };
  }

  private parseAlias(): string | undefined {
    if (this.consumeOptionalKeyword("as")) return this.consumeIdent();
    return undefined;
  }

  private parseIdentifierList(): string[] {
    const ids = [this.consumeIdent()];
    while (this.consumeOptionalPunct(",")) ids.push(this.consumeIdent());
    return ids;
  }

  private parseOrExpression(): WhereNode {
    let left = this.parseAndExpression();
    while (this.consumeOptionalKeyword("or")) {
      const right = this.parseAndExpression();
      left = { type: "logical", operator: "or", left, right };
    }
    return left;
  }

  private parseAndExpression(): WhereNode {
    let left = this.parseNotExpression();
    while (this.consumeOptionalKeyword("and")) {
      const right = this.parseNotExpression();
      left = { type: "logical", operator: "and", left, right };
    }
    return left;
  }

  private parseNotExpression(): WhereNode {
    if (this.consumeOptionalKeyword("not")) {
      return { type: "not", expr: this.parseNotExpression() };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): WhereNode {
    if (this.consumeOptionalPunct("(")) {
      const expr = this.parseOrExpression();
      this.expectPunct(")");
      return expr;
    }
    const column = this.consumeIdent();
    const opToken = this.consume();
    let operator: ComparisonOperator;
    if (opToken.kind === "op") operator = opToken.value as ComparisonOperator;
    else if (opToken.kind === "keyword" && opToken.value === "contains") operator = "contains";
    else if (opToken.kind === "keyword" && opToken.value === "like") operator = "like";
    else if (opToken.kind === "keyword" && opToken.value === "in") operator = "in";
    else throw new Error(`Expected operator in WHERE clause, got ${opToken.kind}:${opToken.value}`);

    if (operator === "in") {
      this.expectPunct("(");
      const values: QueryValue[] = [];
      while (true) {
        values.push(this.parseLiteral());
        if (!this.consumeOptionalPunct(",")) break;
      }
      this.expectPunct(")");
      return { type: "cmp", column, operator, value: values };
    }

    const value = this.parseLiteral();
    return { type: "cmp", column, operator, value };
  }

  private parseLiteral(): QueryValue {
    const token = this.consume();
    if (token.kind === "number") return token.value;
    if (token.kind === "string") return token.value;
    if (token.kind === "keyword" && token.value === "true") return true;
    if (token.kind === "keyword" && token.value === "false") return false;
    if (token.kind === "keyword" && token.value === "null") return null;
    if (token.kind === "ident") return token.value;
    throw new Error(`Expected literal, got ${token.kind}:${token.value}`);
  }

  private consumeIdent(): string {
    const token = this.consume();
    if (token.kind === "ident") return token.value;
    if (token.kind === "string") return token.value;
    throw new Error(`Expected identifier, got ${token.kind}:${token.value}`);
  }

  private consume(): Token {
    const token = this.tokens[this.cursor];
    if (token === undefined) throw new Error("Unexpected end of query");
    this.cursor += 1;
    return token;
  }

  private peek(): Token | undefined {
    return this.tokens[this.cursor];
  }

  private expectKeyword(keyword: string): void {
    const token = this.consume();
    if (token.kind !== "keyword" || token.value !== keyword) throw new Error(`Expected ${keyword.toUpperCase()}`);
  }

  private consumeOptionalKeyword(keyword: string): boolean {
    const token = this.peek();
    if (token?.kind === "keyword" && token.value === keyword) {
      this.cursor += 1;
      return true;
    }
    return false;
  }

  private expectPunct(value: string): void {
    const token = this.consume();
    if (token.kind !== "punct" || token.value !== value) throw new Error(`Expected '${value}'`);
  }

  private consumeOptionalPunct(value: string): boolean {
    const token = this.peek();
    if (token?.kind === "punct" && token.value === value) {
      this.cursor += 1;
      return true;
    }
    return false;
  }
}

// --- Evaluation ---

function evaluateWhere(row: Row, node: WhereNode): boolean {
  if (node.type === "logical") {
    return node.operator === "and"
      ? evaluateWhere(row, node.left) && evaluateWhere(row, node.right)
      : evaluateWhere(row, node.left) || evaluateWhere(row, node.right);
  }
  if (node.type === "not") return !evaluateWhere(row, node.expr);
  return evaluateComparison(row, node);
}

function evaluateComparison(row: Row, clause: Comparison): boolean {
  const raw = row[clause.column];
  const value = raw instanceof Date ? raw.getTime() : raw;
  const expected = clause.value;

  if (clause.operator === "in") {
    const list = expected as QueryValue[];
    return list.some((candidate) => compareEq(value, candidate));
  }
  if (clause.operator === "contains") return String(value ?? "").includes(String(expected));
  if (clause.operator === "like") return likeMatches(String(value ?? ""), String(expected));
  if (clause.operator === "=") return compareEq(value, expected as QueryValue);
  if (clause.operator === "!=") return !compareEq(value, expected as QueryValue);

  const numericValue = typeof value === "number" ? value : Number(value);
  const numericExpected = typeof expected === "number" ? expected : Number(expected);
  if (!Number.isFinite(numericValue) || !Number.isFinite(numericExpected)) return false;
  if (clause.operator === ">") return numericValue > numericExpected;
  if (clause.operator === ">=") return numericValue >= numericExpected;
  if (clause.operator === "<") return numericValue < numericExpected;
  return numericValue <= numericExpected;
}

function compareEq(value: unknown, expected: QueryValue): boolean {
  if (value === expected) return true;
  if (value === null || value === undefined) return expected === null;
  if (typeof value === "number" && typeof expected === "string") return String(value) === expected;
  return String(value) === String(expected);
}

function likeMatches(value: string, pattern: string): boolean {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/%/g, ".*").replace(/_/g, ".");
  return new RegExp(`^${escaped}$`, "i").test(value);
}

function project(row: Row, fields: SelectField[]): Row {
  if (fields.length === 1 && fields[0]!.kind === "column" && fields[0]!.column === "*") return row;
  const output: Row = {};
  for (const field of fields) {
    if (field.kind === "column") output[field.alias] = row[field.column] ?? null;
  }
  return output;
}

function aggregate(rows: Row[], plan: QueryPlan): Row[] {
  if (plan.groupBy.length === 0) {
    return [aggregateRow(rows, plan.fields)];
  }

  const groups = new Map<string, Row[]>();
  for (const row of rows) {
    const key = rowIdentity(row, plan.groupBy);
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  const output: Row[] = [];
  for (const bucket of groups.values()) {
    output.push(aggregateRow(bucket, plan.fields, plan.groupBy, bucket[0] ?? {}));
  }
  return output;
}

function aggregateRow(rows: Row[], fields: SelectField[], groupBy: string[] = [], firstRow: Row = {}): Row {
  const output: Row = {};
  for (const column of groupBy) output[column] = firstRow[column] ?? null;
  for (const field of fields) {
    if (field.kind === "column") {
      if (field.column === "*") continue;
      output[field.alias] = firstRow[field.column] ?? null;
    } else {
      output[field.alias] = computeAggregate(field, rows);
    }
  }
  return output;
}

function computeAggregate(aggregate: Aggregate, rows: Row[]): number | null {
  if (aggregate.kind === "count") {
    if (aggregate.column === "*") return rows.length;
    return rows.filter((row) => row[aggregate.column] !== null && row[aggregate.column] !== undefined).length;
  }
  const values = rows
    .map((row) => row[aggregate.column])
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (values.length === 0) return null;
  if (aggregate.kind === "sum") return values.reduce((total, value) => total + value, 0);
  if (aggregate.kind === "avg") return values.reduce((total, value) => total + value, 0) / values.length;
  if (aggregate.kind === "min") return Math.min(...values);
  return Math.max(...values);
}

function sortRows(rows: Row[], orderBy: OrderByClause[]): Row[] {
  return [...rows].sort((left, right) => {
    for (const clause of orderBy) {
      const result = compareRows(left, right, clause);
      if (result !== 0) return result;
    }
    return 0;
  });
}

function compareRows(left: Row, right: Row, orderBy: OrderByClause): number {
  const a = left[orderBy.column];
  const b = right[orderBy.column];
  const direction = orderBy.direction === "asc" ? 1 : -1;
  if (typeof a === "number" && typeof b === "number") return (a - b) * direction;
  return String(a ?? "").localeCompare(String(b ?? "")) * direction;
}
