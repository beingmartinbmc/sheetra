import type { Row } from "../types.js";

export interface DiffOptions {
  key: string | string[];
}

export interface RowChange {
  key: string;
  before: Row;
  after: Row;
  changedColumns: string[];
}

export interface DiffResult {
  added: Row[];
  removed: Row[];
  changed: RowChange[];
  unchanged: number;
}

export function diff(oldRows: Iterable<Row>, newRows: Iterable<Row>, options: DiffOptions): DiffResult {
  const oldIndex = indexByKey(oldRows, options.key);
  const newIndex = indexByKey(newRows, options.key);
  const added: Row[] = [];
  const removed: Row[] = [];
  const changed: RowChange[] = [];
  let unchanged = 0;

  for (const [key, after] of newIndex) {
    const before = oldIndex.get(key);
    if (before === undefined) {
      added.push(after);
      continue;
    }

    const changedColumns = changedColumnsFor(before, after);
    if (changedColumns.length === 0) unchanged += 1;
    else changed.push({ key, before, after, changedColumns });
  }

  for (const [key, before] of oldIndex) {
    if (!newIndex.has(key)) removed.push(before);
  }

  return { added, removed, changed, unchanged };
}

function indexByKey(rows: Iterable<Row>, key: string | string[]): Map<string, Row> {
  const keys = Array.isArray(key) ? key : [key];
  const index = new Map<string, Row>();
  for (const row of rows) index.set(keys.map((name) => String(row[name] ?? "")).join("\u0000"), row);
  return index;
}

function changedColumnsFor(before: Row, after: Row): string[] {
  const columns = new Set([...Object.keys(before), ...Object.keys(after)]);
  return [...columns].filter((column) => !sameValue(before[column], after[column]));
}

function sameValue(left: unknown, right: unknown): boolean {
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  return JSON.stringify(left) === JSON.stringify(right);
}
