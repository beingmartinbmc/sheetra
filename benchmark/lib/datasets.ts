import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { write } from "../../src/index.js";
import type { Row } from "../../src/index.js";

export type TransformLevel = "none" | "light" | "heavy";

export interface DatasetShape {
  rows: number;
  columns: number;
  messy?: boolean;
}

export function* syntheticRows(shape: DatasetShape): Iterable<Row> {
  for (let index = 0; index < shape.rows; index += 1) {
    const row: Row = {
      id: index + 1,
      email: shape.messy && index % 17 === 0 ? "bad-email" : `user${index}@example.com`,
      amount: index * 3.17,
      quantity: (index % 11) + 1,
      joined: new Date(2020, index % 12, (index % 28) + 1).toISOString(),
    };

    for (let column = 5; column < shape.columns; column += 1) {
      row[`col_${column + 1}`] = shape.messy && column % 13 === 0 && index % 19 === 0 ? "" : `${column}-${index}`;
    }

    yield row;
  }
}

export function transformRow(row: Row, level: TransformLevel): Row {
  if (level === "none") return row;

  const amount = Number(row.amount ?? 0);
  const quantity = Number(row.quantity ?? 0);
  const light = {
    ...row,
    total: amount * quantity,
    active: quantity % 2 === 0,
  };

  if (level === "light") return light;

  const riskScore = Math.sqrt(amount + 1) * Math.log(quantity + 2);
  const segment = riskScore > 50 ? "enterprise" : riskScore > 20 ? "growth" : "starter";
  return {
    ...light,
    riskScore,
    segment,
    normalizedEmail: String(row.email ?? "").trim().toLowerCase(),
    digest: `${row.id}:${segment}:${Math.round(riskScore * 100)}`,
  };
}

export async function ensureCsvFixture(path: string, shape: DatasetShape): Promise<string> {
  await mkdir(dirname(path), { recursive: true });
  await write(syntheticRows(shape), path, { format: "csv" });
  return path;
}

export async function ensureXlsxFixture(path: string, shape: DatasetShape): Promise<string> {
  await mkdir(dirname(path), { recursive: true });
  await write(syntheticRows(shape), path, { format: "xlsx" });
  return path;
}

export function scaleFromEnv(defaultScales: number[]): number[] {
  return (process.env.PRAVAAH_BENCH_SCALES ?? defaultScales.join(","))
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
}

export function limitFromEnv(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
