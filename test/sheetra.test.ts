import { mkdtemp, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { diff, evaluateFormula, query, read, schema, workerMap, write } from "../src/index.js";

describe("Sheetra pipeline", () => {
  it("maps, filters, and collects rows", async () => {
    const rows = await read([
      { name: "Ada", score: 10 },
      { name: "Grace", score: 3 },
    ])
      .map((row) => ({ ...row, total: Number(row.score) * 2 }))
      .filter((row) => Number(row.total) > 10)
      .collect();

    expect(rows).toEqual([{ name: "Ada", score: 10, total: 20 }]);
  });

  it("drains rows without collecting them", async () => {
    const stats = await read([{ id: 1 }, { id: 2 }]).drain();

    expect(stats.rowsProcessed).toBe(2);
    expect(stats.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("validates and cleans typed rows", async () => {
    const rows = await read([{ "E-mail": " ada@example.com ", age: "42", joined: "2026-01-01" }])
      .clean({ trim: true, fuzzyHeaders: { email: ["E-mail"] } })
      .schema({
        email: schema.email(),
        age: schema.number(),
        joined: schema.date(),
      })
      .collect();

    expect(rows[0]?.email).toBe("ada@example.com");
    expect(rows[0]?.age).toBe(42);
    expect(rows[0]?.joined).toBeInstanceOf(Date);
  });

  it("round-trips CSV files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "rows.csv");

    await write([{ name: "Ada", score: 10 }], file, { format: "csv" });
    const rows = await read(file).collect();
    const raw = await readFile(file, "utf8");

    expect(raw).toContain("name,score");
    expect(rows).toEqual([{ name: "Ada", score: 10 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("round-trips basic XLSX files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "rows.xlsx");

    await write([{ name: "Ada", score: 10 }], file, { format: "xlsx" });
    const rows = await read(file).collect();

    expect(rows).toEqual([{ name: "Ada", score: 10 }]);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Sheetra differentiators", () => {
  it("evaluates common formulas", () => {
    expect(evaluateFormula("=SUM(a,b,5)", { a: 2, b: 3 })).toBe(10);
    expect(evaluateFormula("=IF(active,\"yes\",\"no\")", { active: true })).toBe("yes");
  });

  it("queries and diffs rows", async () => {
    const rows = [
      { id: 1, name: "Ada", score: 10 },
      { id: 2, name: "Grace", score: 3 },
    ];

    await expect(query(rows, "SELECT name, score WHERE score > 5")).resolves.toEqual([{ name: "Ada", score: 10 }]);
    expect(diff(rows, [{ id: 1, name: "Ada", score: 11 }], { key: "id" })).toMatchObject({
      added: [],
      removed: [{ id: 2, name: "Grace", score: 3 }],
      unchanged: 0,
    });
  });

  it("runs mapper work in worker threads", async () => {
    await expect(workerMap([{ value: 2 }, { value: 3 }], "(row) => ({ value: row.value * 2 })", { concurrency: 2 })).resolves.toEqual([
      { value: 4 },
      { value: 6 },
    ]);
  });
});
