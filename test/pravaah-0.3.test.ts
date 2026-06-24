import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { afterAll, describe, expect, it } from "vitest";
import { query, workerMap, write } from "../src/index.js";
import { queryStream } from "../src/query/index.js";
import { FormulaEngine, PluginRegistry } from "../src/index.js";

const tmpDirs: string[] = [];

async function tmp(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "pravaah-03-"));
  tmpDirs.push(dir);
  return dir;
}

afterAll(async () => {
  await Promise.all(tmpDirs.map((dir) => rm(dir, { recursive: true, force: true })));
});

describe("Query v3 - JOIN", () => {
  const orders = [
    { id: 1, customer: "alice", total: 100 },
    { id: 2, customer: "bob", total: 50 },
    { id: 3, customer: "alice", total: 25 },
  ];
  const customers = [
    { customer: "alice", region: "west" },
    { customer: "bob", region: "east" },
  ];

  it("performs an inner JOIN ... ON across two sources", async () => {
    const rows = await query(orders, "SELECT id, region, total FROM orders JOIN customers ON customer", {
      join: { customers },
    });
    expect(rows).toEqual([
      { id: 1, region: "west", total: 100 },
      { id: 2, region: "east", total: 50 },
      { id: 3, region: "west", total: 25 },
    ]);
  });

  it("supports aggregates over joined rows with GROUP BY", async () => {
    const rows = await query(
      orders,
      "SELECT region, SUM(total) AS revenue FROM orders JOIN customers ON customer GROUP BY region ORDER BY revenue desc",
      { join: { customers } },
    );
    expect(rows).toEqual([
      { region: "west", revenue: 125 },
      { region: "east", revenue: 50 },
    ]);
  });

  it("drops left rows with no join match (inner join semantics)", async () => {
    const rows = await query(
      [{ customer: "ghost", total: 5 }, { customer: "alice", total: 10 }],
      "SELECT customer, region FROM o JOIN c ON customer",
      { join: { c: customers } },
    );
    expect(rows).toEqual([{ customer: "alice", region: "west" }]);
  });

  it("throws when JOIN is requested without a matching join source", async () => {
    await expect(
      query(orders, "SELECT * FROM orders JOIN missing ON customer", { join: { customers } }),
    ).rejects.toThrow(/join source/i);
  });
});

describe("Query v3 - HAVING", () => {
  const rows = [
    { region: "west", total: 100 },
    { region: "west", total: 25 },
    { region: "east", total: 50 },
  ];

  it("filters grouped rows with HAVING", async () => {
    const out = await query(
      rows,
      "SELECT region, SUM(total) AS revenue FROM r GROUP BY region HAVING revenue > 60",
    );
    expect(out).toEqual([{ region: "west", revenue: 125 }]);
  });
});

describe("Query v3 - multi-column ORDER BY", () => {
  it("orders by multiple keys with mixed directions", async () => {
    const rows = [
      { region: "west", name: "b" },
      { region: "east", name: "z" },
      { region: "west", name: "a" },
    ];
    const out = await query(rows, "SELECT region, name FROM r ORDER BY region asc, name desc");
    expect(out).toEqual([
      { region: "east", name: "z" },
      { region: "west", name: "b" },
      { region: "west", name: "a" },
    ]);
  });
});

describe("Query v3 - streaming queryStream", () => {
  it("streams WHERE + projection without buffering all rows", async () => {
    async function* gen(): AsyncIterable<Record<string, unknown>> {
      for (let i = 0; i < 5; i += 1) yield { i, keep: i % 2 === 0 };
    }
    const collected: unknown[] = [];
    for await (const row of queryStream(gen(), "SELECT i FROM g WHERE keep = true")) {
      collected.push(row);
    }
    expect(collected).toEqual([{ i: 0 }, { i: 2 }, { i: 4 }]);
  });

  it("respects LIMIT lazily in streaming mode", async () => {
    let produced = 0;
    async function* gen(): AsyncIterable<Record<string, unknown>> {
      while (true) {
        produced += 1;
        yield { n: produced };
      }
    }
    const out: unknown[] = [];
    for await (const row of queryStream(gen(), "SELECT n FROM g LIMIT 3")) out.push(row);
    expect(out).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    expect(produced).toBe(3);
  });
});

describe("JSON write streams element-by-element", () => {
  it("writes a valid JSON array without buffering the whole dataset", async () => {
    const dir = await tmp();
    const dest = join(dir, "out.json");
    async function* gen(): AsyncIterable<Record<string, unknown>> {
      for (let i = 0; i < 3; i += 1) yield { i };
    }
    const stats = await write(gen(), dest, { format: "json" });
    expect(stats.rowsWritten).toBe(3);
    const parsed = JSON.parse(await readFile(dest, "utf8"));
    expect(parsed).toEqual([{ i: 0 }, { i: 1 }, { i: 2 }]);
  });

  it("writes an empty array for an empty source", async () => {
    const dir = await tmp();
    const dest = join(dir, "empty.json");
    await write([], dest, { format: "json" });
    expect(JSON.parse(await readFile(dest, "utf8"))).toEqual([]);
  });
});

describe("Worker pool - module-backed mappers reuse workers", () => {
  it("processes all rows correctly through a persistent pool", async () => {
    const dir = await tmp();
    const mapperFile = join(dir, "double.mjs");
    await writeFile(mapperFile, "export default (row, index) => ({ out: row.n * 2, index });\n");

    const input = Array.from({ length: 20 }, (_, i) => ({ n: i }));
    const result = await workerMap(input, new URL(`file://${mapperFile}`), { concurrency: 4 });

    expect(result).toHaveLength(20);
    expect(result.map((r) => (r as { out: number }).out)).toEqual(input.map((row) => row.n * 2));
    expect(result.map((r) => (r as { index: number }).index)).toEqual(input.map((_, i) => i));
  });

  it("does not spawn more workers than the configured concurrency", async () => {
    const dir = await tmp();
    const mapperFile = join(dir, "track.mjs");
    // Each worker records its own pid via a module-level counter that only
    // increments once per worker process. With a pool of size 2 over 10 rows
    // we expect at most 2 distinct worker ids.
    await writeFile(
      mapperFile,
      "let id;\nexport default (row) => { id ??= Math.random(); return { id }; };\n",
    );

    const input = Array.from({ length: 10 }, (_, i) => ({ n: i }));
    const result = await workerMap(input, new URL(`file://${mapperFile}`), { concurrency: 2 });
    const distinct = new Set(result.map((r) => (r as { id: number }).id));
    expect(distinct.size).toBeLessThanOrEqual(2);
  });

  it("propagates errors thrown inside a pooled worker", async () => {
    const dir = await tmp();
    const mapperFile = join(dir, "boom.mjs");
    await writeFile(mapperFile, "export default () => { throw new Error('pool boom'); };\n");
    await expect(
      workerMap([{ n: 1 }], new URL(`file://${mapperFile}`), { concurrency: 2 }),
    ).rejects.toThrow("pool boom");
  });
});


describe("Plugins - wired into FormulaEngine and validation", () => {
  it("exposes plugin formulas to a FormulaEngine via plugins option", () => {
    const registry = new PluginRegistry();
    registry.use({
      name: "tax",
      formulas: {
        WITHTAX: (args) => Number(args[0]) * 1.2,
      },
    });
    const engine = new FormulaEngine({ plugins: registry });
    expect(engine.evaluate("WITHTAX(100)")).toBeCloseTo(120);
  });

  it("merges plugin formulas with built-ins and explicit functions", () => {
    const registry = new PluginRegistry();
    registry.use({ name: "p", formulas: { TRIPLE: (args) => Number(args[0]) * 3 } });
    const engine = new FormulaEngine({
      plugins: registry,
      functions: { QUAD: (args) => Number(args[0]) * 4 },
    });
    expect(engine.evaluate("SUM(TRIPLE(2), QUAD(2))")).toBe(14);
  });

  it("runs plugin validators across rows via the registry", () => {
    const registry = new PluginRegistry();
    registry.use({
      name: "positive",
      validators: [
        (row) =>
          typeof row.amount === "number" && row.amount < 0
            ? [{ code: "negative", message: "amount must be positive", severity: "error" as const }]
            : [],
      ],
    });
    const issues = registry.validateRows([{ amount: 5 }, { amount: -1 }]);
    expect(issues).toHaveLength(1);
    expect(issues[0]?.code).toBe("negative");
  });
});

describe("Pipeline - async field validation", () => {
  it("runs async validate() functions inside read().schema()", async () => {
    const { read, schema } = await import("../src/index.js");
    const src = [
      { email: "ok@example.com" },
      { email: "taken@example.com" },
    ];
    const taken = new Set(["taken@example.com"]);
    const result = await read(src)
      .schema(
        {
          email: schema.email({
            validate: async (value: string) => {
              await Promise.resolve();
              return taken.has(value) ? "email already registered" : undefined;
            },
          }),
        },
        { validation: "collect" },
      )
      .process();

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]?.email).toBe("ok@example.com");
    expect(result.issues.map((i) => i.message)).toContain("email already registered");
  });
});

describe("Adapters - stream uploads into parseDetailed", () => {
  it("parses an uploaded CSV stream against a schema", async () => {
    const { Readable } = await import("node:stream");
    const { parseUpload } = await import("../src/adapters/index.js");
    const { schema } = await import("../src/index.js");

    const csv = "name,age\nAlice,30\nBob,oops\n";
    const stream = Readable.from([Buffer.from(csv)]);
    const result = await parseUpload(
      stream,
      { filename: "people.csv" },
      { name: schema.string(), age: schema.integer() },
      { validation: "collect" },
    );

    expect(result.rows).toEqual([{ name: "Alice", age: 30 }]);
    expect(result.issues.some((i) => i.column === "age")).toBe(true);
    expect(result.stats.rowsProcessed).toBeGreaterThan(0);
  });

  it("infers format from filename extension and supports jsonl", async () => {
    const { Readable } = await import("node:stream");
    const { parseUpload } = await import("../src/adapters/index.js");
    const { schema } = await import("../src/index.js");

    const jsonl = '{"id":1}\n{"id":2}\n';
    const stream = Readable.from([Buffer.from(jsonl)]);
    const result = await parseUpload(
      stream,
      { filename: "data.jsonl" },
      { id: schema.integer() },
    );
    expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("throws a clear error for unsupported upload extensions", async () => {
    const { Readable } = await import("node:stream");
    const { parseUpload } = await import("../src/adapters/index.js");
    const stream = Readable.from([Buffer.from("x")]);
    await expect(
      parseUpload(stream, { filename: "notes.txt" }, { id: "string" }),
    ).rejects.toThrow(/unsupported|format/i);
  });
});

describe("XLSX streaming read", () => {
  it("reads rows correctly from a large generated workbook", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "big.xlsx");

    const rowCount = 5000;
    const rows = Array.from({ length: rowCount }, (_, i) => ({
      id: i + 1,
      name: `name-${i + 1}`,
      note: `shared string value ${i % 7}`,
    }));
    await writeXlsx(rows, file);

    const out: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(file)) out.push(row);

    expect(out).toHaveLength(rowCount);
    expect(out[0]).toEqual({ id: 1, name: "name-1", note: "shared string value 0" });
    expect(out[rowCount - 1]).toEqual({ id: rowCount, name: `name-${rowCount}`, note: "shared string value 1" });
  });

  it("supports early break without inflating the whole sheet", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "early.xlsx");

    const rows = Array.from({ length: 20000 }, (_, i) => ({ id: i + 1, label: `row-${i + 1}` }));
    await writeXlsx(rows, file);

    const collected: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(file)) {
      collected.push(row);
      if (collected.length === 3) break;
    }

    expect(collected).toEqual([
      { id: 1, label: "row-1" },
      { id: 2, label: "row-2" },
      { id: 3, label: "row-3" },
    ]);
  });

  it("streams from a Buffer source too", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "buf.xlsx");
    await writeXlsx([{ a: 1, b: "x" }, { a: 2, b: "y" }], file);
    const buf = await readFile(file);

    const out: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(buf)) out.push(row);
    expect(out).toEqual([{ a: 1, b: "x" }, { a: 2, b: "y" }]);
  });
});

describe("XLSX streaming read - edge cases", () => {
  it("returns no rows for an empty sheet", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "empty-stream.xlsx");
    await writeXlsx([], file);
    const out: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(file)) out.push(row);
    expect(out).toEqual([]);
  });

  it("honours headers:false (positional keys) while streaming", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "headerless-stream.xlsx");
    await writeXlsx([{ a: 1, b: 2 }, { a: 3, b: 4 }], file);
    const out: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(file, { headers: false })) out.push(row);
    expect(out).toEqual([
      { _1: "a", _2: "b" },
      { _1: 1, _2: 2 },
      { _1: 3, _2: 4 },
    ]);
  });

  it("honours an explicit headers array while streaming", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "explicit-stream.xlsx");
    await writeXlsx([{ a: 1, b: 2 }], file);
    const out: Array<Record<string, unknown>> = [];
    for await (const row of readXlsx(file, { headers: ["x", "y"] })) out.push(row);
    expect(out).toEqual([
      { x: "a", y: "b" },
      { x: 1, y: 2 },
    ]);
  });

  it("throws for a missing named sheet", async () => {
    const { writeXlsx, readXlsx } = await import("../src/xlsx/index.js");
    const dir = await tmp();
    const file = join(dir, "named.xlsx");
    await writeXlsx([{ a: 1 }], file);
    await expect(async () => {
      for await (const _row of readXlsx(file, { sheet: "Nope" })) void _row;
    }).rejects.toThrow(/Worksheet not found/);
  });
});
