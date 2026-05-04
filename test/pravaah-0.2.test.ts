import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import {
  evaluateFormula,
  parse,
  query,
  read,
  readJsonl,
  schema,
  validateRows,
  write,
  writeJsonl,
} from "../src/index.js";
import { runCli } from "../src/cli/index.js";

async function tmp(): Promise<string> {
  return mkdtemp(join(tmpdir(), "pravaah-02-"));
}

describe("Schema v2 - kinds and constraints", () => {
  it("validates integer, enum, url, uuid, regex, literal, and array kinds", async () => {
    const rows = await parse(
      Buffer.from(
        "id,stage,home,ref,code,flag,tags\n" +
          "550e8400-e29b-41d4-a716-446655440000,new,https://ex.com,ABC-1,hi,yes,\"a,b,c\"\n",
      ),
      {
        id: schema.uuid(),
        stage: schema.enum(["new", "open", "closed"] as const),
        home: schema.url(),
        ref: schema.regex(/^[A-Z]+-\d+$/),
        code: schema.string({ length: 2 }),
        flag: schema.literal("yes"),
        tags: schema.array<string>("string"),
      },
      { format: "csv" },
    );

    expect(rows[0]?.stage).toBe("new");
    expect(rows[0]?.tags).toEqual(["a", "b", "c"]);
    expect(rows[0]?.flag).toBe("yes");
  });

  it("flags min/max/length constraint violations", () => {
    const result = validateRows(
      [
        { age: 200, name: "ok" },
        { age: 30, name: "x" },
      ],
      {
        age: schema.integer({ min: 0, max: 120 }),
        name: schema.string({ minLength: 2 }),
      },
      { mode: "collect" },
    );

    expect(result.rows).toHaveLength(0);
    expect(result.issues.map((issue) => issue.code).sort()).toEqual(["constraint_failed", "constraint_failed"]);
  });

  it("applies transform before coerce", () => {
    const result = validateRows(
      [{ price: "$1,234.50" }],
      {
        price: schema.number({ transform: (raw) => String(raw).replace(/[$,]/g, "") }),
      },
    );

    expect(result.rows[0]?.price).toBeCloseTo(1234.5);
  });

  it("runs refine hooks for cross-field validation via validateRows", () => {
    const result = validateRows(
      [{ start: "2025-01-01", end: "2024-01-01" }],
      {
        start: schema.date(),
        end: schema.date(),
      },
      {
        mode: "collect",
        refine: (row) => (row.end < row.start ? { column: "end", message: "end must be after start" } : undefined),
      },
    );

    expect(result.issues[0]?.message).toBe("end must be after start");
  });

  it("supports pipeline.refine() after schema()", async () => {
    const result = await read([
      { start: "2025-01-01", end: "2024-01-01" },
    ])
      .schema({ start: schema.date(), end: schema.date() })
      .refine((row) => (row.end < row.start ? "end < start" : undefined))
      .process();

    expect(result.rows).toHaveLength(0);
    expect(result.issues[0]?.code).toBe("refine_failed");
  });
});

describe("Pipeline fusion and progress", () => {
  it("emits progress events", async () => {
    const events: number[] = [];
    const stats = await read(Array.from({ length: 3 }, (_, i) => ({ id: i })))
      .onProgress((event) => events.push(event.rowsProcessed))
      .drain();

    expect(stats.rowsProcessed).toBe(3);
    expect(events.at(-1)).toBe(3);
  });

  it("fuses clean + schema + map + filter in a single pass", async () => {
    let mapCalls = 0;
    const rows = await read([
      { "E-mail": " ada@example.com ", total: "10" },
      { "E-mail": "", total: "5" },
      { "E-mail": " grace@example.com", total: "20" },
    ])
      .clean({ trim: true, fuzzyHeaders: { email: ["E-mail"] } })
      .schema({ email: schema.email(), total: schema.number() }, { validation: "skip" })
      .map((row) => {
        mapCalls += 1;
        return { ...row, doubled: row.total * 2 };
      })
      .filter((row) => row.doubled > 15)
      .collect();

    expect(rows).toHaveLength(2);
    expect(mapCalls).toBe(2);
  });
});

describe("JSONL read/write", () => {
  it("reads and writes JSONL", async () => {
    const dir = await tmp();
    const file = join(dir, "out.jsonl");
    await write(
      [
        { id: 1, name: "Ada" },
        { id: 2, name: "Grace" },
      ],
      file,
    );

    const collected = await read(file).collect();
    expect(collected).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);

    await rm(dir, { recursive: true, force: true });
  });

  it("streams JSONL from buffer", async () => {
    const jsonl = Buffer.from('{"a":1}\n{"a":2}\n');
    const rows: unknown[] = [];
    for await (const row of readJsonl(jsonl)) rows.push(row);
    expect(rows).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it("rejects non-object JSONL lines", async () => {
    const jsonl = Buffer.from("[1,2,3]\n");
    await expect(async () => {
      for await (const _ of readJsonl(jsonl)) void _;
    }).rejects.toThrow(/lines must be objects/);
  });

  it("gzip-round-trips JSONL with .gz extension", async () => {
    const dir = await tmp();
    const file = join(dir, "out.jsonl.gz");
    await writeJsonl([{ id: 1 }, { id: 2 }], file);
    const rows = await read(file, { format: "jsonl" }).collect();
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("CSV gzip support", () => {
  it("writes and reads .csv.gz", async () => {
    const dir = await tmp();
    const file = join(dir, "roundtrip.csv.gz");
    await write([{ id: 1, name: "Ada" }, { id: 2, name: "Grace" }], file);
    const rows = await read(file, { format: "csv" }).collect();
    expect(rows).toEqual([
      { id: "1", name: "Ada" },
      { id: "2", name: "Grace" },
    ]);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Streaming XLSX writer", () => {
  it("writes XLSX that readXlsx can consume", async () => {
    const dir = await tmp();
    const file = join(dir, "book.xlsx");
    await write([{ id: 1, name: "Ada" }, { id: 2, name: "Grace" }], file);
    const rows = await read(file, { format: "xlsx" }).collect();
    expect(rows).toEqual([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ]);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Query v2", () => {
  const dataset = [
    { team: "a", name: "Ada", score: 10 },
    { team: "a", name: "Linus", score: 30 },
    { team: "b", name: "Grace", score: 7 },
  ];

  it("supports AND/OR/NOT combinations", async () => {
    await expect(query(dataset, "SELECT name WHERE team = 'a' AND score > 15")).resolves.toEqual([{ name: "Linus" }]);
    await expect(query(dataset, "SELECT name WHERE team = 'b' OR score >= 30")).resolves.toHaveLength(2);
    await expect(query(dataset, "SELECT name WHERE NOT team = 'a'")).resolves.toEqual([{ name: "Grace" }]);
  });

  it("supports IN and LIKE", async () => {
    await expect(query(dataset, "SELECT name WHERE team IN ('b')")).resolves.toEqual([{ name: "Grace" }]);
    await expect(query(dataset, "SELECT name WHERE name LIKE 'A%'")).resolves.toEqual([{ name: "Ada" }]);
  });

  it("supports GROUP BY with aggregates", async () => {
    const result = await query(dataset, "SELECT team, COUNT(*) AS n, SUM(score) AS total GROUP BY team ORDER BY team");
    expect(result).toEqual([
      { team: "a", n: 2, total: 40 },
      { team: "b", n: 1, total: 7 },
    ]);
  });
});

describe("Formula engine v2", () => {
  it("evaluates nested function calls inside arithmetic", () => {
    expect(evaluateFormula("IF(score > 5, SUM(score, bonus) * 2, 0)", { score: 10, bonus: 3 })).toBe(26);
  });

  it("supports new functions", () => {
    expect(evaluateFormula("ROUND(3.14159, 2)")).toBe(3.14);
    expect(evaluateFormula("ABS(-5)")).toBe(5);
    expect(evaluateFormula("UPPER('abc')")).toBe("ABC");
    expect(evaluateFormula("TRIM('  hi  ')")).toBe("hi");
    expect(evaluateFormula("LEN('pravaah')")).toBe(7);
    expect(evaluateFormula("AND(true, 1, 'yes')")).toBe(true);
    expect(evaluateFormula("OR(false, 0, '')")).toBe(false);
    expect(evaluateFormula("NOT(0)")).toBe(true);
    expect(evaluateFormula("ISBLANK('')")).toBe(true);
  });

  it("supports comparison and string concat", () => {
    expect(evaluateFormula("score > 5", { score: 10 })).toBe(true);
    expect(evaluateFormula("'a' & 'b'")).toBe("ab");
  });
});

describe("CLI", () => {
  it("prints help on no args", async () => {
    const originalWrite = process.stdout.write.bind(process.stdout);
    const output: string[] = [];
    process.stdout.write = ((chunk: string) => {
      output.push(chunk);
      return true;
    }) as typeof process.stdout.write;
    try {
      const code = await runCli([]);
      expect(code).toBe(0);
      expect(output.join("")).toContain("Usage: pravaah");
    } finally {
      process.stdout.write = originalWrite;
    }
  });

  it("runs head, stats, convert, validate, diff, and query commands", async () => {
    const dir = await tmp();
    const csv = join(dir, "data.csv");
    const json = join(dir, "data.jsonl");
    const csvAfter = join(dir, "data-after.csv");
    const schemaPath = join(dir, "schema.json");
    const issues = join(dir, "issues.csv");

    await writeFile(csv, "id,name\n1,Ada\n2,Grace\n");
    await writeFile(csvAfter, "id,name\n1,Ada\n3,Linus\n");
    await writeFile(schemaPath, JSON.stringify({ id: "number", name: "string" }));

    const originalWrite = process.stdout.write.bind(process.stdout);
    const captured: string[] = [];
    process.stdout.write = ((chunk: string) => {
      captured.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      expect(await runCli(["head", csv, "--rows", "1"])).toBe(0);
      expect(await runCli(["stats", csv])).toBe(0);
      expect(await runCli(["convert", csv, json])).toBe(0);
      expect(await runCli(["validate", csv, "--schema", schemaPath])).toBe(0);
      expect(await runCli(["diff", csv, csvAfter, "--key", "id", "--report", issues])).toBe(0);
      expect(await runCli(["query", csv, "--sql", "SELECT name WHERE id = 2"])).toBe(0);
    } finally {
      process.stdout.write = originalWrite;
    }

    const converted = await readFile(json, "utf8");
    expect(converted).toContain("Ada");

    await rm(dir, { recursive: true, force: true });
  });

  it("refuses to import JavaScript schema without --allow-js-schema", async () => {
    const dir = await tmp();
    const csv = join(dir, "data.csv");
    const jsSchema = join(dir, "schema.mjs");
    await writeFile(csv, "id,name\n1,Ada\n");
    await writeFile(jsSchema, "export default { id: 'number', name: 'string' };\n");

    const originalStdout = process.stdout.write.bind(process.stdout);
    const originalStderr = process.stderr.write.bind(process.stderr);
    const errors: string[] = [];
    process.stdout.write = (() => true) as typeof process.stdout.write;
    process.stderr.write = ((chunk: string) => {
      errors.push(chunk);
      return true;
    }) as typeof process.stderr.write;

    try {
      expect(await runCli(["validate", csv, "--schema", jsSchema])).toBe(1);
      expect(errors.join("")).toContain("--allow-js-schema");
      expect(await runCli(["validate", csv, "--schema", jsSchema, "--allow-js-schema"])).toBe(0);
    } finally {
      process.stdout.write = originalStdout;
      process.stderr.write = originalStderr;
    }
    await rm(dir, { recursive: true, force: true });
  });

  it("returns exit code 2 for validation failures", async () => {
    const dir = await tmp();
    const csv = join(dir, "data.csv");
    const schemaPath = join(dir, "schema.json");
    await writeFile(csv, "id,name\nnot-a-number,Ada\n");
    await writeFile(schemaPath, JSON.stringify({ id: "number", name: "string" }));

    const originalStdout = process.stdout.write.bind(process.stdout);
    process.stdout.write = (() => true) as typeof process.stdout.write;
    try {
      expect(await runCli(["validate", csv, "--schema", schemaPath])).toBe(2);
    } finally {
      process.stdout.write = originalStdout;
    }
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Query multi-column SELECT regressions", () => {
  const rows = [
    { team: "a", name: "Ada", score: 10 },
    { team: "a", name: "Linus", score: 30 },
    { team: "b", name: "Grace", score: 7 },
  ];

  it("projects two bare columns without eating one as alias", async () => {
    await expect(query(rows, "SELECT team, name WHERE score > 5 ORDER BY name")).resolves.toEqual([
      { team: "a", name: "Ada" },
      { team: "b", name: "Grace" },
      { team: "a", name: "Linus" },
    ]);
  });

  it("projects three columns", async () => {
    await expect(query(rows, "SELECT team, name, score WHERE team = 'a' ORDER BY score")).resolves.toEqual([
      { team: "a", name: "Ada", score: 10 },
      { team: "a", name: "Linus", score: 30 },
    ]);
  });
});

describe("Pipeline re-iteration and branching isolation", () => {
  it("produces identical results when collected twice", async () => {
    const pipeline = read([
      { id: 1, name: "Ada" },
      { id: 2, name: "Grace" },
    ])
      .schema({ id: schema.number(), name: schema.string() });

    const first = await pipeline.collect();
    const second = await pipeline.collect();
    expect(first).toEqual(second);
    expect(first).toHaveLength(2);
  });

  it("does not leak dedupe state between two runs", async () => {
    const pipeline = read([
      { id: 1 },
      { id: 1 },
      { id: 2 },
    ]).clean({ dedupeKey: "id" });

    const first = await pipeline.collect();
    const second = await pipeline.collect();
    expect(first).toHaveLength(2);
    expect(second).toHaveLength(2);
  });

  it("tracks row numbers from 1 on every iteration", async () => {
    const result = await read([{ id: "x" }, { id: "y" }])
      .schema({ id: schema.number() }, { validation: "collect" })
      .process();
    expect(result.issues.map((issue) => issue.rowNumber)).toEqual([1, 2]);

    const result2 = await read([{ id: "x" }, { id: "y" }])
      .schema({ id: schema.number() }, { validation: "collect" })
      .process();
    expect(result2.issues.map((issue) => issue.rowNumber)).toEqual([1, 2]);
  });

  it("branches independently after .filter()", async () => {
    const base = read([{ v: 1 }, { v: 2 }, { v: 3 }]);
    const evens = base.filter((row) => Number(row.v) % 2 === 0);
    const odds = base.filter((row) => Number(row.v) % 2 === 1);
    expect(await evens.collect()).toEqual([{ v: 2 }]);
    expect(await odds.collect()).toEqual([{ v: 1 }, { v: 3 }]);
  });
});

describe("XLSX writer edge cases", () => {
  it("rejects gzip option on XLSX writes", async () => {
    const dir = await tmp();
    const file = join(dir, "bad.xlsx");
    await expect(write([{ id: 1 }], file, { gzip: true })).rejects.toThrow(/gzip/);
    await rm(dir, { recursive: true, force: true });
  });

  it("writes an empty sheet when no rows are given", async () => {
    const dir = await tmp();
    const file = join(dir, "empty.xlsx");
    await write([], file, { format: "xlsx" });
    const rows = await read(file, { format: "xlsx" }).collect();
    expect(rows).toEqual([]);
    await rm(dir, { recursive: true, force: true });
  });

  it("preserves order and values for a larger streamed batch", async () => {
    const dir = await tmp();
    const file = join(dir, "large.xlsx");
    const input = Array.from({ length: 500 }, (_, i) => ({ id: i, label: `row-${i}` }));
    await write(input, file);
    const rows = await read(file, { format: "xlsx" }).collect();
    expect(rows).toHaveLength(500);
    expect(rows[0]).toEqual({ id: 0, label: "row-0" });
    expect(rows[499]).toEqual({ id: 499, label: "row-499" });
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Formula parser comparisons inside IF", () => {
  it("treats = inside IF() as equality, not assignment", () => {
    expect(evaluateFormula("IF(a = 1, 'eq', 'ne')", { a: 1 })).toBe("eq");
    expect(evaluateFormula("IF(a = 1, 'eq', 'ne')", { a: 2 })).toBe("ne");
  });

  it("handles nested comparisons", () => {
    expect(evaluateFormula("IF(score >= 90, 'A', IF(score >= 80, 'B', 'C'))", { score: 85 })).toBe("B");
  });
});

describe("Formula engine edge cases and error paths", () => {
  it("handles division by zero and arithmetic with non-numeric operands", () => {
    expect(evaluateFormula("10 / 0")).toBe(null);
    expect(evaluateFormula("'hi' + 2")).toBe("'hi' + 2");
  });

  it("supports <>, <=, != string comparisons", () => {
    expect(evaluateFormula("'a' <> 'b'")).toBe(true);
    expect(evaluateFormula("'a' != 'a'")).toBe(false);
    expect(evaluateFormula("'a' <= 'b'")).toBe(true);
    expect(evaluateFormula("'b' >= 'a'")).toBe(true);
  });

  it("compares numbers vs strings and mixed equality", () => {
    expect(evaluateFormula("'1' = 1")).toBe(true);
    expect(evaluateFormula("2 > 1")).toBe(true);
    expect(evaluateFormula("1 < 2")).toBe(true);
  });

  it("evaluates unary plus and minus", () => {
    expect(evaluateFormula("-5 + 2")).toBe(-3);
    expect(evaluateFormula("+3")).toBe(3);
  });

  it("supports TRUE, FALSE, NULL literals", () => {
    expect(evaluateFormula("true")).toBe(true);
    expect(evaluateFormula("false")).toBe(false);
    expect(evaluateFormula("null")).toBe(null);
  });

  it("returns null on ABS of non-number and ROUND of non-number", () => {
    expect(evaluateFormula("ABS('x')")).toBe(null);
    expect(evaluateFormula("ROUND('x', 2)")).toBe(null);
  });

  it("DATEDIF supports D/H/M/Y units and rejects invalid dates", () => {
    expect(evaluateFormula("DATEDIF('2024-01-01', '2024-01-11', 'D')")).toBe(10);
    expect(evaluateFormula("DATEDIF('2024-01-01', '2024-03-01', 'M')")).toBe(2);
    expect(evaluateFormula("DATEDIF('2024-01-01', '2025-01-01', 'Y')")).toBe(1);
    expect(evaluateFormula("DATEDIF('2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z', 'H')")).toBe(24);
    expect(evaluateFormula("DATEDIF('bad', '2024-01-01', 'D')")).toBe(null);
    expect(evaluateFormula("DATEDIF('2024-01-01', '2024-01-02', 'X')")).toBe(null);
  });

  it("TODAY/NOW return Dates", () => {
    expect(evaluateFormula("TODAY()")).toBeInstanceOf(Date);
    expect(evaluateFormula("NOW()")).toBeInstanceOf(Date);
  });

  it("throws on unknown functions", () => {
    expect(() => evaluateFormula("MISSING(1)")).toThrow(/Unsupported formula function/);
  });
});

describe("Schema kind coercion edge cases", () => {
  it("coerces numeric date strings via epoch seconds and ms", () => {
    const a = validateRows([{ ts: "1704067200" }], { ts: schema.date() });
    expect(a.rows[0]?.ts).toBeInstanceOf(Date);
    const b = validateRows([{ ts: "1704067200000" }], { ts: schema.date() });
    expect(b.rows[0]?.ts).toBeInstanceOf(Date);
    const c = validateRows([{ ts: 1704067200000 }], { ts: schema.date() });
    expect(c.rows[0]?.ts).toBeInstanceOf(Date);
  });

  it("rejects invalid Date instances", () => {
    const result = validateRows([{ ts: new Date("bad") }], { ts: schema.date() }, { mode: "collect" });
    expect(result.rows).toHaveLength(0);
    expect(result.issues[0]?.code).toBe("invalid_type");
  });

  it("rejects regex kinds with no pattern and malformed url/uuid", () => {
    const r = validateRows(
      [{ a: "abc", url: "not a url", id: "not-uuid" }],
      {
        a: schema.regex(/^[0-9]+$/),
        url: schema.url(),
        id: schema.uuid(),
      },
      { mode: "collect" },
    );
    expect(r.issues.map((issue) => issue.column).sort()).toEqual(["a", "id", "url"]);
  });

  it("accepts numeric enums via coercion", () => {
    const result = validateRows(
      [{ level: "2" }],
      { level: schema.enum([1, 2, 3] as const) },
    );
    expect(result.rows[0]?.level).toBe(2);
  });

  it("accepts literal via string coercion", () => {
    const result = validateRows([{ flag: "yes" }], { flag: schema.literal("yes") });
    expect(result.rows[0]?.flag).toBe("yes");
  });

  it("array without `of` returns raw trimmed entries", () => {
    const result = validateRows(
      [{ tags: " a ; b | c , d " }],
      { tags: schema.array("string" as const) },
    );
    expect(result.rows[0]?.tags).toEqual(["a", "b", "c", "d"]);
  });

  it("array rejects when an inner entry fails coercion", () => {
    const result = validateRows(
      [{ nums: "1,not-a-number,3" }],
      { nums: schema.array("number" as const) },
      { mode: "collect" },
    );
    expect(result.rows).toHaveLength(0);
    expect(result.issues[0]?.code).toBe("invalid_type");
  });

  it("applies defaultValue and optional for missing cells", () => {
    const result = validateRows(
      [{}, { name: "" }],
      {
        name: schema.string({ optional: true }),
        level: schema.integer({ defaultValue: 1 }),
      },
    );
    expect(result.rows).toEqual([
      { name: undefined, level: 1 },
      { name: undefined, level: 1 },
    ]);
  });

  it("reports transform failures as issues without throwing", () => {
    const result = validateRows(
      [{ raw: "x" }],
      {
        raw: schema.string({
          transform: () => {
            throw new Error("boom");
          },
        }),
      },
      { mode: "collect" },
    );
    expect(result.issues[0]?.code).toBe("transform_failed");
  });

  it("checks date min/max constraints", () => {
    const result = validateRows(
      [{ d: "2020-01-01" }],
      { d: schema.date({ min: new Date("2024-01-01").getTime() }) },
      { mode: "collect" },
    );
    expect(result.issues[0]?.code).toBe("constraint_failed");
  });

  it("checks array minLength/maxLength constraints", () => {
    const result = validateRows(
      [{ tags: "a,b,c,d" }],
      { tags: schema.array<string>("string", { maxLength: 2 }) },
      { mode: "collect" },
    );
    expect(result.issues[0]?.code).toBe("constraint_failed");
  });
});

describe("Pipeline ancillary paths", () => {
  it("processes a schema-validated pipeline with validation='skip'", async () => {
    const result = await read([{ id: "x" }, { id: "1" }])
      .schema({ id: schema.number() }, { validation: "skip" })
      .process();
    expect(result.rows).toHaveLength(1);
    expect(result.issues).toHaveLength(0);
  });

  it("pipeline.drain collects validation issues via process() semantics", async () => {
    const stats = await read([{ id: "x" }, { id: "y" }])
      .schema({ id: schema.number() }, { validation: "collect" })
      .drain();
    expect(stats.errors).toBe(2);
  });

  it("refine with no prior schema throws on fail-fast", async () => {
    const pipeline = read([{ id: 1 }])
      .refine((row) => ((row as { id: number }).id === 1 ? "reject" : undefined));
    await expect(pipeline.collect()).rejects.toThrow(/Pravaah validation failed/);
  });

  it("rejects unsupported read formats", () => {
    expect(() => read(Buffer.from(""), { format: "wat" as "csv" })).toThrow(/Unsupported read format/);
  });

  it("rejects unsupported write formats", async () => {
    const dir = await tmp();
    await expect(write([{ id: 1 }], join(dir, "out.bin"), { format: "bin" as never })).rejects.toThrow();
    await rm(dir, { recursive: true, force: true });
  });

  it("write() auto-infers format for .jsonl.gz writes and reads back", async () => {
    const dir = await tmp();
    const file = join(dir, "x.jsonl.gz");
    await write([{ a: 1 }, { a: 2 }], file);
    const rows = await read(file).collect();
    expect(rows).toEqual([{ a: 1 }, { a: 2 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("parses JSON file format", async () => {
    const dir = await tmp();
    const file = join(dir, "data.json");
    await writeFile(file, JSON.stringify([{ id: 1 }, { id: 2 }]));
    const rows = await read(file).collect();
    expect(rows).toEqual([{ id: 1 }, { id: 2 }]);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("CLI error and help branches", () => {
  it("prints unknown command and returns 1", async () => {
    const originalStdout = process.stdout.write.bind(process.stdout);
    const originalStderr = process.stderr.write.bind(process.stderr);
    const stdout: string[] = [];
    const stderr: string[] = [];
    process.stdout.write = ((chunk: string) => {
      stdout.push(chunk);
      return true;
    }) as typeof process.stdout.write;
    process.stderr.write = ((chunk: string) => {
      stderr.push(chunk);
      return true;
    }) as typeof process.stderr.write;
    try {
      expect(await runCli(["nope"])).toBe(1);
      expect(stderr.join("")).toContain("Unknown command");
      expect(stdout.join("")).toContain("Usage: pravaah");
    } finally {
      process.stdout.write = originalStdout;
      process.stderr.write = originalStderr;
    }
  });

  it("returns 1 when required arguments are missing", async () => {
    const originalStdout = process.stdout.write.bind(process.stdout);
    const originalStderr = process.stderr.write.bind(process.stderr);
    process.stdout.write = (() => true) as typeof process.stdout.write;
    process.stderr.write = (() => true) as typeof process.stderr.write;
    try {
      expect(await runCli(["head"])).toBe(1);
      expect(await runCli(["stats"])).toBe(1);
      expect(await runCli(["validate"])).toBe(1);
      expect(await runCli(["convert"])).toBe(1);
      expect(await runCli(["diff"])).toBe(1);
      expect(await runCli(["query"])).toBe(1);
    } finally {
      process.stdout.write = originalStdout;
      process.stderr.write = originalStderr;
    }
  });

  it("supports --flag=value inline form", async () => {
    const dir = await tmp();
    const csv = join(dir, "data.csv");
    await writeFile(csv, "id,name\n1,Ada\n2,Grace\n");

    const originalStdout = process.stdout.write.bind(process.stdout);
    process.stdout.write = (() => true) as typeof process.stdout.write;
    try {
      expect(await runCli(["head", csv, "--rows=1"])).toBe(0);
    } finally {
      process.stdout.write = originalStdout;
    }
    await rm(dir, { recursive: true, force: true });
  });
});

describe("XLSX reader ancillary paths", () => {
  it("reads a header-less sheet when headers: false is passed", async () => {
    const dir = await tmp();
    const file = join(dir, "headerless.xlsx");
    await write(
      [
        [1, "Ada"],
        [2, "Grace"],
      ],
      file,
      { format: "xlsx", headers: ["id", "name"] },
    );
    const rows = await read(file, { format: "xlsx", headers: false }).collect();
    expect(rows).toHaveLength(3);
    await rm(dir, { recursive: true, force: true });
  });

  it("reads with explicit headers array", async () => {
    const dir = await tmp();
    const file = join(dir, "explicit.xlsx");
    await write([{ a: 1, b: 2 }], file, { format: "xlsx" });
    const rows = await read(file, { format: "xlsx", headers: ["col1", "col2"] }).collect();
    expect(rows).toHaveLength(2);
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Query v2 ancillary paths", () => {
  const rows = [
    { team: "a", name: "Ada", score: 10 },
    { team: "b", name: "Grace", score: 7 },
  ];

  it("handles equality via IN and identity column references", async () => {
    await expect(query(rows, "SELECT name WHERE team IN ('a', 'b')")).resolves.toHaveLength(2);
  });

  it("accepts boolean and null literals in WHERE", async () => {
    await expect(query([{ flag: true }, { flag: false }], "SELECT flag WHERE flag = true")).resolves.toEqual([
      { flag: true },
    ]);
    await expect(query([{ x: null }, { x: 1 }], "SELECT x WHERE x = null")).resolves.toEqual([{ x: null }]);
  });

  it("supports double-quoted string literals", async () => {
    await expect(query(rows, 'SELECT name WHERE team = "a"')).resolves.toEqual([{ name: "Ada" }]);
  });

  it("returns AVG/MIN/MAX as expected", async () => {
    await expect(
      query(rows, "SELECT AVG(score) AS avg, MIN(score) AS min, MAX(score) AS max"),
    ).resolves.toEqual([{ avg: 8.5, min: 7, max: 10 }]);
  });

  it("returns null aggregates on empty groups", async () => {
    const result = await query([{ id: 1 }], "SELECT SUM(missing) AS total");
    expect(result[0]?.total).toBe(null);
  });
});
