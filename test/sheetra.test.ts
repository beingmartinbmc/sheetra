import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { strToU8, zipSync } from "fflate";
import { describe, expect, it } from "vitest";
import { drainCsvViaEvents } from "../src/csv/index.js";
import {
  FormulaEngine,
  PluginRegistry,
  cell,
  cleanRows,
  createIndex,
  createStats,
  diff,
  evaluateFormula,
  finishStats,
  formatBytes,
  formula,
  inferCsv,
  joinRows,
  mergeStats,
  normalizeHeader,
  observeMemory,
  parseDetailed,
  parse,
  plugins,
  query,
  read,
  readCsv,
  readWorkbook,
  schema,
  validateRows,
  writeDiffReport,
  writeIssueReport,
  workbook,
  workerMap,
  worksheet,
  write,
  writeWorkbook,
} from "../src/index.js";

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

  it("returns detailed validation issues and writes issue reports", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const report = join(dir, "issues.csv");
    const result = await parseDetailed(Buffer.from("email,age\nbad,old\nada@example.com,42\n"), {
      email: schema.email(),
      age: schema.number(),
    }, { format: "csv", validation: "collect" });

    await writeIssueReport(result.issues, report);
    const reportText = await readFile(report, "utf8");

    expect(result.rows).toEqual([{ email: "ada@example.com", age: 42 }]);
    expect(result.issues).toHaveLength(2);
    expect(reportText).toContain("invalid_type");
    await rm(dir, { recursive: true, force: true });
  });

  it("round-trips CSV files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "rows.csv");

    await write([{ name: "Ada", score: 10 }], file, { format: "csv" });
    const rows = await read(file).collect();
    const raw = await readFile(file, "utf8");

    expect(raw).toContain("name,score");
    expect(rows).toEqual([{ name: "Ada", score: "10" }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("counts rows via the CSV event-based fast drain", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "drain.csv");

    await write(
      Array.from({ length: 256 }, (_, index) => ({ id: index, name: `User ${index}` })),
      file,
      { format: "csv" },
    );

    const stats = await read(file).drain();
    expect(stats.rowsProcessed).toBe(256);
    await rm(dir, { recursive: true, force: true });
  });

  it("counts CSV rows by scanning quoted records without materializing rows", async () => {
    const input = Buffer.from('id,note\n1,"hello\nworld"\n2,"escaped "" quote"\n\n3,last');
    const stats = await read(input, { format: "csv" }).drain();

    expect(stats.rowsProcessed).toBe(3);
  });

  it("matches CSV drain edge cases for bare quotes, empty rows, and CR line endings", async () => {
    const input = Buffer.from('id,note\r1,abc"def\r,,\r2,last');
    const stats = await read(input, { format: "csv" }).drain();

    expect(stats.rowsProcessed).toBe(2);
  });

  it("rejects malformed quoted CSV in the raw drain scanner", async () => {
    await expect(read(Buffer.from('id,note\n1,"unterminated'), { format: "csv" }).drain()).rejects.toThrow(
      "Unclosed quoted CSV field",
    );
    await expect(read(Buffer.from('id,note\n1,"x"y'), { format: "csv" }).drain()).rejects.toThrow(
      "Invalid quoted CSV field",
    );
  });

  it("falls back to the iterator path when CSV pipelines are transformed", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "transformed.csv");

    await write(
      Array.from({ length: 50 }, (_, index) => ({ id: index, score: index })),
      file,
      { format: "csv" },
    );

    const filtered = await read(file)
      .filter((row) => Number((row as { score: string }).score) % 2 === 0)
      .collect();
    expect(filtered).toHaveLength(25);
    await rm(dir, { recursive: true, force: true });
  });

  it("can infer CSV primitive values when requested", async () => {
    const rows = await read(Buffer.from("name,score,active\nAda,10,true\n"), {
      format: "csv",
      inferTypes: true,
    }).collect();

    expect(rows).toEqual([{ name: "Ada", score: 10, active: true }]);
  });

  it("round-trips basic XLSX files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "rows.xlsx");

    await write([{ name: "Ada", score: 10 }], file, { format: "xlsx" });
    const rows = await read(file).collect();

    expect(rows).toEqual([{ name: "Ada", score: 10 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("reads only the requested sheet from a multi-sheet XLSX", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "multi.xlsx");

    await writeWorkbook(
      workbook([
        worksheet("Leads", [{ name: "Ada", score: 10 }]),
        worksheet("Finance", [{ label: "Gross", amount: 1200 }]),
      ]),
      file,
    );

    const finance = await read(file, { sheet: "Finance" }).collect();
    expect(finance).toEqual([{ label: "Gross", amount: 1200 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("reads shared strings and sparse XLSX cells with the worksheet scanner", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "shared.xlsx");
    const files = {
      "xl/workbook.xml": strToU8(
        '<workbook xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>',
      ),
      "xl/_rels/workbook.xml.rels": strToU8(
        '<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>',
      ),
      "xl/sharedStrings.xml": strToU8("<sst><si><t>name</t></si><si><t>score</t></si><si><t>Ada &#38; Co&#10;LLC</t></si></sst>"),
      "xl/worksheets/sheet1.xml": strToU8(
        "<worksheet><sheetData><row r='1'><c r='A1' t='s'><v>0</v></c><c r='C1' t='s'><v>1</v></c></row><row r='2'><c r='A2' t='s'><v>2</v></c><c r='C2'><v>42</v></c></row></sheetData></worksheet>",
      ),
    };

    await writeFile(file, Buffer.from(zipSync(files)));

    await expect(read(file).collect()).resolves.toEqual([{ name: "Ada & Co\nLLC", _2: null, score: 42 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("round-trips multi-sheet workbooks with formulas and sheet metadata", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "book.xlsx");
    const summary = worksheet("Summary", [{ label: "Total", total: formula("SUM(B2:B3)", 30) }]);
    summary.merges.push("A1:B1");
    summary.validations.push({ range: "B2:B10", type: "whole", formula: "0" });
    summary.tables.push({ name: "SummaryTable", range: "A1:B2", columns: ["label", "total"] });
    summary.frozen = { ySplit: 1, topLeftCell: "A2" };

    await writeWorkbook(
      workbook([
        worksheet("Data", [
          { name: "Ada", score: 10 },
          { name: "Grace", score: 20 },
        ]),
        summary,
      ]),
      file,
    );

    const book = await readWorkbook(file, { formulas: "preserve" });

    expect(book.sheets.map((sheet) => sheet.name)).toEqual(["Data", "Summary"]);
    expect(book.sheets[0]?.rows).toEqual([
      { name: "Ada", score: 10 },
      { name: "Grace", score: 20 },
    ]);
    expect(book.sheets[1]?.rows[0]?.total).toEqual({ formula: "SUM(B2:B3)", result: 30 });
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

    await expect(query(rows, "SELECT name, score WHERE score > 2 ORDER BY score DESC LIMIT 1")).resolves.toEqual([
      { name: "Ada", score: 10 },
    ]);

    const result = diff(rows, [{ id: 1, name: "Ada", score: 11 }], { key: "id" });
    expect(result).toMatchObject({
      added: [],
      removed: [{ id: 2, name: "Grace", score: 3 }],
      unchanged: 0,
    });

    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const report = join(dir, "diff.csv");
    await writeDiffReport(result, report);
    await expect(readFile(report, "utf8")).resolves.toContain("changed");
    await rm(dir, { recursive: true, force: true });
  });

  it("runs mapper work in worker threads", async () => {
    await expect(workerMap([{ value: 2 }, { value: 3 }], "(row) => ({ value: row.value * 2 })", { concurrency: 2 })).resolves.toEqual([
      { value: 4 },
      { value: 6 },
    ]);
  });
});

describe("Schema validation and cleaning", () => {
  it("normalizes headers, dedupes rows, and validates defaults/options", () => {
    const cleaned = cleanRows(
      [
        { "Email Address": " ada@example.com ", id: "1", active: "yes", joined: "2026-01-01", phone: "(555) 123-4567" },
        { "Email Address": " ada@example.com ", id: "1", active: "no", joined: "bad", phone: "x" },
      ],
      {
        trim: true,
        normalizeWhitespace: true,
        dedupeKey: ["id", "Email Address"],
        fuzzyHeaders: { email: ["email address"] },
      },
    );

    expect(normalizeHeader(" Email_ID ")).toBe("email id");
    expect(cleaned).toHaveLength(1);
    expect(cleaned[0]?.email).toBe("ada@example.com");

    const result = validateRows(
      cleaned,
      {
        id: schema.number(),
        email: schema.email(),
        active: schema.boolean(),
        joined: schema.date(),
        phone: schema.phone(),
        role: schema.string({ defaultValue: "lead" }),
        optional: schema.string({ optional: true }),
        raw: schema.any({ defaultValue: { source: "test" } }),
      },
      { mode: "collect" },
    );

    expect(result.issues).toEqual([]);
    expect(result.rows[0]).toMatchObject({
      id: 1,
      email: "ada@example.com",
      active: true,
      role: "lead",
      optional: undefined,
      raw: { source: "test" },
    });
    expect(result.rows[0]?.joined).toBeInstanceOf(Date);
  });

  it("collects custom validation issues and supports fail-fast mode", () => {
    const definition = {
      id: schema.number({ coerce: false }),
      email: schema.email(),
      active: schema.boolean({ coerce: false }),
      joined: schema.date({ coerce: false }),
      phone: schema.phone(),
      score: schema.number({ validate: (value) => (value > 10 ? undefined : "score too low") }),
      missing: "string",
    } as const;

    const result = validateRows(
      [{ id: "1", email: "bad", active: "yes", joined: "2026-01-01", phone: "x", score: 3 }],
      definition,
    );

    expect(result.rows).toEqual([]);
    expect(result.issues.map((issue) => issue.code)).toContain("invalid_type");
    expect(result.issues.map((issue) => issue.code)).toContain("invalid_value");
    expect(result.issues.map((issue) => issue.code)).toContain("missing_column");
    expect(() => validateRows([{ id: "bad" }], { id: schema.number() }, { mode: "fail-fast" })).toThrow(
      "Sheetra validation failed",
    );
  });

  it("writes issue reports with escaped object and date values", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "issues.csv");
    const date = new Date("2026-01-01T00:00:00.000Z");

    await writeIssueReport(
      [
        {
          severity: "error",
          code: "bad",
          message: "contains,comma",
          rowNumber: 1,
          column: "value",
          expected: "string",
          rawValue: { date, text: "a,b" },
        },
        {
          severity: "warning",
          code: "date",
          message: "date",
          rawValue: date,
        },
      ],
      file,
    );

    const text = await readFile(file, "utf8");
    expect(text).toContain("\"contains,comma\"");
    expect(text).toContain("2026-01-01T00:00:00.000Z");
    await rm(dir, { recursive: true, force: true });
  });
});

describe("Query, diff, formula, plugins, and perf utilities", () => {
  it("covers query operators, projection, indexing, and joins", async () => {
    const rows = [
      { id: 1, team: "a", name: "Ada", score: 10, joined: new Date("2026-01-01") },
      { id: 2, team: "a", name: "Grace", score: 3, joined: new Date("2026-01-02") },
      { id: 3, team: "b", name: "Linus", score: 8, joined: new Date("2026-01-03") },
    ];

    await expect(query(undefined, "SELECT *")).rejects.toThrow("query() requires a source");
    await expect(query(rows, "SELECT * WHERE name contains 'a' ORDER BY name ASC LIMIT 2")).resolves.toEqual([
      rows[0],
      rows[1],
    ]);
    await expect(query(rows, "SELECT name, score WHERE score <= 8 ORDER BY score ASC")).resolves.toEqual([
      { name: "Grace", score: 3 },
      { name: "Linus", score: 8 },
    ]);
    await expect(query(rows, "SELECT name WHERE score >= 8")).resolves.toHaveLength(2);
    await expect(query(rows, "SELECT name WHERE score < 8")).resolves.toEqual([{ name: "Grace" }]);
    await expect(query(rows, "SELECT name WHERE score != 3")).resolves.toHaveLength(2);
    await expect(query(rows, "SELECT name WHERE name > 1 LIMIT 10")).resolves.toEqual([]);
    await expect(query(rows, "bad sql")).rejects.toThrow("Unsupported query");

    const index = createIndex(rows, ["team", "score"]);
    expect(index.get("a\u000010")).toEqual([rows[0]]);
    expect(joinRows([{ id: 1, left: true }], [{ id: 1, right: true }, { id: 2, right: false }], "id")).toEqual([
      { id: 1, left: true, right: true },
    ]);
  });

  it("covers diff report branches and date equality", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "diff.csv");
    const sameDate = new Date("2026-01-01T00:00:00.000Z");
    const result = diff(
      [
        { id: 1, team: "a", value: 1, date: sameDate },
        { id: 2, team: "a", value: 2 },
        { id: 3, team: "b", value: "removed" },
      ],
      [
        { id: 1, team: "a", value: 1, date: new Date("2026-01-01T00:00:00.000Z") },
        { id: 2, team: "a", value: 3, extra: "changed" },
        { id: 4, team: "b", value: "added,needs escape" },
      ],
      { key: ["team", "id"] },
    );

    expect(result.unchanged).toBe(1);
    expect(result.changed[0]?.changedColumns.sort()).toEqual(["extra", "value"]);
    expect(result.added).toHaveLength(1);
    expect(result.removed).toHaveLength(1);

    await writeDiffReport(result, file);
    const text = await readFile(file, "utf8");
    expect(text).toContain("added");
    expect(text).toContain("\"{\"\"id\"\":4");
    await rm(dir, { recursive: true, force: true });
  });

  it("covers formula functions, expressions, custom functions, and unsupported formulas", () => {
    const engine = new FormulaEngine({
      functions: {
        DOUBLE: ([value]) => Number(value) * 2,
      },
    });

    expect(engine.evaluate("AVERAGE(2,4,6)")).toBe(4);
    expect(engine.evaluate("MIN(2,4,6)")).toBe(2);
    expect(engine.evaluate("MAX(2,4,6)")).toBe(6);
    expect(engine.evaluate("COUNT(1,\"x\",2)")).toBe(2);
    expect(engine.evaluate("CONCAT(\"a\", name, DATE)", { name: "da", DATE: new Date("2026-01-01") })).toContain("ada");
    expect(engine.evaluate("CONCAT(missing)")).toBe("missing");
    expect(engine.evaluate("DOUBLE(score)", { score: 5 })).toBe(10);
    expect(engine.evaluate("score * (bonus + 2)", { score: 3, bonus: 4 })).toBe(18);
    expect(engine.evaluate("not_math + name", { name: "Ada" })).toBe("not_math + name");
    expect(engine.evaluate("IF(false,\"yes\",\"no\")")).toBe("no");
    expect(() => engine.evaluate("MISSING(1)")).toThrow("Unsupported formula function");
  });

  it("covers plugin registry and perf helpers", () => {
    const registry = new PluginRegistry();
    registry.use({
      name: "quality",
      formulas: { SCORE: ([value]) => Number(value) + 1 },
      validators: [
        (row) =>
          row.valid
            ? []
            : [{ severity: "error", code: "invalid", message: "invalid row", rowNumber: 1 }],
      ],
    });

    expect(() => registry.use({ name: "quality" })).toThrow("Plugin already registered");
    expect(registry.list()).toHaveLength(1);
    expect(registry.formulas().SCORE?.([1])).toBe(2);
    expect(registry.validate({ valid: false })).toHaveLength(1);
    expect(registry.validateRows([{ valid: true }, { valid: false }])).toHaveLength(1);
    expect(plugins.list()).toEqual([]);

    const stats = createStats();
    stats.rowsProcessed = 1;
    observeMemory(stats);
    const finished = finishStats(stats);
    expect(finished.durationMs).toBeGreaterThanOrEqual(0);

    const merged = mergeStats(
      { ...createStats(), rowsProcessed: 1, rowsWritten: 2, errors: 1, warnings: 1, sheets: ["A"], peakRssBytes: 1 },
      { ...createStats(), rowsProcessed: 3, rowsWritten: 4, errors: 2, warnings: 2, sheets: ["A", "B"], peakRssBytes: 2 },
    );
    expect(merged).toMatchObject({ rowsProcessed: 4, rowsWritten: 6, errors: 3, warnings: 3, sheets: ["A", "B"] });
    expect(formatBytes(10)).toBe("10B");
    expect(formatBytes(2048)).toBe("2.0KB");
    expect(formatBytes(2 * 1024 * 1024)).toBe("2.0MB");
  });
});

describe("Additional pipeline, CSV, and XLSX coverage", () => {
  it("reads JSON, processes validation errors, writes JSON, and rejects unsupported formats", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const input = join(dir, "rows.json");
    const output = join(dir, "out.json");
    await writeFile(input, JSON.stringify([{ id: 1 }, { id: 2 }]));

    await expect(read(Buffer.from(JSON.stringify([{ id: 3 }])), { format: "json" }).collect()).resolves.toEqual([
      { id: 3 },
    ]);
    await expect(read(input, { format: "json" }).collect()).resolves.toEqual([{ id: 1 }, { id: 2 }]);

    const processed = await read([{ id: "bad" }])
      .schema({ id: schema.number() }, { validation: "fail-fast" })
      .process();
    expect(processed.rows).toEqual([]);
    expect(processed.issues).toHaveLength(1);
    expect(processed.stats.errors).toBe(1);

    const stats = await write([{ id: 1 }], output, { format: "json" });
    expect(stats.rowsProcessed).toBe(1);
    await expect(readFile(output, "utf8")).resolves.toContain('"id": 1');

    expect(() => read("rows.unknown", { format: "bad" as never })).toThrow("Unsupported read format");
    await expect(write([], join(dir, "out.bad"), { format: "bad" as never })).rejects.toThrow("Unsupported write format");
    await rm(dir, { recursive: true, force: true });
  });

  it("covers parse helpers, pipeline write, and array-row validation diagnostics", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const file = join(dir, "pipeline.csv");

    await expect(parse(Buffer.from("id,name\n1,Ada\n"), { id: schema.number(), name: schema.string() }, { format: "csv" })).resolves.toEqual([
      { id: 1, name: "Ada" },
    ]);

    const detailed = await parseDetailed(Buffer.from("[[1,2]]"), { id: schema.number() }, { format: "json" });
    expect(detailed.issues[0]).toMatchObject({ code: "array_row", rowNumber: 1 });
    await expect(
      parseDetailed(Buffer.from("[[1,2]]"), { id: schema.number() }, { format: "json", validation: "fail-fast" }),
    ).rejects.toThrow("Sheetra validation failed");

    const stats = await read([{ id: 1 }, { id: 2 }]).write(file, { format: "csv" });
    expect(stats.rowsWritten).toBe(2);
    await expect(read(file).collect()).resolves.toEqual([{ id: "1" }, { id: "2" }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("covers pipeline take, clean passthrough, schema warnings, and schema non-row failures", async () => {
    const warnings: string[] = [];
    const originalEmitWarning = process.emitWarning;
    process.emitWarning = ((warning: string | Error, options?: string | NodeJS.EmitWarningOptions) => {
      void options;
      warnings.push(String(warning instanceof Error ? warning.message : warning));
      return process;
    }) as typeof process.emitWarning;

    try {
      await expect(read([[1, 2]]).schema({ id: schema.number() }).collect()).rejects.toThrow("Sheetra validation failed");
      await read([{ id: "bad" }]).schema({ id: schema.number() }).drain();
      expect(warnings).toContain("id must be number");
    } finally {
      process.emitWarning = originalEmitWarning;
    }

    await expect(read([{ value: "  a  " }, ["raw"]]).clean({ trim: true }).take(1).collect()).resolves.toEqual([
      { value: "a" },
    ]);
    await expect(read([{ id: 1 }, { id: 2 }, { id: 3 }]).take(2).collect()).resolves.toEqual([{ id: 1 }, { id: 2 }]);
  });

  it("covers pipeline process success and non-validation error propagation", async () => {
    const processed = await read([{ id: 1 }, { id: 2 }]).process();
    expect(processed.rows).toEqual([{ id: 1 }, { id: 2 }]);
    expect(processed.stats.rowsProcessed).toBe(2);

    const failing = new ReadableStreamLike([{ id: 1 }], new Error("source exploded"));
    await expect(read(failing).process()).rejects.toThrow("source exploded");
    await expect(read(failing).drain()).rejects.toThrow("source exploded");
  });

  it("covers CSV array rows, type inference, custom delimiters, and low-level inference", async () => {
    const rows = await readCsv(Buffer.from("Ada;10;true\nGrace;;false\n"), {
      format: "csv",
      headers: false,
      delimiter: ";",
      inferTypes: true,
    });

    const collected = [];
    for await (const row of rows) collected.push(row);

    expect(collected).toEqual([
      { _1: "Ada", _2: 10, _3: true },
      { _1: "Grace", _2: null, _3: false },
    ]);
    expect(inferCsv("")).toBeNull();
    expect(inferCsv("42")).toBe(42);
    expect(inferCsv("FALSE")).toBe(false);
    expect(inferCsv("Ada")).toBe("Ada");

    await expect(read(Buffer.from('id;note\r\n1;"done";\r\n2;"at eof"'), { format: "csv", delimiter: ";" }).drain()).resolves.toMatchObject({
      rowsProcessed: 2,
    });
    await expect(drainCsvViaEvents(Buffer.from("id||name\n1||Ada\n"), { delimiter: "||" })).rejects.toThrow();
  });

  it("covers schema branches for no-dedupe cleaning, boolean false coercion, and unknown kinds", () => {
    expect(cleanRows([{ value: " x " }], { trim: true })).toEqual([{ value: "x" }]);
    expect(validateRows([{ flag: "0" }, { flag: "n" }, { flag: "no" }], { flag: schema.boolean() }).rows).toEqual([
      { flag: false },
      { flag: false },
      { flag: false },
    ]);
    const result = validateRows([{ value: "x" }], { value: { kind: "unknown" as never } });
    expect(result.issues[0]).toMatchObject({ code: "invalid_type", expected: "unknown" });
  });

  it("covers direct CSV generator object inference paths", async () => {
    const inferred = [];
    for await (const row of readCsv(Buffer.from("name,score,active\nAda,10,true\n"), { inferTypes: true })) inferred.push(row);
    expect(inferred).toEqual([{ name: "Ada", score: 10, active: true }]);

    const raw = [];
    for await (const row of readCsv(Buffer.from("name,score\nAda,10\n"))) raw.push(row);
    expect(raw).toEqual([{ name: "Ada", score: "10" }]);
  });

  it("covers XLSX headerless reads, styles, dates, booleans, formulas, and empty workbooks", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const empty = join(dir, "empty.xlsx");
    const rich = join(dir, "rich.xlsx");

    await writeWorkbook(workbook(), empty);
    await expect(read(empty).collect()).resolves.toEqual([]);

    const sheet = worksheet("Data", [
      { name: cell("Ada", { bold: true }), active: true, joined: new Date("2026-01-01T00:00:00.000Z") },
      { name: "Grace", active: false, joined: formula("SUM(B2:B2)", 1) },
    ]);
    sheet.columns.push({ header: "name", width: 20 });
    await writeWorkbook(workbook([sheet]), rich);

    await expect(read(rich, { headers: false }).collect()).resolves.toEqual([
      { _1: "name", _2: "active", _3: "joined" },
      { _1: "Ada", _2: true, _3: "2026-01-01T00:00:00.000Z" },
      { _1: "Grace", _2: false, _3: 1 },
    ]);
    await expect(read(rich, { formulas: "preserve" }).collect()).resolves.toEqual([
      { name: "Ada", active: true, joined: "2026-01-01T00:00:00.000Z" },
      { name: "Grace", active: false, joined: { formula: "SUM(B2:B2)", result: 1 } },
    ]);
    await expect(read(rich, { sheet: 99 }).collect()).rejects.toThrow("Worksheet not found");
    await rm(dir, { recursive: true, force: true });
  });

  it("covers sparse/self-closing XLSX XML and array-row XLSX writes", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const sparse = join(dir, "sparse.xlsx");
    const arrays = join(dir, "arrays.xlsx");
    const files = {
      "xl/workbook.xml": strToU8(
        '<workbook xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sparse" sheetId="1" r:id="rId1"/></sheets></workbook>',
      ),
      "xl/_rels/workbook.xml.rels": strToU8(
        '<Relationships><Relationship Id="rId1" Target="/xl/worksheets/sheet1.xml"/></Relationships>',
      ),
      "xl/sharedStrings.xml": strToU8("<sst><si><t>title</t></si><si><t>A&#x26;B</t></si></sst>"),
      "xl/worksheets/sheet1.xml": strToU8(
        '<worksheet><sheetData><row r="1"><c t="s"><v>0</v></c><c r="C1"/></row><row r="2"/><row r="3"><c t="s"><v>1</v></c><c/><c><v>7</v></c></row></sheetData></worksheet>',
      ),
    };

    await writeFile(sparse, Buffer.from(zipSync(files)));
    await expect(read(sparse).collect()).resolves.toEqual([
      { title: null, _2: null, _3: null },
      { title: "A&B", _2: null, _3: 7 },
    ]);

    await write(
      [["Ada", 10]],
      arrays,
      { format: "xlsx", headers: ["name", "score"] },
    );
    await expect(read(arrays).collect()).resolves.toEqual([{ name: "Ada", score: 10 }]);
    await rm(dir, { recursive: true, force: true });
  });

  it("covers XLSX worksheet discovery fallback and missing worksheet errors", async () => {
    const dir = await mkdtemp(join(tmpdir(), "sheetra-"));
    const fallback = join(dir, "fallback.xlsx");
    const invalid = join(dir, "invalid.xlsx");

    await writeFile(
      fallback,
      Buffer.from(
        zipSync({
          "xl/worksheets/sheet1.xml": strToU8(
            '<worksheet><sheetData><row><c t="inlineStr"><is><t>name</t></is></c></row><row><c t="inlineStr"><is><t>Ada</t></is></c></row></sheetData></worksheet>',
          ),
        }),
      ),
    );
    await expect(read(fallback).collect()).resolves.toEqual([{ name: "Ada" }]);

    await writeFile(invalid, Buffer.from(zipSync({ "docProps/core.xml": strToU8("<xml/>") })));
    await expect(read(invalid).collect()).rejects.toThrow("No worksheets found");
    await rm(dir, { recursive: true, force: true });
  });

  it("covers worker async input and worker failure", async () => {
    async function* rows() {
      yield { value: 1 };
      yield { value: 2 };
    }

    await expect(workerMap(rows(), "(row, index) => ({ value: row.value + index })", { concurrency: 1 })).resolves.toEqual([
      { value: 1 },
      { value: 3 },
    ]);
    await expect(workerMap([{ value: 1 }], "() => { throw new Error('boom') }", { concurrency: 1 })).rejects.toThrow(
      "boom",
    );
  });
});

class ReadableStreamLike<T> implements AsyncIterable<T> {
  constructor(
    private readonly rows: T[],
    private readonly error: Error,
  ) {}

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    for (const row of this.rows) yield row;
    throw this.error;
  }
}
