import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { strToU8, zipSync } from "fflate";
import { describe, expect, it } from "vitest";
import {
  diff,
  evaluateFormula,
  formula,
  parseDetailed,
  query,
  read,
  readWorkbook,
  schema,
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
