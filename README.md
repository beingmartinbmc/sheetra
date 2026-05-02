# Pravaah

[![npm version](https://img.shields.io/npm/v/pravaah.svg)](https://www.npmjs.com/package/pravaah)
[![npm downloads](https://img.shields.io/npm/dm/pravaah.svg)](https://www.npmjs.com/package/pravaah)
[![CI](https://github.com/beingmartinbmc/pravaah/actions/workflows/ci.yml/badge.svg)](https://github.com/beingmartinbmc/pravaah/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/pravaah.svg)](./LICENSE)

**Stop writing messy CSV import logic. Validate 7 million rows without blowing memory.**

Pravaah is a schema-first, streaming data pipeline library for Excel, CSV, XLS, and JSON in Node.js.

4.5x faster than fast-csv. 49% less memory than SheetJS on XLSX reads. Schema-validated. Streaming-first. TypeScript-native.

Benchmarked on 7M-row datasets with isolated processes and RSS tracking.

```text
 CSV Read: 7M rows ─ time (lower is better)
 ──────────────────────────────────────────
 Pravaah  ■■■■■                          3.25s
 fast-csv ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  9.46s
```

---

## 30-Second Win

No file setup. Paste this into a Node.js ESM project after `npm install pravaah`:

```ts
import { parse, schema } from "pravaah";

const csv = Buffer.from(`email,total
ada@example.com,42
bad-email,99
grace@example.com,120
`);

const rows = await parse(
  csv,
  {
    email: schema.email(),
    total: schema.number(),
  },
  {
    format: "csv",
    validation: "skip",
  },
);

console.log(rows);
// [
//   { email: "ada@example.com", total: 42 },
//   { email: "grace@example.com", total: 120 }
// ]
```

---

## The Problem

You get a CSV from a customer. It has 2 million rows, inconsistent headers, blank emails, negative dollar amounts, and columns named "E-mail Address" instead of "email". Your job: validate it, clean it, reject bad rows, and store the rest.

Here is what that typically looks like:

```ts
import fs from "fs";
import { parse } from "@fast-csv/parse";

const rows = [];
const issues = [];
let count = 0;

fs.createReadStream("upload.csv")
  .pipe(parse({ headers: true }))
  .on("data", (row) => {
    count++;
    const email = (row["E-mail Address"] || row.email || row.Mail || "").trim();
    if (!email || !email.includes("@")) {
      issues.push({ row: count, reason: "invalid email", value: email });
      return;
    }
    const amount = parseFloat(row.total);
    if (isNaN(amount) || amount < 0) {
      issues.push({ row: count, reason: "bad amount", value: row.total });
      return;
    }
    rows.push({ email, amount, name: (row.name || "").trim() });
  })
  .on("end", () => {
    console.log(`${rows.length} valid, ${issues.length} rejected`);
    // now write issues report, handle memory, pray it doesn't OOM...
  });
```

A screenful of fragile header matching. Manual validation. No types. No memory control. No issue report. And it falls apart the moment the file format changes.

**With Pravaah:**

```ts
import { parseDetailed, schema, writeIssueReport } from "pravaah";

const { rows, issues, stats } = await parseDetailed(
  "upload.csv",
  {
    email: schema.email(),
    name: schema.string({ optional: true }),
    total: schema.number({ validate: (v) => (v < 0 ? "cannot be negative" : undefined) }),
  },
  {
    validation: "collect",
    cleaning: {
      trim: true,
      fuzzyHeaders: { email: ["E-mail Address", "Mail", "email id"] },
    },
  },
);

await writeIssueReport(issues, "rejected-rows.csv");
console.log(`${rows.length} valid, ${issues.length} rejected in ${stats.durationMs}ms`);
```

Typed output. Fuzzy header matching. Schema validation. Issue report. Streaming memory. Done.

---

## Why This Exists

We built Pravaah after repeatedly writing the same custom ingestion code across projects: parse a CSV, normalize vendor-specific headers, validate business rules, collect rejected rows, and keep memory stable under large uploads.

Most spreadsheet libraries make you choose between low-level file parsing and your own pile of validation glue. Pravaah puts the import workflow itself behind one pipeline.

---

## What Happens When You Run It

```text
$ node import.ts

Processed 2,041,293 rows in 4.2s
  Valid:    1,987,441
  Rejected: 53,852
  Peak RSS: 112MB

Issue report written to rejected-rows.csv
```

```text
rejected-rows.csv:
severity,code,message,rowNumber,column,expected,rawValue
error,invalid_type,email must be email,14,email,email,not-an-email
error,invalid_value,cannot be negative,203,total,number,-50.00
error,missing_column,email is required,891,email,email,
```

---

## How It Works

```text
┌─────────┐    ┌─────────┐    ┌──────────┐    ┌───────────┐    ┌──────────┐
│  File   │───▶│  Clean  │───▶│ Validate │───▶│ Transform │───▶│  Output  │
│ CSV/XLSX│    │ headers │    │  schema  │    │  map/filt │    │ file/db  │
└─────────┘    └─────────┘    └──────────┘    └───────────┘    └──────────┘
                    │               │                │
                    ▼               ▼                ▼
              fuzzy match     type-safe rows    fused stages
              trim/dedupe     issue report      one pass
```

Pipeline stages are lazy. CSV reads stay streaming end-to-end; XLSX reads target the selected worksheet without inflating a full workbook model. Adjacent transforms are fused into a single pass. Use `.drain()` or `for await` for constant-memory consumption; `.collect()`, `.process()`, and `parseDetailed()` intentionally materialize returned rows and/or issues.

---

## Install

```sh
npm install pravaah
```

Node.js 20+. ESM.

---

## Who This Is For

- **Backend devs** handling customer CSV/XLSX uploads in Express, Fastify, NestJS.
- **SaaS teams** building admin import tools where users upload messy spreadsheets.
- **Data engineers** writing ETL pipelines that need validation before database writes.
- **Platform teams** that want predictable memory and throughput on large files.

Works well with Express, Fastify, NestJS, serverless jobs, queue workers, and backend ingestion pipelines.

If you need Excel styling, charts, drawings, macros, or arbitrary workbook editing, this is not the right tool. Use SheetJS or ExcelJS for workbook manipulation. Pravaah is for treating spreadsheets as **data**, not documents.

---

## Pravaah vs The Alternatives

Existing libraries parse files. Pravaah handles the entire ingestion pipeline: read, clean, validate, transform, report, and write.

Use SheetJS or ExcelJS when you need workbook manipulation. Use fast-csv when you only need CSV parsing. Use Pravaah when the spreadsheet is entering your product and must become trusted application data.

```text
 Ingestion workflow coverage
 ─────────────────────────────────────────────────────────
 Pravaah  Read ■ Clean ■ Validate ■ Transform ■ Report ■ Write
 SheetJS  Read ■                                      Write
 ExcelJS  Read ■                                      Write
 fast-csv Read ■                                      Write
```

| | Pravaah | SheetJS | ExcelJS | fast-csv |
| --- | :---: | :---: | :---: | :---: |
| Streaming CSV read | Yes | No (in-memory) | N/A | Yes |
| Targeted XLSX read | Yes | No (in-memory) | Partial | N/A |
| Schema validation | Built-in | No | No | No |
| Fuzzy header cleaning | Built-in | No | No | No |
| TypeScript type inference | Yes | No | No | No |
| CSV read speed (7M rows) | **3.25s** | N/A | N/A | 9.46s |
| XLSX read speed (36K rows) | **396ms** | 437ms | 572ms | N/A |
| Peak memory (36K row XLSX) | **120MB** | 234MB | 294MB | N/A |
| Issue reports | Built-in | No | No | No |
| Pipeline transforms | Built-in | No | No | No |
| Worker thread parallelism | Built-in | No | No | No |
| Formula engine | Built-in | Read-only | Read-only | No |

---

## The Killer Walkthrough

A user uploads a CRM export. The headers are inconsistent across vendors. Some emails are garbage. Some dollar amounts are negative. You need clean rows in your database and a rejection report for the ops team.

```ts
import { parseDetailed, schema, writeIssueReport } from "pravaah";

// 1. Define your data contract
const contactSchema = {
  email: schema.email(),
  company: schema.string(),
  deal_value: schema.number({
    validate: (v) => (v < 0 ? "deal value cannot be negative" : undefined),
  }),
  stage: schema.string({ defaultValue: "new" }),
};

// 2. Process the upload
const { rows, issues, stats } = await parseDetailed("crm-export.csv", contactSchema, {
  validation: "collect",
  cleaning: {
    trim: true,
    normalizeWhitespace: true,
    fuzzyHeaders: {
      email: ["E-mail", "Email Address", "Contact Email", "mail"],
      company: ["Company Name", "Account", "Organization"],
      deal_value: ["Amount", "Deal Value", "Value (USD)"],
    },
  },
});

// 3. Store valid rows
await db.contacts.insertMany(rows);

// 4. Send rejection report to ops
await writeIssueReport(issues, "upload-rejections.csv");

console.log(`Imported ${rows.length} contacts, rejected ${issues.length} in ${stats.durationMs}ms`);
// → Imported 847,293 contacts, rejected 12,041 in 2,847ms
```

One pipeline. No manual parsing. Controlled memory. Full audit trail.

---

## Quick Start

### Read any file

```ts
import { read } from "pravaah";

for await (const row of read("customers.csv")) {
  console.log(row);
}
```

Auto-detects `.csv`, `.xlsx`, `.xls`, and `.json`. Force a format when reading buffers:

```ts
const rows = await read(buffer, { format: "xlsx", sheet: "Customers" }).collect();
```

### Transform and write

```ts
import { read, schema } from "pravaah";

const stats = await read("orders.csv")
  .schema({ orderId: schema.string(), email: schema.email(), total: schema.number() })
  .filter((row) => row.total > 100)
  .map((row) => ({ ...row, status: "priority" }))
  .write("priority-orders.xlsx", { sheetName: "Priority" });

console.log(`Wrote ${stats.rowsWritten} rows in ${stats.durationMs}ms`);
```

### Count rows without materializing

```ts
import { read } from "pravaah";

const stats = await read("7-million-rows.csv").drain();
// → { rowsProcessed: 7046063, durationMs: 405, peakRssBytes: 113MB }
```

Uses a raw byte scanner — no row objects allocated.

### Parse with full type safety

```ts
import { parse, schema } from "pravaah";

const orders = await parse("orders.csv", {
  orderId: schema.string(),
  email: schema.email(),
  total: schema.number({ validate: (v) => (v < 0 ? "negative" : undefined) }),
  paid: schema.boolean({ defaultValue: false }),
}, { validation: "fail-fast" });

// orders: Array<{ orderId: string; email: string; total: number; paid: boolean }>
```

---

## Core Capabilities

- Streaming CSV ingestion with count-only scans for huge files.
- Schema validation with TypeScript-inferred output rows.
- Cleaning, trimming, fuzzy headers, and deduplication.
- Built-in issue reports, dataset diffs, joins, and SQL-like queries.
- XLSX workbook writing with formulas and sheet helpers.
- Worker-thread mapping for CPU-heavy row transforms.

---

## Pipeline API

`read()` returns a lazy `PravaahPipeline`. Nothing executes until you call `.collect()`, `.drain()`, `.process()`, or `.write()`.

```ts
import { read, schema } from "pravaah";

const pipeline = read("input.csv")
  .clean({ trim: true, fuzzyHeaders: { email: ["E-mail", "mail"] } })
  .schema({ email: schema.email(), name: schema.string({ optional: true }) })
  .map((row) => ({ ...row, importedAt: new Date().toISOString() }))
  .filter((row) => row.email.endsWith("@company.com"))
  .take(10_000);

const rows = await pipeline.collect();
```

| Method | What it does |
| --- | --- |
| `.map(fn)` | Transform each row |
| `.filter(fn)` | Keep matching rows |
| `.clean(opts)` | Normalize headers and values |
| `.schema(def)` | Validate and type rows |
| `.take(n)` | Stop after n rows |
| `.collect()` | Materialize into array |
| `.drain()` | Consume without storing |
| `.process()` | Return rows + issues + stats |
| `.write(dest)` | Write to CSV/XLSX/JSON |

Adjacent `.map()` and `.filter()` calls are **fused** into a single iteration pass.

---

## Schema Validation

```ts
import { schema } from "pravaah";

const userSchema = {
  id: schema.string(),
  name: schema.string({ validate: (v) => (v.length < 2 ? "too short" : undefined) }),
  age: schema.number({ optional: true }),
  active: schema.boolean({ defaultValue: true }),
  signupDate: schema.date(),
  email: schema.email(),
  phone: schema.phone({ optional: true }),
};
```

**Validation modes:**

| Mode | Behavior |
| --- | --- |
| `fail-fast` | Throw on first invalid row |
| `collect` | Keep valid rows, collect all issues |
| `skip` | Silently drop invalid rows |

**Field options:** `optional`, `defaultValue`, `coerce`, `validate`.

---

## File Format Support

| Format | Read | Write | Notes |
| --- | :---: | :---: | --- |
| `.csv` | Streaming | Streaming | Custom parser, zero-row-object count path, backpressure-aware writer |
| `.xlsx` | Targeted | Full | Selective decompression, lazy shared strings; writes zip asynchronously but materialize workbook XML |
| `.xls` | Full | — | Via optional `xlsx` package (`npm install xlsx`) |
| `.json` | Full | Full | For fixtures, snapshots, and intermediate ETL |

### CSV specifics

- RFC-compliant quoted fields, escaped quotes, CRLF.
- `headers: true`, `headers: false`, or explicit header arrays.
- Single-character custom delimiters.
- Optional type inference for numbers, booleans, and nulls.

### XLSX specifics

- Sheet selection by name or index.
- Decompresses workbook metadata and the targeted sheet instead of building a full workbook model.
- Lazy shared-string resolution.
- Numeric cells with date styles are returned as `Date`.
- Formula preservation: `{ formula, result }` cells.
- XLSX writes currently build workbook XML in memory before zipping.

---

## Workbook Authoring

```ts
import { formula, workbook, worksheet, writeWorkbook } from "pravaah";

const summary = worksheet("Summary", [
  { metric: "Revenue", value: 125000 },
  { metric: "Target", value: 100000 },
  { metric: "Delta", value: formula("B2-B3", 25000) },
]);

summary.columns = [{ header: "metric", width: 24 }, { header: "value", width: 16 }];
summary.frozen = { ySplit: 1, topLeftCell: "A2" };

await writeWorkbook(workbook([summary]), "report.xlsx");
```

Supports: multiple sheets, formulas, column widths, merges, data validations, auto-filters, frozen panes, table definitions.

---

## Query, Diff, Join

```ts
import { query, diff, read, createIndex, joinRows, writeDiffReport } from "pravaah";

// SQL-like queries
const top = await query("accounts.csv", "SELECT id, name, revenue WHERE revenue >= 100000 ORDER BY revenue DESC LIMIT 25");

// Dataset diff
const before = await read("customers-v1.csv").collect();
const after = await read("customers-v2.csv").collect();
const changes = diff(before, after, { key: "customerId" });
await writeDiffReport(changes, "changes.csv");

// Index + join
const enriched = joinRows(orders, customers, "customerId");
```

The query parser intentionally supports a compact subset: `SELECT`, one optional `WHERE` predicate, optional `ORDER BY`, and optional `LIMIT`.

---

## Formula Engine

```ts
import { FormulaEngine, evaluateFormula } from "pravaah";

evaluateFormula("SUM(subtotal, tax)", { subtotal: 100, tax: 8.25 }); // 108.25

const engine = new FormulaEngine({
  functions: { DISCOUNT: ([amt, pct]) => Number(amt) * (1 - Number(pct)) },
});
engine.evaluate("DISCOUNT(total, 0.15)", { total: 200 }); // 170
```

Built-in: `SUM`, `AVERAGE`, `MIN`, `MAX`, `COUNT`, `IF`, `CONCAT`, plus arithmetic through a small parser without dynamic code execution.

---

## Plugins

```ts
import { plugins } from "pravaah";

plugins.use({
  name: "business-rules",
  validators: [
    (row) => Number(row.total) < 0
      ? [{ code: "negative_total", message: "total cannot be negative", column: "total", severity: "error" }]
      : [],
  ],
  formulas: { MARGIN: ([revenue, cost]) => Number(revenue) - Number(cost) },
});
```

---

## Parallel Worker Mapping

```ts
import { read, workerMap } from "pravaah";

const rows = await read("large.csv", { inferTypes: true }).collect();
const enriched = await workerMap(rows, (row) => ({ ...row, score: Number(row.revenue) * 0.12 }), { concurrency: 4 });
```

Function mappers run with bounded local concurrency and support closures. For true worker-thread execution, pass a module URL or path and optionally `{ exportName }`; the exported mapper receives `(row, index)`.

---

## Benchmarks

Every number below is from an isolated child process. RSS sampled every 25ms. Best of 3 runs. macOS Apple Silicon, Node.js 22. Reproduce with the benchmark scripts in `benchmark/`; benchmark fixture outputs are generated artifacts and are not committed.

### CSV Read

```text
 7M rows, 146MB ─ time (lower is better)
 ──────────────────────────────────────────
 Pravaah  ■■■■■                          3.25s
 fast-csv ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  9.46s

 1M rows, 244MB ─ time (lower is better)
 ──────────────────────────────────────────
 Pravaah  ■■■■■■                          1.89s
 fast-csv ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  8.48s

 1K rows, 498KB ─ time (lower is better)
 ──────────────────────────────────────────
 Pravaah  ■■■                             8ms
 fast-csv ■■■■■■■■                        27ms
 SheetJS  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  97ms
```

| Workload | Engine | Time | Peak RSS |
| --- | --- | ---: | ---: |
| 7M rows, 146MB | Pravaah | **3.25s** | **112MB** |
| 7M rows, 146MB | fast-csv | 9.46s | 136MB |
| 1M rows, 244MB | Pravaah | **1.89s** | **110MB** |
| 1M rows, 244MB | fast-csv | 8.48s | 150MB |
| 1K rows, 498KB | Pravaah | **8ms** | **84MB** |
| 1K rows, 498KB | fast-csv | 27ms | 95MB |
| 1K rows, 498KB | SheetJS | 97ms | 124MB |

`read(file).drain()` (count-only, no row objects): **405ms** for 7M rows, **752ms** for 1M rows.

### XLSX Read

```text
 36K rows, 1.5MB ─ time (lower is better)
 ──────────────────────────────────────────
 Pravaah ■■■■■■■■■■■■■■■■■■■■            396ms
 SheetJS ■■■■■■■■■■■■■■■■■■■■■■          437ms
 ExcelJS ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■   572ms

 36K rows, 1.5MB ─ peak memory (lower is better)
 ──────────────────────────────────────────
 Pravaah ■■■■■■■■■■■■                    120MB
 SheetJS ■■■■■■■■■■■■■■■■■■■■■■■         234MB
 ExcelJS ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■   294MB
```

| Workload | Engine | Time | Peak RSS |
| --- | --- | ---: | ---: |
| 36K rows, 1.5MB | Pravaah | **396ms** | **120MB** |
| 36K rows, 1.5MB | SheetJS | 437ms | 234MB |
| 36K rows, 1.5MB | ExcelJS | 572ms | 294MB |

### Write

```text
 CSV Write: 100K rows ─ time (lower is better)
 ──────────────────────────────────────────
 fast-csv ■■■■■■■■                        125ms
 Pravaah  ■■■■■■■■■                       141ms
 SheetJS  ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  472ms

 XLSX Write: 100K rows ─ peak memory (lower is better)
 ──────────────────────────────────────────
 Pravaah ■■■■■■                          222MB
 SheetJS ■■■■■■■■■■■                     398MB
 ExcelJS ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  1,074MB
```

| Workload | Engine | Time | Peak RSS |
| --- | --- | ---: | ---: |
| CSV write, 100K rows | Pravaah | **141ms** | **136MB** |
| CSV write, 100K rows | fast-csv | 125ms | 166MB |
| CSV write, 100K rows | SheetJS | 472ms | 315MB |
| XLSX write, 100K rows | Pravaah | **701ms** | **222MB** |
| XLSX write, 100K rows | SheetJS | 687ms | 398MB |
| XLSX write, 100K rows | ExcelJS | 1,788ms | 1,074MB |

### TL;DR

| | vs fast-csv | vs SheetJS | vs ExcelJS |
| --- | --- | --- | --- |
| CSV read speed | **4.5x faster** | — | — |
| CSV read memory | **27% less** | — | — |
| XLSX read speed | — | **10% faster** | **31% faster** |
| XLSX read memory | — | **49% less** | **59% less** |
| XLSX write memory | — | **44% less** | **79% less** |

Run them yourself:

```sh
npm run benchmark:isolated
PRAVAAH_BENCH_RUNS=5 npm run benchmark:isolated
```

If Pravaah saved you from another one-off CSV importer, a star helps others find it. Issues and PRs are welcome; see [CONTRIBUTING.md](./CONTRIBUTING.md) for local development and benchmark notes.

---

## How The Performance Works

**CSV:** Custom streaming parser with a low-allocation hot path. Raw record-boundary scanner for drain-only workloads. No heavy parser dependency on the read side.

**XLSX:** Selective ZIP decompression for workbook metadata and the target sheet. Lazy shared-string indexing. Raw byte scanning instead of DOM construction. Dimension-aware preallocation.

**Pipelines:** Lazy AsyncIterable execution. Fused map/filter stages compiled into a single iterator. Built-in RSS tracking.

---

## API Reference

| Function | Purpose |
| --- | --- |
| `read(source, options)` | Lazy pipeline from CSV, XLSX, XLS, JSON, Buffer, Iterable, or AsyncIterable |
| `write(rows, dest, options)` | Write to CSV, XLSX, or JSON |
| `parse(source, schema, options)` | Validate and collect typed rows |
| `parseDetailed(source, schema, options)` | Rows + issues + stats |
| `query(source, sql)` | SQL-like queries over data |
| `diff(old, new, options)` | Compare datasets by key |
| `writeIssueReport(issues, dest)` | Validation diagnostics as CSV |
| `writeDiffReport(result, dest)` | Diff output as CSV |
| `writeWorkbook(book, dest)` | Multi-sheet XLSX with formulas |
| `workerMap(rows, fn, options)` | Parallel row mapping in workers |

### Read Options

| Option | Description |
| --- | --- |
| `format` | Force `xlsx`, `xls`, `csv`, or `json` |
| `sheet` | Sheet name or zero-based index |
| `headers` | `true`, `false`, or explicit header array |
| `delimiter` | CSV delimiter (single character) |
| `inferTypes` | Convert strings to primitives |
| `formulas` | `"values"` or `"preserve"` |
| `validation` | `"fail-fast"`, `"collect"`, or `"skip"` |
| `cleaning` | Inline cleaning options |

### Write Options

| Option | Description |
| --- | --- |
| `format` | Force `xlsx`, `csv`, or `json` |
| `sheetName` | Output worksheet name |
| `headers` | Column order |
| `delimiter` | CSV output delimiter |

---

## Scripts

```sh
npm run build          # compile TypeScript
npm run typecheck      # type-check without emitting
npm test              # run test suite
npm run lint          # ESLint
npm run benchmark:isolated   # full isolated benchmarks
```

---

## Other Languages

- **Java:** [`pravaah-java`](https://github.com/beingmartinbmc/pravaah-java) — JVM port of Pravaah for Java/Kotlin/Scala backends.

---

## Roadmap

- Streaming XLSX write for extremely large exports.
- `npx pravaah demo` for a zero-setup CLI walkthrough.
- Broader XLSX formatting (styles, conditional formatting).
- More SQL-like query operators (`GROUP BY`, `JOIN`).
- More formula functions.
- First-party adapters for upload frameworks (multer, busboy).

---

## License

MIT
