# Sheetra

Sheetra is a streaming-first spreadsheet pipeline library for Node.js.

It is designed for large, messy, real-world Excel and CSV files, where correctness, memory stability, and data validation matter more than just reading cells.

Unlike SheetJS or ExcelJS, Sheetra treats spreadsheets as:

**data contracts + processing pipelines**

## Why Sheetra

Most Node.js Excel libraries focus on file manipulation.

Sheetra focuses on data ingestion and transformation:

- Typed schemas instead of loose objects.
- Streaming pipelines instead of in-memory loading.
- Validation and cleaning built in.
- Predictable memory usage on large files.

The goal: make Excel safe for backend systems.

## Example

```ts
import { read, schema } from "sheetra";

const leadSchema = {
  email: schema.email(),
  age: schema.number({ optional: true }),
  joined: schema.date(),
};

const rows = await read("leads.xlsx")
  .clean({
    trim: true,
    normalizeWhitespace: true,
    fuzzyHeaders: { email: ["E-mail", "email id", "mail"] },
  })
  .schema(leadSchema, { validation: "fail-fast" })
  .filter((row) => row.age === undefined || row.age > 18)
  .collect();
```

## Core Capabilities

### Streaming-First Pipelines

- AsyncIterable API: `read().map().filter().clean().schema().write()`.
- Backpressure-aware processing.
- Constant-memory CSV pipelines.

### Type-Safe Ingestion

- Schema validation with TypeScript inference.
- Coercion, optional fields, and structured errors.
- Fail-fast or partial recovery modes.

### Data Cleaning & Normalization

- Trim, whitespace normalization, and deduplication.
- Fuzzy header matching.
- Built-in validation helpers for email, number, date, and more.

### XLSX + CSV Support

- CSV: streaming read/write with raw fast path and `drain()`.
- XLSX: multi-sheet workbooks, preserved formulas, merges, tables, panes, and metadata.

### Query, Diffing, and Transformations

- SQL-like queries: `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`.
- Indexing and joins.
- Row-level diffing.

### Extensibility

Plugin system for:

- Validators.
- Parsers.
- Exporters.
- Formula functions.

### Performance & Observability

- Worker-thread mapping.
- Benchmark scripts checked into the repo.
- Memory tracking and timeline sampling.

## Positioning

Sheetra does not aim to be:

- The fastest raw parser.
- A full Excel styling engine.

Instead, it focuses on:

**reliable, memory-stable data pipelines for large spreadsheet workloads.**

## Benchmarks

Benchmarks are included as runnable scripts, not marketing claims.

Run them locally with your own data:

```sh
npm run benchmark:strong
```

This suite answers a practical question:

**Will Sheetra survive large, messy, production-style files?**

### Environment

- macOS Darwin 25.4.0.
- Node.js.
- `SHEETRA_BENCH_ROWS=100000`.
- Local fixtures in `benchmark/files`.

### Synthetic, 100k Rows

| Engine | Time | Peak Memory |
| --- | ---: | ---: |
| `sheetra:csv:pipeline` | 206ms | 153MB |
| `sheetjs:xlsx:json_to_sheet` | 314ms | 308MB |
| `exceljs:workbook:csv` | 139ms | 446MB |

### Real File, 1M Rows, 244MB CSV

| Engine | Time | Peak Memory |
| --- | ---: | ---: |
| `sheetra:csv:stream` | 11.5s | 123MB |
| `fast-csv:stream` | 7.8s | 128MB |

Sheetra is not the fastest raw parser, but it maintains stable, low memory usage on large files.

### XLSX, Current State

| Engine | Rows | Time | Peak Memory |
| --- | ---: | ---: | ---: |
| `sheetra:xlsx` | 35,808 | 1.06s | 491MB |
| `sheetjs:xlsx:readFile` | 35,808 | 334ms | 579MB |

XLSX support is functional and feature-rich, but not yet optimized for performance.

### Interpretation

- CSV: production-ready, streaming, memory-stable.
- XLSX: functional and feature-rich, optimization in progress.
- fast-csv: faster as a raw parser.
- SheetJS: faster for XLSX today.

Sheetra’s advantage is:

**predictable memory usage + composable data pipelines.**

### Benchmark Controls

```sh
SHEETRA_BENCH_ROWS=100000 npm run benchmark:compare
SHEETRA_BENCH_FILE=MOCK_DATA.csv npm run benchmark:files
SHEETRA_BENCH_LIMIT=100000 npm run benchmark:files
SHEETRA_BENCH_INCLUDE_MEMORY=1 npm run benchmark:files
SHEETRA_BENCH_PROFILE=full npm run benchmark:strong
SHEETRA_BENCH_SCALES=100000,500000,1000000,2000000 npm run benchmark:strong
```

### What the Full Suite Covers

- Scale: 100k to 2M+ rows.
- Streaming vs in-memory comparisons.
- CSV raw mode vs feature-enabled pipelines.
- XLSX multi-sheet and formula-preserving reads.
- Tall vs wide datasets.
- Light vs heavy transformations.
- End-to-end pipelines: read, validate, transform, write.
- Fault tolerance: invalid rows, missing values, type errors.
- Worker-thread scaling.
- Cold vs warm runs.
- GC time and memory timelines.

Artifacts are written to:

```text
benchmark/results/
```

## CSV Behavior

By default, CSV values are treated as raw strings for performance.

Options:

- `inferTypes: true`: automatic primitive inference.
- `schema(...)`: explicit typing and validation, recommended for backend ingestion.

## Roadmap

1. Foundation: API, tests, benchmarks.
2. Fast streaming core: constant-memory pipelines.
3. Type-safe ingestion: schemas, diagnostics.
4. Feature parity: XLSX model, formulas, metadata.
5. Differentiators: SQL, diffing, plugins, parallelism.
6. Benchmark proof: reproducible performance claims.

## Scripts

```sh
npm run build
npm test
npm run lint
npm run benchmark
npm run benchmark:compare
npm run benchmark:files
npm run benchmark:strong
```

## License

MIT
