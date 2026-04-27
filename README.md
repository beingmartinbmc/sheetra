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
- Zero heavyweight XML or CSV parser dependencies on the hot path.

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
- Fused map/filter chains: adjacent `.map()` and `.filter()` calls are compiled into a single generator loop, eliminating per-row microtask overhead.

### Type-Safe Ingestion

- Schema validation with TypeScript inference.
- Coercion, optional fields, and structured errors.
- Fail-fast or partial recovery modes.

### Data Cleaning & Normalization

- Trim, whitespace normalization, and deduplication.
- Fuzzy header matching.
- Built-in validation helpers for email, number, date, and more.

### XLSX + CSV Support

- CSV: custom streaming parser with raw fast path and `drain()`.
- XLSX: selective decompression, lazy shared strings, buffer-based XML scanning.

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

- A full Excel styling engine.

Instead, it focuses on:

**the fastest memory-stable data pipeline for large spreadsheet workloads.**

## Benchmarks

Benchmarks are included as runnable scripts, not marketing claims. Every engine runs in its own fresh Node process so RSS is not contaminated by prior runs.

Run them locally:

```sh
npm run benchmark:isolated
```

### Environment

- macOS Darwin 25.4.0, Apple Silicon.
- Node.js v22.
- Each engine in a fresh child process, RSS sampled every 25ms, best-of-three.

---

### CSV Read, 1M Rows, 244MB

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra (row parse) | **1.90s** | **110MB** |
| fast-csv | 8.57s | 149MB |
| SheetJS | 7.47s | 3,413MB |

Sheetra's custom streaming CSV parser is **4.5x faster than fast-csv** and uses **26% less memory**. SheetJS materializes the entire file in memory, which is unsafe for backend ingestion at this scale.

### CSV Read, 1K Rows, 498KB

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra (row parse) | **8ms** | **84MB** |
| fast-csv | 28ms | 95MB |
| SheetJS | 103ms | 124MB |

Even on small files, Sheetra is **3.5x faster** than fast-csv.

### CSV Read, Count-Only Drain, 1M Rows, 244MB

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra (raw drain) | **760ms** | **114MB** |

When only a row count is needed, `read(csv).drain()` scans record boundaries without parsing cells or allocating row objects. Useful for import probes, preflight checks, and progress estimates.

---

### CSV Write, 100K Rows

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra | **141ms** | **138MB** |
| fast-csv | 126ms | 166MB |
| SheetJS | 483ms | 315MB |

Sheetra and fast-csv are at parity on CSV write throughput. Sheetra uses **17% less memory** due to backpressure-aware streaming. SheetJS is 3.4x slower and uses 2.3x more memory.

---

### XLSX Read, 36K Rows, 1.5MB

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra | **390ms** | **123MB** |
| SheetJS | 431ms | 234MB |
| ExcelJS | 575ms | 295MB |

Sheetra beats both SheetJS and ExcelJS on time and uses **47% less memory** than SheetJS. The gains come from selective decompression, lazy shared-string resolution, and buffer-based XML scanning.

---

### XLSX Write, 100K Rows

| Engine | Time | Peak RSS |
| --- | ---: | ---: |
| Sheetra | **710ms** | **222MB** |
| SheetJS | 755ms | 451MB |
| ExcelJS | 1,846ms | 1,075MB |

Sheetra writes XLSX **6% faster than SheetJS** and uses **51% less memory**. ExcelJS is 2.6x slower and uses 4.8x more memory.

---

### Summary

| Workload | Sheetra vs Best Competitor |
| --- | --- |
| CSV Read (1M rows) | **4.5x faster**, 26% less memory than fast-csv |
| CSV Write (100K rows) | At parity with fast-csv, 17% less memory |
| XLSX Read (36K rows) | **10% faster**, 47% less memory than SheetJS |
| XLSX Write (100K rows) | **6% faster**, 51% less memory than SheetJS |

### How It Works

**CSV Read**: custom streaming parser that processes fields directly from the stream buffer. No event-based overhead, no intermediate object allocation per event. For count-only `drain()`, a byte scanner counts record boundaries outside quoted fields without materializing rows.

**CSV Write**: uses `@fast-csv/format` with backpressure-aware streaming. The writable stream's backpressure signal is respected, preventing unbounded memory growth during large writes.

**XLSX Read**: selective decompression extracts only the needed zip entries. Shared strings are lazily resolved on demand via an offset index. The worksheet XML is scanned as a raw `Uint8Array` buffer using byte pattern matching, avoiding full UTF-16 string conversion.

**XLSX Write**: generates XML directly and zips with fflate at compression level 6. No intermediate DOM tree or heavyweight library.

**Pipeline Fusion**: adjacent `.map()` and `.filter()` calls are compiled into a single async generator. A 5-step pipeline on 1M rows makes 1M iterations instead of 5M.

### Benchmark Controls

```sh
SHEETRA_BENCH_WRITE_ROWS=100000 npm run benchmark:isolated
SHEETRA_BENCH_SKIP_WRITE=1 npm run benchmark:isolated
SHEETRA_BENCH_SKIP_READ=1 npm run benchmark:isolated
SHEETRA_BENCH_INCLUDE_MEMORY=1 npm run benchmark:isolated
SHEETRA_BENCH_RUNS=5 npm run benchmark:isolated
```

`benchmark:isolated` runs each engine in a fresh Node process so RSS is not contaminated by previous tests.

### What the Full Suite Covers

- Scale: 1K to 1M+ rows.
- Read and write benchmarks for CSV and XLSX.
- Streaming vs in-memory comparisons.
- CSV raw drain vs row parsing.
- XLSX multi-sheet and formula-preserving reads.
- Light vs heavy transformations.
- End-to-end pipelines: read, validate, transform, write.
- Fault tolerance: invalid rows, missing values, type errors.
- Worker-thread scaling.
- GC time and memory timelines.

## Optimizations

### CSV

- **Custom streaming parser**: replaces `@fast-csv/parse` on the hot path. Parses fields directly from the stream buffer with zero intermediate event overhead.
- **Record boundary scanner**: count-only `drain()` scans newlines outside quoted fields without allocating row objects.
- **Write backpressure**: `writeCsv` respects the writable stream's backpressure signal, preventing unbounded memory growth during large writes.

### XLSX

- **Selective decompression**: `readXlsx` only decompresses the worksheet, shared strings, workbook metadata, and rels files. Images, styles, themes, and other entries are skipped entirely.
- **Lazy shared strings**: builds an offset index on first access, then resolves individual strings on demand. Strings not referenced by the worksheet are never decoded. Frequently accessed strings are cached.
- **Buffer-based XML scanning**: scans the raw `Uint8Array` for `<row>` and `<sheetData>` markers using byte comparisons, avoiding full UTF-16 string conversion of the entire worksheet.
- **Dimension-aware pre-allocation**: when a `<dimension>` tag is present, row arrays are pre-allocated to the correct column count, avoiding V8 sparse-array mode.
- **Dropped fast-xml-parser**: workbook.xml and rels are now parsed with lightweight hand-written XML scanners, removing the ~150KB dependency.

### Pipeline

- **Operation fusion**: adjacent `.map()` and `.filter()` calls are compiled into a single async generator. A 5-step pipeline on 1M rows now makes 1M async iterations instead of 5M.

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
npm run benchmark:isolated
npm run benchmark:strong
```

## License

MIT
