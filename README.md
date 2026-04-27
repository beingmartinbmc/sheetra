# Sheetra

Sheetra is a production-grade spreadsheet pipeline library for Node.js. It is built around typed ingestion, streaming-friendly row transforms, validation, cleaning, query, diffing, formula helpers, and benchmark transparency.

The goal is not to clone SheetJS or ExcelJS. Sheetra treats Excel and CSV files as data contracts and processing pipelines:

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

## What Works Now

- TypeScript-first package with ESM exports and declarations.
- AsyncIterable pipeline API: `read().map().filter().clean().schema().write()`.
- CSV read/write support through Node streams, including a raw fast path and `drain()` for counting/processing without collecting rows.
- XLSX read/write support for row data, multi-sheet workbooks, formula preservation, merges, validations, tables, frozen panes, and metadata primitives.
- Schema validation with TypeScript inference, coercion, fuzzy headers, dedupe cleaning, detailed parse results, and structured validation issues.
- Formula helper for common functions such as `SUM`, `AVERAGE`, `IF`, and custom functions.
- SQL-like `SELECT ... WHERE ... ORDER BY ... LIMIT ...` querying, indexes, joins, and row diffing.
- Plugin registry for validators, exporters, parsers, and formula functions.
- CSV issue reports, diff reports, worker-thread mapping, and reproducible benchmark entrypoints.

## Roadmap

Sheetra is being developed in phases:

1. Foundation: package, tests, build, linting, API, fixtures, and benchmarks.
2. Fast streaming core: constant-memory XLSX/CSV row pipelines and backpressure-aware writers.
3. Type-safe ingestion: schemas, cleaning, diagnostics, and partial recovery.
4. Feature parity: workbook model, formulas, styles, merged cells, validations, hyperlinks, comments, tables, panes, metadata, and images.
5. Differentiators: formula evaluation, SQL, diffing, worker parallelism, plugins, and performance telemetry.
6. Benchmark proof: reproducible performance comparisons before making speed claims.

## Scripts

```sh
npm run build
npm test
npm run lint
npm run benchmark
npm run benchmark:compare
npm run benchmark:files
```

## Benchmarks

Benchmarks are intentionally checked in as scripts, not marketing claims. Run them on your own machine with representative files before choosing Sheetra for a workload. The strongest suite is `npm run benchmark:strong`, which is designed to answer whether Sheetra survives messy, large, production-style files.

Local environment for the numbers below:

- macOS Darwin 25.4.0
- Node.js package scripts from this repository
- `SHEETRA_BENCH_ROWS=100000`
- Local fixtures in `benchmark/files`

Synthetic 100k-row write/transform benchmark:

| Engine | Rows | Time | Peak memory |
| --- | ---: | ---: | ---: |
| `sheetra:csv:pipeline` | 100,000 | 206ms | 153.1MB |
| `sheetjs:xlsx:json_to_sheet` | 100,000 | 314ms | 308.2MB |
| `exceljs:workbook:csv` | 100,000 | 139ms | 446.7MB |

Real local file benchmark:

| File | Engine | Rows | Time | Peak memory |
| --- | --- | ---: | ---: | ---: |
| `Crime_Data_from_2020_to_2024.csv` 243.6MB | `sheetra:csv:stream` | 1,004,894 | 11,577ms | 123.9MB |
| `Crime_Data_from_2020_to_2024.csv` 243.6MB | `fast-csv:stream` | 1,004,894 | 7,851ms | 128.1MB |
| `MOCK_DATA.csv` 497.6KB | `sheetra:csv:stream` | 1,000 | 25ms | 129.1MB |
| `MOCK_DATA.csv` 497.6KB | `sheetjs:xlsx:readFile` | 1,000 | 100ms | 152.0MB |
| `MOCK_DATA.xlsx` 244.2KB | `sheetra:xlsx` | 1,000 | 143ms | 189.8MB |
| `MOCK_DATA.xlsx` 244.2KB | `sheetjs:xlsx:readFile` | 1,000 | 72ms | 196.6MB |
| `hts_2024_revision_9_xlsx.xlsx` 1.5MB | `sheetra:xlsx` | 35,808 | 1,060ms | 491.0MB |
| `hts_2024_revision_9_xlsx.xlsx` 1.5MB | `sheetjs:xlsx:readFile` | 35,808 | 334ms | 579.8MB |

Current interpretation:

- Sheetra’s CSV path is already memory-stable on a 244MB / 1M-row file, but `fast-csv` is still faster as a raw CSV parser.
- Sheetra’s XLSX path is feature-oriented but not yet optimized; SheetJS is faster on the tested XLSX fixtures.
- The next performance milestone is a true streaming XLSX reader and a lower-overhead CSV pipeline.

Benchmark controls:

```sh
SHEETRA_BENCH_ROWS=100000 npm run benchmark:compare
SHEETRA_BENCH_FILE=MOCK_DATA.csv npm run benchmark:files
SHEETRA_BENCH_LIMIT=100000 npm run benchmark:files
SHEETRA_BENCH_INCLUDE_MEMORY=1 npm run benchmark:files
SHEETRA_BENCH_PROFILE=full npm run benchmark:strong
SHEETRA_BENCH_SCALES=100000,500000,1000000,2000000 npm run benchmark:strong
```

The strong suite covers:

- Scale runs from 100k through 2M+ rows.
- Streaming vs in-memory modes for Sheetra, ExcelJS, SheetJS, and fast-csv.
- XLSX read paths, multi-sheet workbooks, and formula-preserving files.
- Tall data, wide data, light transforms, heavy transforms, and full read-validate-transform-write pipelines.
- Fault tolerance with messy rows, invalid types, missing values, and issue collection.
- Worker-thread scaling, cold/warm runs, local file-size fixtures, GC time, and memory samples over time.

The suite writes detailed JSON and Markdown artifacts to `benchmark/results/`, including sampled memory timelines. Those generated artifacts are ignored by git so local large-run evidence stays local unless explicitly published.

CSV performance note: Sheetra keeps CSV values as raw strings by default, matching the lower-overhead behavior expected from stream-first parsers. Use `read(file, { inferTypes: true })` for primitive inference, or prefer `schema(...)` for typed ingestion with explicit validation and coercion.

## License

MIT
