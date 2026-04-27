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
- CSV read/write support through Node streams.
- Basic XLSX read/write support for row data.
- Schema validation with TypeScript inference, coercion, fuzzy headers, cleaning, and structured validation issues.
- Formula helper for common functions such as `SUM`, `AVERAGE`, `IF`, and custom functions.
- SQL-like `SELECT ... WHERE ...` querying, indexes, joins, and row diffing.
- Plugin registry for validators, exporters, parsers, and formula functions.
- Benchmark harness entrypoint.

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
```

## License

MIT
