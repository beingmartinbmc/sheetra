# Pravaah Benchmarks

The benchmark suite is intentionally reproducible and conservative. Use it to compare Pravaah with SheetJS, ExcelJS, and focused CSV tools before making performance claims.

```sh
PRAVAAH_BENCH_ROWS=100000 npm run benchmark
PRAVAAH_BENCH_ROWS=100000 npm run benchmark:compare
npm run benchmark:files
npm run benchmark:strong
```

`benchmark:files` scans `benchmark/files` for local CSV/XLSX fixtures. Large CSV files are ignored by git, but the scripts use them when present locally.

`benchmark:strong` is the production-oriented suite. It covers:

- Scale: 100k, 500k, 1M, and 2M+ rows through `PRAVAAH_BENCH_SCALES`.
- Streaming vs in-memory: Pravaah raw streaming, Pravaah feature-enabled ingestion, fast-csv streaming, ExcelJS in-memory/streaming, and SheetJS in-memory where practical.
- XLSX: generated XLSX files, formula-preserving workbooks, and multi-sheet files.
- Shapes: tall datasets and wide datasets.
- Transform complexity: no transform, light transform, and CPU-heavy transform.
- Fault tolerance: messy rows, invalid types, missing values, and issue collection.
- Memory stability: sampled memory timeline in `benchmark/results/strong-results.json`.
- Parallel processing: worker-thread mapper runs with one worker and multiple workers.
- Cold/warm behavior: first and second runs against the same file.
- Real scenarios: CRM import, log export, financial formulas, and local file-size fixtures.

Pravaah modes are intentionally split:

- Raw mode: no type inference, no schema, no cleaning; closest comparison to stream-first parsers.
- Feature-enabled mode: `inferTypes`, schema validation, cleaning, and issue collection where the suite is testing production ingestion behavior.

Useful controls:

- `PRAVAAH_BENCH_FILE=MOCK_DATA.csv npm run benchmark:files`
- `PRAVAAH_BENCH_LIMIT=100000 npm run benchmark:files`
- `PRAVAAH_BENCH_INCLUDE_MEMORY=1 npm run benchmark:files`
- `PRAVAAH_BENCH_PROFILE=full npm run benchmark:strong`
- `PRAVAAH_BENCH_SCALES=100000,500000,1000000,2000000 npm run benchmark:strong`
- `PRAVAAH_BENCH_XLSX_ROWS=50000 npm run benchmark:strong`
- `PRAVAAH_BENCH_WIDE_ROWS=50000 npm run benchmark:strong`

Track:

- Wall-clock time.
- Rows per second.
- Peak RSS.
- GC time.
- Memory samples over time.
- Output correctness.
- Backpressure behavior for streaming workloads.
