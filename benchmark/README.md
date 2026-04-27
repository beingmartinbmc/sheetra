# Sheetra Benchmarks

The benchmark suite is intentionally reproducible and conservative. Use it to compare Sheetra with SheetJS, ExcelJS, and focused CSV tools before making performance claims.

```sh
SHEETRA_BENCH_ROWS=100000 npm run benchmark
SHEETRA_BENCH_ROWS=100000 npm run benchmark:compare
npm run benchmark:files
```

`benchmark:files` scans `benchmark/files` for local CSV/XLSX fixtures. Large CSV files are ignored by git, but the scripts use them when present locally.

Useful controls:

- `SHEETRA_BENCH_FILE=MOCK_DATA.csv npm run benchmark:files`
- `SHEETRA_BENCH_LIMIT=100000 npm run benchmark:files`
- `SHEETRA_BENCH_INCLUDE_MEMORY=1 npm run benchmark:files`

Track:

- Wall-clock time.
- Rows per second.
- Peak RSS.
- Output correctness.
- Backpressure behavior for streaming workloads.
