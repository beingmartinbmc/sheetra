# Sheetra Benchmarks

The benchmark suite is intentionally reproducible and conservative. Use it to compare Sheetra with SheetJS, ExcelJS, and focused CSV tools before making performance claims.

```sh
SHEETRA_BENCH_ROWS=100000 npm run benchmark
```

Track:

- Wall-clock time.
- Rows per second.
- Peak RSS.
- Output correctness.
- Backpressure behavior for streaming workloads.
