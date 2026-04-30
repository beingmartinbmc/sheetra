# Contributing

Thanks for helping improve Pravaah.

## Development

```sh
npm install
npm run typecheck
npm test
npm run lint
npm run build
```

## Benchmarks

Run isolated benchmarks before changing parser, writer, XLSX, or pipeline performance code:

```sh
npm run benchmark:isolated
```

Use anonymized or synthetic data in benchmark fixtures. Do not commit customer data, secrets, or private exports.

## Pull Requests

- Keep changes focused and reviewable.
- Add or update tests for behavior changes.
- Update the README when public APIs or benchmark claims change.
- Include benchmark notes when performance is part of the change.

Issues and PRs are welcome.
