#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { read, parseDetailed, write } from "../pipeline/index.js";
import { diff, writeDiffReport } from "../diff/index.js";
import { query } from "../query/index.js";
import { writeIssueReport } from "../schema/index.js";
import { formatBytes } from "../perf/index.js";
import type { Row, SchemaDefinition } from "../index.js";

type CliArgs = {
  command: string;
  args: string[];
  flags: Record<string, string | boolean>;
};

export async function runCli(argv: string[] = process.argv.slice(2)): Promise<number> {
  const parsed = parseArgs(argv);
  if (parsed.command === "help" || parsed.command === "" || parsed.flags.help === true) {
    printHelp();
    return 0;
  }

  switch (parsed.command) {
    case "head":
      return await commandHead(parsed);
    case "stats":
      return await commandStats(parsed);
    case "validate":
      return await commandValidate(parsed);
    case "convert":
      return await commandConvert(parsed);
    case "diff":
      return await commandDiff(parsed);
    case "query":
      return await commandQuery(parsed);
    default:
      process.stderr.write(`Unknown command: ${parsed.command}\n\n`);
      printHelp();
      return 1;
  }
}

async function commandHead(parsed: CliArgs): Promise<number> {
  const source = parsed.args[0];
  if (source === undefined) return fail("pravaah head <file> [--rows N]");
  const limit = Number(parsed.flags.rows ?? 10);
  const pipeline = read(source).take(Number.isFinite(limit) ? limit : 10);
  const rows = await pipeline.collect();
  process.stdout.write(JSON.stringify(rows, null, 2) + "\n");
  return 0;
}

async function commandStats(parsed: CliArgs): Promise<number> {
  const source = parsed.args[0];
  if (source === undefined) return fail("pravaah stats <file>");
  const stats = await read(source).drain();
  process.stdout.write(
    [
      `rows:        ${stats.rowsProcessed}`,
      `durationMs:  ${Math.round(stats.durationMs ?? 0)}`,
      `peakRss:     ${formatBytes(stats.peakRssBytes)}`,
      `errors:      ${stats.errors}`,
    ].join("\n") + "\n",
  );
  return 0;
}

async function commandValidate(parsed: CliArgs): Promise<number> {
  const source = parsed.args[0];
  const schemaPath = parsed.flags.schema;
  if (source === undefined || typeof schemaPath !== "string") {
    return fail("pravaah validate <file> --schema <schema.json> [--allow-js-schema] [--report <issues.csv>]");
  }
  const allowJs = parsed.flags["allow-js-schema"] === true;
  let definition: SchemaDefinition;
  try {
    definition = await loadSchemaFromPath(schemaPath, allowJs);
  } catch (error) {
    return fail((error as Error).message);
  }
  const mode = typeof parsed.flags.mode === "string" ? parsed.flags.mode : "collect";
  const result = await parseDetailed(source, definition, { validation: mode as "collect" | "fail-fast" | "skip" });

  if (typeof parsed.flags.report === "string" && result.issues.length > 0) {
    await writeIssueReport(result.issues, parsed.flags.report);
    process.stdout.write(`Issue report → ${parsed.flags.report}\n`);
  }

  process.stdout.write(
    `Validated ${result.rows.length} rows (${result.issues.length} issues) in ${Math.round(result.stats.durationMs ?? 0)}ms\n`,
  );
  return result.issues.length === 0 ? 0 : 2;
}

async function commandConvert(parsed: CliArgs): Promise<number> {
  const source = parsed.args[0];
  const destination = parsed.args[1];
  if (source === undefined || destination === undefined) {
    return fail("pravaah convert <source> <destination>");
  }
  const stats = await write(read(source), destination);
  process.stdout.write(`Converted ${stats.rowsWritten} rows in ${Math.round(stats.durationMs ?? 0)}ms\n`);
  return 0;
}

async function commandDiff(parsed: CliArgs): Promise<number> {
  const left = parsed.args[0];
  const right = parsed.args[1];
  const keyFlag = parsed.flags.key;
  if (left === undefined || right === undefined || typeof keyFlag !== "string") {
    return fail("pravaah diff <before> <after> --key <column>");
  }
  const keys = keyFlag.split(",").map((k) => k.trim());
  const [leftRows, rightRows] = await Promise.all([read(left).collect(), read(right).collect()]);
  const result = diff(leftRows as Row[], rightRows as Row[], { key: keys.length === 1 ? keys[0]! : keys });

  if (typeof parsed.flags.report === "string") {
    await writeDiffReport(result, parsed.flags.report);
    process.stdout.write(`Diff report → ${parsed.flags.report}\n`);
  }

  process.stdout.write(
    `added: ${result.added.length}  removed: ${result.removed.length}  changed: ${result.changed.length}  unchanged: ${result.unchanged}\n`,
  );
  return 0;
}

async function commandQuery(parsed: CliArgs): Promise<number> {
  const source = parsed.args[0];
  const sql = typeof parsed.flags.sql === "string" ? parsed.flags.sql : parsed.args[1];
  if (source === undefined || sql === undefined) {
    return fail('pravaah query <file> --sql "select col from file where col = 1"');
  }
  const rows = await query(source, sql);
  process.stdout.write(JSON.stringify(rows, null, 2) + "\n");
  return 0;
}

function printHelp(): void {
  process.stdout.write(
    [
      "Usage: pravaah <command> [...args] [--flag=value]",
      "",
      "Commands:",
      "  head     <file>                     Preview rows (--rows N, default 10)",
      "  stats    <file>                     Row count, duration, peak RSS",
      "  validate <file> --schema <path>     Validate against a schema module",
      "  convert  <source> <dest>            Convert between csv, jsonl, json, xlsx",
      "  diff     <a> <b> --key <column>     Diff two datasets",
      "  query    <file> --sql <query>       Run a Pravaah SQL query",
      "",
      "Flags:",
      "  --rows N                Rows to print for `head`",
      "  --schema PATH           Path to a JSON schema file (or JS with --allow-js-schema)",
      "  --allow-js-schema       Permit --schema to execute a .js/.mjs/.ts module (unsafe)",
      "  --mode MODE             Validation mode: collect | fail-fast | skip",
      "  --report PATH           Write an issue/diff report as CSV",
      "  --key COLS              Comma-separated key columns for diff",
      "  --sql QUERY             SQL for `query`",
      "",
    ].join("\n"),
  );
}

function parseArgs(argv: string[]): CliArgs {
  const flags: Record<string, string | boolean> = {};
  const positional: string[] = [];
  for (let i = 0; i < argv.length; i += 1) {
    const value = argv[i]!;
    if (value.startsWith("--")) {
      const eq = value.indexOf("=");
      if (eq !== -1) {
        flags[value.slice(2, eq)] = value.slice(eq + 1);
        continue;
      }
      const key = value.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith("--")) {
        flags[key] = true;
      } else {
        flags[key] = next;
        i += 1;
      }
      continue;
    }
    positional.push(value);
  }
  const [command = "help", ...rest] = positional;
  return { command, args: rest, flags };
}

async function loadSchemaFromPath(path: string, allowJs: boolean): Promise<SchemaDefinition> {
  if (path.endsWith(".json")) {
    const json = JSON.parse(await readFile(path, "utf8"));
    return json as SchemaDefinition;
  }
  if (!allowJs) {
    throw new Error(
      `Refusing to import JavaScript schema at ${path}. Pass --allow-js-schema to execute arbitrary code from this file.`,
    );
  }
  const module = await import(pathToFileURL(path).href);
  const candidate = module.default ?? module.schema ?? module;
  if (candidate === undefined || candidate === null || typeof candidate !== "object") {
    throw new Error(`Schema module must export an object at ${path}`);
  }
  return candidate as SchemaDefinition;
}

function fail(message: string): number {
  process.stderr.write(`${message}\n`);
  return 1;
}

const invokedDirectly = process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  runCli().then((code) => {
    process.exit(code);
  }).catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
    process.exit(1);
  });
}
