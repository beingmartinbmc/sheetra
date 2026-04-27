import type { CellValue, Row, PravaahIssue } from "../types.js";

export type ParserPlugin = (value: unknown, column: string, row: Row) => CellValue | undefined;
export type ValidatorPlugin = (row: Row) => PravaahIssue[];
export type ExporterPlugin = (rows: AsyncIterable<Row>, destination: string) => Promise<void>;

export interface PravaahPlugin {
  name: string;
  parsers?: Record<string, ParserPlugin>;
  validators?: ValidatorPlugin[];
  exporters?: Record<string, ExporterPlugin>;
  formulas?: Record<string, (args: CellValue[], row?: Row) => CellValue>;
}

export class PluginRegistry {
  private readonly plugins = new Map<string, PravaahPlugin>();

  use(plugin: PravaahPlugin): this {
    if (this.plugins.has(plugin.name)) throw new Error(`Plugin already registered: ${plugin.name}`);
    this.plugins.set(plugin.name, plugin);
    return this;
  }

  list(): PravaahPlugin[] {
    return [...this.plugins.values()];
  }

  formulas(): NonNullable<PravaahPlugin["formulas"]> {
    return Object.assign({}, ...this.list().map((plugin) => plugin.formulas ?? {}));
  }

  validators(): ValidatorPlugin[] {
    return this.list().flatMap((plugin) => plugin.validators ?? []);
  }

  validate(row: Row): PravaahIssue[] {
    return this.validators().flatMap((validator) => validator(row));
  }

  validateRows(rows: Iterable<Row>): PravaahIssue[] {
    return [...rows].flatMap((row) => this.validate(row));
  }
}

export const plugins = new PluginRegistry();
