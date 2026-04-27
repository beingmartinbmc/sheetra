import type { CellValue, Row, SheetraIssue } from "../types.js";

export type ParserPlugin = (value: unknown, column: string, row: Row) => CellValue | undefined;
export type ValidatorPlugin = (row: Row) => SheetraIssue[];
export type ExporterPlugin = (rows: AsyncIterable<Row>, destination: string) => Promise<void>;

export interface SheetraPlugin {
  name: string;
  parsers?: Record<string, ParserPlugin>;
  validators?: ValidatorPlugin[];
  exporters?: Record<string, ExporterPlugin>;
  formulas?: Record<string, (args: CellValue[], row?: Row) => CellValue>;
}

export class PluginRegistry {
  private readonly plugins = new Map<string, SheetraPlugin>();

  use(plugin: SheetraPlugin): this {
    if (this.plugins.has(plugin.name)) throw new Error(`Plugin already registered: ${plugin.name}`);
    this.plugins.set(plugin.name, plugin);
    return this;
  }

  list(): SheetraPlugin[] {
    return [...this.plugins.values()];
  }

  formulas(): NonNullable<SheetraPlugin["formulas"]> {
    return Object.assign({}, ...this.list().map((plugin) => plugin.formulas ?? {}));
  }

  validators(): ValidatorPlugin[] {
    return this.list().flatMap((plugin) => plugin.validators ?? []);
  }

  validate(row: Row): SheetraIssue[] {
    return this.validators().flatMap((validator) => validator(row));
  }

  validateRows(rows: Iterable<Row>): SheetraIssue[] {
    return [...rows].flatMap((row) => this.validate(row));
  }
}

export const plugins = new PluginRegistry();
