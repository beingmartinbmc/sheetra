import { readFile, writeFile } from "node:fs/promises";
import { strFromU8, strToU8, unzipSync, zip } from "fflate";
import type { CellValue, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

const NEEDED_PATHS = new Set([
  "xl/workbook.xml",
  "xl/_rels/workbook.xml.rels",
  "xl/sharedStrings.xml",
  "xl/styles.xml",
]);

const CELL_BRAND = Symbol.for("pravaah.cell");

function selectiveUnzip(
  bytes: Uint8Array,
  extraPaths: Set<string>,
): Record<string, Uint8Array> {
  const needed = new Set([...NEEDED_PATHS, ...extraPaths]);
  const result: Record<string, Uint8Array> = {};
  const all = unzipSync(bytes, {
    filter: (file) => needed.has(file.name) || needed.has(normalizePath(file.name)),
  });
  for (const [key, value] of Object.entries(all)) {
    result[key] = value;
    result[normalizePath(key)] = value;
  }
  return result;
}

function normalizePath(p: string): string {
  return p.startsWith("/") ? p.slice(1) : p;
}

function fullUnzip(bytes: Uint8Array): Record<string, Uint8Array> {
  const all = unzipSync(bytes);
  const result: Record<string, Uint8Array> = {};
  for (const [key, value] of Object.entries(all)) {
    result[key] = value;
    result[normalizePath(key)] = value;
  }
  return result;
}

export async function* readXlsx(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const bytes = typeof source === "string" ? await readFile(source) : source;
  const raw = new Uint8Array(bytes);

  const metaFiles = selectiveUnzip(raw, new Set());
  const hasWorkbookMeta = metaFiles["xl/workbook.xml"] !== undefined;
  const initialFiles = hasWorkbookMeta ? metaFiles : fullUnzip(raw);
  const sheetEntries = workbookSheets(initialFiles);
  const target =
    typeof options.sheet === "string"
      ? sheetEntries.find((entry) => entry.name === options.sheet)
      : sheetEntries[typeof options.sheet === "number" ? options.sheet : 0];

  if (target === undefined) throw new Error(`Worksheet not found: ${String(options.sheet ?? 0)}`);

  const files = hasWorkbookMeta ? selectiveUnzip(raw, new Set([target.path])) : initialFiles;
  const sheetData = files[target.path];
  if (sheetData === undefined) throw new Error(`Worksheet XML not found: ${target.path}`);

  const ssFile = files["xl/sharedStrings.xml"];
  const sharedStrings = ssFile !== undefined ? new LazySharedStrings(ssFile) : new LazySharedStrings(undefined);
  yield* iterateWorksheetRows(sheetData, sharedStrings, options, parseWorkbookStyles(files));
}

export async function writeXlsx(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<void> {
  const normalizedRows: RowLike[] = [];
  for await (const row of toAsync(rows)) normalizedRows.push(row);

  const headers = options.headers ?? inferHeaders(normalizedRows);
  await writeWorkbook(
    workbook([worksheet(options.sheetName ?? "Sheet1", normalizedRows.map((row) => rowToObject(row, headers)))]),
    destination,
    { ...options, headers },
  );
}

export async function readWorkbook(source: string | Buffer, options: ReadOptions = {}): Promise<Workbook> {
  const bytes = typeof source === "string" ? await readFile(source) : source;
  const files = fullUnzip(new Uint8Array(bytes));
  const ssFile = files["xl/sharedStrings.xml"];
  const sharedStrings = ssFile !== undefined ? new LazySharedStrings(ssFile) : new LazySharedStrings(undefined);
  const styles = parseWorkbookStyles(files);
  const sheets = workbookSheets(files);

  return workbook(
    sheets.map((sheet) => {
      const file = files[sheet.path];
      if (file === undefined) throw new Error(`Worksheet XML not found: ${sheet.path}`);
      return worksheet(sheet.name, readWorksheetRows(file, sharedStrings, options, styles));
    }),
  );
}

export async function writeWorkbook(book: Workbook, destination: string, options: WriteOptions = {}): Promise<void> {
  const sheets = book.sheets.length > 0 ? book.sheets : [worksheet(options.sheetName ?? "Sheet1")];
  const files: Record<string, Uint8Array> = {
    "[Content_Types].xml": strToU8(contentTypesXml(sheets)),
    "_rels/.rels": strToU8(rootRelsXml()),
    "docProps/app.xml": strToU8(appXml(sheets.map((sheet) => sheet.name))),
    "docProps/core.xml": strToU8(coreXml(book.properties)),
    "xl/workbook.xml": strToU8(workbookXml(sheets)),
    "xl/_rels/workbook.xml.rels": strToU8(workbookRelsXml(sheets)),
    "xl/styles.xml": strToU8(stylesXml()),
  };

  sheets.forEach((sheet, index) => {
    const headers = options.headers ?? inferHeaders(sheet.rows);
    const sheetRows = [headers, ...sheet.rows.map((row) => rowToObject(row, headers))];
    files[`xl/worksheets/sheet${index + 1}.xml`] = strToU8(worksheetXml(sheetRows, sheet));
  });

  await writeFile(destination, Buffer.from(await zipAsync(files)));
}

export interface Workbook {
  sheets: Worksheet[];
  properties: Record<string, string>;
}

export interface Worksheet {
  name: string;
  rows: RowLike[];
  columns: ColumnDefinition[];
  merges: string[];
  validations: DataValidation[];
  tables: TableDefinition[];
  comments: CellComment[];
  hyperlinks: HyperlinkDefinition[];
  frozen?: FreezePane | undefined;
}

export interface ColumnDefinition {
  header: string;
  key?: string;
  width?: number;
  style?: CellStyle;
}

export interface CellStyle {
  numberFormat?: string;
  bold?: boolean;
  italic?: boolean;
  horizontal?: "left" | "center" | "right";
}

export interface FormulaCell {
  formula: string;
  result?: unknown;
  style?: CellStyle | undefined;
  readonly [CELL_BRAND]?: "formula";
}

export interface StyledCell {
  value: unknown;
  style?: CellStyle | undefined;
  readonly [CELL_BRAND]?: "styled";
}

export interface DataValidation {
  range: string;
  type: "list" | "whole" | "decimal" | "date" | "textLength" | "custom";
  formula?: string;
}

export interface TableDefinition {
  name: string;
  range: string;
  columns: string[];
}

export interface CellComment {
  cell: string;
  author: string;
  text: string;
}

export interface HyperlinkDefinition {
  cell: string;
  target: string;
  tooltip?: string;
}

export interface FreezePane {
  xSplit?: number;
  ySplit?: number;
  topLeftCell?: string;
}

export function workbook(sheets: Worksheet[] = []): Workbook {
  return { sheets, properties: {} };
}

export function worksheet(name: string, rows: RowLike[] = []): Worksheet {
  return { name, rows, columns: [], merges: [], validations: [], tables: [], comments: [], hyperlinks: [] };
}

export function cell(value: unknown, style?: CellStyle): StyledCell {
  const output: StyledCell = style === undefined ? { value } : { value, style };
  return brandCell(output, "styled");
}

export function formula(formula: string, result?: unknown, style?: CellStyle): FormulaCell {
  const output: FormulaCell = { formula };
  if (result !== undefined) output.result = result;
  if (style !== undefined) output.style = style;
  return brandCell(output, "formula");
}

function brandCell<T extends FormulaCell | StyledCell>(value: T, kind: NonNullable<T[typeof CELL_BRAND]>): T {
  Object.defineProperty(value, CELL_BRAND, { value: kind, enumerable: false });
  return value;
}

// --- Lazy shared-string table: builds offset index on first access ---

class LazySharedStrings {
  private offsets: number[] | undefined;
  private cache: Map<number, string> = new Map();
  private xml: string | undefined;

  constructor(raw: Uint8Array | undefined) {
    if (raw === undefined) {
      this.offsets = [];
      this.xml = undefined;
      return;
    }
    this.xml = strFromU8(raw);
  }

  get length(): number {
    this.ensureIndex();
    return this.offsets!.length;
  }

  get(index: number): string {
    this.ensureIndex();
    const cached = this.cache.get(index);
    if (cached !== undefined) return cached;

    if (index < 0 || index >= this.offsets!.length) return "";

    const xml = this.xml!;
    const start = this.offsets![index]!;
    const openEnd = xml.indexOf(">", start);
    if (openEnd === -1) return "";
    const end = xml.indexOf("</si>", openEnd);
    if (end === -1) return "";
    const value = inlineStringText(xml.slice(openEnd + 1, end)) ?? "";
    this.cache.set(index, value);
    return value;
  }

  private ensureIndex(): void {
    if (this.offsets !== undefined) return;
    this.offsets = [];
    const xml = this.xml!;
    let cursor = 0;
    while (cursor < xml.length) {
      const pos = xml.indexOf("<si", cursor);
      if (pos === -1) break;
      this.offsets.push(pos);
      const end = xml.indexOf("</si>", pos);
      if (end === -1) break;
      cursor = end + 5;
    }
  }
}

// --- Workbook/rels parsing without fast-xml-parser ---

function workbookSheets(files: Record<string, Uint8Array>): Array<{ name: string; path: string }> {
  const workbookFile = files["xl/workbook.xml"];
  const relsFile = files["xl/_rels/workbook.xml.rels"];
  if (workbookFile !== undefined && relsFile !== undefined) {
    const wbXml = strFromU8(workbookFile);
    const relXml = strFromU8(relsFile);
    const rels = parseRelationships(relXml);
    return parseWorkbookSheets(wbXml, rels);
  }

  const paths = Object.keys(files)
    .filter((path) => /^xl\/worksheets\/sheet\d+\.xml$/.test(path))
    .sort();
  if (paths.length === 0) throw new Error("No worksheets found in XLSX file");
  return paths.map((path, index) => ({ name: `Sheet${index + 1}`, path }));
}

function parseRelationships(xml: string): Map<string, string> {
  const map = new Map<string, string>();
  let cursor = 0;
  while (cursor < xml.length) {
    const start = xml.indexOf("<Relationship", cursor);
    if (start === -1) break;
    const end = xml.indexOf(">", start);
    if (end === -1) break;
    const tag = xml.slice(start, end + 1);
    const id = readXmlAttribute(tag, "Id");
    const target = readXmlAttribute(tag, "Target");
    if (id !== undefined && target !== undefined) {
      map.set(id, normalizeWorksheetTarget(target));
    }
    cursor = end + 1;
  }
  return map;
}

function parseWorkbookSheets(xml: string, rels: Map<string, string>): Array<{ name: string; path: string }> {
  const sheets: Array<{ name: string; path: string }> = [];
  let cursor = 0;
  let index = 0;
  while (cursor < xml.length) {
    const start = xml.indexOf("<sheet", cursor);
    if (start === -1) break;
    const nextChar = xml[start + 6];
    if (nextChar !== " " && nextChar !== "/" && nextChar !== ">") {
      cursor = start + 6;
      continue;
    }
    const end = xml.indexOf(">", start);
    if (end === -1) break;
    const tag = xml.slice(start, end + 1);
    const name = readXmlAttribute(tag, "name") ?? `Sheet${index + 1}`;
    const rId = readXmlAttribute(tag, "r:id") ?? "";
    const path = rels.get(rId) ?? `xl/worksheets/sheet${index + 1}.xml`;
    sheets.push({ name, path });
    index += 1;
    cursor = end + 1;
  }
  return sheets;
}

function normalizeWorksheetTarget(target: string): string {
  if (target.startsWith("/")) return target.slice(1);
  if (target.startsWith("xl/")) return target;
  return `xl/${target}`;
}

function parseWorkbookStyles(files: Record<string, Uint8Array>): WorkbookStyles {
  const stylesFile = files["xl/styles.xml"];
  if (stylesFile === undefined) return { dateStyleIds: new Set() };
  const xml = strFromU8(stylesFile);
  const dateNumFmtIds = new Set<number>([14, 15, 16, 17, 22, 27, 30, 36, 45, 46, 47, 50, 57]);
  for (const tag of xml.match(/<numFmt\b[^>]*>/g) ?? []) {
    const id = Number(readXmlAttribute(tag, "numFmtId"));
    const code = readXmlAttribute(tag, "formatCode") ?? "";
    if (Number.isFinite(id) && /[dyhmse]/i.test(code)) dateNumFmtIds.add(id);
  }

  const dateStyleIds = new Set<number>();
  const cellXfsStart = xml.indexOf("<cellXfs");
  const cellXfsEnd = cellXfsStart === -1 ? -1 : xml.indexOf("</cellXfs>", cellXfsStart);
  if (cellXfsStart === -1 || cellXfsEnd === -1) return { dateStyleIds };

  let styleIndex = 0;
  const cellXfsXml = xml.slice(cellXfsStart, cellXfsEnd);
  for (const tag of cellXfsXml.match(/<xf\b[^>]*>/g) ?? []) {
    const id = Number(readXmlAttribute(tag, "numFmtId"));
    if (dateNumFmtIds.has(id)) dateStyleIds.add(styleIndex);
    styleIndex += 1;
  }
  return { dateStyleIds };
}

// --- Buffer-based worksheet scanning ---

const SLASH = 0x2F; // /

function bufIndexOf(buf: Uint8Array, needle: string, start: number): number {
  const len = needle.length;
  const end = buf.length - len;
  outer:
  for (let i = start; i <= end; i++) {
    for (let j = 0; j < len; j++) {
      if (buf[i + j] !== needle.charCodeAt(j)) continue outer;
    }
    return i;
  }
  return -1;
}

function bufSliceToString(buf: Uint8Array, start: number, end: number): string {
  return new TextDecoder().decode(buf.subarray(start, end));
}

interface WorkbookStyles {
  dateStyleIds: Set<number>;
}

function readWorksheetRows(data: Uint8Array, sharedStrings: LazySharedStrings, options: ReadOptions, styles: WorkbookStyles): Row[] {
  return Array.from(iterateWorksheetRows(data, sharedStrings, options, styles));
}

function* iterateWorksheetRows(data: Uint8Array, sharedStrings: LazySharedStrings, options: ReadOptions, styles: WorkbookStyles): Iterable<Row> {
  const useArrayHeaders = Array.isArray(options.headers) && options.headers.length > 0;
  const headerless = options.headers === false;
  let headers = useArrayHeaders ? (options.headers as string[]) : undefined;

  const colCount = parseDimensionColumnCount(data);

  for (const values of iterateWorksheetValueRows(data, sharedStrings, options, colCount, styles)) {
    if (headers === undefined && !headerless) {
      headers = new Array(values.length);
      for (let index = 0; index < values.length; index += 1) headers[index] = String(values[index] ?? `_${index + 1}`);
      continue;
    }

    const obj: Row = {};
    if (headerless) {
      for (let index = 0; index < values.length; index += 1) {
        if (values[index] !== undefined) obj[`_${index + 1}`] = values[index] ?? null;
      }
    } else {
      const resolvedHeaders = headers ?? [];
      for (let index = 0; index < resolvedHeaders.length; index += 1) {
        obj[resolvedHeaders[index]!] = values[index] ?? null;
      }
    }
    yield obj;
  }
}

function parseDimensionColumnCount(data: Uint8Array): number | undefined {
  const dimIdx = bufIndexOf(data, "<dimension", 0);
  if (dimIdx === -1) return undefined;
  const dimEnd = bufIndexOf(data, ">", dimIdx);
  if (dimEnd === -1) return undefined;
  const tag = bufSliceToString(data, dimIdx, dimEnd + 1);
  const ref = readXmlAttribute(tag, "ref");
  if (ref === undefined) return undefined;
  const parts = ref.split(":");
  if (parts.length !== 2) return undefined;
  return cellRefToColumnIndex(parts[1]!) + 1;
}

function* iterateWorksheetValueRows(
  data: Uint8Array,
  sharedStrings: LazySharedStrings,
  options: ReadOptions,
  colCount: number | undefined,
  styles: WorkbookStyles,
): Iterable<CellValue[]> {
  const sdStart = bufIndexOf(data, "<sheetData", 0);
  if (sdStart === -1) return;

  const sdOpenEnd = bufIndexOf(data, ">", sdStart);
  const sdEnd = bufIndexOf(data, "</sheetData>", sdOpenEnd);
  if (sdOpenEnd === -1 || sdEnd === -1) return;

  let cursor = sdOpenEnd + 1;
  while (cursor < sdEnd) {
    const rowStart = bufIndexOf(data, "<row", cursor);
    if (rowStart === -1 || rowStart >= sdEnd) return;

    const rowOpenEnd = bufIndexOf(data, ">", rowStart);
    if (rowOpenEnd === -1) return;

    const selfClose = data[rowOpenEnd - 1] === SLASH;
    if (selfClose) {
      cursor = rowOpenEnd + 1;
      yield [];
      continue;
    }

    const rowEnd = bufIndexOf(data, "</row>", rowOpenEnd);
    if (rowEnd === -1) return;

    const rowXml = bufSliceToString(data, rowOpenEnd + 1, rowEnd);
    yield readWorksheetValueRow(rowXml, sharedStrings, options, colCount, styles);
    cursor = rowEnd + 6;
  }
}

function readWorksheetValueRow(
  rowXml: string,
  sharedStrings: LazySharedStrings,
  options: ReadOptions,
  colCount: number | undefined,
  styles: WorkbookStyles,
): CellValue[] {
  const values: CellValue[] = colCount !== undefined ? new Array<CellValue>(colCount).fill(undefined as unknown as CellValue) : [];
  let cursor = 0;
  let implicitColumn = 0;

  while (cursor < rowXml.length) {
    const cellStart = rowXml.indexOf("<c", cursor);
    if (cellStart === -1) break;

    const cellOpenEnd = rowXml.indexOf(">", cellStart);
    if (cellOpenEnd === -1) break;

    const openTag = rowXml.slice(cellStart, cellOpenEnd + 1);
    const attrs = parseCellAttributes(openTag);
    const ref = attrs.r;
    const columnIndex = ref === undefined ? implicitColumn : cellRefToColumnIndex(ref);
    implicitColumn = columnIndex + 1;

    if (openTag.endsWith("/>")) {
      values[columnIndex] = null;
      cursor = cellOpenEnd + 1;
      continue;
    }

    const cellEnd = rowXml.indexOf("</c>", cellOpenEnd);
    if (cellEnd === -1) break;

    values[columnIndex] = decodeCellXml(attrs, rowXml.slice(cellOpenEnd + 1, cellEnd), sharedStrings, options, styles);
    cursor = cellEnd + 4;
  }
  return values;
}

interface CellAttributes {
  r?: string | undefined;
  t?: string | undefined;
  s?: number | undefined;
}

function decodeCellXml(
  attrs: CellAttributes,
  innerXml: string,
  sharedStrings: LazySharedStrings,
  options: ReadOptions,
  styles: WorkbookStyles,
): CellValue {
  const formulaText = firstTagText(innerXml, "f");
  if (formulaText !== undefined && options.formulas === "preserve") {
    return formula(
      formulaText.startsWith("=") ? formulaText.slice(1) : formulaText,
      decodeCellXml(attrs, innerXml, sharedStrings, { ...options, formulas: "values" }, styles),
    );
  }
  if (attrs.t === "s") return sharedStrings.get(Number(firstTagText(innerXml, "v"))) ?? null;
  if (attrs.t === "inlineStr") return inlineStringText(innerXml);
  if (attrs.t === "b") return firstTagText(innerXml, "v") === "1";

  const rawValue = firstTagText(innerXml, "v");
  if (rawValue === undefined) return null;
  const number = Number(rawValue);
  if (!Number.isFinite(number)) return rawValue;
  return attrs.s !== undefined && styles.dateStyleIds.has(attrs.s) ? excelSerialDate(number) : number;
}

function parseCellAttributes(tag: string): CellAttributes {
  const attrs: CellAttributes = {};
  const ref = readXmlAttribute(tag, "r");
  const type = readXmlAttribute(tag, "t");
  const style = readXmlAttribute(tag, "s");
  if (ref !== undefined) attrs.r = ref;
  if (type !== undefined) attrs.t = type;
  if (style !== undefined) attrs.s = Number(style);
  return attrs;
}

function readXmlAttribute(tag: string, name: string): string | undefined {
  const marker = `${name}=`;
  const markerIndex = tag.indexOf(marker);
  if (markerIndex === -1) return undefined;

  const quoteIndex = markerIndex + marker.length;
  const quote = tag[quoteIndex];
  if (quote !== "\"" && quote !== "'") return undefined;

  const valueStart = quoteIndex + 1;
  const valueEnd = tag.indexOf(quote, valueStart);
  return valueEnd === -1 ? undefined : unescapeXml(tag.slice(valueStart, valueEnd));
}

function firstTagText(xml: string, tag: string): string | undefined {
  const start = xml.indexOf(`<${tag}`);
  if (start === -1) return undefined;
  const openEnd = xml.indexOf(">", start);
  if (openEnd === -1) return undefined;
  const end = xml.indexOf(`</${tag}>`, openEnd);
  return end === -1 ? undefined : unescapeXml(xml.slice(openEnd + 1, end));
}

function inlineStringText(xml: string): string | null {
  const texts: string[] = [];
  const regex = /<t\b[^>]*>([\s\S]*?)<\/t>/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(xml)) !== null) texts.push(unescapeXml(match[1]!));
  return texts.length === 0 ? null : texts.join("");
}

function cellRefToColumnIndex(ref: string): number {
  let index = 0;
  for (let offset = 0; offset < ref.length; offset += 1) {
    const code = ref.charCodeAt(offset);
    if (code >= 65 && code <= 90) index = index * 26 + code - 64;
    else if (code >= 97 && code <= 122) index = index * 26 + code - 96;
    else break;
  }
  return Math.max(0, index - 1);
}

function rowToObject(row: RowLike, headers: string[]): Row {
  if (!Array.isArray(row)) return row;
  return Object.fromEntries(headers.map((header, index) => [header, row[index] ?? null]));
}

function inferHeaders(rows: RowLike[]): string[] {
  const firstObject = rows.find((row): row is Row => !Array.isArray(row));
  if (firstObject !== undefined) return Object.keys(firstObject);
  const firstArray = rows.find(Array.isArray);
  return firstArray?.map((_, index) => `_${index + 1}`) ?? [];
}

async function* toAsync<T>(rows: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
  yield* rows;
}

function zipAsync(files: Record<string, Uint8Array>): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    zip(files, { level: 6 }, (error, data) => {
      if (error !== null) reject(error);
      else resolve(data);
    });
  });
}

function excelSerialDate(value: number): Date {
  const excelEpoch = Date.UTC(1899, 11, 30);
  return new Date(excelEpoch + value * 24 * 60 * 60 * 1000);
}

function worksheetXml(rows: RowLike[], sheet?: Worksheet): string {
  const mergeXml =
    sheet !== undefined && sheet.merges.length > 0
      ? `  <mergeCells count="${sheet.merges.length}">${sheet.merges.map((ref) => `<mergeCell ref="${escapeXml(ref)}"/>`).join("")}</mergeCells>\n`
      : "";
  const validationXml =
    sheet !== undefined && sheet.validations.length > 0
      ? `  <dataValidations count="${sheet.validations.length}">${sheet.validations.map(dataValidationXml).join("")}</dataValidations>\n`
      : "";
  const autoFilterXml =
    sheet !== undefined && sheet.tables[0] !== undefined ? `  <autoFilter ref="${escapeXml(sheet.tables[0].range)}"/>\n` : "";
  const paneAttrs = sheet?.frozen === undefined ? "" : freezeAttrs(sheet.frozen);
  const paneXml = paneAttrs === "" ? "" : `  <sheetViews><sheetView workbookViewId="0"><pane ${paneAttrs}/></sheetView></sheetViews>\n`;

  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
${paneXml}${columnsXml(sheet?.columns)}
  <sheetData>
${rows.map((row, rowIndex) => rowXml(Array.isArray(row) ? row : Object.values(row), rowIndex + 1)).join("\n")}
  </sheetData>
${autoFilterXml}${mergeXml}${validationXml}
</worksheet>`;
}

function rowXml(values: unknown[], rowNumber: number): string {
  return `    <row r="${rowNumber}">${values
    .map((value, index) => cellXml(value, `${columnName(index + 1)}${rowNumber}`))
    .join("")}</row>`;
}

function cellXml(value: unknown, ref: string): string {
  const normalized = normalizeCell(value);
  if (isFormulaCell(normalized)) {
    const formulaValue = normalized.result;
    const valueXml =
      formulaValue === undefined || formulaValue === null ? "" : `<v>${escapeXml(String(formulaValue))}</v>`;
    return `<c r="${ref}"><f>${escapeXml(normalized.formula.replace(/^=/, ""))}</f>${valueXml}</c>`;
  }
  if (isStyledCell(normalized)) return cellXml(normalized.value, ref);
  if (value === null || value === undefined) return `<c r="${ref}"/>`;
  if (value instanceof Date) return `<c r="${ref}" t="inlineStr"><is><t>${escapeXml(value.toISOString())}</t></is></c>`;
  if (typeof value === "number") return `<c r="${ref}"><v>${value}</v></c>`;
  if (typeof value === "boolean") return `<c r="${ref}" t="b"><v>${value ? 1 : 0}</v></c>`;
  return `<c r="${ref}" t="inlineStr"><is><t>${escapeXml(String(value))}</t></is></c>`;
}

function normalizeCell(value: unknown): unknown {
  return value;
}

function isFormulaCell(value: unknown): value is FormulaCell {
  return typeof value === "object" && value !== null && (value as FormulaCell)[CELL_BRAND] === "formula";
}

function isStyledCell(value: unknown): value is StyledCell {
  return typeof value === "object" && value !== null && (value as StyledCell)[CELL_BRAND] === "styled";
}

function columnsXml(columns: ColumnDefinition[] | undefined): string {
  if (columns === undefined || columns.length === 0) return "";
  const cols = columns
    .map((column, index) => {
      const width = column.width ?? 12;
      return `<col min="${index + 1}" max="${index + 1}" width="${width}" customWidth="1"/>`;
    })
    .join("");
  return `  <cols>${cols}</cols>\n`;
}

function dataValidationXml(validation: DataValidation): string {
  const formula = validation.formula === undefined ? "" : `<formula1>${escapeXml(validation.formula)}</formula1>`;
  return `<dataValidation type="${validation.type}" sqref="${escapeXml(validation.range)}">${formula}</dataValidation>`;
}

function freezeAttrs(frozen: FreezePane): string {
  if (frozen.xSplit === undefined && frozen.ySplit === undefined && frozen.topLeftCell === undefined) return "";
  const attrs = [
    frozen.xSplit === undefined ? undefined : `xSplit="${frozen.xSplit}"`,
    frozen.ySplit === undefined ? undefined : `ySplit="${frozen.ySplit}"`,
    frozen.topLeftCell === undefined ? undefined : `topLeftCell="${escapeXml(frozen.topLeftCell)}"`,
    `state="frozen"`,
  ].filter(Boolean);
  return attrs.join(" ");
}

function columnName(index: number): string {
  let name = "";
  let current = index;
  while (current > 0) {
    const remainder = (current - 1) % 26;
    name = String.fromCharCode(65 + remainder) + name;
    current = Math.floor((current - 1) / 26);
  }
  return name;
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function unescapeXml(value: string): string {
  return value
    .replace(/&#x([0-9a-fA-F]+);/g, (_match, hex: string) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_match, decimal: string) => String.fromCodePoint(Number.parseInt(decimal, 10)))
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function contentTypesXml(sheets: Worksheet[]): string {
  const worksheetOverrides = sheets
    .map(
      (_sheet, index) =>
        `  <Override PartName="/xl/worksheets/sheet${index + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`,
    )
    .join("\n");
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
${worksheetOverrides}
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>`;
}

function rootRelsXml(): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>`;
}

function workbookRelsXml(sheets: Worksheet[]): string {
  const sheetRels = sheets
    .map(
      (_sheet, index) =>
        `  <Relationship Id="rId${index + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${index + 1}.xml"/>`,
    )
    .join("\n");
  const styleId = sheets.length + 1;
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
${sheetRels}
  <Relationship Id="rId${styleId}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`;
}

function workbookXml(sheets: Worksheet[]): string {
  const sheetXml = sheets
    .map(
      (sheet, index) =>
        `<sheet name="${escapeXml(sheet.name)}" sheetId="${index + 1}" r:id="rId${index + 1}"/>`,
    )
    .join("");
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>${sheetXml}</sheets>
</workbook>`;
}

function stylesXml(): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="1"><fill><patternFill patternType="none"/></fill></fills>
  <borders count="1"><border/></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>
</styleSheet>`;
}

function appXml(sheetNames: string[]): string {
  const parts = sheetNames.map((sheetName) => `<vt:lpstr>${escapeXml(sheetName)}</vt:lpstr>`).join("");
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
  <Application>Pravaah</Application><TitlesOfParts><vt:vector xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes" size="${sheetNames.length}" baseType="lpstr">${parts}</vt:vector></TitlesOfParts>
</Properties>`;
}

function coreXml(properties: Record<string, string> = {}): string {
  const creator = properties.creator ?? "Pravaah";
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/">
  <dc:creator>${escapeXml(creator)}</dc:creator><dcterms:created>${properties.created ?? new Date().toISOString()}</dcterms:created>
</cp:coreProperties>`;
}
