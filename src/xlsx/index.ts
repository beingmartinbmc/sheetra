import { readFile, writeFile } from "node:fs/promises";
import { strFromU8, strToU8, unzipSync, zipSync } from "fflate";
import { XMLParser } from "fast-xml-parser";
import type { CellValue, ReadOptions, Row, RowLike, WriteOptions } from "../types.js";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  textNodeName: "text",
  parseTagValue: false,
  parseAttributeValue: false,
});

export async function* readXlsx(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const bytes = typeof source === "string" ? await readFile(source) : source;
  const files = unzipSync(new Uint8Array(bytes));
  const sharedStrings = readSharedStrings(files);
  const sheetEntries = workbookSheets(files);

  const target =
    typeof options.sheet === "string"
      ? sheetEntries.find((entry) => entry.name === options.sheet)
      : sheetEntries[typeof options.sheet === "number" ? options.sheet : 0];

  if (target === undefined) throw new Error(`Worksheet not found: ${String(options.sheet ?? 0)}`);
  const sheetXml = files[target.path];
  if (sheetXml === undefined) throw new Error(`Worksheet XML not found: ${target.path}`);

  yield* iterateWorksheetRows(strFromU8(sheetXml), sharedStrings, options);
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
  const files = unzipSync(new Uint8Array(bytes));
  const sharedStrings = readSharedStrings(files);
  const sheets = workbookSheets(files);

  return workbook(
    sheets.map((sheet) => {
      const file = files[sheet.path];
      if (file === undefined) throw new Error(`Worksheet XML not found: ${sheet.path}`);
      return worksheet(sheet.name, readWorksheetRows(strFromU8(file), sharedStrings, options));
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
    const sheetRows = [headers, ...sheet.rows.map((row) => headers.map((header) => row[header] ?? null))];
    files[`xl/worksheets/sheet${index + 1}.xml`] = strToU8(worksheetXml(sheetRows, sheet));
  });

  await writeFile(destination, Buffer.from(zipSync(files, { level: 6 })));
}

export interface Workbook {
  sheets: Worksheet[];
  properties: Record<string, string>;
}

export interface Worksheet {
  name: string;
  rows: Row[];
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
}

export interface StyledCell {
  value: unknown;
  style?: CellStyle | undefined;
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

export function worksheet(name: string, rows: Row[] = []): Worksheet {
  return { name, rows, columns: [], merges: [], validations: [], tables: [], comments: [], hyperlinks: [] };
}

export function cell(value: unknown, style?: CellStyle): StyledCell {
  return style === undefined ? { value } : { value, style };
}

export function formula(formula: string, result?: unknown, style?: CellStyle): FormulaCell {
  const output: FormulaCell = { formula };
  if (result !== undefined) output.result = result;
  if (style !== undefined) output.style = style;
  return output;
}

function readSharedStrings(files: Record<string, Uint8Array>): string[] {
  const file = files["xl/sharedStrings.xml"];
  if (file === undefined) return [];
  const doc = parser.parse(strFromU8(file)) as SharedStringsDoc;
  return arrayify(doc.sst.si).map((entry) => {
    if (typeof entry.t === "string") return entry.t;
    if (entry.t?.text !== undefined) return entry.t.text;
    return arrayify(entry.r)
      .map((run) => (typeof run.t === "string" ? run.t : run.t?.text ?? ""))
      .join("");
  });
}

function workbookSheets(files: Record<string, Uint8Array>): Array<{ name: string; path: string }> {
  const workbookFile = files["xl/workbook.xml"];
  const relsFile = files["xl/_rels/workbook.xml.rels"];
  if (workbookFile !== undefined && relsFile !== undefined) {
    const workbookDoc = parser.parse(strFromU8(workbookFile)) as WorkbookDoc;
    const relsDoc = parser.parse(strFromU8(relsFile)) as RelationshipsDoc;
    const rels = new Map(
      arrayify(relsDoc.Relationships.Relationship).map((rel) => [rel.Id, normalizeWorksheetTarget(rel.Target)]),
    );
    return arrayify(workbookDoc.workbook.sheets.sheet).map((sheet, index) => ({
      name: sheet.name ?? `Sheet${index + 1}`,
      path: rels.get(sheet["r:id"] ?? "") ?? `xl/worksheets/sheet${index + 1}.xml`,
    }));
  }

  const paths = Object.keys(files)
    .filter((path) => /^xl\/worksheets\/sheet\d+\.xml$/.test(path))
    .sort();
  if (paths.length === 0) throw new Error("No worksheets found in XLSX file");
  return paths.map((path, index) => ({ name: `Sheet${index + 1}`, path }));
}

function normalizeWorksheetTarget(target: string): string {
  if (target.startsWith("/")) return target.slice(1);
  if (target.startsWith("xl/")) return target;
  return `xl/${target}`;
}

function readWorksheetRows(xml: string, sharedStrings: string[], options: ReadOptions): Row[] {
  return Array.from(iterateWorksheetRows(xml, sharedStrings, options));
}

function* iterateWorksheetRows(xml: string, sharedStrings: string[], options: ReadOptions): Iterable<Row> {
  const doc = parser.parse(xml) as WorksheetDoc;
  const rows = arrayify(doc.worksheet.sheetData?.row);
  if (rows.length === 0) return;

  const useArrayHeaders = Array.isArray(options.headers) && options.headers.length > 0;
  const headerless = options.headers === false;
  const headers = useArrayHeaders
    ? (options.headers as string[])
    : headerless
      ? []
      : readHeaderRow(rows[0]!, sharedStrings, options);

  const startIndex = headerless || useArrayHeaders ? 0 : 1;
  for (let r = startIndex; r < rows.length; r += 1) {
    const row = rows[r]!;
    const cells = arrayify(row.c);
    const headerCount = headers.length;
    const obj: Row = {};
    if (headerCount > 0) {
      for (let i = 0; i < headerCount; i += 1) {
        const header = headers[i]!;
        const cell = cells[i];
        obj[header] = cell === undefined ? null : decodeCell(cell, sharedStrings, options);
      }
    } else {
      for (let i = 0; i < cells.length; i += 1) {
        obj[`_${i + 1}`] = decodeCell(cells[i]!, sharedStrings, options);
      }
    }
    yield obj;
  }
}

function readHeaderRow(row: WorksheetRow, sharedStrings: string[], options: ReadOptions): string[] {
  const cells = arrayify(row.c);
  const headers: string[] = new Array(cells.length);
  for (let i = 0; i < cells.length; i += 1) {
    const value = decodeCell(cells[i]!, sharedStrings, options);
    headers[i] = String(value ?? `_${i + 1}`);
  }
  return headers;
}

function decodeCell(cell: WorksheetCell, sharedStrings: string[], options: ReadOptions): CellValue {
  const formulaText = typeof cell.f === "string" ? cell.f : cell.f?.text;
  if (formulaText !== undefined && options.formulas === "preserve") {
    const valueCell: WorksheetCell = { ...cell };
    delete valueCell.f;
    return formula(
      formulaText.startsWith("=") ? formulaText.slice(1) : formulaText,
      decodeCell(valueCell, sharedStrings, { ...options, formulas: "values" }),
    );
  }
  if (cell.t === "s") return sharedStrings[Number(cell.v)] ?? null;
  if (cell.t === "inlineStr") {
    const text = cell.is?.t;
    return typeof text === "string" ? text : text?.text ?? null;
  }
  if (cell.t === "b") return cell.v === "1";
  if (cell.v === undefined) return null;
  const number = Number(cell.v);
  return Number.isFinite(number) ? number : cell.v;
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
  const paneXml = sheet?.frozen === undefined ? "" : `  <sheetViews><sheetView workbookViewId="0"><pane ${freezeAttrs(sheet.frozen)}/></sheetView></sheetViews>\n`;

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
  return typeof value === "object" && value !== null && "formula" in value;
}

function isStyledCell(value: unknown): value is StyledCell {
  return typeof value === "object" && value !== null && "value" in value;
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

function arrayify<T>(value: T | T[] | undefined): T[] {
  if (value === undefined) return [];
  return Array.isArray(value) ? value : [value];
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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
  <Application>Sheetra</Application><TitlesOfParts><vt:vector xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes" size="${sheetNames.length}" baseType="lpstr">${parts}</vt:vector></TitlesOfParts>
</Properties>`;
}

function coreXml(properties: Record<string, string> = {}): string {
  const creator = properties.creator ?? "Sheetra";
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/">
  <dc:creator>${escapeXml(creator)}</dc:creator><dcterms:created>${properties.created ?? new Date().toISOString()}</dcterms:created>
</cp:coreProperties>`;
}

interface SharedStringsDoc {
  sst: {
    si: Array<{ t?: string | { text: string }; r?: Array<{ t?: string | { text: string } }> }>;
  };
}

interface WorksheetDoc {
  worksheet: {
    sheetData?: {
      row?: WorksheetRow | WorksheetRow[];
    };
  };
}

interface WorksheetRow {
  c?: WorksheetCell | WorksheetCell[];
}

interface WorksheetCell {
  t?: string;
  v?: string;
  f?: string | { text: string };
  is?: {
    t?: string | { text: string };
  };
}

interface WorkbookDoc {
  workbook: {
    sheets: {
      sheet: WorkbookSheet | WorkbookSheet[];
    };
  };
}

interface WorkbookSheet {
  name?: string;
  "r:id"?: string;
}

interface RelationshipsDoc {
  Relationships: {
    Relationship: Relationship | Relationship[];
  };
}

interface Relationship {
  Id: string;
  Target: string;
}
