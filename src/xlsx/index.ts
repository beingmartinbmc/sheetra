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
  const sheetPath = selectSheetPath(files, options.sheet);
  const sheet = files[sheetPath];
  if (sheet === undefined) throw new Error(`Worksheet XML not found: ${sheetPath}`);
  const xml = strFromU8(sheet);
  const doc = parser.parse(xml) as WorksheetDoc;
  const rows = arrayify(doc.worksheet.sheetData?.row);

  const headerRow = rows[0];
  const headers =
    Array.isArray(options.headers) && options.headers.length > 0
      ? options.headers
      : headerRow !== undefined
        ? readCells(headerRow, sharedStrings).map((cell, index) => String(cell ?? `_${index + 1}`))
        : [];

  const dataRows = options.headers === false ? rows : rows.slice(1);
  for (const row of dataRows) {
    const values = readCells(row, sharedStrings);
    yield Object.fromEntries(headers.map((header, index) => [header, values[index] ?? null]));
  }
}

export async function writeXlsx(
  rows: AsyncIterable<RowLike> | Iterable<RowLike>,
  destination: string,
  options: WriteOptions = {},
): Promise<void> {
  const normalizedRows: RowLike[] = [];
  for await (const row of toAsync(rows)) normalizedRows.push(row);

  const headers = options.headers ?? inferHeaders(normalizedRows);
  const sheetRows = [
    headers,
    ...normalizedRows.map((row) =>
      Array.isArray(row) ? row : headers.map((header) => (row as Row)[header] ?? null),
    ),
  ];

  const files: Record<string, Uint8Array> = {
    "[Content_Types].xml": strToU8(contentTypesXml()),
    "_rels/.rels": strToU8(rootRelsXml()),
    "docProps/app.xml": strToU8(appXml(options.sheetName ?? "Sheet1")),
    "docProps/core.xml": strToU8(coreXml()),
    "xl/workbook.xml": strToU8(workbookXml(options.sheetName ?? "Sheet1")),
    "xl/_rels/workbook.xml.rels": strToU8(workbookRelsXml()),
    "xl/styles.xml": strToU8(stylesXml()),
    "xl/worksheets/sheet1.xml": strToU8(worksheetXml(sheetRows)),
  };

  await writeFile(destination, Buffer.from(zipSync(files, { level: 6 })));
}

export interface Workbook {
  sheets: Worksheet[];
  properties: Record<string, string>;
}

export interface Worksheet {
  name: string;
  rows: Row[];
  merges: string[];
  validations: DataValidation[];
  tables: TableDefinition[];
  comments: CellComment[];
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

export function workbook(sheets: Worksheet[] = []): Workbook {
  return { sheets, properties: {} };
}

export function worksheet(name: string, rows: Row[] = []): Worksheet {
  return { name, rows, merges: [], validations: [], tables: [], comments: [] };
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

function selectSheetPath(files: Record<string, Uint8Array>, sheet?: string | number): string {
  if (typeof sheet === "number") return `xl/worksheets/sheet${sheet + 1}.xml`;
  const paths = Object.keys(files)
    .filter((path) => /^xl\/worksheets\/sheet\d+\.xml$/.test(path))
    .sort();
  if (paths.length === 0) throw new Error("No worksheets found in XLSX file");
  return paths[0]!;
}

function readCells(row: WorksheetRow, sharedStrings: string[]): CellValue[] {
  return arrayify(row.c).map((cell) => decodeCell(cell, sharedStrings));
}

function decodeCell(cell: WorksheetCell, sharedStrings: string[]): CellValue {
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

function inferHeaders(rows: RowLike[]): string[] {
  const firstObject = rows.find((row): row is Row => !Array.isArray(row));
  if (firstObject !== undefined) return Object.keys(firstObject);
  const firstArray = rows.find(Array.isArray);
  return firstArray?.map((_, index) => `_${index + 1}`) ?? [];
}

async function* toAsync<T>(rows: AsyncIterable<T> | Iterable<T>): AsyncIterable<T> {
  yield* rows;
}

function worksheetXml(rows: RowLike[]): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
${rows.map((row, rowIndex) => rowXml(Array.isArray(row) ? row : Object.values(row), rowIndex + 1)).join("\n")}
  </sheetData>
</worksheet>`;
}

function rowXml(values: unknown[], rowNumber: number): string {
  return `    <row r="${rowNumber}">${values
    .map((value, index) => cellXml(value, `${columnName(index + 1)}${rowNumber}`))
    .join("")}</row>`;
}

function cellXml(value: unknown, ref: string): string {
  if (value === null || value === undefined) return `<c r="${ref}"/>`;
  if (value instanceof Date) return `<c r="${ref}" t="inlineStr"><is><t>${escapeXml(value.toISOString())}</t></is></c>`;
  if (typeof value === "number") return `<c r="${ref}"><v>${value}</v></c>`;
  if (typeof value === "boolean") return `<c r="${ref}" t="b"><v>${value ? 1 : 0}</v></c>`;
  return `<c r="${ref}" t="inlineStr"><is><t>${escapeXml(String(value))}</t></is></c>`;
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

function contentTypesXml(): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
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

function workbookRelsXml(): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`;
}

function workbookXml(sheetName: string): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="${escapeXml(sheetName)}" sheetId="1" r:id="rId1"/></sheets>
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

function appXml(sheetName: string): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties">
  <Application>Sheetra</Application><TitlesOfParts><vt:vector xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes" size="1" baseType="lpstr"><vt:lpstr>${escapeXml(sheetName)}</vt:lpstr></vt:vector></TitlesOfParts>
</Properties>`;
}

function coreXml(): string {
  return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/">
  <dc:creator>Sheetra</dc:creator><dcterms:created>${new Date().toISOString()}</dcterms:created>
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
  is?: {
    t?: string | { text: string };
  };
}
