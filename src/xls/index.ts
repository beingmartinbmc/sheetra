import type { ReadOptions, Row } from "../types.js";

let sheetjsModule: typeof import("xlsx") | undefined;

async function loadSheetJs(): Promise<typeof import("xlsx")> {
  if (sheetjsModule) return sheetjsModule;
  try {
    const m = await import("xlsx");
    sheetjsModule = (m.default ?? m) as typeof import("xlsx");
    return sheetjsModule;
  } catch {
    throw new Error(
      'Reading .xls files requires the "xlsx" package. Install it with: npm install xlsx',
    );
  }
}

export async function* readXls(source: string | Buffer, options: ReadOptions = {}): AsyncIterable<Row> {
  const xlsx = await loadSheetJs();

  const workbook =
    typeof source === "string"
      ? xlsx.readFile(source, { dense: true })
      : xlsx.read(source, { dense: true, type: "buffer" });

  const sheetName =
    typeof options.sheet === "string"
      ? options.sheet
      : workbook.SheetNames[typeof options.sheet === "number" ? options.sheet : 0];

  if (!sheetName || !workbook.Sheets[sheetName]) {
    throw new Error(`Worksheet not found: ${String(options.sheet ?? 0)}`);
  }

  const rows: Row[] = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName], {
    defval: null,
  });

  for (const row of rows) {
    yield row;
  }
}
