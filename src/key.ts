export function rowIdentity(row: Record<string, unknown>, key: string | string[]): string {
  const keys = Array.isArray(key) ? key : [key];
  return valuesIdentity(keys.map((name) => row[name]));
}

export function valuesIdentity(values: unknown[]): string {
  return values.map(typedPart).join("\u0000");
}

function typedPart(value: unknown): string {
  if (value === null) return "null:";
  if (value === undefined) return "undefined:";
  if (value instanceof Date) return `date:${value.toISOString()}`;
  return `${typeof value}:${String(value)}`;
}
