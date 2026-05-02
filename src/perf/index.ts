import { performance } from "node:perf_hooks";
import type { ProcessStats } from "../types.js";

export function createStats(): ProcessStats {
  return {
    rowsProcessed: 0,
    rowsWritten: 0,
    errors: 0,
    warnings: 0,
    startedAt: performance.now(),
    sheets: [],
    peakRssBytes: process.memoryUsage().rss,
  };
}

export function observeMemory(stats: ProcessStats): void {
  const rss = process.memoryUsage().rss;
  stats.peakRssBytes = Math.max(stats.peakRssBytes ?? 0, rss);
}

export function finishStats(stats: ProcessStats): ProcessStats {
  observeMemory(stats);
  const endedAt = performance.now();
  stats.endedAt = endedAt;
  stats.durationMs = endedAt - stats.startedAt;
  return stats;
}

export function mergeStats(target: ProcessStats, source: ProcessStats): ProcessStats {
  target.rowsProcessed += source.rowsProcessed;
  target.rowsWritten += source.rowsWritten;
  target.errors += source.errors;
  target.warnings += source.warnings;
  target.peakRssBytes = Math.max(target.peakRssBytes ?? 0, source.peakRssBytes ?? 0);
  target.sheets = Array.from(new Set([...target.sheets, ...source.sheets]));
  target.startedAt = Math.min(target.startedAt, source.startedAt);
  if (source.endedAt !== undefined) target.endedAt = Math.max(target.endedAt ?? source.endedAt, source.endedAt);
  if (target.endedAt !== undefined) target.durationMs = target.endedAt - target.startedAt;
  return target;
}

export function formatBytes(bytes = 0): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)}MB`;
}
