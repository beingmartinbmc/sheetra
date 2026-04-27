import { Worker } from "node:worker_threads";
import { availableParallelism } from "node:os";

export interface WorkerMapOptions {
  concurrency?: number;
}

export async function workerMap<T, U>(
  rows: Iterable<T> | AsyncIterable<T>,
  mapperSource: string,
  options: WorkerMapOptions = {},
): Promise<U[]> {
  const concurrency = Math.max(1, options.concurrency ?? Math.max(1, Math.min(4, availableParallelism())));
  const chunks = chunk(await collect(rows), concurrency);
  const results = await Promise.all(chunks.map((chunkRows) => runWorker<T, U>(chunkRows, mapperSource)));
  return results.flat();
}

function runWorker<T, U>(rows: T[], mapperSource: string): Promise<U[]> {
  const workerCode = `
    const { parentPort, workerData } = require("node:worker_threads");
    const mapper = eval("(" + workerData.mapperSource + ")");
    Promise.resolve(workerData.rows.map((row, index) => mapper(row, index)))
      .then((rows) => parentPort.postMessage({ rows }))
      .catch((error) => parentPort.postMessage({ error: error.message }));
  `;

  return new Promise((resolve, reject) => {
    const worker = new Worker(workerCode, {
      eval: true,
      workerData: { rows, mapperSource },
    });

    worker.once("message", (message: { rows?: U[]; error?: string }) => {
      if (message.error !== undefined) reject(new Error(message.error));
      else resolve(message.rows ?? []);
    });
    worker.once("error", reject);
  });
}

async function collect<T>(rows: Iterable<T> | AsyncIterable<T>): Promise<T[]> {
  const output: T[] = [];
  for await (const row of rows) output.push(row);
  return output;
}

function chunk<T>(rows: T[], count: number): T[][] {
  const size = Math.ceil(rows.length / count);
  if (size === 0) return [];
  const chunks: T[][] = [];
  for (let index = 0; index < rows.length; index += size) chunks.push(rows.slice(index, index + size));
  return chunks;
}

