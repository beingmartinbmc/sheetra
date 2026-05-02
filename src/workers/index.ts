import { Worker } from "node:worker_threads";
import { availableParallelism } from "node:os";

export interface WorkerMapOptions {
  concurrency?: number;
  exportName?: string;
}

export type WorkerMapper<T, U> = (row: T, index: number) => U | Promise<U>;
export type WorkerMapperModule = string | URL;

export async function workerMap<T, U>(
  rows: Iterable<T> | AsyncIterable<T>,
  mapper: WorkerMapper<T, U> | WorkerMapperModule,
  options: WorkerMapOptions = {},
): Promise<U[]> {
  const concurrency = Math.max(1, options.concurrency ?? availableParallelism());
  if (typeof mapper === "function") return localMap(rows, mapper, concurrency);

  const results: U[] = [];
  const inflight = new Set<Promise<void>>();
  let index = 0;

  for await (const row of rows) {
    const currentIndex = index;
    index += 1;
    const task = runWorker<T, U>(row, currentIndex, mapper, options.exportName ?? "default").then((value) => {
      results[currentIndex] = value;
    });
    inflight.add(task);
    task.finally(() => inflight.delete(task)).catch(() => undefined);
    if (inflight.size >= concurrency) await Promise.race(inflight);
  }

  await Promise.all(inflight);
  return results;
}

async function localMap<T, U>(rows: Iterable<T> | AsyncIterable<T>, mapper: WorkerMapper<T, U>, concurrency: number): Promise<U[]> {
  const results: U[] = [];
  const inflight = new Set<Promise<void>>();
  let index = 0;

  for await (const row of rows) {
    const currentIndex = index;
    index += 1;
    const task = Promise.resolve(mapper(row, currentIndex)).then((value) => {
      results[currentIndex] = value;
    });
    inflight.add(task);
    task.finally(() => inflight.delete(task)).catch(() => undefined);
    if (inflight.size >= concurrency) await Promise.race(inflight);
  }

  await Promise.all(inflight);
  return results;
}

function runWorker<T, U>(row: T, index: number, mapperModule: WorkerMapperModule, exportName: string): Promise<U> {
  return new Promise((resolve, reject) => {
    const worker = new Worker(workerRunnerUrl(), {
      workerData: {
        row,
        index,
        mapperModule: mapperModule instanceof URL ? mapperModule.href : mapperModule,
        exportName,
      },
    });

    worker.once("message", (message: { value?: U; error?: string }) => {
      if (message.error !== undefined) reject(new Error(message.error));
      else resolve(message.value as U);
    });
    worker.once("error", reject);
    worker.once("exit", (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

function workerRunnerUrl(): URL {
  const source = `
    import { parentPort, workerData } from "node:worker_threads";

    try {
      const mod = await import(workerData.mapperModule);
      const mapper = mod[workerData.exportName];
      if (typeof mapper !== "function") throw new Error("Worker mapper export not found: " + workerData.exportName);
      const value = await mapper(workerData.row, workerData.index);
      parentPort?.postMessage({ value });
    } catch (error) {
      parentPort?.postMessage({ error: error instanceof Error ? error.message : String(error) });
    }
  `;
  return new URL(`data:text/javascript,${encodeURIComponent(source)}`);
}
