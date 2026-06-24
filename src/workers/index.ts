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

  const moduleHref = mapper instanceof URL ? mapper.href : mapper;
  const pool = new WorkerPool<T, U>(moduleHref, options.exportName ?? "default", concurrency);

  try {
    const results: U[] = [];
    const inflight = new Set<Promise<void>>();
    let index = 0;

    for await (const row of rows) {
      const currentIndex = index;
      index += 1;
      const task = pool.run(row, currentIndex).then((value) => {
        results[currentIndex] = value;
      });
      inflight.add(task);
      task.finally(() => inflight.delete(task)).catch(() => undefined);
      if (inflight.size >= concurrency) await Promise.race(inflight);
    }

    await Promise.all(inflight);
    return results;
  } finally {
    await pool.destroy();
  }
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

interface PendingTask<U> {
  resolve(value: U): void;
  reject(error: Error): void;
}

/**
 * A fixed-size pool of long-lived workers. Each worker imports the mapper
 * module once and then handles many rows over a message channel, so we pay the
 * worker-spawn + module-import cost O(concurrency) times instead of O(rows).
 */
class WorkerPool<T, U> {
  private readonly idle: PooledWorker[] = [];
  private readonly all: PooledWorker[] = [];
  private readonly waiters: Array<(worker: PooledWorker) => void> = [];
  private taskId = 0;
  private destroyed = false;

  constructor(
    private readonly moduleHref: string,
    private readonly exportName: string,
    private readonly size: number,
  ) {}

  run(row: T, index: number): Promise<U> {
    return new Promise<U>((resolve, reject) => {
      this.acquire().then((worker) => {
        const id = (this.taskId += 1);
        worker.pending.set(id, { resolve: resolve as (v: unknown) => void, reject });
        worker.worker.postMessage({ id, row, index });
      }, reject);
    });
  }

  private acquire(): Promise<PooledWorker> {
    const ready = this.idle.pop();
    if (ready !== undefined) return Promise.resolve(ready);
    if (this.all.length < this.size) {
      const worker = this.spawn();
      this.all.push(worker);
      return Promise.resolve(worker);
    }
    return new Promise<PooledWorker>((resolve) => this.waiters.push(resolve));
  }

  private release(worker: PooledWorker): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) waiter(worker);
    else this.idle.push(worker);
  }

  private spawn(): PooledWorker {
    const worker = new Worker(workerRunnerUrl(), {
      workerData: { mapperModule: this.moduleHref, exportName: this.exportName },
    });
    const pooled: PooledWorker = { worker, pending: new Map() };

    worker.on("message", (message: { id: number; value?: unknown; error?: string }) => {
      const task = pooled.pending.get(message.id);
      if (task === undefined) return;
      pooled.pending.delete(message.id);
      if (message.error !== undefined) task.reject(new Error(message.error));
      else task.resolve(message.value);
      if (!this.destroyed) this.release(pooled);
    });
    worker.on("error", (error: unknown) => {
      const normalized = error instanceof Error ? error : new Error(String(error));
      for (const task of pooled.pending.values()) task.reject(normalized);
      pooled.pending.clear();
    });
    return pooled;
  }

  async destroy(): Promise<void> {
    this.destroyed = true;
    await Promise.all(this.all.map((pooled) => pooled.worker.terminate()));
  }
}

interface PooledWorker {
  worker: Worker;
  pending: Map<number, PendingTask<unknown>>;
}

function workerRunnerUrl(): URL {
  const source = `
    import { parentPort, workerData } from "node:worker_threads";

    const ready = (async () => {
      const mod = await import(workerData.mapperModule);
      const mapper = mod[workerData.exportName];
      if (typeof mapper !== "function") throw new Error("Worker mapper export not found: " + workerData.exportName);
      return mapper;
    })();

    parentPort?.on("message", async ({ id, row, index }) => {
      try {
        const mapper = await ready;
        const value = await mapper(row, index);
        parentPort?.postMessage({ id, value });
      } catch (error) {
        parentPort?.postMessage({ id, error: error instanceof Error ? error.message : String(error) });
      }
    });
  `;
  return new URL(`data:text/javascript,${encodeURIComponent(source)}`);
}
