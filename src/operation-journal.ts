import fs from "node:fs";
import { randomUUID } from "node:crypto";
import path from "node:path";

const JOURNAL_VERSION = 1;
const LOCK_WAIT_MS = 2_000;
const RETRY_DELAY_MS = 10;
const waitBuffer = new Int32Array(new SharedArrayBuffer(4));

export type OperationBinding = "dispatching" | "bound" | "unknown";

export interface AcceptedRunJournalRecord {
  runId: string;
  asyncDir?: string;
  acceptedAt: number;
}

export interface OperationJournalRecord {
  operationId: string;
  requestDigest: string;
  ownerRunId?: string;
  binding: OperationBinding;
  runId?: string;
  asyncDir?: string;
  error?: string;
  createdAt: number;
  updatedAt: number;
}

interface JournalDocument {
  version: typeof JOURNAL_VERSION;
  operations: OperationJournalRecord[];
  acceptedRuns: AcceptedRunJournalRecord[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isOperationBinding(value: unknown): value is OperationBinding {
  return value === "dispatching" || value === "bound" || value === "unknown";
}

function parseOperation(value: unknown): OperationJournalRecord | undefined {
  if (!isRecord(value)) return undefined;
  if (
    !isNonEmptyString(value.operationId) ||
    !isNonEmptyString(value.requestDigest) ||
    !isOperationBinding(value.binding) ||
    typeof value.createdAt !== "number" ||
    !Number.isFinite(value.createdAt) ||
    typeof value.updatedAt !== "number" ||
    !Number.isFinite(value.updatedAt)
  ) {
    return undefined;
  }
  if (value.ownerRunId !== undefined && !isNonEmptyString(value.ownerRunId)) return undefined;
  if (value.runId !== undefined && !isNonEmptyString(value.runId)) return undefined;
  if (value.asyncDir !== undefined && !isNonEmptyString(value.asyncDir)) return undefined;
  if (value.error !== undefined && !isNonEmptyString(value.error)) return undefined;
  if (value.binding === "bound" && !isNonEmptyString(value.runId)) return undefined;
  if (value.binding === "unknown" && !isNonEmptyString(value.error)) return undefined;

  const ownerRunId = isNonEmptyString(value.ownerRunId)
    ? value.ownerRunId
    : undefined;
  const runId = isNonEmptyString(value.runId) ? value.runId : undefined;
  const asyncDir = isNonEmptyString(value.asyncDir) ? value.asyncDir : undefined;
  const error = isNonEmptyString(value.error) ? value.error : undefined;
  return {
    operationId: value.operationId,
    requestDigest: value.requestDigest,
    ...(ownerRunId ? { ownerRunId } : {}),
    binding: value.binding,
    ...(runId ? { runId } : {}),
    ...(asyncDir ? { asyncDir } : {}),
    ...(error ? { error } : {}),
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  };
}

function clone(record: OperationJournalRecord): OperationJournalRecord {
  return { ...record };
}

function parseAcceptedRun(value: unknown): AcceptedRunJournalRecord | undefined {
  if (
    !isRecord(value) ||
    !isNonEmptyString(value.runId) ||
    typeof value.acceptedAt !== "number" ||
    !Number.isFinite(value.acceptedAt) ||
    (value.asyncDir !== undefined && !isNonEmptyString(value.asyncDir))
  ) {
    return undefined;
  }
  const asyncDir = isNonEmptyString(value.asyncDir) ? value.asyncDir : undefined;
  return {
    runId: value.runId,
    ...(asyncDir ? { asyncDir } : {}),
    acceptedAt: value.acceptedAt,
  };
}

export class OperationJournal {
  readonly filePath: string;
  readonly #lockPath: string;
  readonly #now: () => number;

  constructor(filePath: string, now: () => number = Date.now) {
    if (!filePath.trim()) throw new Error("Operation journal path cannot be empty");
    this.filePath = path.resolve(filePath);
    this.#lockPath = `${this.filePath}.lock`;
    this.#now = now;
  }

  listAcceptedRuns(): AcceptedRunJournalRecord[] {
    return this.#withLock((document) =>
      document.acceptedRuns.map((record) => ({ ...record })),
    );
  }

  acceptRun(runId: string, asyncDir?: string): void {
    this.#withLock((document, persist) => {
      if (document.acceptedRuns.some((record) => record.runId === runId)) return;
      document.acceptedRuns.push({
        runId,
        ...(asyncDir ? { asyncDir } : {}),
        acceptedAt: this.#now(),
      });
      persist();
    });
  }

  completeRun(runId: string): void {
    this.#withLock((document, persist) => {
      const next = document.acceptedRuns.filter((record) => record.runId !== runId);
      if (next.length === document.acceptedRuns.length) return;
      document.acceptedRuns = next;
      persist();
    });
  }

  get(operationId: string): OperationJournalRecord | undefined {
    return this.#withLock((document) => {
      const record = document.operations.find((item) => item.operationId === operationId);
      return record ? clone(record) : undefined;
    });
  }

  begin(operationId: string, requestDigest: string, ownerRunId?: string): {
    created: boolean;
    record: OperationJournalRecord;
  } {
    return this.#withLock((document, persist) => {
      const existing = document.operations.find((item) => item.operationId === operationId);
      if (existing) return { created: false, record: clone(existing) };

      const now = this.#now();
      const record: OperationJournalRecord = {
        operationId,
        requestDigest,
        ...(ownerRunId ? { ownerRunId } : {}),
        binding: "dispatching",
        createdAt: now,
        updatedAt: now,
      };
      document.operations.push(record);
      persist();
      return { created: true, record: clone(record) };
    });
  }

  bind(
    operationId: string,
    requestDigest: string,
    runId: string,
    asyncDir?: string,
  ): OperationJournalRecord {
    return this.#update(operationId, requestDigest, (record) => {
      const { error: _error, ...rest } = record;
      return {
        ...rest,
        binding: "bound",
        runId,
        ...(asyncDir ? { asyncDir } : {}),
        updatedAt: this.#now(),
      };
    });
  }

  markUnknown(
    operationId: string,
    requestDigest: string,
    error: string,
  ): OperationJournalRecord {
    return this.#update(operationId, requestDigest, (record) => ({
      ...record,
      binding: "unknown",
      error,
      updatedAt: this.#now(),
    }));
  }

  #update(
    operationId: string,
    requestDigest: string,
    update: (record: OperationJournalRecord) => OperationJournalRecord,
  ): OperationJournalRecord {
    return this.#withLock((document, persist) => {
      const index = document.operations.findIndex(
        (item) => item.operationId === operationId,
      );
      if (index < 0) {
        throw new Error(`Operation journal has no record for '${operationId}'`);
      }
      const current = document.operations[index];
      if (!current) {
        throw new Error(`Operation journal has no record for '${operationId}'`);
      }
      if (current.requestDigest !== requestDigest) {
        throw new Error(
          `Operation '${operationId}' was already used with a different request digest`,
        );
      }
      const next = update(current);
      document.operations[index] = next;
      persist();
      return clone(next);
    });
  }

  #withLock<T>(
    action: (document: JournalDocument, persist: () => void) => T,
  ): T {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true, mode: 0o700 });
    const deadline = this.#now() + LOCK_WAIT_MS;
    let locked = false;
    const lockToken = randomUUID();
    while (!locked) {
      try {
        fs.mkdirSync(this.#lockPath, { mode: 0o700 });
        try {
          fs.writeFileSync(
            path.join(this.#lockPath, "owner"),
            lockToken,
            { encoding: "utf8", flag: "wx", mode: 0o600 },
          );
        } catch (error: unknown) {
          fs.rmSync(this.#lockPath, { recursive: true, force: true });
          throw error;
        }
        locked = true;
      } catch (error: unknown) {
        if (!isRecord(error) || error.code !== "EEXIST") {
          throw new Error("Cannot acquire operation journal lock", {
            cause: error,
          });
        }
        if (this.#now() >= deadline) {
          throw new Error(
            `Timed out waiting for operation journal lock '${this.#lockPath}'`,
            { cause: error },
          );
        }
        Atomics.wait(waitBuffer, 0, 0, RETRY_DELAY_MS);
      }
    }

    try {
      const document = this.#read();
      let dirty: boolean | undefined;
      const result = action(document, () => {
        dirty = true;
      });
      if (dirty === true) this.#write(document);
      return result;
    } finally {
      try {
        const owner = fs.readFileSync(
          path.join(this.#lockPath, "owner"),
          "utf8",
        );
        if (owner === lockToken) {
          fs.rmSync(this.#lockPath, { recursive: true, force: true });
        }
      } catch {
        // Keep an unreadable lock in place. Another process must not race it.
      }
    }
  }

  #read(): JournalDocument {
    if (!fs.existsSync(this.filePath)) {
      return { version: JOURNAL_VERSION, operations: [], acceptedRuns: [] };
    }

    let raw: unknown;
    try {
      raw = JSON.parse(fs.readFileSync(this.filePath, "utf8"));
    } catch (error: unknown) {
      throw new Error(
        `Cannot read operation journal '${this.filePath}': ${error instanceof Error ? error.message : String(error)}`,
        { cause: error },
      );
    }
    if (
      !isRecord(raw) ||
      raw.version !== JOURNAL_VERSION ||
      !Array.isArray(raw.operations) ||
      (raw.acceptedRuns !== undefined && !Array.isArray(raw.acceptedRuns))
    ) {
      throw new Error(`Invalid operation journal '${this.filePath}'`);
    }

    const operations: OperationJournalRecord[] = [];
    const ids = new Set<string>();
    for (const value of raw.operations) {
      const operation = parseOperation(value);
      if (!operation || ids.has(operation.operationId)) {
        throw new Error(`Invalid operation journal '${this.filePath}'`);
      }
      ids.add(operation.operationId);
      operations.push(operation);
    }
    const acceptedRuns: AcceptedRunJournalRecord[] = [];
    const runIds = new Set<string>();
    for (const value of raw.acceptedRuns ?? []) {
      const run = parseAcceptedRun(value);
      if (!run || runIds.has(run.runId)) {
        throw new Error(`Invalid operation journal '${this.filePath}'`);
      }
      runIds.add(run.runId);
      acceptedRuns.push(run);
    }
    return { version: JOURNAL_VERSION, operations, acceptedRuns };
  }

  #write(document: JournalDocument): void {
    const temporaryPath = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
    try {
      fs.writeFileSync(temporaryPath, `${JSON.stringify(document, null, 2)}\n`, {
        encoding: "utf8",
        flag: "wx",
        mode: 0o600,
      });
      fs.renameSync(temporaryPath, this.filePath);
    } finally {
      fs.rmSync(temporaryPath, { force: true });
    }
  }
}
