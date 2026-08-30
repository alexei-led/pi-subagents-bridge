import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const JOURNAL_VERSION = 1;
const BUSY_TIMEOUT_MS = 2_000;

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

interface OperationRow {
  operation_id: string;
  request_digest: string;
  owner_run_id: string | null;
  binding: OperationBinding;
  run_id: string | null;
  async_dir: string | null;
  error: string | null;
  created_at: number;
  updated_at: number;
}

interface AcceptedRunRow {
  run_id: string;
  async_dir: string | null;
  accepted_at: number;
}

function operationRecord(row: OperationRow): OperationJournalRecord {
  return {
    operationId: row.operation_id,
    requestDigest: row.request_digest,
    ...(row.owner_run_id ? { ownerRunId: row.owner_run_id } : {}),
    binding: row.binding,
    ...(row.run_id ? { runId: row.run_id } : {}),
    ...(row.async_dir ? { asyncDir: row.async_dir } : {}),
    ...(row.error ? { error: row.error } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function acceptedRunRecord(row: AcceptedRunRow): AcceptedRunJournalRecord {
  return {
    runId: row.run_id,
    ...(row.async_dir ? { asyncDir: row.async_dir } : {}),
    acceptedAt: row.accepted_at,
  };
}

export class OperationJournal {
  readonly filePath: string;
  readonly #db: DatabaseSync;
  readonly #now: () => number;

  constructor(filePath: string, now: () => number = Date.now) {
    if (!filePath.trim()) {
      throw new Error("Operation journal path cannot be empty");
    }
    this.filePath = path.resolve(filePath);
    this.#now = now;
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true, mode: 0o700 });
    this.#db = new DatabaseSync(this.filePath);
    this.#db.exec(`
      PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS};
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      CREATE TABLE IF NOT EXISTS operations (
        operation_id TEXT PRIMARY KEY,
        request_digest TEXT NOT NULL,
        owner_run_id TEXT,
        binding TEXT NOT NULL CHECK (binding IN ('dispatching', 'bound', 'unknown')),
        run_id TEXT,
        async_dir TEXT,
        error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        CHECK (binding != 'bound' OR run_id IS NOT NULL),
        CHECK (binding != 'unknown' OR error IS NOT NULL)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS accepted_runs (
        run_id TEXT PRIMARY KEY,
        async_dir TEXT,
        accepted_at INTEGER NOT NULL
      ) STRICT;
    `);
    const version = this.#db.prepare("PRAGMA user_version").get() as
      | { user_version?: unknown }
      | undefined;
    if (version?.user_version === 0) {
      this.#db.exec(`PRAGMA user_version = ${JOURNAL_VERSION}`);
    } else if (version?.user_version !== JOURNAL_VERSION) {
      this.#db.close();
      throw new Error(
        `Unsupported operation journal version '${String(version?.user_version)}'`,
      );
    }
    fs.chmodSync(this.filePath, 0o600);
  }

  listAcceptedRuns(): AcceptedRunJournalRecord[] {
    const rows = this.#db
      .prepare(
        "SELECT run_id, async_dir, accepted_at FROM accepted_runs ORDER BY accepted_at, run_id",
      )
      .all() as unknown as AcceptedRunRow[];
    return rows.map(acceptedRunRecord);
  }

  acceptRun(runId: string, asyncDir?: string): void {
    this.#db
      .prepare(
        "INSERT OR IGNORE INTO accepted_runs (run_id, async_dir, accepted_at) VALUES (?, ?, ?)",
      )
      .run(runId, asyncDir ?? null, this.#now());
  }

  completeRun(runId: string): void {
    this.#db.prepare("DELETE FROM accepted_runs WHERE run_id = ?").run(runId);
  }

  get(operationId: string): OperationJournalRecord | undefined {
    const row = this.#db
      .prepare(
        `SELECT operation_id, request_digest, owner_run_id, binding, run_id,
                async_dir, error, created_at, updated_at
           FROM operations
          WHERE operation_id = ?`,
      )
      .get(operationId) as OperationRow | undefined;
    return row ? operationRecord(row) : undefined;
  }

  begin(
    operationId: string,
    requestDigest: string,
    ownerRunId?: string,
  ): { created: boolean; record: OperationJournalRecord } {
    return this.#transaction(() => {
      const existing = this.get(operationId);
      if (existing) return { created: false, record: existing };

      const now = this.#now();
      this.#db
        .prepare(
          `INSERT INTO operations
             (operation_id, request_digest, owner_run_id, binding, created_at, updated_at)
           VALUES (?, ?, ?, 'dispatching', ?, ?)`,
        )
        .run(operationId, requestDigest, ownerRunId ?? null, now, now);
      const record = this.get(operationId);
      if (!record) {
        throw new Error(`Operation journal failed to create '${operationId}'`);
      }
      return { created: true, record };
    });
  }

  bind(
    operationId: string,
    requestDigest: string,
    runId: string,
    asyncDir?: string,
  ): OperationJournalRecord {
    return this.#update(operationId, requestDigest, {
      binding: "bound",
      runId,
      ...(asyncDir ? { asyncDir } : {}),
    });
  }

  markUnknown(
    operationId: string,
    requestDigest: string,
    error: string,
  ): OperationJournalRecord {
    return this.#update(operationId, requestDigest, {
      binding: "unknown",
      error,
    });
  }

  #update(
    operationId: string,
    requestDigest: string,
    update:
      | { binding: "bound"; runId: string; asyncDir?: string }
      | { binding: "unknown"; error: string },
  ): OperationJournalRecord {
    return this.#transaction(() => {
      const current = this.get(operationId);
      if (!current) {
        throw new Error(`Operation journal has no record for '${operationId}'`);
      }
      if (current.requestDigest !== requestDigest) {
        throw new Error(
          `Operation '${operationId}' was already used with a different request digest`,
        );
      }
      const updatedAt = this.#now();
      if (update.binding === "bound") {
        this.#db
          .prepare(
            `UPDATE operations
                SET binding = 'bound', run_id = ?, async_dir = ?, error = NULL, updated_at = ?
              WHERE operation_id = ?`,
          )
          .run(update.runId, update.asyncDir ?? null, updatedAt, operationId);
      } else {
        this.#db
          .prepare(
            `UPDATE operations
                SET binding = 'unknown', error = ?, updated_at = ?
              WHERE operation_id = ?`,
          )
          .run(update.error, updatedAt, operationId);
      }
      const record = this.get(operationId);
      if (!record) {
        throw new Error(`Operation journal lost record for '${operationId}'`);
      }
      return record;
    });
  }

  #transaction<T>(action: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      const result = action();
      this.#db.exec("COMMIT");
      return result;
    } catch (error: unknown) {
      try {
        this.#db.exec("ROLLBACK");
      } catch {
        // Preserve the operation error. SQLite will release locks on process exit.
      }
      throw error;
    }
  }
}
