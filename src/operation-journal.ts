import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const JOURNAL_VERSION = 3;
const BUSY_TIMEOUT_MS = 2_000;

export type OperationBinding = "dispatching" | "bound" | "unknown";

export interface AcceptedRunOwner {
  pid: number;
  instanceId: string;
}

export interface AcceptedRunJournalRecord {
  runId: string;
  asyncDir?: string;
  acceptedAt: number;
  sessionId: string;
  ownerPid: number;
  ownerInstanceId: string;
  ownerHeartbeatAt: number;
}

export interface LegacySpawnJournalRecord {
  requestId: string;
  requestDigest: string;
  sessionId: string;
  binding: OperationBinding;
  runId?: string;
  error?: string;
  createdAt: number;
  updatedAt: number;
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

interface LegacySpawnRow {
  request_id: string;
  request_digest: string;
  session_id: string;
  binding: OperationBinding;
  run_id: string | null;
  error: string | null;
  created_at: number;
  updated_at: number;
}

interface AcceptedRunRow {
  run_id: string;
  async_dir: string | null;
  accepted_at: number;
  session_id: string;
  owner_pid: number;
  owner_instance_id: string;
  owner_heartbeat_at: number;
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

function legacySpawnRecord(
  row: LegacySpawnRow,
): LegacySpawnJournalRecord {
  return {
    requestId: row.request_id,
    requestDigest: row.request_digest,
    sessionId: row.session_id,
    binding: row.binding,
    ...(row.run_id ? { runId: row.run_id } : {}),
    ...(row.error ? { error: row.error } : {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function isProcessAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: unknown) {
    return !(
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    );
  }
}

function acceptedRunRecord(row: AcceptedRunRow): AcceptedRunJournalRecord {
  return {
    runId: row.run_id,
    ...(row.async_dir ? { asyncDir: row.async_dir } : {}),
    acceptedAt: row.accepted_at,
    sessionId: row.session_id,
    ownerPid: row.owner_pid,
    ownerInstanceId: row.owner_instance_id,
    ownerHeartbeatAt: row.owner_heartbeat_at,
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
      CREATE TABLE IF NOT EXISTS legacy_spawns (
        request_id TEXT PRIMARY KEY,
        request_digest TEXT NOT NULL,
        session_id TEXT NOT NULL,
        binding TEXT NOT NULL CHECK (binding IN ('dispatching', 'bound', 'unknown')),
        run_id TEXT,
        error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        CHECK (binding != 'bound' OR run_id IS NOT NULL),
        CHECK (binding != 'unknown' OR error IS NOT NULL)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS accepted_runs (
        run_id TEXT PRIMARY KEY,
        async_dir TEXT,
        accepted_at INTEGER NOT NULL,
        session_id TEXT NOT NULL,
        owner_pid INTEGER NOT NULL,
        owner_instance_id TEXT NOT NULL,
        owner_heartbeat_at INTEGER NOT NULL
      ) STRICT;
    `);
    const version = this.#db.prepare("PRAGMA user_version").get() as
      | { user_version?: unknown }
      | undefined;
    if (version?.user_version === 0) {
      this.#db.exec(`PRAGMA user_version = ${JOURNAL_VERSION}`);
    } else if (
      version?.user_version === 1 ||
      version?.user_version === 2
    ) {
      this.#transaction(() => {
        if (version.user_version === 1) {
          this.#db.exec(`
            ALTER TABLE accepted_runs
              ADD COLUMN session_id TEXT NOT NULL DEFAULT '';
            ALTER TABLE accepted_runs
              ADD COLUMN owner_heartbeat_at INTEGER NOT NULL DEFAULT 0;
          `);
        }
        this.#db.exec(`PRAGMA user_version = ${JOURNAL_VERSION}`);
      });
    } else if (version?.user_version !== JOURNAL_VERSION) {
      this.#db.close();
      throw new Error(
        `Unsupported operation journal version '${String(version?.user_version)}'`,
      );
    }
    fs.chmodSync(this.filePath, 0o600);
  }

  claimAcceptedRuns(
    owner: AcceptedRunOwner,
    sessionId: string,
    leaseMs: number,
  ): AcceptedRunJournalRecord[] {
    return this.#transaction(() => {
      const now = this.#now();
      const staleBefore = now - leaseMs;
      const rows = this.#db
        .prepare(
          `SELECT run_id, async_dir, accepted_at, session_id, owner_pid,
                  owner_instance_id, owner_heartbeat_at
             FROM accepted_runs
            WHERE session_id = ?
            ORDER BY accepted_at, run_id`,
        )
        .all(sessionId) as unknown as AcceptedRunRow[];
      const claimed: AcceptedRunJournalRecord[] = [];
      for (const row of rows) {
        if (row.owner_instance_id === owner.instanceId) {
          this.#db
            .prepare(
              `UPDATE accepted_runs SET owner_heartbeat_at = ?
                WHERE run_id = ? AND session_id = ? AND owner_instance_id = ?`,
            )
            .run(now, row.run_id, sessionId, owner.instanceId);
          claimed.push(
            acceptedRunRecord({ ...row, owner_heartbeat_at: now }),
          );
          continue;
        }
        if (
          isProcessAlive(row.owner_pid) &&
          row.owner_heartbeat_at >= staleBefore
        ) {
          continue;
        }
        const result = this.#db
          .prepare(
            `UPDATE accepted_runs
                SET owner_pid = ?, owner_instance_id = ?, owner_heartbeat_at = ?
              WHERE run_id = ? AND session_id = ? AND owner_pid = ?
                AND owner_instance_id = ? AND owner_heartbeat_at = ?`,
          )
          .run(
            owner.pid,
            owner.instanceId,
            now,
            row.run_id,
            sessionId,
            row.owner_pid,
            row.owner_instance_id,
            row.owner_heartbeat_at,
          );
        if (result.changes === 1) {
          claimed.push(
            acceptedRunRecord({
              ...row,
              owner_pid: owner.pid,
              owner_instance_id: owner.instanceId,
              owner_heartbeat_at: now,
            }),
          );
        }
      }
      return claimed;
    });
  }

  acceptRun(
    runId: string,
    owner: AcceptedRunOwner,
    sessionId: string,
    asyncDir?: string,
  ): boolean {
    return this.#transaction(() => {
      const now = this.#now();
      const inserted = this.#db
        .prepare(
          `INSERT OR IGNORE INTO accepted_runs
             (run_id, async_dir, accepted_at, session_id, owner_pid,
              owner_instance_id, owner_heartbeat_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          runId,
          asyncDir ?? null,
          now,
          sessionId,
          owner.pid,
          owner.instanceId,
          now,
        );
      if (inserted.changes === 1) return true;
      const existing = this.#db
        .prepare(
          `SELECT session_id, owner_instance_id FROM accepted_runs
            WHERE run_id = ?`,
        )
        .get(runId) as
        | { session_id: string; owner_instance_id: string }
        | undefined;
      return (
        existing?.session_id === sessionId &&
        existing.owner_instance_id === owner.instanceId
      );
    });
  }

  renewAcceptedRun(
    runId: string,
    ownerInstanceId: string,
    sessionId: string,
  ): boolean {
    return (
      this.#db
        .prepare(
          `UPDATE accepted_runs SET owner_heartbeat_at = ?
            WHERE run_id = ? AND session_id = ? AND owner_instance_id = ?`,
        )
        .run(this.#now(), runId, sessionId, ownerInstanceId).changes === 1
    );
  }

  ownsAcceptedRun(
    runId: string,
    ownerInstanceId: string,
    sessionId: string,
  ): boolean {
    return Boolean(
      this.#db
        .prepare(
          `SELECT 1 FROM accepted_runs
            WHERE run_id = ? AND session_id = ? AND owner_instance_id = ?`,
        )
        .get(runId, sessionId, ownerInstanceId),
    );
  }

  completeRun(
    runId: string,
    ownerInstanceId: string,
    sessionId: string,
  ): void {
    this.#db
      .prepare(
        `DELETE FROM accepted_runs
          WHERE run_id = ? AND session_id = ? AND owner_instance_id = ?`,
      )
      .run(runId, sessionId, ownerInstanceId);
  }

  getLegacySpawn(requestId: string): LegacySpawnJournalRecord | undefined {
    const row = this.#db
      .prepare(
        `SELECT request_id, request_digest, session_id, binding, run_id,
                error, created_at, updated_at
           FROM legacy_spawns
          WHERE request_id = ?`,
      )
      .get(requestId) as LegacySpawnRow | undefined;
    return row ? legacySpawnRecord(row) : undefined;
  }

  beginLegacySpawn(
    requestId: string,
    requestDigest: string,
    sessionId: string,
  ): { created: boolean; record: LegacySpawnJournalRecord } {
    return this.#transaction(() => {
      const existing = this.getLegacySpawn(requestId);
      if (existing) return { created: false, record: existing };

      const now = this.#now();
      this.#db
        .prepare(
          `INSERT INTO legacy_spawns
             (request_id, request_digest, session_id, binding, created_at, updated_at)
           VALUES (?, ?, ?, 'dispatching', ?, ?)`,
        )
        .run(requestId, requestDigest, sessionId, now, now);
      const record = this.getLegacySpawn(requestId);
      if (!record) {
        throw new Error(`Legacy spawn journal failed to create '${requestId}'`);
      }
      return { created: true, record };
    });
  }

  bindLegacySpawn(
    requestId: string,
    requestDigest: string,
    sessionId: string,
    runId: string,
  ): LegacySpawnJournalRecord {
    return this.#updateLegacySpawn(requestId, requestDigest, sessionId, {
      binding: "bound",
      runId,
    });
  }

  markLegacySpawnUnknown(
    requestId: string,
    requestDigest: string,
    sessionId: string,
    error: string,
  ): LegacySpawnJournalRecord {
    return this.#updateLegacySpawn(requestId, requestDigest, sessionId, {
      binding: "unknown",
      error,
    });
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

  #updateLegacySpawn(
    requestId: string,
    requestDigest: string,
    sessionId: string,
    update:
      | { binding: "bound"; runId: string }
      | { binding: "unknown"; error: string },
  ): LegacySpawnJournalRecord {
    return this.#transaction(() => {
      const current = this.getLegacySpawn(requestId);
      if (!current) {
        throw new Error(`Legacy spawn journal has no record for '${requestId}'`);
      }
      if (
        current.requestDigest !== requestDigest ||
        current.sessionId !== sessionId
      ) {
        throw new Error(
          `Legacy spawn '${requestId}' was already used by another request`,
        );
      }
      const updatedAt = this.#now();
      if (update.binding === "bound") {
        this.#db
          .prepare(
            `UPDATE legacy_spawns
                SET binding = 'bound', run_id = ?, error = NULL, updated_at = ?
              WHERE request_id = ?`,
          )
          .run(update.runId, updatedAt, requestId);
      } else {
        this.#db
          .prepare(
            `UPDATE legacy_spawns
                SET binding = 'unknown', error = ?, updated_at = ?
              WHERE request_id = ?`,
          )
          .run(update.error, updatedAt, requestId);
      }
      const record = this.getLegacySpawn(requestId);
      if (!record) {
        throw new Error(`Legacy spawn journal lost record for '${requestId}'`);
      }
      return record;
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
