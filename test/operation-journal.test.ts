import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { OperationJournal } from "../src/operation-journal.js";

test("operation journal recovers after a process exits inside a transaction", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-bridge-sqlite-"));
  const journalPath = path.join(root, "operations.sqlite");
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));

  const journal = new OperationJournal(journalPath);
  journal.begin("operation-1", "sha256:first", "plan-run-1");

  const child = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      `
        import { DatabaseSync } from "node:sqlite";
        const db = new DatabaseSync(${JSON.stringify(journalPath)});
        db.exec("PRAGMA busy_timeout = 2000; BEGIN IMMEDIATE");
        db.prepare("UPDATE operations SET updated_at = updated_at + 1 WHERE operation_id = ?").run("operation-1");
        process.exit(0);
      `,
    ],
    { encoding: "utf8" },
  );
  assert.equal(child.status, 0, child.stderr);

  const bound = journal.bind(
    "operation-1",
    "sha256:first",
    "native-run-1",
  );
  assert.equal(bound.operationId, "operation-1");
  assert.equal(bound.requestDigest, "sha256:first");
  assert.equal(bound.ownerRunId, "plan-run-1");
  assert.equal(bound.binding, "bound");
  assert.equal(bound.runId, "native-run-1");
  assert.ok(Number.isFinite(bound.createdAt));
  assert.ok(Number.isFinite(bound.updatedAt));
});
