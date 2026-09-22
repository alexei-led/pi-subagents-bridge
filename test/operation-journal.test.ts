import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { pathToFileURL } from 'node:url';
import { onTestFinished, test } from 'vitest';
import { OperationJournal } from '../src/operation-journal.js';

for (const version of [4, 5]) {
  test(`operation journal preserves v${version} records and newer fields`, () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-newer-'));
    const journalPath = path.join(root, 'operations.sqlite');
    onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
    const original = new OperationJournal(journalPath);
    original.begin('existing', 'sha256:existing', 'owner');
    original.beginLegacySpawn('legacy', 'sha256:legacy', 'session-a');
    original.bindLegacySpawn(
      'legacy',
      'sha256:legacy',
      'session-a',
      'legacy-run',
    );
    original.acceptRun(
      'accepted-run',
      { pid: process.pid, instanceId: 'instance-a' },
      'session-a',
    );
    const db = new DatabaseSync(journalPath);
    onTestFinished(() => db.close());
    db.exec(`
      UPDATE operations SET execution_lifetime = '{"mode":"unbounded"}',
        native_correlated = 1, cancel_requested = 1;
      PRAGMA user_version = ${version};
    `);
    if (version === 4) {
      // Schema 4 predates native_params; reopening migrates it to the current schema.
      db.exec('ALTER TABLE operations DROP COLUMN native_params;');
    } else {
      db.exec('UPDATE operations SET native_params = \'{"task":"preserved"}\'');
    }

    const journal = new OperationJournal(journalPath);
    assert.equal(journal.get('existing')?.ownerRunId, 'owner');
    journal.bind('existing', 'sha256:existing', 'native-run');
    journal.begin('new', 'sha256:new');
    assert.equal(journal.bind('new', 'sha256:new', 'new-run').runId, 'new-run');
    assert.equal(journal.getLegacySpawn('legacy')?.runId, 'legacy-run');
    assert.equal(
      journal.ownsAcceptedRun('accepted-run', 'instance-a', 'session-a'),
      true,
    );
    assert.equal(db.prepare('PRAGMA user_version').get()?.user_version, 5);
    const row = db
      .prepare("SELECT * FROM operations WHERE operation_id = 'existing'")
      .get();
    assert.ok(row);
    assert.equal(row.execution_lifetime, '{"mode":"unbounded"}');
    assert.equal(row.native_correlated, 1);
    assert.equal(row.cancel_requested, 1);
    assert.equal(
      row.native_params,
      version === 5 ? '{"task":"preserved"}' : null,
    );
  });
}

test('operation journal rejects unknown versions without modifying the database', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-unknown-'));
  const journalPath = path.join(root, 'operations.sqlite');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const db = new DatabaseSync(journalPath);
  db.exec(
    "CREATE TABLE future_data (value TEXT); INSERT INTO future_data VALUES ('preserved'); PRAGMA user_version = 6;",
  );
  db.close();
  const before = fs.readFileSync(journalPath);

  assert.throws(
    () => new OperationJournal(journalPath),
    /Unsupported operation journal version '6'.*Update.*extension/,
  );
  assert.deepEqual(fs.readFileSync(journalPath), before);
  assert.equal(fs.existsSync(`${journalPath}-wal`), false);
});

test('operation journal migrates v1 accepted runs without guessing a session', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-migrate-'));
  const journalPath = path.join(root, 'operations.sqlite');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const db = new DatabaseSync(journalPath);
  db.exec(`
    CREATE TABLE operations (
      operation_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL,
      owner_run_id TEXT, binding TEXT NOT NULL, run_id TEXT, async_dir TEXT,
      error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    ) STRICT;
    CREATE TABLE accepted_runs (
      run_id TEXT PRIMARY KEY, async_dir TEXT, accepted_at INTEGER NOT NULL,
      owner_pid INTEGER NOT NULL, owner_instance_id TEXT NOT NULL
    ) STRICT;
    INSERT INTO accepted_runs VALUES ('old-run', NULL, 1, 99999, 'old-owner');
    PRAGMA user_version = 1;
  `);
  db.close();

  const journal = new OperationJournal(journalPath);
  assert.equal(
    journal.claimAcceptedRuns(
      { pid: process.pid, instanceId: 'new-owner' },
      'session-a',
      1,
    ).length,
    0,
  );
  assert.equal(
    journal.renewAcceptedRun('old-run', 'new-owner', 'session-a'),
    false,
  );
});

test('operation journal recovers after a process exits inside a transaction', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-sqlite-'));
  const journalPath = path.join(root, 'operations.sqlite');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));

  const journal = new OperationJournal(journalPath);
  journal.begin('operation-1', 'sha256:first', 'plan-run-1');

  const child = spawnSync(
    process.execPath,
    [
      '--input-type=module',
      '--eval',
      `
        import { DatabaseSync } from "node:sqlite";
        const db = new DatabaseSync(${JSON.stringify(journalPath)});
        db.exec("PRAGMA busy_timeout = 2000; BEGIN IMMEDIATE");
        db.prepare("UPDATE operations SET updated_at = updated_at + 1 WHERE operation_id = ?").run("operation-1");
        process.exit(0);
      `,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(child.status, 0, child.stderr);

  const bound = journal.bind('operation-1', 'sha256:first', 'native-run-1');
  assert.equal(bound.operationId, 'operation-1');
  assert.equal(bound.requestDigest, 'sha256:first');
  assert.equal(bound.ownerRunId, 'plan-run-1');
  assert.equal(bound.binding, 'bound');
  assert.equal(bound.runId, 'native-run-1');
  assert.ok(Number.isFinite(bound.createdAt));
  assert.ok(Number.isFinite(bound.updatedAt));
});

test('legacy request IDs cannot collide with plan operation IDs', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-namespaces-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(path.join(root, 'operations.sqlite'));

  journal.begin('shared-id', 'sha256:plan', 'plan-run');
  journal.beginLegacySpawn('shared-id', 'sha256:legacy', 'session-a');
  journal.bind('shared-id', 'sha256:plan', 'plan-native-run');
  journal.bindLegacySpawn(
    'shared-id',
    'sha256:legacy',
    'session-a',
    'legacy-native-run',
  );

  assert.equal(journal.get('shared-id')?.runId, 'plan-native-run');
  assert.equal(journal.getLegacySpawn('shared-id')?.runId, 'legacy-native-run');
});

test('accepted-run ownership is session-scoped and rejects foreign conflicts', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-sessions-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(path.join(root, 'operations.sqlite'));
  const owner = { pid: process.pid, instanceId: 'owner-a' };

  assert.equal(journal.acceptRun('run-1', owner, 'session-a'), true);
  assert.equal(
    journal.acceptRun(
      'run-1',
      { pid: process.pid, instanceId: 'owner-b' },
      'session-a',
    ),
    false,
  );
  assert.equal(journal.acceptRun('run-1', owner, 'session-b'), false);
  assert.deepEqual(
    journal.claimAcceptedRuns(owner, 'session-b', 1).map((run) => run.runId),
    [],
  );
  assert.equal(
    journal.ownsAcceptedRun('run-1', owner.instanceId, 'session-a'),
    true,
  );
  assert.equal(
    journal.ownsAcceptedRun('run-1', owner.instanceId, 'session-b'),
    false,
  );
});

test('an expired heartbeat does not prove a live owner has exited', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-leases-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  let now = 1_000;
  const journal = new OperationJournal(
    path.join(root, 'operations.sqlite'),
    () => now,
  );
  const oldOwner = { pid: process.pid, instanceId: 'old-owner' };
  const newOwner = { pid: process.pid, instanceId: 'new-owner' };
  journal.acceptRun('live-run', oldOwner, 'session-a');
  now = 2_001;
  assert.deepEqual(journal.claimAcceptedRuns(newOwner, 'session-a', 1_000), []);
  assert.equal(
    journal.ownsAcceptedRun('live-run', oldOwner.instanceId, 'session-a'),
    true,
  );
  assert.equal(
    journal.ownsAcceptedRun('live-run', newOwner.instanceId, 'session-a'),
    false,
  );
  journal.completeRun('live-run', newOwner.instanceId, 'session-a');
  assert.equal(
    journal.ownsAcceptedRun('live-run', oldOwner.instanceId, 'session-a'),
    true,
  );
});

test('accepted runs stay with one live process and transfer after owner exit', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-owners-'));
  const journalPath = path.join(root, 'operations.sqlite');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(journalPath);
  const parentOwner = { pid: process.pid, instanceId: 'parent-owner' };
  assert.equal(journal.acceptRun('parent-run', parentOwner, 'session-a'), true);

  const moduleUrl = pathToFileURL(
    path.resolve('src/operation-journal.ts'),
  ).href;
  const liveClaim = spawnSync(
    process.execPath,
    [
      '--import',
      'jiti/register',
      '--input-type=module',
      '--eval',
      `
        import journalModule from ${JSON.stringify(moduleUrl)};
        const journal = new journalModule.OperationJournal(${JSON.stringify(journalPath)});
        const claimed = journal.claimAcceptedRuns(
          { pid: process.pid, instanceId: "live-contender" },
          "session-a",
          30_000,
        );
        console.log(JSON.stringify(claimed.map((run) => run.runId)));
      `,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(liveClaim.status, 0, liveClaim.stderr);
  assert.deepEqual(JSON.parse(liveClaim.stdout.trim()), []);

  const deadOwner = spawnSync(
    process.execPath,
    [
      '--import',
      'jiti/register',
      '--input-type=module',
      '--eval',
      `
        import journalModule from ${JSON.stringify(moduleUrl)};
        const journal = new journalModule.OperationJournal(${JSON.stringify(journalPath)});
        journal.acceptRun(
          "dead-owner-run",
          { pid: process.pid, instanceId: "dead-owner" },
          "session-a",
        );
      `,
    ],
    { encoding: 'utf8' },
  );
  assert.equal(deadOwner.status, 0, deadOwner.stderr);

  assert.deepEqual(
    journal
      .claimAcceptedRuns(parentOwner, 'session-a', 30_000)
      .map((run) => run.runId)
      .sort(),
    ['dead-owner-run', 'parent-run'],
  );
});
