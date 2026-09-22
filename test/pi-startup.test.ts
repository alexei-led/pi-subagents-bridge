import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { onTestFinished, test } from 'vitest';

function seedV5Journal(journalPath: string): void {
  fs.mkdirSync(path.dirname(journalPath), { recursive: true });
  const db = new DatabaseSync(journalPath);
  db.exec(`
    CREATE TABLE operations (
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
    INSERT INTO operations
      (operation_id, request_digest, owner_run_id, binding, run_id, created_at, updated_at)
    VALUES ('startup-record', 'sha256:startup', 'owner-run', 'bound', 'native-run', 1, 1);
    ALTER TABLE operations ADD COLUMN execution_lifetime TEXT;
    ALTER TABLE operations ADD COLUMN native_correlated INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE operations ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE operations ADD COLUMN native_params TEXT;
    UPDATE operations SET
      execution_lifetime = '{"mode":"unbounded"}',
      native_correlated = 1,
      cancel_requested = 1,
      native_params = '{"task":"preserved"}';
    PRAGMA user_version = 5;
  `);
  db.close();
}

test('Pi loads the bridge extension with a v5 journal', {
  timeout: 20_000,
}, () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-startup-'));
  const home = path.join(root, 'home');
  const agentDir = path.join(root, 'agent');
  const cwd = path.join(root, 'cwd');
  const journalPath = path.join(
    home,
    '.pi',
    'pi-subagents-bridge',
    'plan-exec-operations.sqlite',
  );
  fs.mkdirSync(home, { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  fs.mkdirSync(cwd, { recursive: true });
  seedV5Journal(journalPath);
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));

  const result = spawnSync(
    process.execPath,
    [
      path.resolve('node_modules/@earendil-works/pi-coding-agent/dist/cli.js'),
      '--mode',
      'rpc',
      '--extension',
      path.resolve('src/index.ts'),
      '--no-extensions',
      '--no-skills',
      '--no-prompt-templates',
      '--no-themes',
      '--no-context-files',
      '--no-session',
    ],
    {
      cwd,
      encoding: 'utf8',
      env: {
        ...process.env,
        HOME: home,
        PI_CODING_AGENT_DIR: agentDir,
        PI_OFFLINE: '1',
      },
      input: '{"id":"startup-state","type":"get_state"}\n',
      timeout: 15_000,
    },
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.signal, null);
  assert.doesNotMatch(
    result.stderr,
    /Failed to load extension|Unsupported operation journal version/,
  );
  const response = result.stdout
    .split('\n')
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .find((line) => line.id === 'startup-state');
  assert.ok(response);
  assert.equal(response.type, 'response');
  assert.equal(response.command, 'get_state');
  assert.equal(response.success, true);
});
