import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test } from 'vitest';

test('lost-reply fixture refuses nonempty roots before altering any config', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-fixture-safety-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  fs.mkdirSync(path.join(root, 'agent'));
  const settings = path.join(root, 'agent/settings.json');
  fs.writeFileSync(settings, 'original configuration');
  const run = spawnSync(
    process.execPath,
    [path.resolve('test/fixtures/lost-reply-server.mjs'), root],
    { encoding: 'utf8', timeout: 1000 },
  );
  assert.equal(fs.readFileSync(settings, 'utf8'), 'original configuration');
  assert.notEqual(run.status, 0);
  assert.match(run.stderr, /empty/);
});

test('lost-reply fixture refuses a symlink root', () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), 'bridge-fixture-symlink-'),
  );
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const target = path.join(root, 'target');
  fs.mkdirSync(target, { mode: 0o700 });
  const link = path.join(root, 'link');
  fs.symlinkSync(target, link);
  const run = spawnSync(
    process.execPath,
    [path.resolve('test/fixtures/lost-reply-server.mjs'), link],
    { encoding: 'utf8', timeout: 1000 },
  );
  assert.notEqual(run.status, 0);
  assert.match(run.stderr, /symlink/);
  assert.deepEqual(fs.readdirSync(target), []);
});
