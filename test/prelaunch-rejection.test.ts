import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  type ExtensionContext,
  SessionManager,
} from '@earendil-works/pi-coding-agent';
import { onTestFinished, test, vi } from 'vitest';
import { registerSubagentRpcBridge } from '../node_modules/pi-subagents/src/extension/rpc.js';
import { OperationJournal } from '../src/operation-journal.js';
import { registerPlanExecRpc } from '../src/plan-exec-rpc.js';

class Bus {
  private emitter = new EventEmitter();
  on(event: string, handler: (data: unknown) => void): () => void {
    this.emitter.on(event, handler);
    return () => this.emitter.off(event, handler);
  }
  emit(event: string, data: unknown): void {
    this.emitter.emit(event, data);
  }
}
function record(value: unknown): Record<string, unknown> {
  assert.ok(
    typeof value === 'object' && value !== null && !Array.isArray(value),
  );
  return value as Record<string, unknown>;
}
function call(
  bus: Bus,
  method: string,
  fields: object,
): Promise<Record<string, unknown>> {
  const requestId = randomUUID();
  return new Promise((resolve) => {
    const off = bus.on(`plan-exec:bridge:v2:reply:${requestId}`, (raw) => {
      off();
      resolve(record(raw));
    });
    bus.emit('plan-exec:bridge:v2:request', {
      version: 2,
      requestId,
      method,
      ...fields,
    });
  });
}
function fixture(lifetime: boolean, real = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-rejection-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new Bus();
  const params = {
    agent: 'worker',
    ...(lifetime ? { executionLifetime: { mode: 'unbounded' } } : {}),
    task: 'Never execute',
    workflowScriptPath: './removed.js',
  };
  const operationId = randomUUID();
  const requestDigest =
    'sha256:' +
    createHash('sha256').update(JSON.stringify({ params })).digest('hex');
  const owner = {
    kind: 'pi-plan-exec',
    runId: 'owner',
    key: operationId,
    requestDigest,
  };
  const fields = { operationId, owner, params };
  let spawnCount = 0;
  let executeCount = 0;
  bus.on('subagents:rpc:v1:request', (raw) => {
    if (record(raw).method === 'spawn') spawnCount++;
  });
  if (real) {
    const upstream = registerSubagentRpcBridge({
      events: bus,
      getContext: () =>
        ({
          cwd: root,
          sessionManager: SessionManager.inMemory(root),
        }) as unknown as ExtensionContext,
      execute: async () => {
        executeCount++;
        throw new Error('must not execute');
      },
    });
    onTestFinished(() => upstream.dispose());
  }
  const journalPath = path.join(root, 'operations.sqlite');
  const journal = new OperationJournal(journalPath);
  const bridge = registerPlanExecRpc(bus, { timeoutMs: 30, journal });
  onTestFinished(() => bridge.dispose());
  return {
    bus,
    fields,
    bridge,
    journalPath,
    journal,
    counts: () => ({ spawnCount, executeCount }),
  };
}
for (const lifetime of [false, true]) {
  test(`correlated prelaunch rejection is durable and permanently fenced (lifetime ${lifetime})`, async () => {
    const f = fixture(lifetime);
    const rejected = await call(f.bus, 'spawn', f.fields);
    assert.equal(rejected.success, false);
    assert.equal(record(rejected.error).upstreamCode, 'invalid_params');
    for (const bus of [f.bus, new Bus()]) {
      if (bus !== f.bus) {
        f.bridge.dispose();
        const restarted = registerPlanExecRpc(bus, {
          timeoutMs: 30,
          journalPath: f.journalPath,
        });
        onTestFinished(() => restarted.dispose());
        bus.on('subagents:rpc:v1:request', () =>
          assert.fail('recovery must not need another upstream call'),
        );
      }
      const lookup = record((await call(bus, 'operation', f.fields)).data);
      assert.equal(lookup.state, 'not_started');
      assert.equal(lookup.neverStarted, true);
      assert.equal(lookup.replaySafe, false);
      const proof = record(lookup.launchRejection);
      assert.equal(proof.operationId, f.fields.operationId);
      assert.equal(proof.requestDigest, f.fields.owner.requestDigest);
      assert.equal(proof.ownerRunId, 'owner');
      assert.equal(proof.method, 'spawn');
      assert.equal(proof.code, 'invalid_params');
      assert.equal(typeof proof.requestId, 'string');
      for (const owner of [
        { ...f.fields.owner, runId: 'other' },
        { ...f.fields.owner, requestDigest: 'sha256:other' },
      ])
        assert.equal(
          (await call(bus, 'cancelOperation', { ...f.fields, owner })).success,
          false,
        );
      const cancelled = record(
        (await call(bus, 'cancelOperation', f.fields)).data,
      );
      assert.equal(cancelled.state, 'cancelled');
      assert.equal(cancelled.neverStarted, true);
      assert.equal(cancelled.cancellationRequested, true);
      const replays = await Promise.all([
        call(bus, 'spawn', f.fields),
        call(bus, 'spawn', f.fields),
      ]);
      assert.ok(replays.every((reply) => reply.success === false));
    }
    assert.deepEqual(f.counts(), { spawnCount: 1, executeCount: 0 });
  });
}

test('failed evidence persistence stays unknown across restart and replay', async () => {
  const f = fixture(true);
  vi.spyOn(f.journal, 'recordLaunchRejection').mockImplementation(() => {
    throw new Error('disk full');
  });
  const rejected = await call(f.bus, 'spawn', f.fields);
  assert.match(
    String(record(rejected.error).message),
    /journal could not be updated/,
  );
  f.bridge.dispose();
  const bus = new Bus();
  const restarted = registerPlanExecRpc(bus, {
    timeoutMs: 30,
    journalPath: f.journalPath,
  });
  onTestFinished(() => restarted.dispose());
  bus.on('subagents:rpc:v1:request', () => assert.fail('must not dispatch'));
  const lookup = record((await call(bus, 'operation', f.fields)).data);
  assert.equal(lookup.state, 'unknown');
  assert.equal(lookup.launchRejection, undefined);
  assert.equal((await call(bus, 'spawn', f.fields)).success, false);
  assert.equal(f.counts().spawnCount, 1);
});

test('cancel and duplicate spawn racing a rejection preserve one dispatch', async () => {
  const f = fixture(true, false);
  let replySpawn: (() => void) | undefined;
  f.bus.on('subagents:rpc:v1:request', (raw) => {
    const req = record(raw);
    const reply = (data: object) =>
      f.bus.emit(`subagents:rpc:v1:reply:${req.requestId}`, {
        ...req,
        ...data,
      });
    if (req.method === 'ping')
      reply({
        success: true,
        data: { capabilities: { asyncSpawn: true, stop: true } },
      });
    else if (req.method === 'spawn')
      replySpawn = () =>
        reply({
          success: false,
          error: { code: 'invalid_params', message: 'rejected' },
        });
  });
  const spawned = call(f.bus, 'spawn', f.fields);
  // Drain only the capability handshake, leaving the actual spawn reply pending.
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  const replay = call(f.bus, 'spawn', f.fields);
  const cancelled = record(
    (await call(f.bus, 'cancelOperation', f.fields)).data,
  );
  assert.equal(cancelled.neverStarted, false);
  assert.ok(replySpawn);
  replySpawn();
  await Promise.all([spawned, replay]);
  const fenced = record((await call(f.bus, 'cancelOperation', f.fields)).data);
  assert.equal(fenced.neverStarted, true);
  assert.equal(fenced.cancellationRequested, true);
  assert.equal((await call(f.bus, 'spawn', f.fields)).success, false);
  assert.equal(f.counts().spawnCount, 1);
});

test('bound child cannot be overwritten with rejection evidence', async () => {
  const f = fixture(true);
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  f.journal.bind(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'running-child',
  );
  assert.throws(
    () =>
      f.journal.recordLaunchRejection({
        version: 1,
        source: 'subagents-rpc',
        requestId: 'unrelated',
        method: 'spawn',
        code: 'invalid_params',
        message: 'rejected',
        operationId: f.fields.operationId,
        requestDigest: f.fields.owner.requestDigest,
        ownerRunId: 'owner',
      }),
    /unbound dispatch/,
  );
  const lookup = record((await call(f.bus, 'operation', f.fields)).data);
  assert.equal(lookup.state, 'found');
  assert.equal(lookup.runId, 'running-child');
  assert.equal((await call(f.bus, 'spawn', f.fields)).success, true);
  assert.equal(f.counts().spawnCount, 0);
});

for (const variant of [
  'lost',
  'execution_failed',
  'missing-method',
  'wrong-method',
  'missing-message',
  'wrong-id',
  'string-error',
]) {
  test(`uncertain ${variant} rejection cannot prove no launch`, async () => {
    const f = fixture(true, false);
    f.bus.on('subagents:rpc:v1:request', (raw) => {
      const req = record(raw);
      if (req.method === 'ping') {
        f.bus.emit(`subagents:rpc:v1:reply:${req.requestId}`, {
          ...req,
          success: true,
          data: { capabilities: { asyncSpawn: true, stop: true } },
        });
      } else if (variant !== 'lost') {
        f.bus.emit(`subagents:rpc:v1:reply:${req.requestId}`, {
          version: 1,
          requestId: variant === 'wrong-id' ? 'wrong' : req.requestId,
          ...(variant === 'missing-method'
            ? {}
            : { method: variant === 'wrong-method' ? 'status' : 'spawn' }),
          success: false,
          error:
            variant === 'string-error'
              ? 'invalid_params'
              : {
                  code:
                    variant === 'execution_failed' ? variant : 'invalid_params',
                  ...(variant === 'missing-message'
                    ? {}
                    : { message: 'failed' }),
                },
        });
      }
    });
    await call(f.bus, 'spawn', f.fields);
    const lookup = record((await call(f.bus, 'operation', f.fields)).data);
    assert.equal(lookup.state, 'unknown');
    assert.equal(lookup.neverStarted, undefined);
    assert.equal(lookup.launchRejection, undefined);
    await call(f.bus, 'spawn', f.fields);
    assert.equal(f.counts().spawnCount, 1);
  });
}
