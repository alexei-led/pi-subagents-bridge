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
import { onTestFinished, test } from 'vitest';
import { registerSubagentRpcBridge } from '../node_modules/pi-subagents/src/extension/rpc.js';
import { registerBridge } from '../src/index.js';
import { OperationJournal } from '../src/operation-journal.js';
import { registerPlanExecRpc } from '../src/plan-exec-rpc.js';

class Bus {
  private readonly emitter = new EventEmitter();
  on(event: string, handler: (data: unknown) => void): () => void {
    this.emitter.on(event, handler);
    return () => this.emitter.off(event, handler);
  }
  emit(event: string, data: unknown): void {
    this.emitter.emit(event, data);
  }
}

function request(
  bus: Bus,
  channel: string,
  replyPrefix: string,
  fields: object,
): Promise<unknown> {
  const requestId = randomUUID();
  return new Promise((resolve) => {
    const unsubscribe = bus.on(replyPrefix + requestId, (reply) => {
      unsubscribe();
      resolve(reply);
    });
    bus.emit(channel, { requestId, ...fields });
  });
}

function upstream(bus: Bus, params: object): Promise<unknown> {
  return request(bus, 'subagents:rpc:v1:request', 'subagents:rpc:v1:reply:', {
    version: 1,
    method: 'spawn',
    params,
  });
}

function fixture(executionFailure?: 'throw' | 'tool-error') {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-rpc-contract-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new Bus();
  const executed: Record<string, unknown>[] = [];
  // Only the executor boundary is stubbed; the released RPC validator runs unchanged.
  const ctx = {
    cwd: root,
    sessionManager: SessionManager.inMemory(root),
  } as unknown as ExtensionContext;
  const rpc = registerSubagentRpcBridge({
    events: bus,
    getContext: () => ctx,
    execute: async (_id, params) => {
      executed.push({ ...params });
      if (executionFailure === 'throw')
        throw Object.assign(new Error('post-dispatch invalid_params'), {
          code: 'invalid_params',
        });
      if (executionFailure === 'tool-error')
        return {
          isError: true,
          content: [{ type: 'text', text: 'invalid_params' }],
          details: { mode: 'workflow', results: [] },
        };
      return {
        content: [{ type: 'text', text: 'started' }],
        details: {
          mode: 'workflow',
          results: [],
          runId: 'native-run',
          asyncId: 'native-run',
        },
      };
    },
  });
  onTestFinished(() => rpc.dispose());
  return { bus, root, executed };
}

test('released RPC rejects the removed public field before the executor runs', async () => {
  const { bus, executed } = fixture();
  const reply = await upstream(bus, {
    workflowScript: 'return 1',
    async: true,
  });
  assert.ok(isRecord(reply) && isRecord(reply.error));
  assert.equal(reply.success, false);
  assert.equal(reply.error.code, 'invalid_params');
  assert.match(String(reply.error.message), /workflowScript was removed/);
  assert.equal(executed.length, 0);
});

for (const failure of ['throw', 'tool-error'] as const) {
  test(`released RPC classifies post-executor ${failure} as execution_failed, not invalid_params`, async () => {
    const { bus, executed } = fixture(failure);
    const reply = await upstream(bus, { script: 'return 1', async: true });
    assert.ok(isRecord(reply) && isRecord(reply.error));
    assert.equal(reply.error.code, 'execution_failed');
    assert.equal(executed.length, 1);
  });
}

test('released RPC schema validation rejects before executor invocation', async () => {
  const { bus, executed } = fixture();
  const reply = await upstream(bus, {
    script: 'return 1',
    async: true,
    timeoutMs: 'invalid',
  });
  assert.ok(isRecord(reply) && isRecord(reply.error));
  assert.equal(reply.error.code, 'invalid_params');
  assert.equal(executed.length, 0);
});

test('TaskExecute crosses the released RPC boundary with a one-child script', async () => {
  const { bus, executed } = fixture();
  const bridge = registerBridge({ events: bus });
  onTestFinished(() => bridge.dispose());
  const reply = await request(
    bus,
    'subagents:rpc:spawn',
    'subagents:rpc:spawn:reply:',
    {
      type: 'general-purpose',
      prompt: 'Return the fixture result',
      options: { maxTurns: 3 },
    },
  );
  assert.deepEqual(reply, { success: true, data: { id: 'native-run' } });
  assert.equal(executed.length, 1);
  assert.match(String(executed[0]?.workflowScript), /"agent":"delegate"/);
  assert.deepEqual(executed[0]?.turnBudget, { maxTurns: 3 });
});

for (const lifetime of [
  undefined,
  { mode: 'unbounded' },
  { mode: 'bounded', timeoutMs: 30_000 },
] as const) {
  test(`plan-exec crosses released RPC with ${lifetime?.mode ?? 'legacy'} lifetime and keeps its operation identity`, async () => {
    const { bus, root, executed } = fixture();
    const bridge = registerPlanExecRpc(bus, {
      timeoutMs: 1000,
      journalPath: path.join(root, 'operations.sqlite'),
    });
    onTestFinished(() => bridge.dispose());
    const params = {
      agent: 'worker',
      task: 'Return fixture result',
      ...(lifetime ? { executionLifetime: lifetime } : {}),
    };
    const operationId = randomUUID();
    const requestDigest = digest({ params });
    const owner = {
      kind: 'pi-plan-exec',
      runId: 'owner',
      key: operationId,
      requestDigest,
    };
    const call = (method: string) =>
      request(
        bus,
        'plan-exec:bridge:v2:request',
        'plan-exec:bridge:v2:reply:',
        { version: 2, method, operationId, owner, params },
      );
    const reply = await call('spawn');
    assert.ok(isRecord(reply) && isRecord(reply.data), JSON.stringify(reply));
    assert.equal(reply.success, true, JSON.stringify(reply));
    assert.equal(reply.data.requestDigest, requestDigest);
    if (lifetime)
      assert.deepEqual(reply.data.effectiveExecutionLifetime, lifetime);
    const lookup = await call('operation');
    assert.ok(isRecord(lookup) && isRecord(lookup.data));
    assert.equal(lookup.data.state, 'found');
    assert.equal(lookup.data.runId, 'native-run');
    await call('spawn');
    assert.equal(executed.length, 1);
    assert.match(String(executed[0]?.workflowScript), /"agent":"worker"/);
    assert.equal(
      executed[0]?.timeoutMs,
      lifetime?.mode === 'bounded' ? 30_000 : undefined,
    );
  });

  test(`legacy invalid_params remains unknown without unsafe replay after restart (${lifetime?.mode ?? 'legacy'})`, async () => {
    const { bus, root, executed } = fixture();
    const journalPath = path.join(root, 'operations.sqlite');
    const bridge = registerPlanExecRpc(bus, { timeoutMs: 1000, journalPath });
    onTestFinished(() => bridge.dispose());
    // A removed caller field reaches the real RPC validator, not a permissive mock.
    const params = {
      agent: 'worker',
      task: 'Never execute',
      workflowScriptPath: './removed.js',
      ...(lifetime ? { executionLifetime: lifetime } : {}),
    };
    const operationId = randomUUID();
    const owner = {
      kind: 'pi-plan-exec',
      runId: 'owner',
      key: operationId,
      requestDigest: digest({ params }),
    };
    let spawnRequests = 0;
    bus.on('subagents:rpc:v1:request', (raw) => {
      if (isRecord(raw) && raw.method === 'spawn') spawnRequests++;
    });
    const call = (target: Bus, method: string) =>
      request(
        target,
        'plan-exec:bridge:v2:request',
        'plan-exec:bridge:v2:reply:',
        { version: 2, method, operationId, owner, params },
      );
    // Historical journals kept only an error string, not a correlated rejection.
    const journal = new OperationJournal(journalPath);
    journal.begin(operationId, owner.requestDigest, owner.runId, lifetime);
    journal.markUnknown(
      operationId,
      owner.requestDigest,
      'RPC spawn workflowScriptPath was removed',
    );
    const reply = await call(bus, 'spawn');
    assert.ok(isRecord(reply) && isRecord(reply.error));
    assert.equal(reply.success, false);
    assert.match(String(reply.error.message), /workflowScriptPath was removed/);
    const cancelled = await call(bus, 'cancelOperation');
    assert.ok(isRecord(cancelled));
    if (lifetime) {
      assert.ok(isRecord(cancelled.data));
      assert.equal(cancelled.data.state, 'unknown');
      assert.equal(cancelled.data.neverStarted, false);
    } else {
      assert.equal(cancelled.success, false);
    }
    for (const target of [bus, new Bus()]) {
      if (target !== bus) {
        bridge.dispose();
        const restarted = registerPlanExecRpc(target, {
          timeoutMs: 1000,
          journalPath,
        });
        onTestFinished(() => restarted.dispose());
        target.on('subagents:rpc:v1:request', () =>
          assert.fail('must not redispatch'),
        );
      }
      const lookup = await call(target, 'operation');
      assert.ok(isRecord(lookup) && isRecord(lookup.data));
      assert.equal(lookup.data.state, 'unknown');
      assert.equal(lookup.data.processTerminalProof, undefined);
      assert.equal(lookup.data.workflowTerminalProof, undefined);
      const replay = await call(target, 'spawn');
      assert.ok(isRecord(replay));
      assert.equal(replay.success, false);
    }
    assert.equal(spawnRequests, 0);
    assert.equal(executed.length, 0);
  });
}

function digest(value: unknown): string {
  const canonical = (value: unknown): string => {
    if (isRecord(value))
      return (
        '{' +
        Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
          .join(',') +
        '}'
      );
    return JSON.stringify(value);
  };
  return `sha256:${createHash('sha256').update(canonical(value)).digest('hex')}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
