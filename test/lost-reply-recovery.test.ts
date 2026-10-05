import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test, vi } from 'vitest';
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
function rec(value: unknown): Record<string, unknown> {
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
      resolve(rec(raw));
    });
    bus.emit('plan-exec:bridge:v2:request', {
      version: 2,
      requestId,
      method,
      ...fields,
    });
  });
}
function fixture(lifetime = true) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-lost-reply-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(path.join(root, 'operations.sqlite'));
  const bus = new Bus();
  const params = {
    agent: 'worker',
    ...(lifetime ? { executionLifetime: { mode: 'unbounded' as const } } : {}),
    task: 'one writer',
  };
  const operationId = randomUUID();
  const digest =
    'sha256:' +
    createHash('sha256').update(JSON.stringify({ params })).digest('hex');
  const fields = {
    operationId,
    params,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'owner',
      key: operationId,
      requestDigest: digest,
    },
  };
  let rpcRequestId = '';
  let spawns = 0;
  let persistedBeforeEmit = false;
  bus.on('subagents:rpc:v1:request', (raw) => {
    const req = rec(raw);
    if (req.method === 'ping')
      reply(bus, req, { capabilities: { asyncSpawn: true, stop: true } });
    if (req.method === 'spawn') {
      spawns++;
      rpcRequestId = String(req.requestId);
      persistedBeforeEmit =
        journal.get(operationId)?.nativeParams?.rpcRequestId === rpcRequestId;
    }
  });
  const bridge = registerPlanExecRpc(bus, { timeoutMs: 20, journal });
  onTestFinished(() => bridge.dispose());
  return {
    root,
    journal,
    bus,
    fields,
    bridge,
    counts: () => ({ spawns, persistedBeforeEmit }),
    toolCallId: () => `rpc-spawn-${rpcRequestId}`,
  };
}
function reply(bus: Bus, req: Record<string, unknown>, data: unknown) {
  bus.emit(`subagents:rpc:v1:reply:${req.requestId}`, {
    ...req,
    success: true,
    data,
  });
}
function restart(
  f: ReturnType<typeof fixture>,
  status: (req: Record<string, unknown>, bus: Bus) => void,
) {
  f.bridge.dispose();
  const bus = new Bus();
  bus.on('subagents:rpc:v1:request', (raw) => {
    const req = rec(raw);
    if (req.method === 'ping')
      reply(bus, req, { capabilities: { asyncSpawn: true, stop: true } });
    else status(req, bus);
  });
  const bridge = registerPlanExecRpc(bus, {
    timeoutMs: 20,
    journal: new OperationJournal(f.journal.filePath),
  });
  onTestFinished(() => bridge.dispose());
  return bus;
}
function nativeStatus(toolCallId: string, state = 'running') {
  return {
    text: `Status target: run ${toolCallId}\nSpawn budget: unlimited\nActive async capacity: 1/unlimited used\nRun: actual-worker\nTool call: ${toolCallId}\nState: ${state}\nDir: /fixture/actual-worker`,
  };
}

for (const lifetime of [true, false]) {
  test(`lost reply reattaches exact same child after restart (lifetime ${lifetime})`, async () => {
    const f = fixture(lifetime);
    assert.equal((await call(f.bus, 'spawn', f.fields)).success, false);
    assert.deepEqual(f.counts(), { spawns: 1, persistedBeforeEmit: true });
    const bus = restart(f, (req, bus) => {
      assert.equal(req.method, 'status');
      assert.equal(rec(req.params).id, f.toolCallId());
      reply(bus, req, nativeStatus(f.toolCallId()));
    });
    const results = await Promise.all([
      call(bus, 'operation', f.fields),
      call(bus, 'operation', f.fields),
    ]);
    for (const result of results) {
      const found = rec(result.data);
      assert.equal(found.state, 'found');
      assert.equal(found.runId, 'actual-worker');
      assert.equal(found.requestDigest, f.fields.owner.requestDigest);
      if (lifetime)
        assert.deepEqual(found.effectiveExecutionLifetime, {
          mode: 'unbounded',
        });
    }
    assert.equal(
      rec((await call(bus, 'spawn', f.fields)).data).runId,
      'actual-worker',
    );
    assert.equal(f.journal.get(f.fields.operationId)?.runId, 'actual-worker');
    assert.equal(f.counts().spawns, 1);
  });

  test(`cancel of lost reply stops recovered child, not a replacement (lifetime ${lifetime})`, async () => {
    const f = fixture(lifetime);
    await call(f.bus, 'spawn', f.fields);
    let stops = 0;
    const bus = restart(f, (req, bus) => {
      assert.equal(
        f.journal.get(f.fields.operationId)?.cancelRequested,
        true,
        'cancel intent precedes lookup',
      );
      if (req.method === 'status')
        reply(bus, req, nativeStatus(f.toolCallId()));
      else {
        assert.equal(req.method, 'stop');
        assert.equal(rec(req.params).id, 'actual-worker');
        stops++;
        reply(bus, req, { runId: 'actual-worker', state: 'stopping' });
      }
    });
    const cancelled = rec((await call(bus, 'cancelOperation', f.fields)).data);
    assert.equal(cancelled.neverStarted, false);
    assert.equal(cancelled.runId, 'actual-worker');
    assert.equal(stops, 1);
    assert.equal(
      rec((await call(bus, 'spawn', f.fields)).data).runId,
      'actual-worker',
    );
    assert.equal(f.counts().spawns, 1);
  });
}

for (const variant of [
  'not-found',
  'timeout',
  'wrong-target',
  'wrong-tool',
  'missing-tool',
  'embedded-headers',
  'prefix-tool',
  'missing-run',
]) {
  test(`lookup ${variant} stays unknown without redispatch`, async () => {
    const f = fixture();
    await call(f.bus, 'spawn', f.fields);
    const bus = restart(f, (req, bus) => {
      assert.equal(req.method, 'status');
      if (variant === 'timeout') return;
      if (variant === 'not-found') {
        bus.emit(`subagents:rpc:v1:reply:${req.requestId}`, {
          ...req,
          success: false,
          error: { code: 'not_found', message: 'absent' },
        });
        return;
      }
      const data = nativeStatus(f.toolCallId());
      if (variant === 'wrong-target')
        data.text = data.text.replace(
          `Status target: run ${f.toolCallId()}`,
          'Status target: run other',
        );
      if (variant === 'wrong-tool')
        data.text = data.text.replace(
          `Tool call: ${f.toolCallId()}`,
          'Tool call: other',
        );
      if (variant === 'prefix-tool')
        data.text = data.text.replace(
          `Tool call: ${f.toolCallId()}`,
          `Tool call: ${f.toolCallId()}-other`,
        );
      if (variant === 'missing-tool')
        data.text = data.text.replace(`Tool call: ${f.toolCallId()}\n`, '');
      if (variant === 'missing-run')
        data.text = data.text.replace('Run: actual-worker\n', '');
      if (variant === 'embedded-headers')
        data.text = `Untrusted summary\n${data.text}`;
      reply(bus, req, data);
    });
    const lookup = rec((await call(bus, 'operation', f.fields)).data);
    assert.equal(lookup.state, 'unknown');
    assert.equal(lookup.neverStarted, undefined);
    assert.equal(lookup.launchRejection, undefined);
    assert.equal((await call(bus, 'spawn', f.fields)).success, false);
    assert.equal(f.counts().spawns, 1);
  });
}

test('exact completion binding survives native alias retirement and host restart', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  f.bus.emit('subagent:async-complete', {
    runId: 'completed-worker',
    toolCallId: f.toolCallId(),
    asyncDir: '/fixture/completed-worker',
    success: true,
  });
  const bus = restart(f, () =>
    assert.fail('durable binding must not need retired native alias'),
  );
  const found = rec((await call(bus, 'operation', f.fields)).data);
  assert.equal(found.runId, 'completed-worker');
  assert.equal(found.processTerminalProof, undefined);
  assert.equal(found.workflowTerminalProof, undefined);
  assert.equal(
    rec((await call(bus, 'spawn', f.fields)).data).runId,
    'completed-worker',
  );
  assert.equal(f.counts().spawns, 1);
});

test('completion binding persistence retries after a transient failure', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  f.bridge.dispose();
  const bridge = registerPlanExecRpc(f.bus, {
    timeoutMs: 20,
    journal: f.journal,
    bindingReconcileIntervalMs: 10,
  });
  onTestFinished(() => bridge.dispose());
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
  onTestFinished(() => logged.mockRestore());
  const bind = vi.spyOn(f.journal, 'bind').mockImplementationOnce(() => {
    throw new Error('disk full');
  });
  f.bus.emit('subagent:async-complete', {
    runId: 'completed-worker',
    toolCallId: f.toolCallId(),
    success: true,
  });
  assert.equal(f.journal.get(f.fields.operationId)?.runId, undefined);
  await vi.waitFor(
    () =>
      assert.equal(
        f.journal.get(f.fields.operationId)?.runId,
        'completed-worker',
      ),
    { timeout: 500, interval: 10 },
  );
  assert.equal(bind.mock.calls.length, 2);
  assert.equal(f.counts().spawns, 1);
});

test('completion identity survives a transient journal lookup failure', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  f.bridge.dispose();
  const bridge = registerPlanExecRpc(f.bus, {
    timeoutMs: 20,
    journal: f.journal,
    bindingReconcileIntervalMs: 10,
  });
  onTestFinished(() => bridge.dispose());
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
  onTestFinished(() => logged.mockRestore());
  vi.spyOn(f.journal, 'getByRpcRequestId').mockImplementationOnce(() => {
    throw new Error('journal busy');
  });
  f.bus.emit('subagent:async-complete', {
    runId: 'completed-worker',
    toolCallId: f.toolCallId(),
    success: true,
  });
  assert.equal(f.journal.get(f.fields.operationId)?.runId, undefined);
  await vi.waitFor(
    () =>
      assert.equal(
        f.journal.get(f.fields.operationId)?.runId,
        'completed-worker',
      ),
    { timeout: 500, interval: 10 },
  );
  assert.equal(f.counts().spawns, 1);
});

test('active non-durable registration rejects late journal attachment', () => {
  const f = fixture();
  const bus = new Bus();
  const bridge = registerPlanExecRpc(bus, { timeoutMs: 20 });
  onTestFinished(() => bridge.dispose());
  assert.throws(
    () => registerPlanExecRpc(bus, { timeoutMs: 20, journal: f.journal }),
    /Dispose.*before attaching/,
  );
  bridge.dispose();
  const durable = registerPlanExecRpc(bus, {
    timeoutMs: 20,
    journal: f.journal,
  });
  onTestFinished(() => durable.dispose());
});

test('unmatched completion cannot bind a lost operation', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  f.bus.emit('subagent:async-complete', {
    runId: 'other-worker',
    toolCallId: `${f.toolCallId()}-other`,
    success: true,
  });
  assert.equal(f.journal.get(f.fields.operationId)?.runId, undefined);
  assert.equal(f.counts().spawns, 1);
});

test('ambiguous persisted RPC identity cannot bind either operation', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  const requestId = f.journal.get(f.fields.operationId)?.nativeParams
    ?.rpcRequestId;
  f.journal.begin(
    'other-operation',
    'other-digest',
    'other-owner',
    { mode: 'unbounded' },
    { rpcRequestId: requestId },
  );
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
  onTestFinished(() => logged.mockRestore());
  f.bus.emit('subagent:async-complete', {
    runId: 'ambiguous-worker',
    toolCallId: f.toolCallId(),
    success: true,
  });
  const bus = restart(f, () =>
    assert.fail('ambiguous caller mapping must not query native'),
  );
  const lookup = rec((await call(bus, 'operation', f.fields)).data);
  assert.equal(lookup.state, 'unknown');
  assert.equal(f.journal.get(f.fields.operationId)?.runId, undefined);
  assert.equal(f.journal.get('other-operation')?.runId, undefined);
  assert.equal(f.counts().spawns, 1);
});

test('permanent RPC ambiguity is discarded without an endless retry loop', async () => {
  vi.useFakeTimers();
  onTestFinished(() => {
    vi.useRealTimers();
  });
  const f = fixture();
  f.bridge.dispose();
  const requestId = randomUUID();
  f.journal.begin(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'owner',
    { mode: 'unbounded' },
    { rpcRequestId: requestId },
  );
  f.journal.begin(
    'other-operation',
    'other-digest',
    'other-owner',
    { mode: 'unbounded' },
    { rpcRequestId: requestId },
  );
  const bridge = registerPlanExecRpc(f.bus, {
    timeoutMs: 20,
    journal: f.journal,
    bindingReconcileIntervalMs: 10,
  });
  onTestFinished(() => bridge.dispose());
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
  onTestFinished(() => logged.mockRestore());
  const lookup = vi.spyOn(f.journal, 'getByRpcRequestId');
  f.bus.emit('subagent:async-complete', {
    runId: 'ambiguous-worker',
    toolCallId: `rpc-spawn-${requestId}`,
    success: true,
  });
  assert.equal(lookup.mock.calls.length, 1);
  await vi.advanceTimersByTimeAsync(100);
  assert.equal(lookup.mock.calls.length, 1);
  assert.equal(logged.mock.calls.length, 1);
  assert.equal(f.journal.get(f.fields.operationId)?.runId, undefined);
});

test('wrong owner or digest cannot probe native identity', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  let probes = 0;
  const bus = restart(f, () => {
    probes++;
  });
  for (const owner of [
    { ...f.fields.owner, runId: 'other' },
    { ...f.fields.owner, requestDigest: 'sha256:other' },
  ])
    for (const method of ['operation', 'cancelOperation'])
      assert.equal(
        (await call(bus, method, { ...f.fields, owner })).success,
        false,
      );
  assert.equal(probes, 0);
});

test('historical bindings work; historical no-request-ID row remains fenced', async () => {
  const f = fixture();
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  const bus = restart(f, () => assert.fail('no UUID means no native probe'));
  assert.equal(
    rec((await call(bus, 'operation', f.fields)).data).state,
    'unknown',
  );
  assert.equal((await call(bus, 'spawn', f.fields)).success, false);
  f.journal.bind(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'historical-worker',
  );
  const found = rec((await call(bus, 'operation', f.fields)).data);
  assert.equal(found.runId, 'historical-worker');
  assert.deepEqual(found.effectiveExecutionLifetime, { mode: 'unbounded' });
  assert.equal(
    rec((await call(bus, 'spawn', f.fields)).data).runId,
    'historical-worker',
  );
  assert.equal(f.counts().spawns, 0);
});

test('failed binding persistence remains unknown and retries observation only', async () => {
  const f = fixture();
  await call(f.bus, 'spawn', f.fields);
  f.bridge.dispose();
  f.bus.on('subagents:rpc:v1:request', (raw) => {
    const req = rec(raw);
    if (req.method === 'status')
      reply(f.bus, req, nativeStatus(f.toolCallId(), 'complete'));
  });
  const bridge = registerPlanExecRpc(f.bus, {
    timeoutMs: 20,
    journal: f.journal,
  });
  onTestFinished(() => bridge.dispose());
  const bind = vi.spyOn(f.journal, 'bind').mockImplementationOnce(() => {
    throw new Error('disk full');
  });
  assert.equal(
    rec((await call(f.bus, 'operation', f.fields)).data).state,
    'unknown',
  );
  assert.equal(
    rec((await call(f.bus, 'operation', f.fields)).data).runId,
    'actual-worker',
  );
  assert.equal(bind.mock.calls.length, 2);
  assert.equal(f.counts().spawns, 1);
});

test('legacy unknown cancellation persists its fence without native availability or claiming exit', async () => {
  const f = fixture();
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  const bus = restart(f, () =>
    assert.fail('unknown legacy fence must not need native runtime'),
  );
  const cancelled = rec((await call(bus, 'cancelOperation', f.fields)).data);
  assert.equal(cancelled.cancellationRequested, true);
  assert.equal(cancelled.neverStarted, false);
  assert.equal(cancelled.state, 'unknown');
  assert.equal(f.journal.get(f.fields.operationId)?.cancelRequested, true);
  assert.equal((await call(bus, 'spawn', f.fields)).success, false);
  assert.equal(f.counts().spawns, 0);
});

test('native stop failure still acknowledges the durable fence without exit proof', async () => {
  const f = fixture();
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  f.journal.bind(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'live-worker',
  );
  const bus = restart(f, () => {});
  const cancelled = rec((await call(bus, 'cancelOperation', f.fields)).data);
  assert.equal(cancelled.state, 'unknown');
  assert.equal(cancelled.cancellationRequested, true);
  assert.equal(cancelled.neverStarted, false);
  assert.equal(cancelled.runId, 'live-worker');
  assert.match(String(cancelled.error), /timed out/);
  assert.equal(f.journal.get(f.fields.operationId)?.cancelRequested, true);
  assert.equal(
    rec((await call(bus, 'spawn', f.fields)).data).runId,
    'live-worker',
  );
  assert.equal(f.counts().spawns, 0);
});

test('failed cancellation persistence cannot acknowledge a fence', async () => {
  const f = fixture();
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  vi.spyOn(f.journal, 'requestNativeCancel').mockImplementation(() => {
    throw new Error('disk full');
  });
  const reply = await call(f.bus, 'cancelOperation', f.fields);
  assert.equal(reply.success, false);
  assert.match(String(rec(reply.error).message), /disk full/);
  assert.equal(f.journal.get(f.fields.operationId)?.cancelRequested, undefined);
  assert.equal(f.counts().spawns, 0);
});

test('late unknown and conflicting replies cannot erase a recovered binding', () => {
  const f = fixture();
  f.journal.begin(f.fields.operationId, f.fields.owner.requestDigest, 'owner', {
    mode: 'unbounded',
  });
  f.journal.bind(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'actual-worker',
    '/fixture/actual-worker',
  );
  f.journal.markUnknown(
    f.fields.operationId,
    f.fields.owner.requestDigest,
    'late timeout',
  );
  assert.throws(
    () =>
      f.journal.bind(
        f.fields.operationId,
        f.fields.owner.requestDigest,
        'other-worker',
      ),
    /cannot change native identity/,
  );
  assert.throws(
    () =>
      f.journal.bind(
        f.fields.operationId,
        f.fields.owner.requestDigest,
        'actual-worker',
        '/other',
      ),
    /cannot change native identity/,
  );
  assert.equal(f.journal.get(f.fields.operationId)?.binding, 'bound');
  assert.equal(f.journal.get(f.fields.operationId)?.runId, 'actual-worker');
});
