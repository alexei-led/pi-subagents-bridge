import assert from 'node:assert/strict';
import { type ChildProcessByStdio, spawn } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Readable } from 'node:stream';
import { onTestFinished, test } from 'vitest';
import { registerBridge } from '../src/index.js';
import { OperationJournal } from '../src/operation-journal.js';

const PING_CHANNEL = 'subagents:rpc:ping';
const SPAWN_CHANNEL = 'subagents:rpc:spawn';
const STOP_CHANNEL = 'subagents:rpc:stop';
const COMPLETED_EVENT = 'subagents:completed';
const FAILED_EVENT = 'subagents:failed';
const READY_EVENT = 'subagents:ready';
const NB_REQUEST_CHANNEL = 'subagents:rpc:v1:request';
const NB_COMPLETE_EVENT = 'subagent:async-complete';
const NB_REPLY_PREFIX = 'subagents:rpc:v1:reply:';

test('registerBridge announces readiness and answers v2 ping', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  assert.equal(bus.count(READY_EVENT), 1);
  assert.deepEqual(bus.lastPayload(READY_EVENT), {});

  const reply = once(bus, replyChannel(PING_CHANNEL, 'ping-1'));
  bus.emit(PING_CHANNEL, { requestId: 'ping-1' });

  assert.deepEqual(await reply, { success: true, data: { version: 2 } });
});

test('spawn waits for an authoritative Pi session identity', async () => {
  const bus = new FakeEventBus();
  const session = { id: undefined as string | undefined };
  const bridge = registerBridge(
    { events: bus },
    { getSessionId: () => session.id },
  );

  const unavailable = once(
    bus,
    replyChannel(SPAWN_CHANNEL, 'spawn-before-session'),
  );
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-before-session',
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  assert.deepEqual(await unavailable, {
    success: false,
    error: 'Pi session identity is not initialized',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 0);

  session.id = 'session-a';
  const available = once(
    bus,
    replyChannel(SPAWN_CHANNEL, 'spawn-after-session'),
  );
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-after-session',
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'session-ready-run' } },
  });
  assert.deepEqual(await available, {
    success: true,
    data: { id: 'session-ready-run' },
  });
  bridge.dispose();
});

test('spawn forwards the normalized pi-tasks request and returns the launched run id', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-1'));
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-1',
    type: 'general-purpose',
    prompt: 'Do the task',
    options: { model: 'anthropic/claude-sonnet-4', maxTurns: 5 },
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.equal(request.version, 1);
  assert.equal(request.method, 'spawn');
  assert.deepEqual(request.params, {
    workflowScript:
      'return runs.run("main", {"agent":"delegate","task":"Do the task","control":{"enabled":false}})',
    async: true,
    context: 'fresh',
    acceptance: {
      level: 'none',
      reason:
        'pi-tasks bridge manages task lifecycle and result propagation; do not require pi-subagents acceptance reports.',
    },
    control: { enabled: false },
    model: 'anthropic/claude-sonnet-4',
    turnBudget: { maxTurns: 5 },
  });

  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: 'spawn',
    success: true,
    data: { text: 'started', details: { runId: 'run-1', asyncId: 'run-1' } },
  });

  assert.deepEqual(await reply, { success: true, data: { id: 'run-1' } });
});

test('spawn defaults to a twelve-turn budget', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-default-turns'));
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-default-turns',
    type: 'general-purpose',
    prompt: 'Do the task',
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.ok(isRecord(request.params));
  assert.deepEqual(request.params.turnBudget, { maxTurns: 12 });

  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'default-turns' } },
  });
  assert.deepEqual(await reply, {
    success: true,
    data: { id: 'default-turns' },
  });
});

test('spawn supports documented aliases, keeps custom agent names unchanged, and disables pi-subagents acceptance/control gates', async () => {
  const cases = [
    { type: 'general-purpose', expectedAgent: 'delegate' },
    { type: 'Explore', expectedAgent: 'scout' },
    { type: 'explore', expectedAgent: 'scout' },
    { type: 'my-agent', expectedAgent: 'my-agent' },
  ] as const;

  for (const { type, expectedAgent } of cases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });

    const reply = once(bus, replyChannel(SPAWN_CHANNEL, `spawn-${type}`));
    bus.emit(SPAWN_CHANNEL, {
      requestId: `spawn-${type}`,
      type,
      prompt: 'Do the task',
    });

    const request = bus.lastPayload(NB_REQUEST_CHANNEL);
    assert.ok(isRecord(request));
    assert.ok(isRecord(request.params));
    assert.equal(
      request.params.workflowScript,
      `return runs.run("main", {"agent":"${expectedAgent}","task":"Do the task","control":{"enabled":false}})`,
    );
    assert.equal(Object.hasOwn(request.params, 'agent'), false);
    assert.equal(Object.hasOwn(request.params, 'task'), false);
    assert.equal(Object.hasOwn(request.params, 'clarify'), false);
    assert.deepEqual(request.params.acceptance, {
      level: 'none',
      reason:
        'pi-tasks bridge manages task lifecycle and result propagation; do not require pi-subagents acceptance reports.',
    });
    assert.deepEqual(request.params.control, { enabled: false });

    bus.emit(nbReplyChannel(String(request.requestId)), {
      version: 1,
      requestId: request.requestId,
      success: true,
      data: { details: { runId: `${expectedAgent}-run` } },
    });

    assert.deepEqual(await reply, {
      success: true,
      data: { id: `${expectedAgent}-run` },
    });
  }
});

test('spawn rejects missing required pi-tasks fields', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-invalid'));
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-invalid',
    type: 'general-purpose',
  });

  assert.deepEqual(await reply, {
    success: false,
    error: 'spawn requires string type and prompt',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 0);
});

test('spawn accepts fallback run ids and surfaces reply-shape errors', async () => {
  const successCases = [
    {
      label: 'details.asyncId',
      data: { details: { asyncId: 'async-only' } },
      expectedId: 'async-only',
    },
    {
      label: 'top-level runId',
      data: { runId: 'top-run' },
      expectedId: 'top-run',
    },
    {
      label: 'top-level asyncId',
      data: { asyncId: 'top-async' },
      expectedId: 'top-async',
    },
  ] as const;

  for (const successCase of successCases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });

    const reply = once(
      bus,
      replyChannel(SPAWN_CHANNEL, `spawn-${successCase.label}`),
    );
    bus.emit(SPAWN_CHANNEL, {
      requestId: `spawn-${successCase.label}`,
      type: 'general-purpose',
      prompt: 'Do the task',
    });

    const request = bus.lastPayload(NB_REQUEST_CHANNEL);
    assert.ok(isRecord(request));

    bus.emit(nbReplyChannel(String(request.requestId)), {
      version: 1,
      requestId: request.requestId,
      method: 'spawn',
      success: true,
      data: successCase.data,
    });

    assert.deepEqual(await reply, {
      success: true,
      data: { id: successCase.expectedId },
    });
  }

  const failureCases = [
    {
      label: 'malformed-reply',
      payload: { requestId: 'req', method: 'spawn' },
      expectedError: 'Malformed nicobailon RPC reply.',
    },
    {
      label: 'missing-run-id',
      payload: {
        version: 1,
        requestId: 'req',
        success: true,
        data: { details: {} },
      },
      expectedError: 'nicobailon spawn reply did not include a run id',
    },
    {
      label: 'remote-error-without-message',
      payload: {
        version: 1,
        requestId: 'req',
        success: false,
        error: { code: 'bad' },
      },
      expectedError: 'nicobailon RPC error',
    },
  ] as const;

  for (const failureCase of failureCases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });

    const reply = once(
      bus,
      replyChannel(SPAWN_CHANNEL, `spawn-${failureCase.label}`),
    );
    bus.emit(SPAWN_CHANNEL, {
      requestId: `spawn-${failureCase.label}`,
      type: 'general-purpose',
      prompt: 'Do the task',
    });

    const request = bus.lastPayload(NB_REQUEST_CHANNEL);
    assert.ok(isRecord(request));

    bus.emit(nbReplyChannel(String(request.requestId)), failureCase.payload);

    assert.deepEqual(await reply, {
      success: false,
      error: failureCase.expectedError,
    });
  }
});

test('duplicate spawn requests start one run and replay its response', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-duplicate'));
  const request = {
    requestId: 'spawn-duplicate',
    type: 'general-purpose',
    prompt: 'Do the task',
  };
  bus.emit(SPAWN_CHANNEL, request);
  bus.emit(SPAWN_CHANNEL, request);

  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);
  const spawned = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(spawned));
  bus.emit(nbReplyChannel(String(spawned.requestId)), {
    version: 1,
    requestId: spawned.requestId,
    success: true,
    data: { details: { runId: 'deduped-run' } },
  });

  assert.deepEqual(await reply, { success: true, data: { id: 'deduped-run' } });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(bus.count(replyChannel(SPAWN_CHANNEL, 'spawn-duplicate')), 2);
});

test('in-memory legacy replay rejects request and session mismatches', async () => {
  const bus = new FakeEventBus();
  const session = { id: 'session-a' };
  const bridge = registerBridge(
    { events: bus },
    { getSessionId: () => session.id },
  );
  const request = {
    requestId: 'in-memory-identity',
    type: 'general-purpose',
    prompt: 'Original task',
  };
  bus.emit(SPAWN_CHANNEL, request);
  const upstream = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(upstream));

  const payloadConflict = once(
    bus,
    replyChannel(SPAWN_CHANNEL, request.requestId),
  );
  bus.emit(SPAWN_CHANNEL, { ...request, prompt: 'Different task' });
  assert.deepEqual(await payloadConflict, {
    success: false,
    error: 'spawn requestId was already used by another request',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);

  session.id = 'session-b';
  const sessionConflict = once(
    bus,
    replyChannel(SPAWN_CHANNEL, request.requestId),
  );
  bus.emit(SPAWN_CHANNEL, request);
  assert.deepEqual(await sessionConflict, {
    success: false,
    error: 'spawn requestId was already used by another request',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);

  bus.emit(nbReplyChannel(String(upstream.requestId)), {
    version: 1,
    requestId: upstream.requestId,
    success: true,
    data: { details: { runId: 'in-memory-run' } },
  });
  await waitFor(
    () => bus.count(replyChannel(SPAWN_CHANNEL, request.requestId)) === 3,
  );
  bridge.dispose();
});

test('durable legacy request identity prevents redispatch after reply-cache expiry', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-legacy-'));
  const journalPath = join(root, 'operations.sqlite');
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));
  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    {
      planExecJournalPath: journalPath,
      spawnReplyCacheTtlMs: 1,
      getSessionId: () => 'session-a',
    },
  );
  onTestFinished(() => bridge.dispose());

  const request = {
    requestId: 'durable-legacy-request',
    type: 'general-purpose',
    prompt: 'Do the task',
  };
  const firstReply = once(bus, replyChannel(SPAWN_CHANNEL, request.requestId));
  bus.emit(SPAWN_CHANNEL, request);
  const spawned = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(spawned));
  bus.emit(nbReplyChannel(String(spawned.requestId)), {
    version: 1,
    requestId: spawned.requestId,
    success: true,
    data: { details: { runId: 'durable-legacy-run' } },
  });
  assert.deepEqual(await firstReply, {
    success: true,
    data: { id: 'durable-legacy-run' },
  });

  await new Promise((resolve) => setTimeout(resolve, 5));
  const replay = once(bus, replyChannel(SPAWN_CHANNEL, request.requestId));
  bus.emit(SPAWN_CHANNEL, request);
  assert.deepEqual(await replay, {
    success: true,
    data: { id: 'durable-legacy-run' },
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);

  await new Promise((resolve) => setTimeout(resolve, 5));
  const conflict = once(bus, replyChannel(SPAWN_CHANNEL, request.requestId));
  bus.emit(SPAWN_CHANNEL, { ...request, prompt: 'Different task' });
  assert.deepEqual(await conflict, {
    success: false,
    error: 'spawn requestId was already used by another request',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);
});

test('legacy binding persistence is retried after a transient failure', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-binding-'));
  const journal = new OperationJournal(join(root, 'operations.sqlite'));
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));
  const bind = journal.bindLegacySpawn.bind(journal);
  let attempts = 0;
  journal.bindLegacySpawn = (...args) => {
    attempts += 1;
    if (attempts === 1) throw new Error('transient binding failure');
    return bind(...args);
  };
  const originalError = console.error;
  console.error = () => undefined;
  onTestFinished(() => {
    console.error = originalError;
  });

  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    {
      operationJournal: journal,
      getSessionId: () => 'session-a',
      acceptedRunReconcileIntervalMs: 5,
      spawnReplyCacheTtlMs: 1,
    },
  );
  onTestFinished(() => bridge.dispose());
  await spawnOwnedRun(bus, 'binding-retry-run');
  await waitFor(() => attempts >= 2);
  await new Promise((resolve) => setTimeout(resolve, 5));

  const replay = once(
    bus,
    replyChannel(SPAWN_CHANNEL, 'spawn-binding-retry-run'),
  );
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-binding-retry-run',
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  assert.deepEqual(await replay, {
    success: true,
    data: { id: 'binding-retry-run' },
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);
});

test('bridge registration is idempotent and cannot duplicate spawn handlers', async () => {
  const bus = new FakeEventBus();
  const firstBridge = registerBridge({ events: bus });
  const secondBridge = registerBridge({ events: bus });
  assert.equal(secondBridge, firstBridge);

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-single-handler'));
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-single-handler',
    type: 'general-purpose',
    prompt: 'Do the task',
  });

  assert.equal(bus.count(NB_REQUEST_CHANNEL), 1);
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'single-handler-run' } },
  });
  assert.deepEqual(await reply, {
    success: true,
    data: { id: 'single-handler-run' },
  });
});

test('bridge limits active runs to two and permits retry after capacity frees', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, 'capacity-one');
  await spawnOwnedRun(bus, 'capacity-two');

  const requestsBefore = bus.count(NB_REQUEST_CHANNEL);
  const spawnRequest = {
    requestId: 'capacity-three',
    type: 'general-purpose',
    prompt: 'Do the task',
  };
  const rejected = once(bus, replyChannel(SPAWN_CHANNEL, 'capacity-three'));
  bus.emit(SPAWN_CHANNEL, spawnRequest);

  assert.deepEqual(await rejected, {
    success: false,
    error: 'bridge capacity reached: at most 2 active runs',
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), requestsBefore);

  bus.emit(NB_COMPLETE_EVENT, {
    runId: 'capacity-one',
    success: true,
    state: 'complete',
    summary: 'done',
  });

  const retried = once(bus, replyChannel(SPAWN_CHANNEL, 'capacity-three'));
  bus.emit(SPAWN_CHANNEL, spawnRequest);
  assert.equal(bus.count(NB_REQUEST_CHANNEL), requestsBefore + 1);
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'capacity-three' } },
  });
  assert.deepEqual(await retried, {
    success: true,
    data: { id: 'capacity-three' },
  });
});

test('spawn timeout uses the configured timeout and cleans up the listener', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus }, { spawnTimeoutMs: 1 });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-timeout'), 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-timeout',
    type: 'general-purpose',
    prompt: 'Do the task',
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.equal(bus.listenerCount(nbReplyChannel(String(request.requestId))), 1);

  assert.deepEqual(await reply, {
    success: false,
    error: 'nicobailon "spawn" RPC timed out after 1ms',
  });
  assert.equal(bus.listenerCount(nbReplyChannel(String(request.requestId))), 0);
});

test('stop replies success, dedupes owned runs, and ignores unknown runs', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, 'run-stop');

  const requestsBeforeStop = bus.count(NB_REQUEST_CHANNEL);
  for (const requestShape of [
    { requestId: 'stop-agentId', agentId: 'run-stop' },
    { requestId: 'stop-id', id: 'run-stop' },
    { requestId: 'stop-runId', runId: 'run-stop' },
  ]) {
    const reply = once(
      bus,
      replyChannel(STOP_CHANNEL, String(requestShape.requestId)),
    );
    bus.emit(STOP_CHANNEL, requestShape);
    assert.deepEqual(await reply, { success: true, data: undefined });
  }

  assert.equal(bus.count(NB_REQUEST_CHANNEL), requestsBeforeStop + 1);
  const stopRequest = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(stopRequest));
  assert.equal(stopRequest.method, 'stop');
  assert.deepEqual(stopRequest.params, { id: 'run-stop' });

  const before = bus.count(NB_REQUEST_CHANNEL);
  const reply = once(bus, replyChannel(STOP_CHANNEL, 'stop-unknown'));
  bus.emit(STOP_CHANNEL, { requestId: 'stop-unknown', agentId: 'not-owned' });

  assert.deepEqual(await reply, { success: true, data: undefined });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), before);
});

test('completion events use the documented result and error fallbacks', async () => {
  const completedCases = [
    {
      label: 'summary',
      payload: {
        runId: 'run-summary',
        success: true,
        state: 'complete',
        summary: 'summary text',
        output: 'output text',
        results: [{ output: 'child output' }],
      },
      expected: { id: 'run-summary', result: 'summary text' },
    },
    {
      label: 'workflow child output',
      payload: {
        runId: 'run-workflow',
        mode: 'workflow',
        success: true,
        state: 'complete',
        summary: 'Workflow completed successfully (1 child).',
        output: 'Workflow completed successfully (1 child).',
        results: [{ output: 'child output' }],
      },
      expected: { id: 'run-workflow', result: 'child output' },
    },
    {
      label: 'top-level output',
      payload: {
        runId: 'run-output',
        success: true,
        state: 'complete',
        output: 'output text',
      },
      expected: { id: 'run-output', result: 'output text' },
    },
    {
      label: 'child outputs',
      payload: {
        runId: 'run-children',
        success: true,
        state: 'complete',
        results: [{ output: 'child one' }, { error: 'child two' }],
      },
      expected: { id: 'run-children', result: 'child one\n\nchild two' },
    },
    {
      label: 'no result text',
      payload: {
        runId: 'run-empty',
        success: true,
        state: 'complete',
      },
      expected: { id: 'run-empty' },
    },
  ] as const;

  for (const completedCase of completedCases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });
    await spawnOwnedRun(bus, String(completedCase.payload.runId));

    const completed = once(bus, COMPLETED_EVENT);
    bus.emit(NB_COMPLETE_EVENT, completedCase.payload);

    assert.deepEqual(await completed, completedCase.expected);
  }

  const failedCases = [
    {
      label: 'explicit error',
      payload: {
        runId: 'run-failed',
        success: false,
        state: 'failed',
        error: 'failed error',
      },
      expected: { id: 'run-failed', error: 'failed error', status: 'failed' },
    },
    {
      label: 'aborted child error fallback',
      payload: {
        runId: 'run-aborted',
        success: false,
        state: 'aborted',
        results: [{ error: 'child failure' }],
      },
      expected: { id: 'run-aborted', error: 'child failure', status: 'failed' },
    },
    {
      label: 'partial output is retained with failure',
      payload: {
        runId: 'run-partial-failure',
        success: false,
        state: 'failed',
        error: 'validation failed',
        results: [{ output: 'useful partial result' }],
      },
      expected: {
        id: 'run-partial-failure',
        error: 'validation failed\n\nPartial output:\nuseful partial result',
        status: 'failed',
      },
    },
    {
      label: 'generic failure fallback',
      payload: {
        runId: 'run-generic',
        success: false,
        results: [{}],
      },
      expected: { id: 'run-generic', error: 'Agent failed', status: 'failed' },
    },
  ] as const;

  for (const failedCase of failedCases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });
    await spawnOwnedRun(bus, String(failedCase.payload.runId));

    const failed = once(bus, FAILED_EVENT);
    bus.emit(NB_COMPLETE_EVENT, failedCase.payload);

    assert.deepEqual(await failed, failedCase.expected);
  }

  const stoppedCases = [
    {
      label: 'paused child output',
      payload: {
        runId: 'run-paused',
        success: false,
        state: 'paused',
        summary: 'Paused after interrupt. Waiting for explicit next action.',
        results: [{ output: 'partial output', success: false }],
      },
      expected: {
        id: 'run-paused',
        result: 'partial output',
        status: 'stopped',
      },
    },
    {
      label: 'stopped top-level output fallback',
      payload: {
        runId: 'run-stopped',
        success: true,
        state: 'stopped',
        output: 'partial top-level output',
      },
      expected: {
        id: 'run-stopped',
        result: 'partial top-level output',
        status: 'stopped',
      },
    },
    {
      label: 'stopped summary fallback',
      payload: {
        runId: 'run-stopped-summary',
        success: true,
        state: 'stopped',
        summary: 'stopped summary',
      },
      expected: {
        id: 'run-stopped-summary',
        result: 'stopped summary',
        status: 'stopped',
      },
    },
  ] as const;

  for (const stoppedCase of stoppedCases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });
    await spawnOwnedRun(bus, String(stoppedCase.payload.runId));

    const failed = once(bus, FAILED_EVENT);
    bus.emit(NB_COMPLETE_EVENT, stoppedCase.payload);

    assert.deepEqual(await failed, stoppedCase.expected);
  }
});

test('status polling emits completion when async-complete never arrives', async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    { completionPollIntervalMs: 1 },
  );
  onTestFinished(() => bridge.dispose());

  const tempDir = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-status-'));
  onTestFinished(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  const resultPath = join(tempDir, 'run-polled.json');
  writeFileSync(
    resultPath,
    JSON.stringify({
      runId: 'run-polled',
      success: true,
      state: 'complete',
      summary: 'polled output',
    }),
    'utf8',
  );

  const statusSeen = new Promise<void>((resolve) => {
    bus.on(NB_REQUEST_CHANNEL, (payload) => {
      if (!isRecord(payload) || payload.method !== 'status') return;
      bus.emit(nbReplyChannel(String(payload.requestId)), {
        version: 1,
        requestId: payload.requestId,
        method: 'status',
        success: true,
        data: {
          text: `Run: run-polled\nState: complete\nResult: ${resultPath}`,
        },
      });
      resolve();
    });
  });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-polled'));
  const completed = once(bus, COMPLETED_EVENT, 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-polled',
    type: 'general-purpose',
    prompt: 'Do the task',
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: 'spawn',
    success: true,
    data: { details: { runId: 'run-polled' } },
  });

  assert.deepEqual(await reply, { success: true, data: { id: 'run-polled' } });
  await statusSeen;
  assert.deepEqual(await completed, {
    id: 'run-polled',
    result: 'polled output',
  });
});

test('status polling waits for a terminal result file before completing', async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    { completionPollIntervalMs: 1, terminalResultGraceMs: 100 },
  );
  onTestFinished(() => bridge.dispose());

  const tempDir = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-retry-'));
  onTestFinished(() => rmSync(tempDir, { recursive: true, force: true }));
  const resultPath = join(tempDir, 'late-result.json');
  let statusChecks = 0;
  bus.on(NB_REQUEST_CHANNEL, (payload) => {
    if (!isRecord(payload) || payload.method !== 'status') return;
    statusChecks += 1;
    if (statusChecks === 2) {
      writeFileSync(
        resultPath,
        JSON.stringify({
          runId: 'run-late-result',
          success: true,
          state: 'complete',
          summary: 'late output',
        }),
        'utf8',
      );
    }
    bus.emit(nbReplyChannel(String(payload.requestId)), {
      version: 1,
      requestId: payload.requestId,
      method: 'status',
      success: true,
      data: {
        text: `Run: run-late-result\nState: complete\nResult: ${resultPath}`,
      },
    });
  });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-late-result'));
  const completed = once(bus, COMPLETED_EVENT, 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-late-result',
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'run-late-result' } },
  });

  assert.deepEqual(await reply, {
    success: true,
    data: { id: 'run-late-result' },
  });
  assert.deepEqual(await completed, {
    id: 'run-late-result',
    result: 'late output',
  });
  assert.equal(statusChecks, 2);
});

test('status polling reports a missing terminal result after its grace period', async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    { completionPollIntervalMs: 1, terminalResultGraceMs: 1 },
  );
  onTestFinished(() => bridge.dispose());

  const resultPath = join(tmpdir(), 'pi-subagents-bridge-missing-result.json');
  rmSync(resultPath, { force: true });
  onTestFinished(() => rmSync(resultPath, { force: true }));
  bus.on(NB_REQUEST_CHANNEL, (payload) => {
    if (!isRecord(payload) || payload.method !== 'status') return;
    bus.emit(nbReplyChannel(String(payload.requestId)), {
      version: 1,
      requestId: payload.requestId,
      method: 'status',
      success: true,
      data: {
        text: `Run: run-missing-result\nState: complete\nResult: ${resultPath}`,
      },
    });
  });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, 'spawn-missing-result'));
  const completed = once(bus, COMPLETED_EVENT, 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-missing-result',
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'run-missing-result' } },
  });

  assert.deepEqual(await reply, {
    success: true,
    data: { id: 'run-missing-result' },
  });
  assert.deepEqual(await completed, {
    id: 'run-missing-result',
    result: 'Bridge warning: result payload is not readable yet after 1ms.',
  });
});

test('completion handling supports runId fallbacks, ignores unrelated events, and dedupes repeats', async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, 'run-fallback');

  bus.emit(NB_COMPLETE_EVENT, {
    id: 'not-owned',
    success: true,
    state: 'complete',
    summary: 'ignored',
  });
  assert.equal(bus.count(COMPLETED_EVENT), 0);
  assert.equal(bus.count(FAILED_EVENT), 0);

  bus.emit(NB_COMPLETE_EVENT, {
    asyncId: 'run-fallback',
    success: true,
    state: 'complete',
    summary: 'first',
  });
  bus.emit(NB_COMPLETE_EVENT, {
    runId: 'run-fallback',
    success: true,
    state: 'complete',
    summary: 'second',
  });

  assert.equal(bus.count(COMPLETED_EVENT), 1);
  assert.deepEqual(bus.lastPayload(COMPLETED_EVENT), {
    id: 'run-fallback',
    result: 'first',
  });
});

test('re-register keeps active run ownership for stop and completion', async () => {
  const bus = new FakeEventBus();
  const firstBridge = registerBridge({ events: bus });
  await spawnOwnedRun(bus, 'run-stop-after-reregister');
  await spawnOwnedRun(bus, 'run-complete-after-reregister');

  firstBridge.dispose();
  registerBridge({ events: bus });

  const stopReply = once(
    bus,
    replyChannel(STOP_CHANNEL, 'stop-after-reregister'),
  );
  const requestsBeforeStop = bus.count(NB_REQUEST_CHANNEL);
  bus.emit(STOP_CHANNEL, {
    requestId: 'stop-after-reregister',
    agentId: 'run-stop-after-reregister',
  });

  assert.deepEqual(await stopReply, { success: true, data: undefined });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), requestsBeforeStop + 1);
  const stopRequest = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(stopRequest));
  assert.equal(stopRequest.version, 1);
  assert.equal(stopRequest.method, 'stop');
  assert.deepEqual(stopRequest.params, { id: 'run-stop-after-reregister' });

  const completed = once(bus, COMPLETED_EVENT);
  bus.emit(NB_COMPLETE_EVENT, {
    runId: 'run-complete-after-reregister',
    success: true,
    state: 'complete',
    summary: 'done after re-register',
  });

  assert.deepEqual(await completed, {
    id: 'run-complete-after-reregister',
    result: 'done after re-register',
  });
});

test('dispose cancels in-flight spawn work and ignores late replies', async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge({ events: bus }, { spawnTimeoutMs: 100 });

  bus.emit(SPAWN_CHANNEL, {
    requestId: 'spawn-disposed',
    type: 'general-purpose',
    prompt: 'Do the task',
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  const nicobailonReplyEvent = nbReplyChannel(String(request.requestId));
  assert.equal(bus.listenerCount(nicobailonReplyEvent), 1);

  bridge.dispose();

  assert.equal(bus.listenerCount(nicobailonReplyEvent), 0);
  bus.emit(nicobailonReplyEvent, {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: 'late-run' } },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(bus.count(replyChannel(SPAWN_CHANNEL, 'spawn-disposed')), 0);
});

test('accepted legacy runs survive a full bridge restart until completion delivery', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-runs-'));
  const journalPath = join(root, 'bridge-journal.json');
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));

  const firstBus = new FakeEventBus();
  const first = registerBridge(
    { events: firstBus },
    { planExecJournalPath: journalPath, completionPollIntervalMs: 1_000 },
  );
  await spawnOwnedRun(firstBus, 'run-after-restart');
  first.dispose();

  const secondBus = new FakeEventBus();
  const second = registerBridge(
    { events: secondBus },
    { planExecJournalPath: journalPath, completionPollIntervalMs: 1_000 },
  );
  const completed = once(secondBus, COMPLETED_EVENT);
  secondBus.emit(NB_COMPLETE_EVENT, {
    runId: 'run-after-restart',
    success: true,
    state: 'complete',
    summary: 'recovered',
  });
  assert.deepEqual(await completed, {
    id: 'run-after-restart',
    result: 'recovered',
  });
  second.dispose();

  const thirdBus = new FakeEventBus();
  const third = registerBridge(
    { events: thirdBus },
    { planExecJournalPath: journalPath, completionPollIntervalMs: 1_000 },
  );
  thirdBus.emit(NB_COMPLETE_EVENT, {
    runId: 'run-after-restart',
    success: true,
    state: 'complete',
    summary: 'duplicate',
  });
  assert.equal(thirdBus.count(COMPLETED_EVENT), 0);
  third.dispose();
});

test("a foreign Pi session cannot consume another session's accepted completion", async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-session-'));
  const journalPath = join(root, 'operations.sqlite');
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));

  const originBus = new FakeEventBus();
  const origin = registerBridge(
    { events: originBus },
    { planExecJournalPath: journalPath, getSessionId: () => 'session-a' },
  );
  await spawnOwnedRun(originBus, 'session-owned-run');
  origin.dispose();

  const foreignBus = new FakeEventBus();
  const foreign = registerBridge(
    { events: foreignBus },
    { planExecJournalPath: journalPath, getSessionId: () => 'session-b' },
  );
  foreignBus.emit(NB_COMPLETE_EVENT, {
    runId: 'session-owned-run',
    success: true,
    state: 'complete',
    summary: 'wrong session',
  });
  assert.equal(foreignBus.count(COMPLETED_EVENT), 0);
  foreign.dispose();

  const resumedBus = new FakeEventBus();
  const resumed = registerBridge(
    { events: resumedBus },
    { planExecJournalPath: journalPath, getSessionId: () => 'session-a' },
  );
  const completed = once(resumedBus, COMPLETED_EVENT);
  resumedBus.emit(NB_COMPLETE_EVENT, {
    runId: 'session-owned-run',
    success: true,
    state: 'complete',
    summary: 'right session',
  });
  assert.deepEqual(await completed, {
    id: 'session-owned-run',
    result: 'right session',
  });
  resumed.dispose();
});

test('accepted-run reconciliation claims a run after its owner exits', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-takeover-'));
  const journalPath = join(root, 'operations.sqlite');
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));

  const moduleUrl = new URL('../src/operation-journal.ts', import.meta.url)
    .href;
  const owner = spawn(
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
          "takeover-after-exit",
          { pid: process.pid, instanceId: "exiting-owner" },
          "session-a",
        );
        process.stdout.write("ready\\n");
        setInterval(() => {}, 1_000);
      `,
    ],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  );
  onTestFinished(() => {
    owner.kill();
  });
  await waitForChildReady(owner);

  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    {
      planExecJournalPath: journalPath,
      getSessionId: () => 'session-a',
      acceptedRunReconcileIntervalMs: 5,
      completionPollIntervalMs: 1_000,
    },
  );
  onTestFinished(() => bridge.dispose());
  const completed = once(bus, COMPLETED_EVENT, 1_000);
  owner.kill();
  await waitForChildExit(owner);

  const completionTimer = setInterval(() => {
    bus.emit(NB_COMPLETE_EVENT, {
      runId: 'takeover-after-exit',
      success: true,
      state: 'complete',
      summary: 'taken over',
    });
  }, 5);
  try {
    assert.deepEqual(await completed, {
      id: 'takeover-after-exit',
      result: 'taken over',
    });
  } finally {
    clearInterval(completionTimer);
  }
});

test('accepted-run reconciliation retries after a transient journal failure', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-retry-'));
  const journal = new OperationJournal(join(root, 'operations.sqlite'));
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));
  journal.acceptRun(
    'retry-claimed-run',
    { pid: 2_147_483_647, instanceId: 'exited-owner' },
    'session-a',
  );
  const claim = journal.claimAcceptedRuns.bind(journal);
  let attempts = 0;
  journal.claimAcceptedRuns = (...args) => {
    attempts += 1;
    if (attempts === 1) throw new Error('transient claim failure');
    return claim(...args);
  };

  const bus = new FakeEventBus();
  const bridge = registerBridge(
    { events: bus },
    {
      operationJournal: journal,
      getSessionId: () => 'session-a',
      acceptedRunReconcileIntervalMs: 5,
      completionPollIntervalMs: 1_000,
    },
  );
  onTestFinished(() => bridge.dispose());
  await waitFor(() => attempts >= 2);

  const completed = once(bus, COMPLETED_EVENT);
  bus.emit(NB_COMPLETE_EVENT, {
    runId: 'retry-claimed-run',
    success: true,
    state: 'complete',
    summary: 'claimed after retry',
  });
  assert.deepEqual(await completed, {
    id: 'retry-claimed-run',
    result: 'claimed after retry',
  });
});

test('volatile accepted ownership is retried and survives restart', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-subagents-bridge-volatile-'));
  const journal = new OperationJournal(join(root, 'operations.sqlite'));
  onTestFinished(() => rmSync(root, { recursive: true, force: true }));
  const accept = journal.acceptRun.bind(journal);
  let attempts = 0;
  journal.acceptRun = (...args) => {
    attempts += 1;
    if (attempts === 1) throw new Error('transient accept failure');
    return accept(...args);
  };

  const firstBus = new FakeEventBus();
  const first = registerBridge(
    { events: firstBus },
    {
      operationJournal: journal,
      getSessionId: () => 'session-a',
      acceptedRunReconcileIntervalMs: 5,
      completionPollIntervalMs: 1_000,
    },
  );
  const originalError = console.error;
  console.error = () => undefined;
  onTestFinished(() => {
    console.error = originalError;
  });
  await spawnOwnedRun(firstBus, 'volatile-retry-run');
  await waitFor(() => attempts >= 2);
  first.dispose();

  const secondBus = new FakeEventBus();
  const second = registerBridge(
    { events: secondBus },
    {
      operationJournal: journal,
      getSessionId: () => 'session-a',
      acceptedRunReconcileIntervalMs: 5,
      completionPollIntervalMs: 1_000,
    },
  );
  const completed = once(secondBus, COMPLETED_EVENT);
  secondBus.emit(NB_COMPLETE_EVENT, {
    runId: 'volatile-retry-run',
    success: true,
    state: 'complete',
    summary: 'recovered',
  });
  assert.deepEqual(await completed, {
    id: 'volatile-retry-run',
    result: 'recovered',
  });
  second.dispose();
});

test('dispose unsubscribes handlers and ignores later events until re-registered', async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge({ events: bus });
  await spawnOwnedRun(bus, 'run-dispose');

  const listenerCountsBefore = [
    PING_CHANNEL,
    SPAWN_CHANNEL,
    STOP_CHANNEL,
    NB_COMPLETE_EVENT,
  ].map((event) => bus.listenerCount(event));
  assert.deepEqual(listenerCountsBefore, [1, 1, 1, 1]);

  bridge.dispose();

  const listenerCountsAfter = [
    PING_CHANNEL,
    SPAWN_CHANNEL,
    STOP_CHANNEL,
    NB_COMPLETE_EVENT,
  ].map((event) => bus.listenerCount(event));
  assert.deepEqual(listenerCountsAfter, [0, 0, 0, 0]);

  bus.emit(NB_COMPLETE_EVENT, {
    runId: 'run-dispose',
    success: true,
    state: 'complete',
    summary: 'should not emit',
  });
  assert.equal(bus.count(COMPLETED_EVENT), 0);
});

async function spawnOwnedRun(bus: FakeEventBus, runId: string): Promise<void> {
  const reply = once(bus, replyChannel(SPAWN_CHANNEL, `spawn-${runId}`));
  bus.emit(SPAWN_CHANNEL, {
    requestId: `spawn-${runId}`,
    type: 'general-purpose',
    prompt: 'Do the task',
  });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: 'spawn',
    success: true,
    data: { details: { runId } },
  });
  assert.deepEqual(await reply, { success: true, data: { id: runId } });
}

function replyChannel(channel: string, requestId: string): string {
  return `${channel}:reply:${requestId}`;
}

function nbReplyChannel(requestId: string): string {
  return `${NB_REPLY_PREFIX}${requestId}`;
}

function once(
  bus: FakeEventBus,
  event: string,
  timeoutMs = 100,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(new Error(`Timed out waiting for ${event}`));
    }, timeoutMs);
    const unsubscribe = bus.on(event, (payload) => {
      clearTimeout(timeout);
      unsubscribe();
      resolve(payload);
    });
  });
}

class FakeEventBus {
  readonly emitted: Array<{ event: string; payload: unknown }> = [];
  private readonly handlers = new Map<
    string,
    Set<(payload: unknown) => void>
  >();

  on(event: string, handler: (payload: unknown) => void): () => void {
    const handlers = this.handlers.get(event) ?? new Set();
    handlers.add(handler);
    this.handlers.set(event, handlers);
    return () => {
      handlers.delete(handler);
      if (handlers.size === 0) this.handlers.delete(event);
    };
  }

  emit(event: string, payload: unknown): void {
    this.emitted.push({ event, payload });
    const handlers = this.handlers.get(event);
    if (!handlers) return;
    for (const handler of [...handlers]) handler(payload);
  }

  count(event: string): number {
    return this.emitted.filter((entry) => entry.event === event).length;
  }

  listenerCount(event: string): number {
    return this.handlers.get(event)?.size ?? 0;
  }

  lastPayload(event: string): unknown {
    const entry = this.emitted.findLast((item) => item.event === event);
    assert.ok(entry, `expected emitted event ${event}`);
    return entry.payload;
  }
}

async function waitForChildReady(
  child: ChildProcessByStdio<null, Readable, null>,
  timeoutMs = 1_000,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      finish(new Error(`child did not become ready within ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref();
    const cleanup = (): void => {
      clearTimeout(timer);
      child.stdout.off('data', onData);
      child.off('error', onError);
      child.off('exit', onExit);
    };
    const finish = (error?: Error): void => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) {
        child.kill();
        reject(error);
      } else {
        resolve();
      }
    };
    const onData = (chunk: Buffer | string): void => {
      if (String(chunk).includes('ready')) finish();
    };
    const onError = (error: Error): void => finish(error);
    const onExit = (code: number | null, signal: string | null): void =>
      finish(
        new Error(
          `child exited before ready (code=${String(code)}, signal=${String(signal)})`,
        ),
      );
    child.stdout.on('data', onData);
    child.once('error', onError);
    child.once('exit', onExit);
    if (child.exitCode !== null || child.signalCode !== null)
      onExit(child.exitCode, child.signalCode);
  });
}

async function waitForChildExit(
  child: ChildProcessByStdio<null, Readable, null>,
  timeoutMs = 1_000,
): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`child did not exit within ${timeoutMs}ms`));
    }, timeoutMs);
    timer.unref();
    const cleanup = (): void => {
      clearTimeout(timer);
      child.off('error', onError);
      child.off('exit', onExit);
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const onExit = (): void => {
      cleanup();
      resolve();
    };
    child.once('error', onError);
    child.once('exit', onExit);
  });
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 200,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline)
      throw new Error('condition was not met in time');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
