import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test } from 'vitest';
import { OperationJournal } from '../src/operation-journal.js';
import {
  PLAN_EXEC_REPLY_PREFIX,
  PLAN_EXEC_REQUEST_EVENT,
  PLAN_EXEC_V2_REPLY_PREFIX,
  PLAN_EXEC_V2_REQUEST_EVENT,
  registerPlanExecRpc,
} from '../src/plan-exec-rpc.js';

const SUBAGENTS_REQUEST_EVENT = 'subagents:rpc:v1:request';
const SUBAGENTS_REPLY_PREFIX = 'subagents:rpc:v1:reply:';

test('plan-exec v2 exposes durable lookup and native terminal proof', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
  onTestFinished(() => rpc.dispose());

  const ping = once(bus, v2ReplyEvent('v2-ping'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-ping',
    method: 'ping',
  });
  const pingUpstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(pingUpstream));
  replyUpstream(bus, pingUpstream, 'ping', {
    version: 1,
    capabilities: {
      asyncSpawn: true,
      processTerminalProof: { version: 1, lifecycleArtifactVersion: 3 },
    },
  });
  assert.deepEqual(await ping, {
    success: true,
    data: {
      version: 2,
      protocol: 'plan-exec-bridge',
      capabilities: {
        workflowScriptSpawn: true,
        durableOperationLookup: { version: 1 },
        processTerminalProof: { version: 1 },
      },
      methods: [
        'ping',
        'spawn',
        'operation',
        'status',
        'result',
        'stop',
        'adopt',
        'cancelOperation',
        'diagnoseOperation',
      ],
    },
  });

  const operationId = 'v2-operation';
  const params = {
    agent: 'worker',
    task: 'Use the v2 contract.',
    mission: false,
  };
  const requestDigest = digest({ cwd: '/tmp/v2-worktree', params });
  const owner = {
    kind: 'pi-plan-exec',
    runId: 'plan-run-1',
    key: operationId,
    requestDigest,
  } as const;
  const spawned = once(bus, v2ReplyEvent('v2-spawn'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-spawn',
    method: 'spawn',
    operationId,
    owner,
    cwd: '/tmp/v2-worktree',
    params,
  });
  const spawnUpstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(spawnUpstream));
  const pending = once(bus, v2ReplyEvent('v2-pending'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-pending',
    method: 'operation',
    operationId,
    owner,
  });
  assert.deepEqual(await pending, {
    success: true,
    data: { state: 'pending', operationId, requestDigest },
  });
  replyUpstream(bus, spawnUpstream, 'spawn', {
    details: { runId: 'v2-run', asyncDir: '/tmp/v2-run' },
  });
  assert.deepEqual(await spawned, {
    success: true,
    data: { runId: 'v2-run', asyncDir: '/tmp/v2-run', requestDigest },
  });

  const noCwdOperationId = 'v2-no-cwd-operation';
  const noCwdParams = {
    agent: 'worker',
    task: 'Use the default worktree.',
    mission: false,
  };
  const noCwdDigest = digest({ params: noCwdParams });
  const noCwdSpawn = once(bus, v2ReplyEvent('v2-no-cwd'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-no-cwd',
    method: 'spawn',
    operationId: noCwdOperationId,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-no-cwd',
      key: noCwdOperationId,
      requestDigest: noCwdDigest,
    },
    params: noCwdParams,
  });
  const noCwdUpstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(noCwdUpstream));
  replyUpstream(bus, noCwdUpstream, 'spawn', {
    details: { runId: 'v2-no-cwd-run', asyncDir: '/tmp/v2-no-cwd-run' },
  });
  assert.deepEqual(await noCwdSpawn, {
    success: true,
    data: {
      runId: 'v2-no-cwd-run',
      asyncDir: '/tmp/v2-no-cwd-run',
      requestDigest: noCwdDigest,
    },
  });

  const paramsCwdOperationId = 'v2-params-cwd-operation';
  const paramsCwd = {
    agent: 'worker',
    task: 'Use params cwd.',
    mission: false,
    cwd: '/tmp/v2-params-worktree',
  };
  const paramsCwdDigest = digest({
    cwd: '/tmp/v2-params-worktree',
    params: paramsCwd,
  });
  const paramsCwdSpawn = once(bus, v2ReplyEvent('v2-params-cwd'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-params-cwd',
    method: 'spawn',
    operationId: paramsCwdOperationId,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-params-cwd',
      key: paramsCwdOperationId,
      requestDigest: paramsCwdDigest,
    },
    params: paramsCwd,
  });
  const paramsCwdUpstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(paramsCwdUpstream));
  replyUpstream(bus, paramsCwdUpstream, 'spawn', {
    details: { runId: 'v2-params-cwd-run' },
  });
  assert.deepEqual(await paramsCwdSpawn, {
    success: true,
    data: { runId: 'v2-params-cwd-run', requestDigest: paramsCwdDigest },
  });

  const absentOperationId = 'v2-absent-operation';
  const absentDigest = digest({ params });
  const absent = once(bus, v2ReplyEvent('v2-absent'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-absent',
    method: 'operation',
    operationId: absentOperationId,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-absent',
      key: absentOperationId,
      requestDigest: absentDigest,
    },
  });
  assert.deepEqual(await absent, {
    success: true,
    data: {
      state: 'absent',
      operationId: absentOperationId,
      requestDigest: absentDigest,
    },
  });

  const lookup = once(bus, v2ReplyEvent('v2-operation'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-operation',
    method: 'operation',
    operationId,
    owner,
  });
  assert.deepEqual(await lookup, {
    success: true,
    data: {
      state: 'found',
      operationId,
      requestDigest,
      runId: 'v2-run',
      asyncDir: '/tmp/v2-run',
    },
  });

  const status = once(bus, v2ReplyEvent('v2-status'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-status',
    method: 'status',
    params: { runId: 'v2-run', asyncDir: '/tmp/v2-run' },
  });
  const statusUpstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(statusUpstream));
  const processTerminal = {
    version: 1,
    state: 'observed',
    runId: 'v2-run',
    runnerProcessInstanceId: 'runner-1',
    observedAt: 1234,
    instances: [],
  };
  replyUpstream(bus, statusUpstream, 'status', {
    text: 'Run: v2-run\nState: complete\nDir: /tmp/v2-run',
    details: { lifecycleStatus: { processTerminal } },
  });
  assert.deepEqual(await status, {
    success: true,
    data: {
      runId: 'v2-run',
      state: 'complete',
      asyncDir: '/tmp/v2-run',
      text: 'Run: v2-run\nState: complete\nDir: /tmp/v2-run',
      processTerminal,
      processTerminalProof: processTerminal,
      lifecycleStatus: { processTerminal },
    },
  });
  rpc.dispose();

  const recoveredBus = new FakeEventBus();
  const recovered = registerPlanExecRpc(recoveredBus, {
    timeoutMs: 100,
    journalPath,
  });
  onTestFinished(() => recovered.dispose());
  const recoveredLookup = once(recoveredBus, v2ReplyEvent('v2-recovered'));
  recoveredBus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'v2-recovered',
    method: 'operation',
    operationId,
    owner,
  });
  assert.deepEqual(await recoveredLookup, {
    success: true,
    data: {
      state: 'found',
      operationId,
      requestDigest,
      runId: 'v2-run',
      asyncDir: '/tmp/v2-run',
    },
  });
});

test('plan-exec v2 rejects a mismatched request digest before dispatch', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
  onTestFinished(() => rpc.dispose());

  const reply = once(bus, v2ReplyEvent('bad-digest'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'bad-digest',
    method: 'spawn',
    operationId: 'bad-digest-operation',
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-1',
      key: 'bad-digest-operation',
      requestDigest: `sha256:${'0'.repeat(64)}`,
    },
    params: { agent: 'worker', task: 'Reject this.' },
  });
  assert.deepEqual(await reply, {
    success: false,
    error: {
      code: 'invalid_request',
      message: 'spawn owner requestDigest does not match cwd and params',
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test('plan-exec spawn forwards actual pi-subagents parameters and coalesces a durable operation', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const request = {
    version: 1,
    method: 'spawn',
    operationId: 'operation-1',
    cwd: '/tmp/shared-worktree',
    params: {
      agent: 'worker',
      task: 'Implement the task.',
      async: true,
      clarify: false,
      context: 'fresh',
      model: 'test/model',
      turnBudget: { maxTurns: 40 },
      toolBudget: { hard: 10 },
      control: { enabled: false },
      acceptance: { level: 'verified' },
      completionGuard: false,
      timeout: 30_000,
    },
  } as const;
  const firstReply = once(bus, replyEvent('spawn-1'));
  const duplicateReply = once(bus, replyEvent('spawn-2'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: 'spawn-1' });
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: 'spawn-2' });

  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.equal(upstream.version, 1);
  assert.equal(upstream.method, 'spawn');
  assert.deepEqual(upstream.params, {
    workflowScript:
      'return runs.run("main", {"agent":"worker","task":"Implement the task.","completionGuard":false})',
    async: true,
    context: 'fresh',
    model: 'test/model',
    turnBudget: { maxTurns: 40 },
    toolBudget: { hard: 10 },
    control: { enabled: false },
    acceptance: { level: 'verified' },
    cwd: '/tmp/shared-worktree',
    timeoutMs: 30_000,
  });

  replyUpstream(bus, upstream, 'spawn', {
    details: { runId: 'run-1', asyncDir: '/tmp/async-run' },
  });
  const expected = {
    success: true,
    data: { runId: 'run-1', asyncDir: '/tmp/async-run' },
  };
  assert.deepEqual(await firstReply, expected);
  assert.deepEqual(await duplicateReply, expected);

  const replay = once(bus, replyEvent('spawn-3'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: 'spawn-3' });
  assert.deepEqual(await replay, expected);
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  const conflict = once(bus, replyEvent('spawn-conflict'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: 'spawn-conflict',
    params: { ...request.params, task: 'Different task.' },
  });
  assert.deepEqual(await conflict, {
    success: false,
    error: {
      code: 'invalid_request',
      message: 'spawn operationId was already used with different parameters',
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test('plan-exec accepts cwd from params and rejects conflicting cwd values', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const reply = once(bus, replyEvent('params-cwd'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'params-cwd',
    method: 'spawn',
    operationId: 'params-cwd-operation',
    params: {
      agent: 'worker',
      task: 'Use the requested worktree.',
      cwd: '/tmp/params-worktree',
    },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.ok(isRecord(upstream.params));
  assert.equal(upstream.params.cwd, '/tmp/params-worktree');
  assert.equal(upstream.params.async, true);
  assert.equal(
    upstream.params.workflowScript,
    'return runs.run("main", {"agent":"worker","task":"Use the requested worktree."})',
  );
  assert.equal(Object.hasOwn(upstream.params, 'agent'), false);
  assert.equal(Object.hasOwn(upstream.params, 'task'), false);
  assert.equal(Object.hasOwn(upstream.params, 'clarify'), false);
  replyUpstream(bus, upstream, 'spawn', {
    details: { runId: 'params-cwd-run' },
  });
  assert.deepEqual(await reply, {
    success: true,
    data: { runId: 'params-cwd-run' },
  });

  const conflict = once(bus, replyEvent('cwd-conflict'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'cwd-conflict',
    method: 'spawn',
    operationId: 'cwd-conflict-operation',
    cwd: '/tmp/top-level',
    params: {
      agent: 'worker',
      task: 'This must be rejected.',
      cwd: '/tmp/params',
    },
  });
  assert.deepEqual(await conflict, {
    success: false,
    error: {
      code: 'invalid_request',
      message:
        'spawn cwd must match when supplied at both the request and params levels',
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test('plan-exec retains an in-flight operation across re-registration', async () => {
  const bus = new FakeEventBus();
  const first = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => first.dispose());

  const request = {
    version: 1,
    method: 'spawn',
    operationId: 'durable-operation',
    params: { agent: 'worker', task: 'Launch once.' },
  } as const;
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: 'first-attempt' });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));

  first.dispose();
  const second = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => second.dispose());
  const retry = once(bus, replyEvent('retry'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: 'retry' });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  replyUpstream(bus, upstream, 'spawn', {
    details: { runId: 'durable-run', asyncDir: '/tmp/durable-run' },
  });
  assert.deepEqual(await retry, {
    success: true,
    data: { runId: 'durable-run', asyncDir: '/tmp/durable-run' },
  });
});

test('plan-exec reports operation lookup states without launching duplicates', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const absent = once(bus, replyEvent('operation-absent'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'operation-absent',
    method: 'operation',
    operationId: 'missing',
  });
  assert.deepEqual(await absent, { success: true, data: { state: 'absent' } });

  const spawned = once(bus, replyEvent('spawn-pending'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'spawn-pending',
    method: 'spawn',
    operationId: 'operation-1',
    params: { agent: 'worker', task: 'Do work.' },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));

  const pending = once(bus, replyEvent('operation-pending'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'operation-pending',
    method: 'operation',
    operationId: 'operation-1',
  });
  assert.deepEqual(await pending, {
    success: true,
    data: { state: 'pending' },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  replyUpstream(bus, upstream, 'spawn', { details: { runId: 'run-1' } });
  await spawned;
  const found = once(bus, replyEvent('operation-found'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'operation-found',
    method: 'operation',
    operationId: 'operation-1',
  });
  assert.deepEqual(await found, {
    success: true,
    data: { state: 'found', runId: 'run-1' },
  });
});

test('plan-exec retains failed operation lookup across re-registration', async () => {
  const bus = new FakeEventBus();
  const first = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => first.dispose());

  const spawned = once(bus, replyEvent('spawn-failed'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'spawn-failed',
    method: 'spawn',
    operationId: 'failed-operation',
    params: { agent: 'worker', task: 'Fail once.' },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  bus.emit(upstreamReplyEvent(String(upstream.requestId)), {
    version: 1,
    requestId: upstream.requestId,
    method: 'spawn',
    success: false,
    error: { message: 'provider unavailable' },
  });
  assert.deepEqual(await spawned, {
    success: false,
    error: { code: 'upstream_error', message: 'provider unavailable' },
  });

  first.dispose();
  const second = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => second.dispose());
  const lookup = once(bus, replyEvent('failed-operation-lookup'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'failed-operation-lookup',
    method: 'operation',
    operationId: 'failed-operation',
  });
  assert.deepEqual(await lookup, {
    success: true,
    data: { state: 'unknown', error: 'provider unavailable' },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test('plan-exec recovers bound operations from a durable journal', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));

  const firstBus = new FakeEventBus();
  const first = registerPlanExecRpc(firstBus, { timeoutMs: 100, journalPath });
  const request = {
    version: 1,
    method: 'spawn',
    operationId: 'durable-operation',
    params: { agent: 'worker', task: 'Survive a full restart.' },
  } as const;
  const spawned = once(firstBus, replyEvent('durable-spawn'));
  firstBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: 'durable-spawn',
  });
  const upstream = firstBus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  replyUpstream(firstBus, upstream, 'spawn', {
    details: { runId: 'durable-run', asyncDir: '/tmp/durable-run' },
  });
  assert.deepEqual(await spawned, {
    success: true,
    data: { runId: 'durable-run', asyncDir: '/tmp/durable-run' },
  });
  first.dispose();

  const secondBus = new FakeEventBus();
  const second = registerPlanExecRpc(secondBus, {
    timeoutMs: 100,
    journalPath,
  });
  onTestFinished(() => second.dispose());
  const lookup = once(secondBus, replyEvent('durable-lookup'));
  secondBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'durable-lookup',
    method: 'operation',
    operationId: 'durable-operation',
  });
  assert.deepEqual(await lookup, {
    success: true,
    data: {
      state: 'found',
      runId: 'durable-run',
      asyncDir: '/tmp/durable-run',
    },
  });

  const replay = once(secondBus, replyEvent('durable-replay'));
  secondBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: 'durable-replay',
  });
  assert.deepEqual(await replay, {
    success: true,
    data: { runId: 'durable-run', asyncDir: '/tmp/durable-run' },
  });
  assert.equal(secondBus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test('plan-exec durable journal prevents a second process from redispatching', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const request = {
    version: 1,
    method: 'spawn',
    operationId: 'cross-process-operation',
    params: { agent: 'worker', task: 'Dispatch only once.' },
  } as const;

  const firstBus = new FakeEventBus();
  const first = registerPlanExecRpc(firstBus, {
    timeoutMs: 1_000,
    journalPath,
  });
  firstBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: 'first-process',
  });
  assert.equal(firstBus.count(SUBAGENTS_REQUEST_EVENT), 1);

  const secondBus = new FakeEventBus();
  const second = registerPlanExecRpc(secondBus, {
    timeoutMs: 100,
    journalPath,
  });
  const replay = once(secondBus, replyEvent('second-process'));
  secondBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: 'second-process',
  });
  assert.deepEqual(await replay, {
    success: false,
    error: {
      code: 'upstream_error',
      message: 'pi-subagents spawn outcome is unknown after bridge restart',
    },
  });
  assert.equal(secondBus.count(SUBAGENTS_REQUEST_EVENT), 0);

  first.dispose();
  second.dispose();
});

test('plan-exec returns a known native run when binding persistence fails', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
  onTestFinished(() => rpc.dispose());

  const params = { agent: 'worker', task: 'Keep the known run.' };
  const requestDigest = digest({ params });
  const reply = once(bus, v2ReplyEvent('known-run'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'known-run',
    method: 'spawn',
    operationId: 'known-run-operation',
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-known',
      key: 'known-run-operation',
      requestDigest,
    },
    params,
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  fs.writeFileSync(journalPath, '{', 'utf8');
  const originalConsoleError = console.error;
  console.error = () => undefined;
  onTestFinished(() => {
    console.error = originalConsoleError;
  });
  replyUpstream(bus, upstream, 'spawn', {
    details: { runId: 'native-known-run', asyncDir: '/tmp/native-known-run' },
  });

  assert.deepEqual(await reply, {
    success: true,
    data: {
      runId: 'native-known-run',
      asyncDir: '/tmp/native-known-run',
      requestDigest,
    },
  });
});

test('plan-exec retries a transient durable binding failure', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(path.join(root, 'operations.sqlite'));
  const bind = journal.bind.bind(journal);
  let attempts = 0;
  journal.bind = (...args) => {
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
  const rpc = registerPlanExecRpc(bus, {
    timeoutMs: 100,
    journal,
    bindingReconcileIntervalMs: 5,
  });
  onTestFinished(() => rpc.dispose());

  const params = { agent: 'worker', task: 'Retry binding.' };
  const requestDigest = digest({ params });
  const reply = once(bus, v2ReplyEvent('retry-binding'));
  bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
    version: 2,
    requestId: 'retry-binding',
    method: 'spawn',
    operationId: 'retry-binding-operation',
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan-run-retry',
      key: 'retry-binding-operation',
      requestDigest,
    },
    params,
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  replyUpstream(bus, upstream, 'spawn', {
    details: { runId: 'native-retry-binding' },
  });
  assert.equal(((await reply) as { success: boolean }).success, true);
  for (let attempt = 0; attempts < 2 && attempt < 20; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.equal(attempts >= 2, true);
  assert.equal(journal.get('retry-binding-operation')?.binding, 'bound');
});

test('plan-exec recovers an unknown launch instead of reporting it absent', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-subagents-bridge-'));
  const journalPath = path.join(root, 'operations.json');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));

  const firstBus = new FakeEventBus();
  const first = registerPlanExecRpc(firstBus, { timeoutMs: 5, journalPath });
  const reply = once(firstBus, replyEvent('unknown-spawn'));
  firstBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'unknown-spawn',
    method: 'spawn',
    operationId: 'unknown-operation',
    params: { agent: 'worker', task: 'Lose the native reply.' },
  });
  assert.equal(firstBus.count(SUBAGENTS_REQUEST_EVENT), 1);
  assert.deepEqual(await reply, {
    success: false,
    error: {
      code: 'upstream_error',
      message: 'pi-subagents spawn RPC timed out after 5ms',
    },
  });
  first.dispose();

  const secondBus = new FakeEventBus();
  const second = registerPlanExecRpc(secondBus, {
    timeoutMs: 100,
    journalPath,
  });
  onTestFinished(() => second.dispose());
  const lookup = once(secondBus, replyEvent('unknown-lookup'));
  secondBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'unknown-lookup',
    method: 'operation',
    operationId: 'unknown-operation',
  });
  assert.deepEqual(await lookup, {
    success: true,
    data: {
      state: 'unknown',
      error: 'pi-subagents spawn RPC timed out after 5ms',
    },
  });
});

test('plan-exec validates operation lookup requests', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  for (const [requestId, operationId] of [
    ['missing-operation-id', undefined],
    ['empty-operation-id', ''],
  ] as const) {
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method: 'operation',
      ...(operationId === undefined ? {} : { operationId }),
    });
    assert.deepEqual(await reply, {
      success: false,
      error: {
        code: 'invalid_request',
        message: 'operation requires a non-empty operationId',
      },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test('plan-exec bounds completed operation history without evicting the newest outcome', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  for (let index = 0; index < 129; index += 1) {
    const requestId = `history-spawn-${index}`;
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method: 'spawn',
      operationId: `history-operation-${index}`,
      params: { agent: 'worker', task: `Work ${index}.` },
    });
    const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
    assert.ok(isRecord(upstream));
    replyUpstream(bus, upstream, 'spawn', {
      details: { runId: `history-run-${index}` },
    });
    await reply;
  }

  const oldest = once(bus, replyEvent('history-oldest'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'history-oldest',
    method: 'operation',
    operationId: 'history-operation-0',
  });
  assert.deepEqual(await oldest, { success: true, data: { state: 'absent' } });

  const newest = once(bus, replyEvent('history-newest'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'history-newest',
    method: 'operation',
    operationId: 'history-operation-128',
  });
  assert.deepEqual(await newest, {
    success: true,
    data: { state: 'found', runId: 'history-run-128' },
  });
});

test('plan-exec refuses a new spawn when operation history is all active', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pi-bridge-capacity-'));
  const journalPath = path.join(root, 'operations.sqlite');
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
  onTestFinished(() => rpc.dispose());

  for (let index = 0; index < 128; index += 1) {
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId: `active-spawn-${index}`,
      method: 'spawn',
      operationId: `active-operation-${index}`,
      params: { agent: 'worker', task: `Work ${index}.` },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 128);

  const reply = once(bus, replyEvent('capacity'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'capacity',
    method: 'spawn',
    operationId: 'active-operation-128',
    params: { agent: 'worker', task: 'One too many.' },
  });
  assert.deepEqual(await reply, {
    success: false,
    error: {
      code: 'operation_capacity',
      message: 'plan-exec operation history is full of active operations',
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 128);

  const recoveredBus = new FakeEventBus();
  const recoveredRpc = registerPlanExecRpc(recoveredBus, {
    timeoutMs: 100,
    journalPath,
  });
  onTestFinished(() => recoveredRpc.dispose());
  const recoveredReply = once(recoveredBus, replyEvent('capacity-retry'));
  recoveredBus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'capacity-retry',
    method: 'spawn',
    operationId: 'active-operation-128',
    params: { agent: 'worker', task: 'Retry after restart.' },
  });
  assert.equal(recoveredBus.count(SUBAGENTS_REQUEST_EVENT), 1);
  const recoveredUpstream = recoveredBus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(recoveredUpstream));
  replyUpstream(recoveredBus, recoveredUpstream, 'spawn', {
    details: { runId: 'capacity-retry-run' },
  });
  assert.deepEqual(await recoveredReply, {
    success: true,
    data: { runId: 'capacity-retry-run' },
  });
});

test('plan-exec normalizes status, result, and observational adoption', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const expected = {
    runId: 'run-1',
    state: 'complete',
    asyncDir: '/tmp/async-run',
    resultPath: '/tmp/result.json',
    text: 'Run: run-1\nState: complete\nDir: /tmp/async-run\nResult: /tmp/result.json',
  };
  for (const method of ['status', 'result', 'adopt'] as const) {
    const requestId = `${method}-1`;
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method,
      params: { runId: 'run-1', asyncDir: '/tmp/async-run' },
    });
    const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
    assert.ok(isRecord(upstream));
    assert.equal(upstream.method, 'status');
    assert.deepEqual(upstream.params, { id: 'run-1', dir: '/tmp/async-run' });
    replyUpstream(bus, upstream, 'status', { text: expected.text });
    assert.deepEqual(await reply, {
      success: true,
      data: {
        ...expected,
        ...(method === 'adopt' ? { observed: true } : {}),
      },
    });
  }
});

test('plan-exec stop forwards asyncDir and normalizes the upstream response', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const reply = once(bus, replyEvent('stop-1'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'stop-1',
    method: 'stop',
    params: { runId: 'run-1', asyncDir: '/tmp/async-run' },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.equal(upstream.method, 'stop');
  assert.deepEqual(upstream.params, { id: 'run-1', dir: '/tmp/async-run' });
  replyUpstream(bus, upstream, 'stop', {
    runId: 'resolved-run',
    asyncDir: '/tmp/resolved-run',
    previousState: 'running',
    state: 'stopping',
    message: 'Stop requested.',
  });
  assert.deepEqual(await reply, {
    success: true,
    data: {
      runId: 'resolved-run',
      asyncDir: '/tmp/resolved-run',
      state: 'stopping',
    },
  });
});

test('plan-exec validates its request contract before emitting upstream RPC', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const cases = [
    {
      requestId: 'bad-version',
      payload: { version: 2, method: 'ping' },
      expected: 'unsupported plan-exec RPC version: 2',
    },
    {
      requestId: 'missing-method',
      payload: { version: 1 },
      expected: 'request requires a method',
    },
    {
      requestId: 'missing-operation',
      payload: {
        version: 1,
        method: 'spawn',
        params: { agent: 'worker', task: 'Missing ID.' },
      },
      expected: 'spawn requires a non-empty operationId',
    },
    {
      requestId: 'sync-spawn',
      payload: {
        version: 1,
        method: 'spawn',
        operationId: 'sync-operation',
        params: { agent: 'worker', task: 'Not allowed.', async: false },
      },
      expected: 'spawn only supports detached async execution',
    },
    {
      requestId: 'clarify-spawn',
      payload: {
        version: 1,
        method: 'spawn',
        operationId: 'clarify-operation',
        params: { agent: 'worker', task: 'Not allowed.', clarify: true },
      },
      expected: 'spawn cannot set clarify to true',
    },
    {
      requestId: 'invalid-completion-guard',
      payload: {
        version: 1,
        method: 'spawn',
        operationId: 'invalid-completion-guard-operation',
        params: {
          agent: 'worker',
          task: 'Not allowed.',
          completionGuard: 'disabled',
        },
      },
      expected: 'spawn completionGuard must be a boolean',
    },
    {
      requestId: 'bad-adopt',
      payload: {
        version: 1,
        method: 'adopt',
        params: { runId: '' },
      },
      expected: 'adopt requires a non-empty runId',
    },
  ] as const;

  for (const { requestId, payload, expected } of cases) {
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, { requestId, ...payload });
    assert.deepEqual(await reply, {
      success: false,
      error: { code: 'invalid_request', message: expected },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test('plan-exec normalizes upstream errors and rejects malformed upstream envelopes', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const rejected = once(bus, replyEvent('status-rejected'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'status-rejected',
    method: 'status',
    params: { runId: 'missing-run' },
  });
  const rejectedRequest = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(rejectedRequest));
  bus.emit(upstreamReplyEvent(String(rejectedRequest.requestId)), {
    version: 1,
    requestId: rejectedRequest.requestId,
    method: 'status',
    success: false,
    error: { code: 'not_found', message: 'Async run not found' },
  });
  assert.deepEqual(await rejected, {
    success: false,
    error: { code: 'upstream_error', message: 'Async run not found' },
  });

  const malformed = once(bus, replyEvent('status-malformed'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'status-malformed',
    method: 'status',
    params: { runId: 'run-1' },
  });
  const malformedRequest = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(malformedRequest));
  bus.emit(upstreamReplyEvent(String(malformedRequest.requestId)), {
    version: 2,
    requestId: malformedRequest.requestId,
    method: 'status',
    success: true,
    data: { text: 'Run: run-1\nState: running' },
  });
  assert.deepEqual(await malformed, {
    success: false,
    error: {
      code: 'upstream_error',
      message: 'Malformed pi-subagents RPC reply',
    },
  });
});

test('plan-exec ping advertises the supported generic methods', async () => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  onTestFinished(() => rpc.dispose());

  const reply = once(bus, replyEvent('ping-1'));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: 'ping-1',
    method: 'ping',
  });
  assert.deepEqual(await reply, {
    success: true,
    data: {
      version: 1,
      capabilities: { workflowScriptSpawn: true },
      methods: [
        'ping',
        'spawn',
        'operation',
        'status',
        'result',
        'stop',
        'adopt',
        'cancelOperation',
        'diagnoseOperation',
      ],
    },
  });
});

function replyEvent(requestId: string): string {
  return `${PLAN_EXEC_REPLY_PREFIX}${requestId}`;
}

function v2ReplyEvent(requestId: string): string {
  return `${PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`;
}

function canonicalJson(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function digest(value: unknown): string {
  return `sha256:${createHash('sha256').update(canonicalJson(value)).digest('hex')}`;
}

function upstreamReplyEvent(requestId: string): string {
  return `${SUBAGENTS_REPLY_PREFIX}${requestId}`;
}

function replyUpstream(
  bus: FakeEventBus,
  request: Record<string, unknown>,
  method: 'ping' | 'spawn' | 'status' | 'stop',
  data: unknown,
): void {
  bus.emit(upstreamReplyEvent(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method,
    success: true,
    data,
  });
}

function once(bus: FakeEventBus, event: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(new Error(`Timed out waiting for ${event}`));
    }, 100);
    const unsubscribe = bus.on(event, (payload) => {
      clearTimeout(timeout);
      unsubscribe();
      resolve(payload);
    });
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

class FakeEventBus {
  private readonly handlers = new Map<
    string,
    Set<(payload: unknown) => void>
  >();
  private readonly emitted: Array<{ event: string; payload: unknown }> = [];

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
    for (const handler of this.handlers.get(event) ?? []) handler(payload);
  }

  count(event: string): number {
    return this.emitted.filter((entry) => entry.event === event).length;
  }

  last(event: string): unknown {
    const entry = this.emitted.findLast((item) => item.event === event);
    assert.ok(entry, `expected emitted ${event}`);
    return entry.payload;
  }
}
