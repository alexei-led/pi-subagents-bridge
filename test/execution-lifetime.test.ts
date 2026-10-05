import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test } from 'vitest';
import { OperationJournal } from '../src/operation-journal.js';
import {
  PLAN_EXEC_V2_REPLY_PREFIX,
  PLAN_EXEC_V2_REQUEST_EVENT,
  registerPlanExecRpc,
} from '../src/plan-exec-rpc.js';

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object')
    return `{${Object.entries(value)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`)
      .join(',')}}`;
  return JSON.stringify(value);
}

const capabilities = {
  asyncSpawn: true,
  stop: true,
  processTerminalProof: { version: 1, lifecycleArtifactVersion: 1 },
};
// The released runtime returns `events` next to `capabilities`, not inside it.
const pingData = {
  capabilities,
  events: { processTerminal: 'subagent:process-terminal' },
};

function fingerprint(params: Record<string, unknown>): string {
  return `sha256:${createHash('sha256').update(canonical({ params })).digest('hex')}`;
}

function harness(
  journalPath: string,
  native: (method: string, params: Record<string, unknown>) => unknown,
) {
  const emitter = new EventEmitter();
  const bus = {
    on(event: string, handler: (value: unknown) => void) {
      emitter.on(event, handler);
      return () => {
        emitter.off(event, handler);
      };
    },
    emit(event: string, value: unknown) {
      emitter.emit(event, value);
    },
  };
  emitter.on(
    'subagents:rpc:v1:request',
    (raw: {
      requestId: string;
      method: string;
      params: Record<string, unknown>;
    }) => {
      const data = native(raw.method, raw.params);
      if (data !== undefined)
        queueMicrotask(() =>
          bus.emit(`subagents:rpc:v1:reply:${raw.requestId}`, {
            version: 1,
            requestId: raw.requestId,
            method: raw.method,
            success: true,
            data,
          }),
        );
    },
  );
  const rpc = registerPlanExecRpc(bus, { journalPath, timeoutMs: 20 });
  let sequence = 0;
  const request = (
    method: string,
    body = {},
  ): Promise<Record<string, unknown>> =>
    new Promise((resolve) => {
      const requestId = String(++sequence);
      emitter.once(`${PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, resolve);
      bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
        version: 2,
        requestId,
        method,
        ...body,
      });
    });
  return { bus, emitter, request, dispose: () => rpc.dispose() };
}

function temporary(): { root: string; journalPath: string } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-v2lite-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  return { root, journalPath: path.join(root, 'operations.sqlite') };
}

function spawnBody(operationId: string, params: Record<string, unknown>) {
  const digest = fingerprint(params);
  return {
    digest,
    body: {
      operationId,
      owner: {
        kind: 'pi-plan-exec',
        runId: 'plan',
        key: operationId,
        requestDigest: digest,
      },
      params,
    },
  };
}

test('a bounded lifetime is forwarded as timeoutMs and echoed without provider attestation', async () => {
  const { journalPath } = temporary();
  const params = {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'bounded', timeoutMs: 5_000 },
  };
  const { body } = spawnBody('operation-bounded', params);
  let spawned: Record<string, unknown> | undefined;
  const h = harness(journalPath, (method, input) => {
    if (method === 'ping') return pingData;
    if (method === 'spawn') {
      spawned = input;
      return { details: { runId: 'run-1', asyncDir: '/tmp/async-1' } };
    }
    throw new Error(`unexpected upstream method ${method}`);
  });
  onTestFinished(() => h.dispose());

  const reply = await h.request('spawn', body);
  assert.equal(reply.success, true, JSON.stringify(reply));
  const data = reply.data as Record<string, unknown>;
  assert.equal(data.runId, 'run-1');
  assert.deepEqual(data.effectiveExecutionLifetime, {
    mode: 'bounded',
    timeoutMs: 5_000,
  });
  assert.ok(spawned);
  assert.equal(spawned.timeoutMs, 5_000);
  assert.equal(spawned.executionLifetime, undefined);
  assert.equal(spawned.executionOwnership, undefined);
  assert.match(String(spawned.script), /runs\.run/);
});

test('an unbounded lifetime forwards no timeout', async () => {
  const { journalPath } = temporary();
  const params = {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'unbounded' },
  };
  const { body } = spawnBody('operation-unbounded', params);
  let spawned: Record<string, unknown> | undefined;
  const h = harness(journalPath, (method, input) => {
    if (method === 'ping') return pingData;
    if (method === 'spawn') {
      spawned = input;
      return { details: { runId: 'run-2' } };
    }
    throw new Error(`unexpected upstream method ${method}`);
  });
  onTestFinished(() => h.dispose());

  const reply = await h.request('spawn', body);
  assert.equal(reply.success, true);
  assert.deepEqual(
    (reply.data as Record<string, unknown>).effectiveExecutionLifetime,
    { mode: 'unbounded' },
  );
  assert.equal(spawned?.timeoutMs, undefined);
});

test('a lost spawn reply is never redispatched', async () => {
  const { journalPath } = temporary();
  const { body } = spawnBody('operation-lost', {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'unbounded' },
  });
  let spawns = 0;
  const h = harness(journalPath, (method) => {
    if (method === 'ping') return pingData;
    if (method === 'spawn') {
      spawns += 1;
      return undefined;
    }
    throw new Error(`unexpected upstream method ${method}`);
  });
  onTestFinished(() => h.dispose());

  const first = await h.request('spawn', body);
  assert.equal(first.success, false);
  const second = await h.request('spawn', body);
  assert.equal(second.success, false);
  const secondError = second.error as { message?: string } | undefined;
  assert.match(secondError?.message ?? '', /unknown/i);
  assert.equal(spawns, 1);
});

test('cancel after a lost spawn reply cannot claim the worker never started', async () => {
  const { journalPath } = temporary();
  const { body } = spawnBody('operation-cancel', {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'unbounded' },
  });
  let stops = 0;
  const h = harness(journalPath, (method) => {
    if (method === 'ping') return pingData;
    if (method === 'spawn') return undefined;
    if (method === 'stop') {
      stops += 1;
      return { runId: 'never', state: 'stopping' };
    }
    throw new Error(`unexpected upstream method ${method}`);
  });
  onTestFinished(() => h.dispose());

  await h.request('spawn', body);
  const cancelled = await h.request('cancelOperation', {
    operationId: 'operation-cancel',
    requestDigest: body.owner.requestDigest,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan',
      key: 'operation-cancel',
      requestDigest: body.owner.requestDigest,
    },
  });
  assert.equal(cancelled.success, true, JSON.stringify(cancelled));
  const data = cancelled.data as Record<string, unknown>;
  assert.equal(data.state, 'unknown');
  assert.equal(data.neverStarted, false);
  assert.equal(data.cancellationRequested, true);
  assert.equal(stops, 0);
});

test('a new cancellation fence prevents dispatch but does not invent proof on replay', async () => {
  const { journalPath } = temporary();
  const { body } = spawnBody('operation-before-dispatch', {
    agent: 'worker',
    task: 'Never launch',
    executionLifetime: { mode: 'unbounded' },
  });
  const h = harness(journalPath, (method) => {
    assert.equal(method, 'ping', 'cancellation must prevent native spawn');
    return pingData;
  });
  onTestFinished(() => h.dispose());
  const first = await h.request('cancelOperation', body);
  const data = first.data as Record<string, unknown>;
  assert.equal(data.state, 'cancelled');
  assert.equal(data.neverStarted, true);
  assert.equal((await h.request('spawn', body)).success, false);
  const replay = await h.request('cancelOperation', body);
  const replayData = replay.data as Record<string, unknown>;
  assert.equal(replayData.state, 'unknown');
  assert.equal(replayData.neverStarted, false);
  assert.equal((await h.request('spawn', body)).success, false);
});

test('dispatch committed by another journal before cancellation is not a never-started fence', async () => {
  const { journalPath } = temporary();
  const params = {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'unbounded' },
  };
  const { body, digest } = spawnBody('racing-operation', params);
  const other = new OperationJournal(journalPath);
  const h = harness(journalPath, () => {
    assert.fail('local cancellation fence must not wait for a native probe');
  });
  onTestFinished(() => h.dispose());
  other.begin('racing-operation', digest, 'plan', { mode: 'unbounded' });
  const reply = await h.request('cancelOperation', body);
  const data = reply.data as Record<string, unknown>;
  assert.equal(data.state, 'unknown');
  assert.equal(data.neverStarted, false);
  assert.equal(data.cancellationRequested, true);
});

test('a bound operation survives restart and exposes the upstream terminal proof', async () => {
  const { journalPath } = temporary();
  const { body } = spawnBody('operation-restart', {
    agent: 'worker',
    task: 'Review',
    executionLifetime: { mode: 'bounded', timeoutMs: 9_000 },
  });
  const first = harness(journalPath, (method) => {
    if (method === 'ping') return pingData;
    if (method === 'spawn') return { details: { runId: 'run-restart' } };
    throw new Error(`unexpected upstream method ${method}`);
  });
  const launched = await first.request('spawn', body);
  assert.equal(launched.success, true);
  first.dispose();

  let spawns = 0;
  const second = harness(journalPath, (method) => {
    if (method === 'ping') return pingData;
    spawns += 1;
    throw new Error(`unexpected upstream method ${method}`);
  });
  onTestFinished(() => second.dispose());
  await second.request('ping');

  const lookupBody = {
    operationId: 'operation-restart',
    requestDigest: body.owner.requestDigest,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'plan',
      key: 'operation-restart',
      requestDigest: body.owner.requestDigest,
    },
  };
  const lookup = await second.request('operation', lookupBody);
  assert.equal(lookup.success, true, JSON.stringify(lookup));
  assert.equal((lookup.data as Record<string, unknown>).state, 'found');
  assert.equal((lookup.data as Record<string, unknown>).runId, 'run-restart');

  second.emitter.emit('subagent:process-terminal', {
    version: 1,
    state: 'observed',
    runId: 'run-restart',
    runnerProcessInstanceId: 'runner-1',
    observedAt: Date.now(),
    instances: [
      {
        processInstanceId: 'runner-1',
        kind: 'runner',
        closeObservedAt: Date.now(),
        exitCode: 0,
        signal: null,
      },
    ],
  });
  const withProof = await second.request('operation', lookupBody);
  const proof = (withProof.data as Record<string, unknown>)
    .processTerminalProof as Record<string, unknown>;
  assert.equal(proof.runId, 'run-restart');
  assert.equal(proof.state, 'observed');
  assert.equal(spawns, 0);
});
