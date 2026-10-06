import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test } from 'vitest';
import { OperationJournal } from '../src/operation-journal.js';
import { registerPlanExecRpc } from '../src/plan-exec-rpc.js';

function record(value: unknown): Record<string, unknown> {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}

function fixture() {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), 'bridge-cancel-delivery-'),
  );
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
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
  const journalPath = path.join(root, 'operations.sqlite');
  const journal = new OperationJournal(journalPath);
  journal.begin(
    'op',
    'sha256:digest',
    'owner',
    { mode: 'unbounded' },
    { rpcRequestId: 'native-request' },
  );
  const stops: Record<string, unknown>[] = [];
  let failStop = false;
  let bindingAvailable = false;
  let stopResult: unknown = { runId: 'native-run', state: 'stopping' };
  bus.on('subagents:rpc:v1:request', (raw) => {
    const request = record(raw);
    if (request.method === 'stop') stops.push(record(request.params));
    if (request.method === 'status' && bindingAvailable) {
      bus.emit(`subagents:rpc:v1:reply:${request.requestId}`, {
        version: 1,
        requestId: request.requestId,
        method: request.method,
        success: true,
        data: {
          text: 'Status target: run rpc-spawn-native-request\nSpawn budget: 1\nActive async capacity: 1\nRun: native-run\nTool call: rpc-spawn-native-request\nState: running',
        },
      });
      return;
    }
    bus.emit(`subagents:rpc:v1:reply:${request.requestId}`, {
      version: 1,
      requestId: request.requestId,
      method: request.method,
      ...(request.method === 'stop' && !failStop
        ? { success: true, data: stopResult }
        : {
            success: false,
            error: {
              code: 'invalid_state',
              message: 'stop only supports running async runs',
            },
          }),
    });
  });
  let bridge = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
  onTestFinished(() => bridge.dispose());
  const call = (method = 'cancelOperation') =>
    new Promise<Record<string, unknown>>((resolve) => {
      const requestId = randomUUID();
      emitter.once(`plan-exec:bridge:v2:reply:${requestId}`, (reply) =>
        resolve(record(reply)),
      );
      bus.emit('plan-exec:bridge:v2:request', {
        version: 2,
        requestId,
        method,
        operationId: 'op',
        owner: {
          kind: 'pi-plan-exec',
          key: 'op',
          runId: 'owner',
          requestDigest: 'sha256:digest',
        },
      });
    });
  return {
    bus,
    journal,
    stops,
    call,
    bindingAvailable() {
      bindingAvailable = true;
    },
    failStop(value: boolean) {
      failStop = value;
    },
    stopResult(value: unknown) {
      stopResult = value;
    },
    reload() {
      bridge.dispose();
      bridge = registerPlanExecRpc(bus, { timeoutMs: 100, journalPath });
    },
  };
}

test('pending intent is redelivered after exact late binding, then replayed across reload without another stop', async () => {
  const f = fixture();
  const pending = record((await f.call()).data);
  assert.equal(pending.cancellationDelivery, 'pending');
  assert.equal(pending.neverStarted, false);
  assert.equal(f.stops.length, 0);
  f.bindingAvailable();
  const delivered = record((await f.call()).data);
  assert.equal(delivered.cancellationDelivery, 'delivered');
  assert.equal(delivered.nativeState, 'stopping');
  assert.equal(f.journal.get('op')?.runId, 'native-run');
  assert.equal(f.journal.get('op')?.stopReceiptState, 'stopping');
  assert.equal(delivered.neverStarted, false);
  assert.equal(delivered.processTerminalProof, undefined);
  assert.equal(delivered.workflowTerminalProof, undefined);
  f.reload();
  assert.equal(record((await f.call()).data).cancellationDelivery, 'delivered');
  assert.equal(f.stops.length, 1);
  assert.equal(f.journal.get('op')?.cancelRequested, true);
});

test('failed native stop retains intent and upstream code, then retries once for concurrent callers', async () => {
  const f = fixture();
  f.journal.bind('op', 'sha256:digest', 'native-run');
  f.failStop(true);
  const failed = record((await f.call()).data);
  assert.equal(failed.cancellationDelivery, 'pending');
  assert.equal(failed.upstreamCode, 'invalid_state');
  assert.equal(failed.neverStarted, false);
  f.failStop(false);
  const replies = await Promise.all([f.call(), f.call()]);
  for (const reply of replies)
    assert.equal(record(reply.data).cancellationDelivery, 'delivered');
  assert.equal(f.stops.length, 2);
});

for (const result of [
  undefined,
  {},
  { runId: 'other-run', state: 'stopping' },
  { runId: 'native-run', state: 'running' },
  { runId: 'native-run', state: 'stopping', childId: 'main' },
]) {
  test(`malformed or mismatched stop receipt is not delivered: ${JSON.stringify(result)}`, async () => {
    const f = fixture();
    f.journal.bind('op', 'sha256:digest', 'native-run');
    f.stopResult(result);
    const reply = record((await f.call()).data);
    assert.equal(reply.cancellationDelivery, 'pending');
    assert.equal(reply.neverStarted, false);
  });
}
