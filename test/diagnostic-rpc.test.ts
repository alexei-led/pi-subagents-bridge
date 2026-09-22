import assert from 'node:assert/strict';
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

const capability = {
  version: 1,
  idempotent: true,
  mode: 'follow_up',
  confirmedToolFailure: true,
};
const body = {
  operationId: 'caller-operation',
  owner: {
    kind: 'pi-plan-exec',
    runId: 'plan',
    key: 'caller-operation',
    requestDigest: 'caller-digest',
  },
  params: {
    diagnosticId: 'repair-action',
    toolCallId: 'failed-tool',
    message: 'Retry the confirmed failed tool with the corrected argument.\n',
  },
};
type Reply = { success: boolean; data?: Record<string, unknown> };

function harness(
  journalPath: string,
  native: (method: string, params: Record<string, unknown>) => unknown,
) {
  const emitter = new EventEmitter();
  const events = {
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
          events.emit(`subagents:rpc:v1:reply:${raw.requestId}`, {
            version: 1,
            requestId: raw.requestId,
            method: raw.method,
            success: true,
            data,
          }),
        );
    },
  );
  const rpc = registerPlanExecRpc(events, { timeoutMs: 20, journalPath });
  let sequence = 0;
  const request = (method: string, payload = body): Promise<Reply> =>
    new Promise((resolve) => {
      const requestId = String(++sequence);
      emitter.once(`${PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, resolve);
      events.emit(PLAN_EXEC_V2_REQUEST_EVENT, {
        version: 2,
        requestId,
        method,
        ...payload,
      });
    });
  return { request, dispose: () => rpc.dispose() };
}

function createJournal(journalPath: string) {
  const journal = new OperationJournal(journalPath);
  journal.begin(
    'caller-operation',
    'caller-digest',
    'plan',
    { mode: 'unbounded' },
    {
      operationId: 'native-operation',
      digest: 'native-digest',
      executionOwnership: { mode: 'kernel' },
    },
  );
  journal.bind('caller-operation', 'caller-digest', 'native-run');
  return journal;
}

test('diagnostic reply loss and bridge restart keep one durable native action identity', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-diagnostic-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journalPath = path.join(root, 'journal.sqlite');
  createJournal(journalPath);
  let enqueued = 0;
  let receipt: Record<string, unknown> | undefined;
  const native = (method: string, params: Record<string, unknown>) => {
    if (method === 'ping')
      return { capabilities: { diagnosticGuidance: capability } };
    assert.equal(method, 'diagnose');
    assert.deepEqual(params, {
      operationId: 'native-operation',
      digest: 'native-digest',
      ...body.params,
    });
    if (receipt) return receipt;
    enqueued++;
    receipt = {
      operationId: 'native-operation',
      digest: 'native-digest',
      runId: 'native-run',
      diagnosticId: body.params.diagnosticId,
      toolCallId: body.params.toolCallId,
      state: 'queued',
      guidanceOnly: true,
    };
    return undefined;
  };
  const first = harness(journalPath, native);
  assert.equal((await first.request('diagnoseOperation')).success, false);
  first.dispose();
  const resumed = harness(journalPath, native);
  onTestFinished(resumed.dispose);
  const replay = await resumed.request('diagnoseOperation');
  assert.equal(replay.success, true);
  assert.equal(replay.data?.state, 'queued');
  assert.equal(replay.data.guidanceOnly, true);
  assert.deepEqual(replay.data.callerBinding, {
    operationId: 'caller-operation',
    requestDigest: 'caller-digest',
  });
  assert.equal(enqueued, 1);
});

test('a stop committed during guidance preflight fences the delayed diagnostic request', async () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), 'bridge-diagnostic-stop-'),
  );
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journalPath = path.join(root, 'journal.sqlite');
  const journal = createJournal(journalPath);
  const methods: string[] = [];
  const bridge = harness(journalPath, (method) => {
    methods.push(method);
    assert.equal(method, 'ping');
    journal.requestNativeCancel('caller-operation', 'caller-digest', 'plan');
    return { capabilities: { diagnosticGuidance: capability } };
  });
  onTestFinished(bridge.dispose);
  const reply = await bridge.request('diagnoseOperation');
  assert.equal(reply.data?.state, 'cancelled');
  assert.equal(reply.data.guidanceOnly, true);
  assert.deepEqual(methods, ['ping']);
});

test('guidance requires the native safety capability and never upgrades an ambiguous or rejected receipt', async () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), 'bridge-diagnostic-guard-'),
  );
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const journalPath = path.join(root, 'journal.sqlite');
  createJournal(journalPath);
  let supported = false;
  let state = 'pending';
  let calls = 0;
  const bridge = harness(journalPath, (method) => {
    if (method === 'ping')
      return {
        capabilities: supported ? { diagnosticGuidance: capability } : {},
      };
    assert.equal(method, 'diagnose');
    calls++;
    return {
      operationId: 'native-operation',
      digest: 'native-digest',
      runId: 'native-run',
      diagnosticId: body.params.diagnosticId,
      toolCallId: body.params.toolCallId,
      state,
      guidanceOnly: true,
    };
  });
  onTestFinished(bridge.dispose);
  assert.equal(
    (await bridge.request('diagnoseOperation')).data?.state,
    'rejected',
  );
  assert.equal(calls, 0);
  supported = true;
  assert.equal(
    (await bridge.request('diagnoseOperation')).data?.state,
    'pending',
  );
  state = 'rejected';
  assert.equal(
    (await bridge.request('diagnoseOperation')).data?.state,
    'rejected',
  );
  state = 'repaired';
  assert.equal((await bridge.request('diagnoseOperation')).success, false);
  const wrongOwner = {
    ...body,
    owner: { ...body.owner, requestDigest: 'wrong-digest' },
  };
  assert.equal(
    (await bridge.request('diagnoseOperation', wrongOwner)).success,
    false,
  );
  assert.equal(calls, 3);
});
