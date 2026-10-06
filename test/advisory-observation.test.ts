import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test, vi } from 'vitest';
import { OperationJournal } from '../src/operation-journal.js';
import { registerPlanExecRpc } from '../src/plan-exec-rpc.js';

const now = 1_800_000_000_000;
function snapshot(): Record<string, unknown> {
  return {
    kind: 'pi-subagents.async-status-snapshot',
    version: 1,
    generatedAt: now,
    caps: {
      maxRuns: 20,
      maxChildrenPerNode: 8,
      maxDepth: 3,
      maxStringLength: 160,
      maxSerializedBytes: 32768,
    },
    omitted: { runs: 0, children: 2, byteLimitExceeded: false },
    runs: [
      {
        id: 'native-run',
        kind: 'workflow',
        state: 'running',
        activity: {
          state: 'working',
          currentTool: 'read',
          lastActivityAt: now - 1000,
          currentToolStartedAt: now - 500,
          turnCount: 2,
          toolCount: 3,
        },
        children: [
          {
            id: 'main',
            kind: 'step',
            activity: { currentTool: 'unverified-child' },
          },
        ],
      },
    ],
  };
}

async function observe(value: unknown): Promise<Record<string, unknown>> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-advisory-'));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  vi.spyOn(Date, 'now').mockReturnValue(now);
  onTestFinished(() => {
    vi.restoreAllMocks();
  });
  const journal = new OperationJournal(path.join(root, 'journal.sqlite'));
  journal.begin(
    'op',
    'sha256:op',
    'owner',
    { mode: 'unbounded' },
    { rpcRequestId: 'request' },
  );
  journal.bind('op', 'sha256:op', 'native-run');
  const emitter = new EventEmitter();
  const bus = {
    on(event: string, listener: (value: unknown) => void) {
      emitter.on(event, listener);
      return () => {
        emitter.off(event, listener);
      };
    },
    emit(event: string, value: unknown) {
      emitter.emit(event, value);
    },
  };
  const bridge = registerPlanExecRpc(bus, { timeoutMs: 100, journal });
  onTestFinished(() => bridge.dispose());
  bus.on('subagents:rpc:v1:request', (raw) => {
    const request = raw as { requestId: string };
    bus.emit(`subagents:rpc:v1:reply:${request.requestId}`, {
      version: 1,
      requestId: request.requestId,
      success: true,
      data: { text: 'State: running', asyncSnapshot: value },
    });
  });
  const reply = new Promise<Record<string, unknown>>((resolve) =>
    emitter.once('plan-exec:bridge:v2:reply:status', resolve),
  );
  bus.emit('plan-exec:bridge:v2:request', {
    version: 2,
    requestId: 'status',
    method: 'status',
    params: { runId: 'native-run' },
  });
  const result = await reply;
  assert.equal(result.success, true, JSON.stringify(result));
  return result.data as Record<string, unknown>;
}

test('advisory activity preserves only the exact root and omission flags, never usage or proof', async () => {
  const data = await observe(snapshot());
  assert.deepEqual(data.advisoryObservation, {
    version: 1,
    source: 'pi-subagents.async-status-snapshot',
    runId: 'native-run',
    generatedAt: now,
    activity: {
      state: 'working',
      currentTool: 'read',
      lastActivityAt: now - 1000,
      currentToolStartedAt: now - 500,
      turnCount: 2,
      toolCount: 3,
    },
    omitted: { runs: 0, children: 2, byteLimitExceeded: false },
  });
  for (const key of [
    'activity',
    'totalTokens',
    'totalCost',
    'terminationReason',
    'processTerminalProof',
    'workflowTerminalProof',
  ])
    assert.equal(data[key], undefined);
});

for (const [name, mutate] of [
  [
    'stale',
    (s: Record<string, unknown>) => {
      s.generatedAt = now - 31_000;
    },
  ],
  [
    'pre-request',
    (s: Record<string, unknown>) => {
      s.generatedAt = now - 1;
    },
  ],
  [
    'future',
    (s: Record<string, unknown>) => {
      s.generatedAt = now + 1;
    },
  ],
  [
    'unknown-version',
    (s: Record<string, unknown>) => {
      s.version = 2;
    },
  ],
  [
    'foreign',
    (s: Record<string, unknown>) => {
      s.runs = [
        { id: 'foreign', kind: 'workflow', activity: { currentTool: 'read' } },
      ];
    },
  ],
  [
    'child-only',
    (s: Record<string, unknown>) => {
      s.runs = [
        {
          id: 'foreign',
          kind: 'workflow',
          children: [{ id: 'native-run', activity: { currentTool: 'read' } }],
        },
      ];
    },
  ],
  [
    'duplicate-root',
    (s: Record<string, unknown>) => {
      s.runs = [s.runs, s.runs].flat();
    },
  ],
  [
    'malformed-omitted',
    (s: Record<string, unknown>) => {
      s.omitted = { runs: -1, children: 0, byteLimitExceeded: false };
    },
  ],
  ...[
    { toolCount: NaN },
    { turnCount: -1 },
    { lastActivityAt: Infinity },
    { currentToolStartedAt: now + 1 },
    { currentTool: 'x'.repeat(161) },
    { currentTool: '\u001b[31mread' },
    { state: 1 },
  ].map(
    (activity) =>
      [
        JSON.stringify(activity),
        (s: Record<string, unknown>) => {
          s.runs = [{ id: 'native-run', kind: 'workflow', activity }];
        },
      ] as const,
  ),
] as const) {
  test(`unknown advisory stays absent: ${name}`, async () => {
    const value = snapshot();
    mutate(value);
    assert.equal((await observe(value)).advisoryObservation, undefined);
  });
}
