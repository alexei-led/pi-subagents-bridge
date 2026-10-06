import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type {
  ExtensionAPI,
  ExtensionContext,
} from '@earendil-works/pi-coding-agent';
import { onTestFinished, test, vi } from 'vitest';
import bridgeExtension from '../src/index.js';
import { registerPlanExecRpc } from '../src/plan-exec-rpc.js';

test('proof subscriptions are removed by idempotent disposal and restored once on registration', async () => {
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
  bus.on('subagents:rpc:v1:request', (raw) => {
    const request = raw as { requestId: string };
    bus.emit(`subagents:rpc:v1:reply:${request.requestId}`, {
      version: 1,
      requestId: request.requestId,
      success: true,
      data: {
        capabilities: { asyncSpawn: true, stop: true },
        events: { processTerminal: 'proof' },
      },
    });
  });
  for (let i = 0; i < 3; i++) {
    const bridge = registerPlanExecRpc(bus, { timeoutMs: 100 });
    const reply = new Promise((resolve) =>
      emitter.once('plan-exec:bridge:v2:reply:ping', resolve),
    );
    bus.emit('plan-exec:bridge:v2:request', {
      version: 2,
      requestId: 'ping',
      method: 'ping',
    });
    await reply;
    assert.equal(emitter.listenerCount('proof'), 1);
    bridge.dispose();
    bridge.dispose();
    assert.equal(emitter.listenerCount('proof'), 0);
  }
});

test('factory owns no timers or RPC listeners until session start and cleans each session', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-lifecycle-'));
  vi.spyOn(os, 'homedir').mockReturnValue(root);
  vi.useFakeTimers();
  onTestFinished(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    fs.rmSync(root, { recursive: true, force: true });
  });
  const events = new EventEmitter();
  const handlers = new Map<
    string,
    (_event: unknown, ctx: ExtensionContext) => void
  >();
  bridgeExtension({
    on(
      event: string,
      handler: (_event: unknown, ctx: ExtensionContext) => void,
    ) {
      handlers.set(event, handler);
    },
    events: {
      on(event: string, handler: (value: unknown) => void) {
        events.on(event, handler);
        return () => {
          events.off(event, handler);
        };
      },
      emit(event: string, value: unknown) {
        events.emit(event, value);
      },
    },
  } as unknown as ExtensionAPI);
  assert.equal(vi.getTimerCount(), 0);
  assert.equal(events.listenerCount('plan-exec:bridge:v2:request'), 0);
  const ctx = {
    sessionManager: { getSessionId: () => 'session-a' },
  } as unknown as ExtensionContext;
  for (let i = 0; i < 3; i++) {
    handlers.get('session_start')?.({ reason: i ? 'reload' : 'startup' }, ctx);
    handlers.get('session_start')?.({}, ctx);
    assert.equal(events.listenerCount('plan-exec:bridge:v2:request'), 1);
    assert.ok(vi.getTimerCount() > 0);
    handlers.get('session_shutdown')?.({}, ctx);
    handlers.get('session_shutdown')?.({}, ctx);
    assert.equal(vi.getTimerCount(), 0);
    assert.equal(events.listenerCount('plan-exec:bridge:v2:request'), 0);
  }
});
