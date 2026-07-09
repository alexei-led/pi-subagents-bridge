import assert from "node:assert/strict";
import test from "node:test";
import { registerBridge } from "../src/index.js";

const PING_CHANNEL = "subagents:rpc:ping";
const SPAWN_CHANNEL = "subagents:rpc:spawn";
const STOP_CHANNEL = "subagents:rpc:stop";
const COMPLETED_EVENT = "subagents:completed";
const FAILED_EVENT = "subagents:failed";
const NB_REQUEST_CHANNEL = "subagents:rpc:v1:request";
const NB_COMPLETE_EVENT = "subagent:async-complete";
const NB_REPLY_PREFIX = "subagents:rpc:v1:reply:";

test("ping handshake returns protocol v2", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(PING_CHANNEL, "ping-1"));
  bus.emit(PING_CHANNEL, { requestId: "ping-1" });

  assert.deepEqual(await reply, { success: true, data: { version: 2 } });
});

test("spawn maps pi-tasks params, forwards through pi-subagents RPC, and returns the run id", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-1"));
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-1",
    type: "general-purpose",
    prompt: "Do the task",
    options: { model: "anthropic/claude-sonnet-4", maxTurns: 5 },
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.equal(request.version, 1);
  assert.equal(request.method, "spawn");
  assert.deepEqual(request.params, {
    agent: "delegate",
    task: "Do the task",
    async: true,
    clarify: false,
    context: "fresh",
    model: "anthropic/claude-sonnet-4",
    turnBudget: { maxTurns: 5 },
  });

  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: "spawn",
    success: true,
    data: { text: "started", details: { runId: "run-1", asyncId: "run-1" } },
  });

  assert.deepEqual(await reply, { success: true, data: { id: "run-1" } });
});

test("spawn leaves custom agent names unchanged", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-custom"));
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-custom",
    type: "my-agent",
    prompt: "Do the task",
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.ok(isRecord(request.params));
  assert.equal(request.params.agent, "my-agent");

  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    success: true,
    data: { details: { runId: "custom-run" } },
  });

  assert.deepEqual(await reply, {
    success: true,
    data: { id: "custom-run" },
  });
});

test("spawn error path returns a pi-tasks RPC error envelope", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-error"));
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-error",
    type: "Explore",
    prompt: "Explore this",
  });

  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.ok(isRecord(request.params));
  assert.equal(request.params.agent, "scout");

  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: "spawn",
    success: false,
    error: { code: "invalid_params", message: "agent not found" },
  });

  assert.deepEqual(await reply, {
    success: false,
    error: "agent not found",
  });
});

test("spawn timeout returns a pi-tasks RPC error envelope", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus }, {
    spawnTimeoutMs: 1,
  });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-timeout"), 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-timeout",
    type: "general-purpose",
    prompt: "Do the task",
  });

  assert.deepEqual(await reply, {
    success: false,
    error: 'nicobailon "spawn" RPC timed out after 1ms',
  });
});

test("stop always replies success and forwards owned run ids", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-stop");

  const reply = once(bus, replyChannel(STOP_CHANNEL, "stop-1"));
  bus.emit(STOP_CHANNEL, { requestId: "stop-1", agentId: "run-stop" });

  assert.deepEqual(await reply, { success: true, data: undefined });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  assert.equal(request.method, "stop");
  assert.deepEqual(request.params, { id: "run-stop" });
});

test("async complete emits subagents:completed with result text", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-complete");

  const completed = once(bus, COMPLETED_EVENT);
  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-complete",
    success: true,
    state: "complete",
    summary: "delegate:\nDone",
    results: [{ agent: "delegate", output: "Done", success: true }],
  });

  assert.deepEqual(await completed, {
    id: "run-complete",
    result: "delegate:\nDone",
  });
});

test("async failed and aborted states emit subagents:failed", async () => {
  for (const state of ["failed", "aborted"] as const) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });
    await spawnOwnedRun(bus, `run-${state}`);

    const failed = once(bus, FAILED_EVENT);
    bus.emit(NB_COMPLETE_EVENT, {
      runId: `run-${state}`,
      success: false,
      state,
      error: `${state} error`,
    });

    assert.deepEqual(await failed, {
      id: `run-${state}`,
      error: `${state} error`,
      status: "failed",
    });
  }
});

test("paused async completion maps to stopped with partial result", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-paused");

  const failed = once(bus, FAILED_EVENT);
  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-paused",
    success: false,
    state: "paused",
    summary: "Paused after interrupt. Waiting for explicit next action.",
    results: [{ agent: "delegate", output: "partial output", success: false }],
  });

  assert.deepEqual(await failed, {
    id: "run-paused",
    result: "partial output",
    status: "stopped",
  });
});

test("events for unowned run ids are ignored", () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  bus.emit(NB_COMPLETE_EVENT, {
    runId: "not-owned",
    success: true,
    state: "complete",
    summary: "should not emit",
  });

  assert.equal(bus.count(COMPLETED_EVENT), 0);
  assert.equal(bus.count(FAILED_EVENT), 0);
});

test("double async-complete events are deduped", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-dedupe");

  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-dedupe",
    success: true,
    state: "complete",
    summary: "first",
  });
  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-dedupe",
    success: true,
    state: "complete",
    summary: "second",
  });

  assert.equal(bus.count(COMPLETED_EVENT), 1);
  assert.deepEqual(bus.lastPayload(COMPLETED_EVENT), {
    id: "run-dedupe",
    result: "first",
  });
});

async function spawnOwnedRun(bus: FakeEventBus, runId: string): Promise<void> {
  const reply = once(bus, replyChannel(SPAWN_CHANNEL, `spawn-${runId}`));
  bus.emit(SPAWN_CHANNEL, {
    requestId: `spawn-${runId}`,
    type: "general-purpose",
    prompt: "Do the task",
  });
  const request = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(request));
  bus.emit(nbReplyChannel(String(request.requestId)), {
    version: 1,
    requestId: request.requestId,
    method: "spawn",
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

  lastPayload(event: string): unknown {
    const entry = this.emitted.findLast((item) => item.event === event);
    assert.ok(entry, `expected emitted event ${event}`);
    return entry.payload;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
