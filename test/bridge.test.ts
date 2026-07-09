import assert from "node:assert/strict";
import test from "node:test";
import { registerBridge } from "../src/index.js";

const PING_CHANNEL = "subagents:rpc:ping";
const SPAWN_CHANNEL = "subagents:rpc:spawn";
const STOP_CHANNEL = "subagents:rpc:stop";
const COMPLETED_EVENT = "subagents:completed";
const FAILED_EVENT = "subagents:failed";
const READY_EVENT = "subagents:ready";
const NB_REQUEST_CHANNEL = "subagents:rpc:v1:request";
const NB_COMPLETE_EVENT = "subagent:async-complete";
const NB_REPLY_PREFIX = "subagents:rpc:v1:reply:";

test("registerBridge announces readiness and answers v2 ping", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  assert.equal(bus.count(READY_EVENT), 1);
  assert.deepEqual(bus.lastPayload(READY_EVENT), {});

  const reply = once(bus, replyChannel(PING_CHANNEL, "ping-1"));
  bus.emit(PING_CHANNEL, { requestId: "ping-1" });

  assert.deepEqual(await reply, { success: true, data: { version: 2 } });
});

test("spawn forwards the normalized pi-tasks request and returns the launched run id", async () => {
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

test("spawn supports documented aliases and keeps custom agent names unchanged", async () => {
  const cases = [
    { type: "general-purpose", expectedAgent: "delegate" },
    { type: "Explore", expectedAgent: "scout" },
    { type: "explore", expectedAgent: "scout" },
    { type: "my-agent", expectedAgent: "my-agent" },
  ] as const;

  for (const { type, expectedAgent } of cases) {
    const bus = new FakeEventBus();
    registerBridge({ events: bus });

    const reply = once(bus, replyChannel(SPAWN_CHANNEL, `spawn-${type}`));
    bus.emit(SPAWN_CHANNEL, {
      requestId: `spawn-${type}`,
      type,
      prompt: "Do the task",
    });

    const request = bus.lastPayload(NB_REQUEST_CHANNEL);
    assert.ok(isRecord(request));
    assert.ok(isRecord(request.params));
    assert.equal(request.params.agent, expectedAgent);

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

test("spawn rejects missing required pi-tasks fields", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-invalid"));
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-invalid",
    type: "general-purpose",
  });

  assert.deepEqual(await reply, {
    success: false,
    error: "spawn requires string type and prompt",
  });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), 0);
});

test("spawn accepts fallback run ids and surfaces reply-shape errors", async () => {
  const successCases = [
    {
      label: "details.asyncId",
      data: { details: { asyncId: "async-only" } },
      expectedId: "async-only",
    },
    {
      label: "top-level runId",
      data: { runId: "top-run" },
      expectedId: "top-run",
    },
    {
      label: "top-level asyncId",
      data: { asyncId: "top-async" },
      expectedId: "top-async",
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
      data: successCase.data,
    });

    assert.deepEqual(await reply, {
      success: true,
      data: { id: successCase.expectedId },
    });
  }

  const failureCases = [
    {
      label: "malformed-reply",
      payload: { requestId: "req", method: "spawn" },
      expectedError: "Malformed nicobailon RPC reply.",
    },
    {
      label: "missing-run-id",
      payload: {
        version: 1,
        requestId: "req",
        success: true,
        data: { details: {} },
      },
      expectedError: "nicobailon spawn reply did not include a run id",
    },
    {
      label: "remote-error-without-message",
      payload: {
        version: 1,
        requestId: "req",
        success: false,
        error: { code: "bad" },
      },
      expectedError: "nicobailon RPC error",
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
      type: "general-purpose",
      prompt: "Do the task",
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

test("spawn timeout uses the configured timeout and cleans up the listener", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus }, { spawnTimeoutMs: 1 });

  const reply = once(bus, replyChannel(SPAWN_CHANNEL, "spawn-timeout"), 250);
  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-timeout",
    type: "general-purpose",
    prompt: "Do the task",
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

test("stop always replies success, forwards owned runs, and ignores unknown runs", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-stop");

  for (const requestShape of [
    { requestId: "stop-agentId", agentId: "run-stop" },
    { requestId: "stop-id", id: "run-stop" },
    { requestId: "stop-runId", runId: "run-stop" },
  ]) {
    const before = bus.count(NB_REQUEST_CHANNEL);
    const reply = once(
      bus,
      replyChannel(STOP_CHANNEL, String(requestShape.requestId)),
    );
    bus.emit(STOP_CHANNEL, requestShape);

    assert.deepEqual(await reply, { success: true, data: undefined });
    assert.equal(bus.count(NB_REQUEST_CHANNEL), before + 1);
    const request = bus.lastPayload(NB_REQUEST_CHANNEL);
    assert.ok(isRecord(request));
    assert.equal(request.method, "stop");
    assert.deepEqual(request.params, { id: "run-stop" });
  }

  const before = bus.count(NB_REQUEST_CHANNEL);
  const reply = once(bus, replyChannel(STOP_CHANNEL, "stop-unknown"));
  bus.emit(STOP_CHANNEL, { requestId: "stop-unknown", agentId: "not-owned" });

  assert.deepEqual(await reply, { success: true, data: undefined });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), before);
});

test("completion events use the documented result and error fallbacks", async () => {
  const completedCases = [
    {
      label: "summary",
      payload: {
        runId: "run-summary",
        success: true,
        state: "complete",
        summary: "summary text",
        output: "output text",
        results: [{ output: "child output" }],
      },
      expected: { id: "run-summary", result: "summary text" },
    },
    {
      label: "top-level output",
      payload: {
        runId: "run-output",
        success: true,
        state: "complete",
        output: "output text",
      },
      expected: { id: "run-output", result: "output text" },
    },
    {
      label: "child outputs",
      payload: {
        runId: "run-children",
        success: true,
        state: "complete",
        results: [{ output: "child one" }, { error: "child two" }],
      },
      expected: { id: "run-children", result: "child one\n\nchild two" },
    },
    {
      label: "no result text",
      payload: {
        runId: "run-empty",
        success: true,
        state: "complete",
      },
      expected: { id: "run-empty" },
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
      label: "explicit error",
      payload: {
        runId: "run-failed",
        success: false,
        state: "failed",
        error: "failed error",
      },
      expected: { id: "run-failed", error: "failed error", status: "failed" },
    },
    {
      label: "aborted child error fallback",
      payload: {
        runId: "run-aborted",
        success: false,
        state: "aborted",
        results: [{ error: "child failure" }],
      },
      expected: { id: "run-aborted", error: "child failure", status: "failed" },
    },
    {
      label: "generic failure fallback",
      payload: {
        runId: "run-generic",
        success: false,
        results: [{}],
      },
      expected: { id: "run-generic", error: "Agent failed", status: "failed" },
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
      label: "paused child output",
      payload: {
        runId: "run-paused",
        success: false,
        state: "paused",
        summary: "Paused after interrupt. Waiting for explicit next action.",
        results: [{ output: "partial output", success: false }],
      },
      expected: {
        id: "run-paused",
        result: "partial output",
        status: "stopped",
      },
    },
    {
      label: "stopped top-level output fallback",
      payload: {
        runId: "run-stopped",
        success: true,
        state: "stopped",
        output: "partial top-level output",
      },
      expected: {
        id: "run-stopped",
        result: "partial top-level output",
        status: "stopped",
      },
    },
    {
      label: "stopped summary fallback",
      payload: {
        runId: "run-stopped-summary",
        success: true,
        state: "stopped",
        summary: "stopped summary",
      },
      expected: {
        id: "run-stopped-summary",
        result: "stopped summary",
        status: "stopped",
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

test("completion handling supports runId fallbacks, ignores unrelated events, and dedupes repeats", async () => {
  const bus = new FakeEventBus();
  registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-fallback");

  bus.emit(NB_COMPLETE_EVENT, {
    id: "not-owned",
    success: true,
    state: "complete",
    summary: "ignored",
  });
  assert.equal(bus.count(COMPLETED_EVENT), 0);
  assert.equal(bus.count(FAILED_EVENT), 0);

  bus.emit(NB_COMPLETE_EVENT, {
    asyncId: "run-fallback",
    success: true,
    state: "complete",
    summary: "first",
  });
  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-fallback",
    success: true,
    state: "complete",
    summary: "second",
  });

  assert.equal(bus.count(COMPLETED_EVENT), 1);
  assert.deepEqual(bus.lastPayload(COMPLETED_EVENT), {
    id: "run-fallback",
    result: "first",
  });
});

test("re-register keeps active run ownership for stop and completion", async () => {
  const bus = new FakeEventBus();
  const firstBridge = registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-stop-after-reregister");
  await spawnOwnedRun(bus, "run-complete-after-reregister");

  firstBridge.dispose();
  registerBridge({ events: bus });

  const stopReply = once(
    bus,
    replyChannel(STOP_CHANNEL, "stop-after-reregister"),
  );
  const requestsBeforeStop = bus.count(NB_REQUEST_CHANNEL);
  bus.emit(STOP_CHANNEL, {
    requestId: "stop-after-reregister",
    agentId: "run-stop-after-reregister",
  });

  assert.deepEqual(await stopReply, { success: true, data: undefined });
  assert.equal(bus.count(NB_REQUEST_CHANNEL), requestsBeforeStop + 1);
  const stopRequest = bus.lastPayload(NB_REQUEST_CHANNEL);
  assert.ok(isRecord(stopRequest));
  assert.equal(stopRequest.version, 1);
  assert.equal(stopRequest.method, "stop");
  assert.deepEqual(stopRequest.params, { id: "run-stop-after-reregister" });

  const completed = once(bus, COMPLETED_EVENT);
  bus.emit(NB_COMPLETE_EVENT, {
    runId: "run-complete-after-reregister",
    success: true,
    state: "complete",
    summary: "done after re-register",
  });

  assert.deepEqual(await completed, {
    id: "run-complete-after-reregister",
    result: "done after re-register",
  });
});

test("dispose cancels in-flight spawn work and ignores late replies", async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge({ events: bus }, { spawnTimeoutMs: 100 });

  bus.emit(SPAWN_CHANNEL, {
    requestId: "spawn-disposed",
    type: "general-purpose",
    prompt: "Do the task",
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
    data: { details: { runId: "late-run" } },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(bus.count(replyChannel(SPAWN_CHANNEL, "spawn-disposed")), 0);
});

test("dispose unsubscribes handlers and ignores later events until re-registered", async () => {
  const bus = new FakeEventBus();
  const bridge = registerBridge({ events: bus });
  await spawnOwnedRun(bus, "run-dispose");

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
    runId: "run-dispose",
    success: true,
    state: "complete",
    summary: "should not emit",
  });
  assert.equal(bus.count(COMPLETED_EVENT), 0);
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

  listenerCount(event: string): number {
    return this.handlers.get(event)?.size ?? 0;
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
