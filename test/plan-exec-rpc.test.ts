import assert from "node:assert/strict";
import test from "node:test";
import {
  PLAN_EXEC_REPLY_PREFIX,
  PLAN_EXEC_REQUEST_EVENT,
  registerPlanExecRpc,
} from "../src/plan-exec-rpc.js";

const SUBAGENTS_REQUEST_EVENT = "subagents:rpc:v1:request";
const SUBAGENTS_REPLY_PREFIX = "subagents:rpc:v1:reply:";

test("plan-exec spawn forwards actual pi-subagents parameters and coalesces a durable operation", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const request = {
    version: 1,
    method: "spawn",
    operationId: "operation-1",
    cwd: "/tmp/shared-worktree",
    params: {
      agent: "worker",
      task: "Implement the task.",
      async: true,
      clarify: false,
      context: "fresh",
      model: "test/model",
      turnBudget: { maxTurns: 40 },
      toolBudget: { hard: 10 },
      control: { enabled: false },
      acceptance: { level: "verified" },
      timeout: 30_000,
    },
  } as const;
  const firstReply = once(bus, replyEvent("spawn-1"));
  const duplicateReply = once(bus, replyEvent("spawn-2"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: "spawn-1" });
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: "spawn-2" });

  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.equal(upstream.version, 1);
  assert.equal(upstream.method, "spawn");
  assert.deepEqual(upstream.params, {
    agent: "worker",
    task: "Implement the task.",
    async: true,
    clarify: false,
    context: "fresh",
    model: "test/model",
    turnBudget: { maxTurns: 40 },
    toolBudget: { hard: 10 },
    control: { enabled: false },
    acceptance: { level: "verified" },
    cwd: "/tmp/shared-worktree",
    timeoutMs: 30_000,
  });

  replyUpstream(bus, upstream, "spawn", {
    details: { runId: "run-1", asyncDir: "/tmp/async-run" },
  });
  const expected = {
    success: true,
    data: { runId: "run-1", asyncDir: "/tmp/async-run" },
  };
  assert.deepEqual(await firstReply, expected);
  assert.deepEqual(await duplicateReply, expected);

  const replay = once(bus, replyEvent("spawn-3"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: "spawn-3" });
  assert.deepEqual(await replay, expected);
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  const conflict = once(bus, replyEvent("spawn-conflict"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    ...request,
    requestId: "spawn-conflict",
    params: { ...request.params, task: "Different task." },
  });
  assert.deepEqual(await conflict, {
    success: false,
    error: {
      code: "invalid_request",
      message: "spawn operationId was already used with different parameters",
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test("plan-exec accepts cwd from params and rejects conflicting cwd values", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const reply = once(bus, replyEvent("params-cwd"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "params-cwd",
    method: "spawn",
    operationId: "params-cwd-operation",
    params: {
      agent: "worker",
      task: "Use the requested worktree.",
      cwd: "/tmp/params-worktree",
    },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.ok(isRecord(upstream.params));
  assert.equal(upstream.params.cwd, "/tmp/params-worktree");
  assert.equal(upstream.params.async, true);
  assert.equal(upstream.params.clarify, false);
  replyUpstream(bus, upstream, "spawn", {
    details: { runId: "params-cwd-run" },
  });
  assert.deepEqual(await reply, {
    success: true,
    data: { runId: "params-cwd-run" },
  });

  const conflict = once(bus, replyEvent("cwd-conflict"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "cwd-conflict",
    method: "spawn",
    operationId: "cwd-conflict-operation",
    cwd: "/tmp/top-level",
    params: {
      agent: "worker",
      task: "This must be rejected.",
      cwd: "/tmp/params",
    },
  });
  assert.deepEqual(await conflict, {
    success: false,
    error: {
      code: "invalid_request",
      message:
        "spawn cwd must match when supplied at both the request and params levels",
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test("plan-exec retains an in-flight operation across re-registration", async (t) => {
  const bus = new FakeEventBus();
  const first = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => first.dispose());

  const request = {
    version: 1,
    method: "spawn",
    operationId: "durable-operation",
    params: { agent: "worker", task: "Launch once." },
  } as const;
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: "first-attempt" });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));

  first.dispose();
  const second = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => second.dispose());
  const retry = once(bus, replyEvent("retry"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, { ...request, requestId: "retry" });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  replyUpstream(bus, upstream, "spawn", {
    details: { runId: "durable-run", asyncDir: "/tmp/durable-run" },
  });
  assert.deepEqual(await retry, {
    success: true,
    data: { runId: "durable-run", asyncDir: "/tmp/durable-run" },
  });
});

test("plan-exec reports operation lookup states without launching duplicates", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const absent = once(bus, replyEvent("operation-absent"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "operation-absent",
    method: "operation",
    operationId: "missing",
  });
  assert.deepEqual(await absent, { success: true, data: { state: "absent" } });

  const spawned = once(bus, replyEvent("spawn-pending"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "spawn-pending",
    method: "spawn",
    operationId: "operation-1",
    params: { agent: "worker", task: "Do work." },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));

  const pending = once(bus, replyEvent("operation-pending"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "operation-pending",
    method: "operation",
    operationId: "operation-1",
  });
  assert.deepEqual(await pending, { success: true, data: { state: "pending" } });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);

  replyUpstream(bus, upstream, "spawn", { details: { runId: "run-1" } });
  await spawned;
  const found = once(bus, replyEvent("operation-found"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "operation-found",
    method: "operation",
    operationId: "operation-1",
  });
  assert.deepEqual(await found, {
    success: true,
    data: { state: "found", runId: "run-1" },
  });
});

test("plan-exec retains failed operation lookup across re-registration", async (t) => {
  const bus = new FakeEventBus();
  const first = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => first.dispose());

  const spawned = once(bus, replyEvent("spawn-failed"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "spawn-failed",
    method: "spawn",
    operationId: "failed-operation",
    params: { agent: "worker", task: "Fail once." },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  bus.emit(upstreamReplyEvent(String(upstream.requestId)), {
    version: 1,
    requestId: upstream.requestId,
    method: "spawn",
    success: false,
    error: { message: "provider unavailable" },
  });
  assert.deepEqual(await spawned, {
    success: false,
    error: { code: "upstream_error", message: "provider unavailable" },
  });

  first.dispose();
  const second = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => second.dispose());
  const lookup = once(bus, replyEvent("failed-operation-lookup"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "failed-operation-lookup",
    method: "operation",
    operationId: "failed-operation",
  });
  assert.deepEqual(await lookup, {
    success: true,
    data: { state: "unknown", error: "provider unavailable" },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 1);
});

test("plan-exec validates operation lookup requests", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  for (const [requestId, operationId] of [
    ["missing-operation-id", undefined],
    ["empty-operation-id", ""],
  ] as const) {
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method: "operation",
      ...(operationId === undefined ? {} : { operationId }),
    });
    assert.deepEqual(await reply, {
      success: false,
      error: {
        code: "invalid_request",
        message: "operation requires a non-empty operationId",
      },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test("plan-exec bounds completed operation history without evicting the newest outcome", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  for (let index = 0; index < 129; index += 1) {
    const requestId = `history-spawn-${index}`;
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method: "spawn",
      operationId: `history-operation-${index}`,
      params: { agent: "worker", task: `Work ${index}.` },
    });
    const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
    assert.ok(isRecord(upstream));
    replyUpstream(bus, upstream, "spawn", {
      details: { runId: `history-run-${index}` },
    });
    await reply;
  }

  const oldest = once(bus, replyEvent("history-oldest"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "history-oldest",
    method: "operation",
    operationId: "history-operation-0",
  });
  assert.deepEqual(await oldest, { success: true, data: { state: "absent" } });

  const newest = once(bus, replyEvent("history-newest"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "history-newest",
    method: "operation",
    operationId: "history-operation-128",
  });
  assert.deepEqual(await newest, {
    success: true,
    data: { state: "found", runId: "history-run-128" },
  });
});

test("plan-exec refuses a new spawn when operation history is all active", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  for (let index = 0; index < 128; index += 1) {
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId: `active-spawn-${index}`,
      method: "spawn",
      operationId: `active-operation-${index}`,
      params: { agent: "worker", task: `Work ${index}.` },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 128);

  const reply = once(bus, replyEvent("capacity"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "capacity",
    method: "spawn",
    operationId: "active-operation-128",
    params: { agent: "worker", task: "One too many." },
  });
  assert.deepEqual(await reply, {
    success: false,
    error: {
      code: "operation_capacity",
      message: "plan-exec operation history is full of active operations",
    },
  });
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 128);
});

test("plan-exec normalizes status, result, and observational adoption", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const expected = {
    runId: "run-1",
    state: "complete",
    asyncDir: "/tmp/async-run",
    resultPath: "/tmp/result.json",
    text: "Run: run-1\nState: complete\nDir: /tmp/async-run\nResult: /tmp/result.json",
  };
  for (const method of ["status", "result", "adopt"] as const) {
    const requestId = `${method}-1`;
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, {
      version: 1,
      requestId,
      method,
      params: { runId: "run-1", asyncDir: "/tmp/async-run" },
    });
    const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
    assert.ok(isRecord(upstream));
    assert.equal(upstream.method, "status");
    assert.deepEqual(upstream.params, { id: "run-1", dir: "/tmp/async-run" });
    replyUpstream(bus, upstream, "status", { text: expected.text });
    assert.deepEqual(await reply, {
      success: true,
      data: {
        ...expected,
        ...(method === "adopt" ? { observed: true } : {}),
      },
    });
  }
});

test("plan-exec stop forwards asyncDir and normalizes the upstream response", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const reply = once(bus, replyEvent("stop-1"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "stop-1",
    method: "stop",
    params: { runId: "run-1", asyncDir: "/tmp/async-run" },
  });
  const upstream = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(upstream));
  assert.equal(upstream.method, "stop");
  assert.deepEqual(upstream.params, { id: "run-1", dir: "/tmp/async-run" });
  replyUpstream(bus, upstream, "stop", {
    runId: "resolved-run",
    asyncDir: "/tmp/resolved-run",
    previousState: "running",
    state: "stopping",
    message: "Stop requested.",
  });
  assert.deepEqual(await reply, {
    success: true,
    data: {
      runId: "resolved-run",
      asyncDir: "/tmp/resolved-run",
      state: "stopping",
    },
  });
});

test("plan-exec validates its request contract before emitting upstream RPC", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const cases = [
    {
      requestId: "bad-version",
      payload: { version: 2, method: "ping" },
      expected: "unsupported plan-exec RPC version: 2",
    },
    {
      requestId: "missing-method",
      payload: { version: 1 },
      expected: "request requires a method",
    },
    {
      requestId: "missing-operation",
      payload: {
        version: 1,
        method: "spawn",
        params: { agent: "worker", task: "Missing ID." },
      },
      expected: "spawn requires a non-empty operationId",
    },
    {
      requestId: "sync-spawn",
      payload: {
        version: 1,
        method: "spawn",
        operationId: "sync-operation",
        params: { agent: "worker", task: "Not allowed.", async: false },
      },
      expected: "spawn only supports detached async execution",
    },
    {
      requestId: "clarify-spawn",
      payload: {
        version: 1,
        method: "spawn",
        operationId: "clarify-operation",
        params: { agent: "worker", task: "Not allowed.", clarify: true },
      },
      expected: "spawn cannot set clarify to true",
    },
    {
      requestId: "bad-adopt",
      payload: {
        version: 1,
        method: "adopt",
        params: { runId: "" },
      },
      expected: "adopt requires a non-empty runId",
    },
  ] as const;

  for (const { requestId, payload, expected } of cases) {
    const reply = once(bus, replyEvent(requestId));
    bus.emit(PLAN_EXEC_REQUEST_EVENT, { requestId, ...payload });
    assert.deepEqual(await reply, {
      success: false,
      error: { code: "invalid_request", message: expected },
    });
  }
  assert.equal(bus.count(SUBAGENTS_REQUEST_EVENT), 0);
});

test("plan-exec normalizes upstream errors and rejects malformed upstream envelopes", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const rejected = once(bus, replyEvent("status-rejected"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "status-rejected",
    method: "status",
    params: { runId: "missing-run" },
  });
  const rejectedRequest = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(rejectedRequest));
  bus.emit(upstreamReplyEvent(String(rejectedRequest.requestId)), {
    version: 1,
    requestId: rejectedRequest.requestId,
    method: "status",
    success: false,
    error: { code: "not_found", message: "Async run not found" },
  });
  assert.deepEqual(await rejected, {
    success: false,
    error: { code: "upstream_error", message: "Async run not found" },
  });

  const malformed = once(bus, replyEvent("status-malformed"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "status-malformed",
    method: "status",
    params: { runId: "run-1" },
  });
  const malformedRequest = bus.last(SUBAGENTS_REQUEST_EVENT);
  assert.ok(isRecord(malformedRequest));
  bus.emit(upstreamReplyEvent(String(malformedRequest.requestId)), {
    version: 2,
    requestId: malformedRequest.requestId,
    method: "status",
    success: true,
    data: { text: "Run: run-1\nState: running" },
  });
  assert.deepEqual(await malformed, {
    success: false,
    error: {
      code: "upstream_error",
      message: "Malformed pi-subagents RPC reply",
    },
  });
});

test("plan-exec ping advertises the supported generic methods", async (t) => {
  const bus = new FakeEventBus();
  const rpc = registerPlanExecRpc(bus, { timeoutMs: 100 });
  t.after(() => rpc.dispose());

  const reply = once(bus, replyEvent("ping-1"));
  bus.emit(PLAN_EXEC_REQUEST_EVENT, {
    version: 1,
    requestId: "ping-1",
    method: "ping",
  });
  assert.deepEqual(await reply, {
    success: true,
    data: {
      version: 1,
      methods: [
        "ping",
        "spawn",
        "operation",
        "status",
        "result",
        "stop",
        "adopt",
      ],
    },
  });
});

function replyEvent(requestId: string): string {
  return `${PLAN_EXEC_REPLY_PREFIX}${requestId}`;
}

function upstreamReplyEvent(requestId: string): string {
  return `${SUBAGENTS_REPLY_PREFIX}${requestId}`;
}

function replyUpstream(
  bus: FakeEventBus,
  request: Record<string, unknown>,
  method: "spawn" | "status" | "stop",
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
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
