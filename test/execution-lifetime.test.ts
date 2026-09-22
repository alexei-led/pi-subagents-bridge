import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { registerPlanExecRpc, PLAN_EXEC_V2_REPLY_PREFIX, PLAN_EXEC_V2_REQUEST_EVENT } from "../src/plan-exec-rpc.js";

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
  return JSON.stringify(value);
}

const capabilities = {
  asyncSpawn: true,
  stop: true,
  processTerminalProof: { version: 1, lifecycleArtifactVersion: 1 },
  events: { processTerminal: "subagent:process-terminal" },
};

function fingerprint(params: Record<string, unknown>): string {
  return `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
}

function harness(journalPath: string, native: (method: string, params: Record<string, unknown>) => unknown) {
  const emitter = new EventEmitter();
  const bus = {
    on(event: string, handler: (value: unknown) => void) { emitter.on(event, handler); return () => { emitter.off(event, handler); }; },
    emit(event: string, value: unknown) { emitter.emit(event, value); },
  };
  emitter.on("subagents:rpc:v1:request", (raw: { requestId: string; method: string; params: Record<string, unknown> }) => {
    const data = native(raw.method, raw.params);
    if (data !== undefined) queueMicrotask(() => bus.emit(`subagents:rpc:v1:reply:${raw.requestId}`, { version: 1, requestId: raw.requestId, method: raw.method, success: true, data }));
  });
  const rpc = registerPlanExecRpc(bus, { journalPath, timeoutMs: 20 });
  let sequence = 0;
  const request = (method: string, body = {}): Promise<Record<string, unknown>> => new Promise((resolve) => {
    const requestId = String(++sequence);
    emitter.once(`${PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, resolve);
    bus.emit(PLAN_EXEC_V2_REQUEST_EVENT, { version: 2, requestId, method, ...body });
  });
  return { bus, emitter, request, dispose: () => rpc.dispose() };
}

function temporary(t: { after(callback: () => void): void }): { root: string; journalPath: string } {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-v2lite-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  return { root, journalPath: path.join(root, "operations.sqlite") };
}

function spawnBody(operationId: string, params: Record<string, unknown>) {
  const digest = fingerprint(params);
  return {
    digest,
    body: { operationId, owner: { kind: "pi-plan-exec", runId: "plan", key: operationId, requestDigest: digest }, params },
  };
}

test("a bounded lifetime is forwarded as timeoutMs and echoed without provider attestation", async (t) => {
  const { journalPath } = temporary(t);
  const params = { agent: "worker", task: "Review", executionLifetime: { mode: "bounded", timeoutMs: 5_000 } };
  const { body } = spawnBody("operation-bounded", params);
  let spawned: Record<string, unknown> | undefined;
  const h = harness(journalPath, (method, input) => {
    if (method === "ping") return { capabilities };
    if (method === "spawn") { spawned = input; return { details: { runId: "run-1", asyncDir: "/tmp/async-1" } }; }
    throw new Error(`unexpected upstream method ${method}`);
  });
  t.after(() => h.dispose());

  const reply = await h.request("spawn", body);
  assert.equal(reply.success, true, JSON.stringify(reply));
  const data = reply.data as Record<string, unknown>;
  assert.equal(data.runId, "run-1");
  assert.deepEqual(data.effectiveExecutionLifetime, { mode: "bounded", timeoutMs: 5_000 });
  assert.ok(spawned);
  assert.equal(spawned.timeoutMs, 5_000);
  assert.equal(spawned.executionLifetime, undefined);
  assert.equal(spawned.executionOwnership, undefined);
  assert.match(String(spawned.workflowScript), /runs\.run/);
});

test("an unbounded lifetime forwards no timeout", async (t) => {
  const { journalPath } = temporary(t);
  const params = { agent: "worker", task: "Review", executionLifetime: { mode: "unbounded" } };
  const { body } = spawnBody("operation-unbounded", params);
  let spawned: Record<string, unknown> | undefined;
  const h = harness(journalPath, (method, input) => {
    if (method === "ping") return { capabilities };
    if (method === "spawn") { spawned = input; return { details: { runId: "run-2" } }; }
    throw new Error(`unexpected upstream method ${method}`);
  });
  t.after(() => h.dispose());

  const reply = await h.request("spawn", body);
  assert.equal(reply.success, true);
  assert.deepEqual((reply.data as Record<string, unknown>).effectiveExecutionLifetime, { mode: "unbounded" });
  assert.equal(spawned?.timeoutMs, undefined);
});

test("a lost spawn reply is never redispatched", async (t) => {
  const { journalPath } = temporary(t);
  const { body } = spawnBody("operation-lost", { agent: "worker", task: "Review", executionLifetime: { mode: "unbounded" } });
  let spawns = 0;
  const h = harness(journalPath, (method) => {
    if (method === "ping") return { capabilities };
    if (method === "spawn") { spawns += 1; return undefined; }
    throw new Error(`unexpected upstream method ${method}`);
  });
  t.after(() => h.dispose());

  const first = await h.request("spawn", body);
  assert.equal(first.success, false);
  const second = await h.request("spawn", body);
  assert.equal(second.success, false);
  const secondError = second.error as { message?: string } | undefined;
  assert.match(secondError?.message ?? "", /unknown/i);
  assert.equal(spawns, 1);
});

test("cancel before dispatch reports a never-started fence without calling upstream", async (t) => {
  const { journalPath } = temporary(t);
  const { body } = spawnBody("operation-cancel", { agent: "worker", task: "Review", executionLifetime: { mode: "unbounded" } });
  let stops = 0;
  const h = harness(journalPath, (method) => {
    if (method === "ping") return { capabilities };
    if (method === "spawn") return undefined;
    if (method === "stop") { stops += 1; return { runId: "never", state: "stopping" }; }
    throw new Error(`unexpected upstream method ${method}`);
  });
  t.after(() => h.dispose());

  await h.request("spawn", body);
  const cancelled = await h.request("cancelOperation", {
    operationId: "operation-cancel",
    requestDigest: body.owner.requestDigest,
    owner: { kind: "pi-plan-exec", runId: "plan", key: "operation-cancel", requestDigest: body.owner.requestDigest },
  });
  assert.equal(cancelled.success, true, JSON.stringify(cancelled));
  const data = cancelled.data as Record<string, unknown>;
  assert.equal(data.state, "cancelled");
  assert.equal(data.neverStarted, true);
  assert.equal(stops, 0);
});

test("a bound operation survives restart and exposes the upstream terminal proof", async (t) => {
  const { journalPath } = temporary(t);
  const { body } = spawnBody("operation-restart", { agent: "worker", task: "Review", executionLifetime: { mode: "bounded", timeoutMs: 9_000 } });
  const first = harness(journalPath, (method) => {
    if (method === "ping") return { capabilities };
    if (method === "spawn") return { details: { runId: "run-restart" } };
    throw new Error(`unexpected upstream method ${method}`);
  });
  const launched = await first.request("spawn", body);
  assert.equal(launched.success, true);
  first.dispose();

  let spawns = 0;
  const second = harness(journalPath, (method) => {
    if (method === "ping") return { capabilities };
    spawns += 1;
    throw new Error(`unexpected upstream method ${method}`);
  });
  t.after(() => second.dispose());
  await second.request("ping");

  const lookupBody = {
    operationId: "operation-restart",
    requestDigest: body.owner.requestDigest,
    owner: { kind: "pi-plan-exec", runId: "plan", key: "operation-restart", requestDigest: body.owner.requestDigest },
  };
  const lookup = await second.request("operation", lookupBody);
  assert.equal(lookup.success, true, JSON.stringify(lookup));
  assert.equal((lookup.data as Record<string, unknown>).state, "found");
  assert.equal((lookup.data as Record<string, unknown>).runId, "run-restart");

  second.emitter.emit("subagent:process-terminal", {
    version: 1, state: "observed", runId: "run-restart", runnerProcessInstanceId: "runner-1",
    observedAt: Date.now(), writers: {},
  });
  const withProof = await second.request("operation", lookupBody);
  const proof = (withProof.data as Record<string, unknown>).processTerminalProof as Record<string, unknown>;
  assert.equal(proof.runId, "run-restart");
  assert.equal(proof.state, "observed");
  assert.equal(spawns, 0);
});
