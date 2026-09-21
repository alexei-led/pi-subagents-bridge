import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { registerPlanExecRpc, PLAN_EXEC_V2_REPLY_PREFIX, PLAN_EXEC_V2_REQUEST_EVENT } from "../src/plan-exec-rpc.js";
import { parseExecutionLifetime } from "../src/execution-lifetime.js";
import { OperationJournal } from "../src/operation-journal.js";

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
  return JSON.stringify(value);
}

const capabilities = {
  asyncSpawn: true,
  executionLifetime: { version: 1, modes: ["unbounded", "bounded"] },
  durableOperations: { version: 1, lookup: true, replay: true, cancelFence: true, scope: "repository" },
};

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
  return { request, dispose: () => rpc.dispose() };
}

test("explicit lifetime reaches emitted workflow and real child parameters, and survives restart and lost spawn replies", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lifetime-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const params = { agent: "worker", task: "Long silent tool", executionLifetime: { mode: "unbounded" } };
  const digest = `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
  const owner = { kind: "pi-plan-exec", runId: "plan", key: "operation", requestDigest: digest };
  const body = { operationId: "operation", owner, params };
  let launches = 0;
  let record: Record<string, unknown> | undefined;
  const native = (method: string, input: Record<string, unknown>): unknown => {
    if (method === "ping") return { capabilities };
    assert.equal(input.operationId, "operation");
    assert.equal(input.digest, digest);
    if (method === "spawn") {
      if (record) return record;
      launches++;
      assert.deepEqual(input.executionLifetime, { mode: "unbounded" });
      assert.equal(input.timeoutMs, undefined);
      const runs = { run(_name: string, child: Record<string, unknown>) {
        assert.equal(child.async, true);
        assert.equal(JSON.stringify(child.executionLifetime), JSON.stringify({ mode: "unbounded" }));
        assert.equal(child.timeoutMs, undefined);
        assert.equal(child.agent, "worker");
      } };
      vm.runInNewContext(`(function() { ${String(input.workflowScript)} })()`, { runs });
      record = { state: "found", operationId: "operation", digest, runId: "native-run", effectiveExecutionLifetime: params.executionLifetime };
      return undefined;
    }
    if (method === "cancel") record = { ...record, state: "cancelled", cancellationRequested: true };
    return record;
  };
  const first = harness(path.join(root, "journal.sqlite"), native);
  assert.equal((await first.request("spawn", body)).success, false);
  first.dispose();
  const resumed = harness(path.join(root, "journal.sqlite"), native);
  t.after(resumed.dispose);
  const lookup = await resumed.request("operation", body);
  assert.equal((lookup.data as Record<string, unknown>).runId, "native-run");
  assert.deepEqual((lookup.data as Record<string, unknown>).effectiveExecutionLifetime, params.executionLifetime);
  const replay = await resumed.request("spawn", body);
  assert.equal(replay.success, true);
  assert.equal(launches, 1);
  const cancelled = await resumed.request("cancelOperation", body);
  assert.equal((cancelled.data as Record<string, unknown>).cancellationRequested, true);
  assert.equal((cancelled.data as Record<string, unknown>).processTerminalProof, undefined);
});

test("unsupported native runtime cannot advertise or dispatch explicit lifetime", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-lifetime-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const nativeMethods: string[] = [];
  const bridge = harness(path.join(root, "journal.sqlite"), (method) => {
    nativeMethods.push(method);
    return { capabilities: { asyncSpawn: true } };
  });
  t.after(bridge.dispose);
  const ping = await bridge.request("ping");
  assert.equal(((ping.data as Record<string, unknown>).capabilities as Record<string, unknown>).executionLifetime, undefined);
  const params = { agent: "worker", task: "work", executionLifetime: { mode: "unbounded" } };
  const requestDigest = `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
  const reply = await bridge.request("spawn", { operationId: "op", owner: { kind: "pi-plan-exec", runId: "plan", key: "op", requestDigest }, params });
  assert.equal(reply.success, false);
  assert.deepEqual(nativeMethods, ["ping", "ping"]);
});

test("lifetime validation rejects hidden sentinels and ambiguous objects", () => {
  for (const value of [{ mode: "bounded", timeoutMs: 0 }, { mode: "bounded", timeoutMs: Infinity }, { mode: "bounded", timeoutMs: 2_147_483_648 }, { mode: "unbounded", timeoutMs: 0 }]) {
    assert.equal(parseExecutionLifetime(value), undefined);
  }
  assert.deepEqual(parseExecutionLifetime({ mode: "bounded", timeoutMs: 10 }), { mode: "bounded", timeoutMs: 10 });
});

test("cancel before delayed dispatch persists its native fence through a bridge restart", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-fence-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const params = { agent: "worker", task: "work", executionLifetime: { mode: "bounded", timeoutMs: 1234 } };
  const digest = `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
  const owner = { kind: "pi-plan-exec", runId: "plan", key: "op", requestDigest: digest };
  const body = { operationId: "op", owner, params };
  let fenced = false;
  let launches = 0;
  const native = (method: string): unknown => {
    if (method === "ping") return { capabilities };
    if (method === "cancel") fenced = true;
    if (method === "spawn" && !fenced) launches++;
    return { operationId: "op", digest, state: fenced ? "cancelled" : "absent", cancellationRequested: fenced };
  };
  const first = harness(path.join(root, "journal.sqlite"), native);
  assert.equal((await first.request("cancelOperation", body)).success, true);
  first.dispose();
  const resumed = harness(path.join(root, "journal.sqlite"), native);
  t.after(resumed.dispose);
  assert.equal(((await resumed.request("operation", body)).data as Record<string, unknown>).state, "cancelled");
  assert.equal((await resumed.request("spawn", body)).success, false);
  assert.equal(launches, 0);
});

test("bounded lifetime and effective mode are preserved without legacy timeout aliases", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-bounded-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const params = { agent: "worker", task: "work", executionLifetime: { mode: "bounded", timeoutMs: 1234 } };
  const digest = `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
  const bridge = harness(path.join(root, "journal.sqlite"), (method, input) => {
    if (method === "ping") return { capabilities };
    assert.deepEqual(input.executionLifetime, params.executionLifetime);
    assert.equal(input.timeoutMs, undefined);
    vm.runInNewContext(`(function() { ${String(input.workflowScript)} })()`, {
      runs: { run(_name: string, child: Record<string, unknown>) {
        assert.equal(child.async, true);
        assert.equal(JSON.stringify(child.executionLifetime), JSON.stringify(params.executionLifetime));
      } },
    });
    return { runId: "bounded", effectiveExecutionLifetime: params.executionLifetime };
  });
  t.after(bridge.dispose);
  const result = await bridge.request("spawn", { operationId: "op", owner: { kind: "pi-plan-exec", runId: "plan", key: "op", requestDigest: digest }, params });
  assert.deepEqual((result.data as Record<string, unknown>).effectiveExecutionLifetime, params.executionLifetime);
});

test("a crash after local cancellation intent resumes the fence before any native dispatch", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-cancel-intent-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const journalPath = path.join(root, "journal.sqlite");
  const params = { agent: "worker", task: "work", executionLifetime: { mode: "unbounded" } };
  const digest = `sha256:${createHash("sha256").update(canonical({ params })).digest("hex")}`;
  const persisted = new OperationJournal(journalPath).requestNativeCancel("op", digest, "plan");
  assert.equal(persisted.nativeCorrelated, true);
  assert.equal(persisted.cancelRequested, true);
  let cancelled = 0;
  const resumed = harness(journalPath, (method) => {
    if (method === "ping") return { capabilities };
    assert.equal(method, "cancel");
    cancelled++;
    return { state: "cancelled", operationId: "op", digest, neverStarted: true };
  });
  t.after(resumed.dispose);
  const reply = await resumed.request("spawn", { operationId: "op", owner: { kind: "pi-plan-exec", runId: "plan", key: "op", requestDigest: digest }, params });
  assert.equal(reply.success, false);
  assert.equal(cancelled, 1);
});
