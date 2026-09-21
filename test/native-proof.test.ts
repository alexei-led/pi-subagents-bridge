import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { attestNativeTerminalProof } from "../src/native-proof.js";
import { OperationJournal, type OperationJournalRecord } from "../src/operation-journal.js";
import { registerPlanExecRpc, PLAN_EXEC_V2_REPLY_PREFIX, PLAN_EXEC_V2_REQUEST_EVENT } from "../src/plan-exec-rpc.js";

function fixture() {
  const binding = { operationId: "kernel-operation", requestDigest: "kernel-request-digest", hostId: "host", bootId: "boot" };
  const identity = { ...binding, version: 1, backend: "darwin-resource-coalition-v1", coalitionId: "123",
    leader: { pid: 456, uniqueId: "process-generation", pidVersion: 7 } };
  const proof = { version: 1, state: "observed", runId: "native-run", runnerProcessInstanceId: "runner-instance", observedAt: 1234, instances: [],
    nativeOperation: { operationId: "native-operation", digest: "native-request-digest" },
    processTreeOwnership: { version: 1, scope: "owned-process-tree", escapedDescendants: "contained" },
    kernelBinding: binding,
    kernelProof: { status: "retired", operationDirectory: "/native/owned", binding: { ...binding }, identity: structuredClone(identity),
      proof: { ...binding, kind: "darwin-coalition-retired", observedAt: new Date(1234).toISOString(), identity: structuredClone(identity) } } };
  const operation: OperationJournalRecord = { operationId: "caller-operation", requestDigest: "caller-request-digest", ownerRunId: "plan",
    binding: "bound", runId: "native-run", createdAt: 1, updatedAt: 2, nativeCorrelated: true,
    executionLifetime: { mode: "unbounded" }, nativeParams: { agent: "worker", task: "work", async: true,
      executionOwnership: { mode: "kernel" }, executionLifetime: { mode: "unbounded" }, operationId: "native-operation", digest: "native-request-digest" } };
  return { proof, operation };
}

test("caller, native, and kernel namespaces remain distinct in an attested proof", () => {
  const { proof, operation } = fixture();
  const parsed = attestNativeTerminalProof({ ...proof, callerBinding: { operationId: "forged", requestDigest: "forged" } }, operation, "native-run");
  assert.ok(parsed);
  assert.deepEqual(parsed.callerBinding, { operationId: "caller-operation", requestDigest: "caller-request-digest" });
  assert.deepEqual(parsed.nativeOperation, proof.nativeOperation);
  assert.deepEqual(parsed.kernelBinding, proof.kernelBinding);
  assert.deepEqual(parsed.kernelProof, proof.kernelProof);
});

test("proof binding mismatches and weaker evidence cannot attest retirement", () => {
  const mutations: Array<(proof: ReturnType<typeof fixture>["proof"]) => void> = [
    (proof) => { proof.nativeOperation.digest = "caller-request-digest"; },
    (proof) => { proof.nativeOperation.operationId = "different-native-operation"; },
    (proof) => { proof.kernelBinding.requestDigest = "native-request-digest"; },
    (proof) => { proof.kernelProof.binding.hostId = "different-host"; },
    (proof) => { proof.kernelProof.identity.bootId = "different-boot"; },
    (proof) => { proof.kernelProof.proof.requestDigest = "different-request"; },
    (proof) => { proof.kernelProof.proof.identity.leader.uniqueId = "reused-process-id"; },
    (proof) => { proof.kernelProof.status = "active"; },
    (proof) => { proof.kernelProof.proof.kind = "never-started"; },
    (proof) => { proof.processTreeOwnership.scope = "posix-process-group"; },
    (proof) => { proof.observedAt = 2345; },
  ];
  for (const mutate of mutations) {
    const { proof, operation } = fixture();
    mutate(proof);
    assert.equal(attestNativeTerminalProof(proof, operation, "native-run"), undefined);
  }
  const { proof, operation } = fixture();
  delete operation.nativeParams;
  assert.equal(attestNativeTerminalProof(proof, operation, "native-run"), undefined);
});

test("status resolves native identity from the journal and refuses missing or ambiguous run mappings", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-proof-mapping-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const journal = new OperationJournal(path.join(root, "journal.sqlite"));
  const { proof, operation } = fixture();
  journal.begin(operation.operationId, operation.requestDigest, operation.ownerRunId, operation.executionLifetime, operation.nativeParams);
  journal.bind(operation.operationId, operation.requestDigest, "native-run");
  const emitter = new EventEmitter();
  const events = {
    on(event: string, handler: (value: unknown) => void) { emitter.on(event, handler); return () => { emitter.off(event, handler); }; },
    emit(event: string, value: unknown) { emitter.emit(event, value); },
  };
  let calls = 0;
  emitter.on("subagents:rpc:v1:request", (raw: { requestId: string; method: string; params: Record<string, unknown> }) => {
    calls++;
    assert.equal(raw.method, "lookup");
    assert.deepEqual(raw.params, { operationId: "native-operation", digest: "native-request-digest" });
    queueMicrotask(() => events.emit(`subagents:rpc:v1:reply:${raw.requestId}`, { version: 1, requestId: raw.requestId, method: raw.method,
      success: true, data: { operationId: "native-operation", digest: "native-request-digest", runId: "native-run", state: "found",
        status: "complete", processTerminalProof: proof, statusPayload: { processTerminalProof: proof } } }));
  });
  const rpc = registerPlanExecRpc(events, { timeoutMs: 100, journal });
  t.after(() => rpc.dispose());
  let sequence = 0;
  const status = (runId: string): Promise<{ success: boolean; data: Record<string, unknown> }> => new Promise((resolve) => {
    const requestId = String(++sequence);
    emitter.once(`${PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, resolve);
    events.emit(PLAN_EXEC_V2_REQUEST_EVENT, { version: 2, requestId, method: "status", params: { runId } });
  });
  const found = await status("native-run");
  assert.equal(found.data.state, "complete");
  assert.deepEqual(found.data.callerBinding, { operationId: "caller-operation", requestDigest: "caller-request-digest" });
  assert.deepEqual((found.data.processTerminalProof as Record<string, unknown>).callerBinding, found.data.callerBinding);
  proof.nativeOperation.digest = "incorrect-native-digest";
  const incorrect = await status("native-run");
  assert.equal(incorrect.data.state, "unknown");
  assert.equal(incorrect.data.processTerminalProof, undefined);
  assert.equal((incorrect.data.statusPayload as Record<string, unknown>).processTerminalProof, undefined);
  assert.equal((await status("missing-run")).data.state, "unknown");
  journal.begin("other-caller", "other-digest", "plan", { mode: "unbounded" }, operation.nativeParams);
  journal.bind("other-caller", "other-digest", "native-run");
  assert.equal((await status("native-run")).data.state, "unknown");
  assert.equal(calls, 2);
});
