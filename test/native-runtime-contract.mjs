import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, it } from "node:test";
import { createJiti } from "jiti";

const nativeRoot = process.env.PI_SUBAGENTS_SOURCE;
assert.ok(nativeRoot, "Set PI_SUBAGENTS_SOURCE to the native source checkout with its dependencies installed");
const manifest = JSON.parse(fs.readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const pinnedRevision = /#([a-f0-9]{40})$/.exec(manifest.devDependencies["pi-subagents"])?.[1];
assert.ok(pinnedRevision, "Native integration requires an immutable pi-subagents dependency pin");
assert.equal(execFileSync("git", ["-C", nativeRoot, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(), pinnedRevision);
execFileSync("git", ["-C", nativeRoot, "diff", "--quiet", "HEAD", "--"]);
const artifacts = fs.mkdtempSync(new URL("../.native-test-", import.meta.url));
process.env.PI_SUBAGENTS_TEMP_ROOT = artifacts;
await import(pathToFileURL(path.join(nativeRoot, "test/support/register-loader.mjs")).href);
const fixture = await import(pathToFileURL(path.join(nativeRoot, "test/support/async-execution-fixture.ts")).href);
const helpers = await import(pathToFileURL(path.join(nativeRoot, "test/support/helpers.ts")).href);
const native = await import(pathToFileURL(path.join(nativeRoot, "src/extension/rpc.ts")).href);
const bridge = await import("../src/plan-exec-rpc.ts");
const { readProcessTerminal } = await import(pathToFileURL(path.join(nativeRoot, "src/runs/background/process-terminal.ts")).href);

function assertFixtureProof(proof) {
  assert.equal(proof.dispatchClosed, true);
  if (proof.state === "observed") {
    assert.ok(proof.children.every((child) => child.instances.every((instance) => instance.kind !== "pi-writer")));
  } else {
    assert.equal(proof.state, "unknown");
    assert.match(proof.reason, /containment/);
  }
}

function childHasClosed(observation) {
  const childId = observation.data?.statusPayload?.steps?.[0]?.runId;
  return childId && readProcessTerminal(path.join(fixture.ASYNC_DIR, childId), { runId: childId })?.state === "observed";
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

describe("bridge with native RPC, async workflow, and detached child runner", () => {
  fixture.installAsyncExecutionHooks();
  it("keeps explicit lifetime and reconciles the actual generated child through native lookup", async (t) => {
    assert.ok(fixture.available && fixture.isAsyncAvailable(), "native async fixture must be available");
    const emitter = new EventEmitter();
    let dropSpawnReply = true;
    let nativeSpawnRequests = 0;
    const events = {
      on(event, handler) { emitter.on(event, handler); return () => emitter.off(event, handler); },
      emit(event, value) {
        if (event === "subagents:rpc:v1:request" && value.method === "spawn") nativeSpawnRequests++;
        if (dropSpawnReply && event.startsWith("subagents:rpc:v1:reply:") && value.method === "spawn") {
          dropSpawnReply = false;
          return;
        }
        emitter.emit(event, value);
      },
    };
    const ctx = helpers.makeMinimalCtx(fixture.tempDir);
    ctx.sessionManager.getSessionId = () => "bridge-native-contract";
    const state = { baseCwd: fixture.tempDir, currentSessionId: "bridge-native-contract", asyncJobs: new Map(),
      foregroundControls: new Map(), lastForegroundControlId: null, workflowControllers: new Map() };
    const executor = fixture.createSubagentExecutor({ pi: { events, getSessionName: () => undefined, sendMessage() {} }, state,
      config: { timeoutMs: 5 }, asyncByDefault: false, tempArtifactsDir: fixture.tempDir,
      getSubagentSessionRoot: () => fixture.tempDir, expandTilde: (value) => value,
      discoverAgents: () => ({ agents: [{ ...helpers.makeAgent("worker"), defaultTimeoutMs: 5 }] }), });
    fixture.mockPi.onCall({ delay: 200, output: "native detached child finished" });
    const nativeRpc = native.registerSubagentRpcBridge({ events, state, asyncDirRoot: fixture.ASYNC_DIR, getContext: () => ctx,
      execute: (...args) => executor.execute(...args), });
    const journalPath = path.join(artifacts, "bridge.sqlite");
    let bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 2000, journalPath });
    t.after(() => { bridgeRpc.dispose(); nativeRpc.dispose(); });
    let sequence = 0;
    const request = (method, body) => new Promise((resolve, reject) => {
      const requestId = `native-${++sequence}`;
      const timer = setTimeout(() => reject(new Error(`bridge ${method} response missing`)), 5000);
      emitter.once(`${bridge.PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, (reply) => { clearTimeout(timer); resolve(reply); });
      events.emit(bridge.PLAN_EXEC_V2_REQUEST_EVENT, { version: 2, requestId, method, ...body });
    });
    const params = { agent: "worker", task: "Say native detached child finished", executionLifetime: { mode: "unbounded" }, acceptance: false, mission: false, context: "fresh" };
    const requestDigest = `sha256:${createHash("sha256").update(canonical({ cwd: fixture.tempDir, params })).digest("hex")}`;
    const body = { cwd: fixture.tempDir, params, operationId: "bridge-native-operation",
      owner: { kind: "pi-plan-exec", runId: "plan", key: "bridge-native-operation", requestDigest } };
    if (process.env.PI_PLAN_EXEC_SOURCE) {
      const { BridgeClient } = await createJiti(import.meta.url).import(path.join(process.env.PI_PLAN_EXEC_SOURCE, "src/bridge.ts"));
      const client = new BridgeClient(events, 2000);
      const negotiated = await client.capabilities();
      assert.deepEqual(negotiated.processTreeOwnership, { version: 1, scope: "posix-process-group", escapedDescendants: "unverified" });
      const rejected = await client.spawn("strict-must-not-dispatch", params);
      assert.equal(rejected.success, false);
      assert.equal(rejected.error.code, "unsupported");
      assert.equal(nativeSpawnRequests, 0);
      t.diagnostic("Main BridgeClient rejected actual native process-group ownership before dispatch");
    }
    const lost = await request("spawn", body);
    assert.equal(lost.success, false, JSON.stringify(lost));
    const recovered = await request("operation", body);
    assert.equal(recovered.success, true, JSON.stringify(recovered));
    const spawned = await request("spawn", body);
    assert.equal(spawned.success, true, JSON.stringify(spawned));
    assert.equal(spawned.data.runId, recovered.data.runId);
    assert.deepEqual(spawned.data.effectiveExecutionLifetime, params.executionLifetime);
    const childRuntime = await fixture.waitForMockPiRuntime(fixture.mockPi, 0, 30_000);
    assert.deepEqual(childRuntime.executionLifetime, params.executionLifetime);
    const resultFile = await fixture.waitForAsyncResultFile(spawned.data.runId, 30_000);
    const result = JSON.parse(fs.readFileSync(resultFile, "utf8"));
    assert.equal(result.success, true, JSON.stringify(result));
    assert.doesNotMatch(result.output ?? result.summary ?? "", /remains running or uncollected/);
    assert.match(result.output ?? result.summary ?? "", /native detached child finished/);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 2000, journalPath });
    let lookup = await request("operation", body);
    const proofDeadline = Date.now() + 30_000;
    while (!childHasClosed(lookup) && Date.now() < proofDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      lookup = await request("operation", body);
    }
    assert.equal(lookup.success, true, JSON.stringify(lookup));
    assert.equal(lookup.data.runId, spawned.data.runId);
    assert.deepEqual(lookup.data.effectiveExecutionLifetime, params.executionLifetime);
    assertFixtureProof(lookup.data.workflowTerminalProof);
    const completedChild = lookup.data.statusPayload.steps[0].runId;
    assert.equal(readProcessTerminal(path.join(fixture.ASYNC_DIR, completedChild), { runId: completedChild }).state, "observed");
    assert.equal(fixture.mockPi.callCount(), 1);
    const fencedBody = { ...body, operationId: "cancel-before-dispatch", owner: { ...body.owner, key: "cancel-before-dispatch" } };
    const fenced = await request("cancelOperation", fencedBody);
    assert.equal(fenced.success, true, JSON.stringify(fenced));
    assert.equal(fenced.data.neverStarted, true);
    assert.equal(fenced.data.processTerminalProof, undefined);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 2000, journalPath });
    const delayed = await request("spawn", fencedBody);
    assert.equal(delayed.success, false, JSON.stringify(delayed));
    const cancellation = await request("operation", fencedBody);
    assert.equal(cancellation.data.neverStarted, true);
    assert.equal(cancellation.data.cancellationRequested, true);
    assert.equal(fixture.mockPi.callCount(), 1);
    fixture.mockPi.onCall({ delay: 10_000, output: "should be cancelled first" });
    const stoppingBody = { ...body, operationId: "cancel-live-child", owner: { ...body.owner, key: "cancel-live-child" } };
    const live = await request("spawn", stoppingBody);
    assert.equal(live.success, true, JSON.stringify(live));
    await fixture.waitForMockPiRuntime(fixture.mockPi, 1, 30_000);
    ctx.sessionManager.getSessionId = () => "restarted-session";
    const adopted = await request("adopt", { params: { runId: live.data.runId } });
    assert.equal(adopted.success, true, JSON.stringify(adopted));
    assert.equal(adopted.data.state, "running");
    const stop = await request("stop", { params: { runId: live.data.runId } });
    assert.equal(stop.success, true, JSON.stringify(stop));
    assert.equal(stop.data.cancellationRequested, true);
    assert.equal(stop.data.neverStarted, false);
    let stopped = await request("operation", stoppingBody);
    const stopDeadline = Date.now() + 30_000;
    while (!childHasClosed(stopped) && Date.now() < stopDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      stopped = await request("operation", stoppingBody);
    }
    assertFixtureProof(stopped.data.workflowTerminalProof);
    const childId = stopped.data.statusPayload.steps[0].runId;
    assert.equal(readProcessTerminal(path.join(fixture.ASYNC_DIR, childId), { runId: childId }).state, "observed");
    const childStatus = JSON.parse(fs.readFileSync(path.join(fixture.ASYNC_DIR, childId, "status.json"), "utf8"));
    assert.equal(childStatus.state, "stopped", JSON.stringify(childStatus));
    assert.equal(fixture.mockPi.callCount(), 2);
  });
});
