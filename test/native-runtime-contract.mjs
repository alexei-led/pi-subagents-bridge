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
if (process.env.PI_NATIVE_CONTRACT_DEVELOPMENT !== "1") {
  assert.equal(execFileSync("git", ["-C", nativeRoot, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(), pinnedRevision);
  execFileSync("git", ["-C", nativeRoot, "diff", "--quiet", "HEAD", "--"]);
}
const artifacts = fs.mkdtempSync(new URL("../.native-test-", import.meta.url));
process.env.PI_SUBAGENTS_TEMP_ROOT = artifacts;
await import(pathToFileURL(path.join(nativeRoot, "test/support/register-loader.mjs")).href);
const fixture = await import(pathToFileURL(path.join(nativeRoot, "test/support/async-execution-fixture.ts")).href);
const helpers = await import(pathToFileURL(path.join(nativeRoot, "test/support/helpers.ts")).href);
const native = await import(pathToFileURL(path.join(nativeRoot, "src/extension/rpc.ts")).href);
const bridge = await import("../src/plan-exec-rpc.ts");
const { setChildSessionFactoryModule } = await import(pathToFileURL(path.join(nativeRoot, "src/runs/shared/child-session.ts")).href);

function descendantFactory(marker) {
  const escapedScript = path.join(artifacts, "escaped-descendant.mjs");
  const intermediateScript = path.join(artifacts, "detached-intermediate.mjs");
  const factoryPath = path.join(artifacts, "owned-descendant-factory.mjs");
  fs.writeFileSync(escapedScript, `import fs from "node:fs";
    fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({pid:process.pid}));
    setInterval(() => fs.appendFileSync(${JSON.stringify(`${marker}.heartbeat`)}, "."), 20);
    setTimeout(() => process.exit(0), 120000);
  `);
  fs.writeFileSync(intermediateScript, `import {spawn} from "node:child_process";
    spawn(process.execPath, [${JSON.stringify(escapedScript)}], {detached:true,stdio:"ignore"}).unref();
  `);
  fs.writeFileSync(factoryPath, `import {spawn} from "node:child_process";
    import {createFakeChildSessions} from ${JSON.stringify(pathToFileURL(path.join(nativeRoot, "test/support/fake-child-session.ts")).href)};
    export default function() {
      const fake = createFakeChildSessions(() => process.env.MOCK_PI_QUEUE_DIR).factory;
      return {create: async (launch) => {
        spawn(process.execPath, [${JSON.stringify(intermediateScript)}], {detached:true,stdio:"ignore"}).unref();
        return fake.create(launch);
      }, dispose: () => fake.dispose()};
    }
  `);
  return { factoryPath, escapedScript };
}

function assertOwnedProof(observation) {
  const proof = observation.data.processTerminalProof;
  assert.equal(proof?.state, "observed", JSON.stringify(observation));
  assert.equal(proof.runId, observation.data.runId);
  assert.equal(typeof proof.runnerProcessInstanceId, "string");
  assert.ok(Array.isArray(proof.instances));
  assert.deepEqual(proof.callerBinding, { operationId: observation.data.operationId, requestDigest: observation.data.requestDigest });
  assert.notEqual(proof.kernelBinding.operationId, proof.nativeOperation.operationId);
  assert.notEqual(proof.kernelBinding.requestDigest, proof.nativeOperation.digest);
}

function childHasClosed(observation) {
  return observation.data?.processTerminalProof?.state === "observed";
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

describe("bridge with native RPC and a kernel-owned direct async runner", () => {
  fixture.installAsyncExecutionHooks();
  it("keeps explicit lifetime and reconciles the owned child through native lookup", async (t) => {
    assert.ok(fixture.available && fixture.isAsyncAvailable(), "native async fixture must be available");
    const emitter = new EventEmitter();
    let dropSpawnReply = true;
    let nativeSpawnRequests = 0;
    let weakProvider = false;
    const events = {
      on(event, handler) { emitter.on(event, handler); return () => emitter.off(event, handler); },
      emit(event, value) {
        if (weakProvider && event.startsWith("subagents:rpc:v1:reply:") && value.method === "ping" && value.success) {
          value = { ...value, data: { ...value.data, capabilities: { ...value.data.capabilities,
            processTreeOwnership: { version: 1, scope: "posix-process-group", escapedDescendants: "unverified" } } } };
        }
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
    let cleanupBody;
    t.after(async () => {
      if (cleanupBody) await request("cancelOperation", cleanupBody).catch(() => {});
      bridgeRpc.dispose();
      nativeRpc.dispose();
    });
    let sequence = 0;
    const request = (method, body) => new Promise((resolve, reject) => {
      const requestId = `native-${++sequence}`;
      const timer = setTimeout(() => reject(new Error(`bridge ${method} response missing`)), 15000);
      emitter.once(`${bridge.PLAN_EXEC_V2_REPLY_PREFIX}${requestId}`, (reply) => { clearTimeout(timer); resolve(reply); });
      events.emit(bridge.PLAN_EXEC_V2_REQUEST_EVENT, { version: 2, requestId, method, ...body });
    });
    const params = { agent: "worker", task: "Say native detached child finished", executionLifetime: { mode: "unbounded" }, acceptance: false, mission: false, context: "fresh" };
    const requestDigest = `sha256:${createHash("sha256").update(canonical({ cwd: fixture.tempDir, params })).digest("hex")}`;
    const body = { cwd: fixture.tempDir, params, operationId: "bridge-native-operation",
      owner: { kind: "pi-plan-exec", runId: "plan", key: "bridge-native-operation", requestDigest } };
    const ping = await request("ping", {});
    const ownership = ping.data?.capabilities?.processTreeOwnership;
    assert.equal(ownership?.scope, "owned-process-tree", JSON.stringify(ping));
    assert.equal(ownership.escapedDescendants, "contained");
    assert.ok(ownership.routes.includes("single-async"));
    let mainProofValidator;
    if (process.env.PI_PLAN_EXEC_SOURCE) {
      const { BridgeClient, hasTerminalOwnershipProof } = await createJiti(import.meta.url).import(path.join(process.env.PI_PLAN_EXEC_SOURCE, "src/bridge.ts"));
      mainProofValidator = hasTerminalOwnershipProof;
      weakProvider = true;
      const weakClient = new BridgeClient(events, 2000);
      const weakCapabilities = await weakClient.capabilities();
      assert.equal(weakCapabilities.processTreeOwnership?.scope, "posix-process-group");
      const rejected = await weakClient.spawn("strict-must-not-dispatch", params);
      assert.equal(rejected.success, false);
      assert.equal(rejected.error.code, "unsupported");
      assert.equal(nativeSpawnRequests, 0);
      weakProvider = false;
      const mainClient = new BridgeClient(events, 2000);
      const negotiated = await mainClient.capabilities();
      assert.equal(negotiated.processTreeOwnership?.scope, "owned-process-tree");
      assert.equal(nativeSpawnRequests, 0);
      t.diagnostic("Main BridgeClient negotiated the actual native kernel-owned direct route");
    }
    const lost = await request("spawn", body);
    assert.equal(lost.success, false, JSON.stringify(lost));
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
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
    assert.match(result.results?.[0]?.output ?? result.output ?? result.summary ?? "", /native detached child finished/);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    let lookup = await request("operation", body);
    const proofDeadline = Date.now() + 30_000;
    while (!childHasClosed(lookup) && Date.now() < proofDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      lookup = await request("operation", body);
    }
    assert.equal(lookup.success, true, JSON.stringify(lookup));
    assert.equal(lookup.data.runId, spawned.data.runId);
    assert.deepEqual(lookup.data.effectiveExecutionLifetime, params.executionLifetime);
    assertOwnedProof(lookup);
    if (mainProofValidator) assert.equal(mainProofValidator(lookup.data, lookup.data.runId, { operationId: body.operationId, requestDigest }), true);
    assert.equal(fixture.mockPi.callCount(), 1);
    const fencedBody = { ...body, operationId: "cancel-before-dispatch", owner: { ...body.owner, key: "cancel-before-dispatch" } };
    const fenced = await request("cancelOperation", fencedBody);
    assert.equal(fenced.success, true, JSON.stringify(fenced));
    assert.equal(fenced.data.neverStarted, true);
    assert.equal(fenced.data.processTerminalProof, undefined);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    const delayed = await request("spawn", fencedBody);
    assert.equal(delayed.success, false, JSON.stringify(delayed));
    const cancellation = await request("operation", fencedBody);
    assert.equal(cancellation.data.neverStarted, true);
    assert.equal(cancellation.data.cancellationRequested, true);
    assert.equal(fixture.mockPi.callCount(), 1);
    const marker = path.join(artifacts, "escaped-descendant.json");
    const escaped = descendantFactory(marker);
    let escapedPid;
    t.after(() => {
      if (!escapedPid) return;
      try {
        const command = execFileSync("ps", ["-p", String(escapedPid), "-o", "command="], { encoding: "utf8" });
        if (command.includes(escaped.escapedScript)) process.kill(escapedPid, "SIGKILL");
      } catch { return; }
    });
    setChildSessionFactoryModule(escaped.factoryPath);
    fixture.mockPi.onCall({ delay: 120000, output: "must be cancelled first" });
    const stoppingBody = { ...body, operationId: "cancel-live-child", owner: { ...body.owner, key: "cancel-live-child" } };
    cleanupBody = stoppingBody;
    const live = await request("spawn", stoppingBody);
    assert.equal(live.success, true, JSON.stringify(live));
    await fixture.waitForMockPiRuntime(fixture.mockPi, 1, 30_000);
    const escapedDeadline = Date.now() + 10_000;
    while (!fs.existsSync(marker) && Date.now() < escapedDeadline) await new Promise((resolve) => setTimeout(resolve, 50));
    escapedPid = JSON.parse(fs.readFileSync(marker, "utf8")).pid;
    assert.equal(typeof escapedPid, "number");
    assert.doesNotThrow(() => process.kill(escapedPid, 0));
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
    assertOwnedProof(stopped);
    const childId = stopped.data.runId;
    const childStatus = JSON.parse(fs.readFileSync(path.join(fixture.ASYNC_DIR, childId, "status.json"), "utf8"));
    assert.equal(childStatus.stopped, true, JSON.stringify(childStatus));
    assert.equal(stopped.data.cancellationRequested, true);
    if (mainProofValidator) assert.equal(mainProofValidator(stopped.data, stopped.data.runId, { operationId: stoppingBody.operationId, requestDigest }), true);
    assert.throws(() => process.kill(escapedPid, 0), { code: "ESRCH" });
    cleanupBody = undefined;
    assert.equal(fixture.mockPi.callCount(), 2);
    t.diagnostic("Known-ID stop retired the owned root and killed its reparented detached descendant");
    fs.rmSync(marker);
    fixture.mockPi.onCall({ delay: 120000, output: "must not outlive a crashed root" });
    const crashBody = { ...body, operationId: "crash-live-child", owner: { ...body.owner, key: "crash-live-child" } };
    cleanupBody = crashBody;
    const crashLaunch = await request("spawn", crashBody);
    assert.equal(crashLaunch.success, true, JSON.stringify(crashLaunch));
    await fixture.waitForMockPiRuntime(fixture.mockPi, 2, 30_000);
    const markerDeadline = Date.now() + 10_000;
    while (!fs.existsSync(marker) && Date.now() < markerDeadline) await new Promise((resolve) => setTimeout(resolve, 50));
    escapedPid = JSON.parse(fs.readFileSync(marker, "utf8")).pid;
    assert.doesNotThrow(() => process.kill(escapedPid, 0));
    const running = await request("status", { params: { runId: crashLaunch.data.runId } });
    const runnerPid = running.data.statusPayload.pid;
    assert.equal(running.data.state, "running");
    assert.ok(Number.isSafeInteger(runnerPid) && runnerPid > 0 && runnerPid !== process.pid);
    const command = execFileSync("ps", ["-p", String(runnerPid), "-o", "command="], { encoding: "utf8" });
    assert.ok(command.includes(artifacts));
    process.kill(runnerPid, "SIGKILL");
    let crashed = await request("operation", crashBody);
    const crashDeadline = Date.now() + 30_000;
    while ((!childHasClosed(crashed) || crashed.data.status !== "failed") && Date.now() < crashDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      crashed = await request("operation", crashBody);
    }
    assertOwnedProof(crashed);
    assert.equal(crashed.data.status, "failed");
    assert.throws(() => process.kill(escapedPid, 0), { code: "ESRCH" });
    if (mainProofValidator) assert.equal(mainProofValidator(crashed.data, crashed.data.runId, { operationId: crashBody.operationId, requestDigest }), true);
    cleanupBody = undefined;
    assert.equal(fixture.mockPi.callCount(), 3);
    t.diagnostic("Confirmed root crash triggered scoped cleanup and a bound retirement proof after its detached descendant exited");
  });
});
