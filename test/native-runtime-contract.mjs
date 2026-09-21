import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import { constants as osConstants } from "node:os";
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
const kernel = await import(pathToFileURL(path.join(nativeRoot, "src/runs/background/kernel-owned-process.mjs")).href);
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

function delayedNodeExecutable() {
  const directory = path.join(artifacts, "delayed-node");
  fs.mkdirSync(directory, { mode: 0o700 });
  const executable = path.join(directory, "node");
  const quotedNode = `'${process.execPath.replaceAll("'", "'\\''")}'`;
  fs.writeFileSync(executable, `#!/bin/sh\n/bin/sleep 6\nexec ${quotedNode} "$@"\n`, { mode: 0o700 });
  return executable;
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
    let dropDiagnosticReply = false;
    let nativeSpawnRequests = 0;
    const nativeSpawnIdentities = [];
    let weakProvider = false;
    const events = {
      on(event, handler) { emitter.on(event, handler); return () => emitter.off(event, handler); },
      emit(event, value) {
        if (weakProvider && event.startsWith("subagents:rpc:v1:reply:") && value.method === "ping" && value.success) {
          value = { ...value, data: { ...value.data, capabilities: { ...value.data.capabilities,
            processTreeOwnership: { version: 1, scope: "posix-process-group", escapedDescendants: "unverified" } } } };
        }
        if (event === "subagents:rpc:v1:request" && value.method === "spawn") {
          nativeSpawnRequests++;
          nativeSpawnIdentities.push({ operationId: value.params.operationId, digest: value.params.digest });
        }
        if (dropSpawnReply && event.startsWith("subagents:rpc:v1:reply:") && value.method === "spawn") {
          dropSpawnReply = false;
          return;
        }
        if (dropDiagnosticReply && event.startsWith("subagents:rpc:v1:reply:") && value.method === "diagnose") {
          dropDiagnosticReply = false;
          return;
        }
        emitter.emit(event, value);
      },
    };
    execFileSync("git", ["init", "--quiet", fixture.tempDir]);
    execFileSync("git", ["-C", fixture.tempDir, "-c", "user.name=Bridge Test", "-c", "user.email=bridge-test@example.invalid",
      "commit", "--quiet", "--allow-empty", "-m", "Fixture base"]);
    const originalHead = execFileSync("git", ["-C", fixture.tempDir, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    const worktreeBaseDir = path.join(artifacts, "ambient-worktrees");
    const ctx = helpers.makeMinimalCtx(fixture.tempDir);
    ctx.sessionManager.getSessionId = () => "bridge-native-contract";
    const state = { baseCwd: fixture.tempDir, currentSessionId: "bridge-native-contract", asyncJobs: new Map(),
      foregroundControls: new Map(), lastForegroundControlId: null, workflowControllers: new Map() };
    const executor = fixture.createSubagentExecutor({ pi: { events, getSessionName: () => undefined, sendMessage() {} }, state,
      config: { timeoutMs: 5, worktree: true, worktreeProvider: "native", worktreeBaseDir }, asyncByDefault: false, tempArtifactsDir: fixture.tempDir,
      getSubagentSessionRoot: () => fixture.tempDir, expandTilde: (value) => value,
      discoverAgents: () => ({ agents: [{ ...helpers.makeAgent("worker"), defaultTimeoutMs: 5 }] }), });
    fixture.mockPi.onCall({ delay: 200, output: "native detached child finished" });
    const delayedNode = delayedNodeExecutable();
    let firstExecution = true;
    let nativeRpc = native.registerSubagentRpcBridge({ events, state, asyncDirRoot: fixture.ASYNC_DIR, getContext: () => ctx,
      execute: async (...args) => {
        if (!firstExecution || !args[1].rpcOperationRunId) return executor.execute(...args);
        firstExecution = false;
        const originalExecutable = process.execPath;
        process.execPath = delayedNode;
        try { return await executor.execute(...args); }
        finally { process.execPath = originalExecutable; }
      }, });
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
    const params = { agent: "worker", task: "Say native detached child finished", executionLifetime: { mode: "unbounded" }, worktree: false, acceptance: false, mission: false, context: "fresh" };
    const requestDigest = `sha256:${createHash("sha256").update(canonical({ cwd: fixture.tempDir, params })).digest("hex")}`;
    const body = { cwd: fixture.tempDir, params, operationId: "bridge-native-operation",
      owner: { kind: "pi-plan-exec", runId: "plan", key: "bridge-native-operation", requestDigest } };
    const mainModule = process.env.PI_PLAN_EXEC_SOURCE
      ? await createJiti(import.meta.url).import(path.join(process.env.PI_PLAN_EXEC_SOURCE, "src/bridge.ts")) : undefined;
    const coldClient = mainModule ? new mainModule.BridgeClient(events, 2000) : undefined;
    let ping;
    let coldCapabilities;
    const capabilityDeadline = Date.now() + 15000;
    do {
      if (coldClient) coldCapabilities = await coldClient.capabilities();
      ping = await request("ping", {});
      if (ping.data?.capabilities?.processTreeOwnership?.scope === "owned-process-tree" &&
        (!coldClient || coldCapabilities.processTreeOwnership?.scope === "owned-process-tree")) break;
      await new Promise((resolve) => setTimeout(resolve, 250));
    } while (Date.now() < capabilityDeadline);
    const ownership = ping.data?.capabilities?.processTreeOwnership;
    assert.equal(ownership?.scope, "owned-process-tree", JSON.stringify(ping));
    assert.equal(ownership.escapedDescendants, "contained");
    assert.ok(ownership.routes.includes("single-async"));
    let mainProofValidator;
    if (mainModule) {
      const { BridgeClient, hasTerminalOwnershipProof } = mainModule;
      assert.equal(coldCapabilities.processTreeOwnership?.scope, "owned-process-tree");
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
    ctx.cwd = fs.mkdtempSync(path.join(artifacts, "restarted-context-"));
    ctx.sessionManager.getSessionId = () => "restarted-session";
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    const recovered = await request("operation", body);
    assert.equal(recovered.success, true, JSON.stringify(recovered));
    assert.equal(fixture.mockPi.callCount(), 0);
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
    const rejectedParams = { ...params, agent: "unknown-agent" };
    const rejectedDigest = `sha256:${createHash("sha256").update(canonical({ cwd: fixture.tempDir, params: rejectedParams })).digest("hex")}`;
    const rejectedBody = { ...body, params: rejectedParams, operationId: "unknown-agent-operation",
      owner: { ...body.owner, key: "unknown-agent-operation", requestDigest: rejectedDigest } };
    const rejectedLaunch = coldClient
      ? await coldClient.spawn(rejectedBody.operationId, { ...rejectedParams, cwd: fixture.tempDir }, rejectedBody.owner)
      : await request("spawn", rejectedBody);
    assert.equal(rejectedLaunch.success, true, JSON.stringify(rejectedLaunch));
    const rejectedLookup = coldClient
      ? await coldClient.operation(rejectedBody.operationId, rejectedBody.owner)
      : await request("operation", rejectedBody);
    assert.equal(rejectedLookup.success, true, JSON.stringify(rejectedLookup));
    assert.equal(rejectedLookup.data.neverStarted, true);
    assert.equal(rejectedLookup.data.operationId, rejectedBody.operationId);
    assert.equal(rejectedLookup.data.requestDigest, rejectedDigest);
    assert.equal(rejectedLookup.data.runId, rejectedLaunch.data.runId);
    assert.notEqual(rejectedLookup.data.processTerminalProof?.state, "observed");
    if (mainProofValidator) assert.equal(mainProofValidator(rejectedLookup.data, rejectedLookup.data.runId,
      { operationId: rejectedBody.operationId, requestDigest: rejectedDigest }), false);
    const admissionFiles = fs.readdirSync(fixture.ASYNC_DIR, { recursive: true })
      .filter((entry) => entry.endsWith("dispatch-decision.json"));
    const admissionFile = admissionFiles.map((entry) => path.join(fixture.ASYNC_DIR, entry))
      .find((file) => JSON.parse(fs.readFileSync(file, "utf8")).operationId === rejectedBody.operationId);
    assert.ok(admissionFile);
    assert.equal(JSON.parse(fs.readFileSync(admissionFile, "utf8")).state, "rejected");
    assert.equal(fs.existsSync(path.join(path.dirname(admissionFile), "owned", "request.json")), false);
    const rejectedFence = coldClient
      ? await coldClient.cancelOperation(rejectedBody.operationId, rejectedBody.owner)
      : await request("cancelOperation", rejectedBody);
    assert.equal(rejectedFence.success, true, JSON.stringify(rejectedFence));
    assert.equal(rejectedFence.data.state, "cancelled");
    assert.equal(rejectedFence.data.neverStarted, true);
    assert.equal(rejectedFence.data.cancellationRequested, true);
    assert.equal(rejectedFence.data.operationId, rejectedBody.operationId);
    assert.equal(rejectedFence.data.requestDigest, rejectedDigest);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    const rejectedReplay = coldClient
      ? await coldClient.spawn(rejectedBody.operationId, { ...rejectedParams, cwd: fixture.tempDir }, rejectedBody.owner)
      : await request("spawn", rejectedBody);
    assert.equal(rejectedReplay.success, false, JSON.stringify(rejectedReplay));
    assert.equal(fixture.mockPi.callCount(), 1);
    t.diagnostic("Unknown-agent admission produced a caller-bound no-start cancellation fence without a kernel proof or worker dispatch");
    const invalidCwd = path.join(artifacts, "regular-file-cwd");
    fs.writeFileSync(invalidCwd, "not a directory");
    const invalidCwdDigest = `sha256:${createHash("sha256").update(canonical({ cwd: invalidCwd, params })).digest("hex")}`;
    const invalidCwdBody = { ...body, cwd: invalidCwd, operationId: "invalid-cwd-operation",
      owner: { ...body.owner, key: "invalid-cwd-operation", requestDigest: invalidCwdDigest } };
    const invalidCwdLaunch = coldClient
      ? await coldClient.spawn(invalidCwdBody.operationId, { ...params, cwd: invalidCwd }, invalidCwdBody.owner)
      : await request("spawn", invalidCwdBody);
    assert.equal(invalidCwdLaunch.success, true, JSON.stringify(invalidCwdLaunch));
    const invalidCwdLookup = coldClient
      ? await coldClient.operation(invalidCwdBody.operationId, invalidCwdBody.owner)
      : await request("operation", invalidCwdBody);
    assert.equal(invalidCwdLookup.success, true, JSON.stringify(invalidCwdLookup));
    assert.equal(invalidCwdLookup.data.neverStarted, true);
    assert.equal(invalidCwdLookup.data.operationId, invalidCwdBody.operationId);
    assert.equal(invalidCwdLookup.data.requestDigest, invalidCwdDigest);
    assert.equal(invalidCwdLookup.data.runId, invalidCwdLaunch.data.runId);
    assert.notEqual(invalidCwdLookup.data.processTerminalProof?.state, "observed");
    if (mainProofValidator) assert.equal(mainProofValidator(invalidCwdLookup.data, invalidCwdLookup.data.runId,
      { operationId: invalidCwdBody.operationId, requestDigest: invalidCwdDigest }), false);
    const invalidCwdAdmission = fs.readdirSync(fixture.ASYNC_DIR, { recursive: true })
      .filter((entry) => entry.endsWith("dispatch-decision.json"))
      .map((entry) => path.join(fixture.ASYNC_DIR, entry))
      .find((file) => JSON.parse(fs.readFileSync(file, "utf8")).operationId === invalidCwdBody.operationId);
    assert.ok(invalidCwdAdmission);
    assert.equal(JSON.parse(fs.readFileSync(invalidCwdAdmission, "utf8")).state, "rejected");
    assert.equal(invalidCwdLookup.data.details.admission.reason, "launch-validation-rejected");
    assert.equal(fs.existsSync(path.join(path.dirname(invalidCwdAdmission), "owned", "request.json")), false);
    const invalidCwdFence = coldClient
      ? await coldClient.cancelOperation(invalidCwdBody.operationId, invalidCwdBody.owner)
      : await request("cancelOperation", invalidCwdBody);
    assert.equal(invalidCwdFence.success, true, JSON.stringify(invalidCwdFence));
    assert.equal(invalidCwdFence.data.state, "cancelled");
    assert.equal(invalidCwdFence.data.neverStarted, true);
    assert.equal(invalidCwdFence.data.cancellationRequested, true);
    assert.equal(invalidCwdFence.data.operationId, invalidCwdBody.operationId);
    assert.equal(invalidCwdFence.data.requestDigest, invalidCwdDigest);
    const invalidCwdReplay = await request("spawn", invalidCwdBody);
    assert.equal(invalidCwdReplay.success, false, JSON.stringify(invalidCwdReplay));
    assert.equal(fixture.mockPi.callCount(), 1);
    t.diagnostic("Existing-agent regular-file cwd rejection produced a correlated no-start fence without launching a kernel request");
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
    fixture.mockPi.onCall({ steps: [
      { jsonl: [
        { type: "tool_execution_start", toolCallId: "failed-tool", toolName: "read", args: { path: "missing-file" } },
        { type: "tool_execution_end", toolCallId: "failed-tool", toolName: "read", isError: true,
          result: { content: [{ type: "text", text: "ENOENT: missing-file" }] } },
      ] },
      { delay: 120000, jsonl: [helpers.events.assistantMessage("must be cancelled first")] },
    ] });
    const stoppingBody = { ...body, operationId: "cancel-live-child", owner: { ...body.owner, key: "cancel-live-child" } };
    cleanupBody = stoppingBody;
    const live = await request("spawn", stoppingBody);
    assert.equal(live.success, true, JSON.stringify(live));
    const originalLiveIdentity = nativeSpawnIdentities.at(-1);
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
    let failure = await request("operation", stoppingBody);
    const failureDeadline = Date.now() + 10_000;
    while (failure.data?.activity?.lastToolFailure?.toolCallId !== "failed-tool" && Date.now() < failureDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      failure = await request("operation", stoppingBody);
    }
    assert.equal(failure.data.activity.lastToolFailure.toolCallId, "failed-tool");
    const diagnostic = { operationId: stoppingBody.operationId, owner: stoppingBody.owner,
      params: { diagnosticId: "confirmed-tool-diagnosis", toolCallId: "failed-tool", message: "Use the corrected path for the confirmed missing-file error." } };
    const rejected = await request("diagnoseOperation", { ...diagnostic, params: { ...diagnostic.params, diagnosticId: "unconfirmed-diagnosis", toolCallId: "healthy-tool" } });
    assert.equal(rejected.data.state, "rejected");
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 2000, journalPath });
    dropDiagnosticReply = true;
    const uncertainGuidance = await request("diagnoseOperation", diagnostic);
    assert.equal(uncertainGuidance.success, false);
    bridgeRpc.dispose();
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    const guidanceReceipt = await request("diagnoseOperation", diagnostic);
    assert.equal(guidanceReceipt.success, true, JSON.stringify(guidanceReceipt));
    assert.equal(guidanceReceipt.data.state, "queued");
    assert.equal(guidanceReceipt.data.guidanceOnly, true);
    const steerLog = path.join(fixture.mockPi.dir, "steers.jsonl");
    const guidanceDeadline = Date.now() + 10_000;
    while (!fs.existsSync(steerLog) && Date.now() < guidanceDeadline) await new Promise((resolve) => setTimeout(resolve, 50));
    const messages = fs.readFileSync(steerLog, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    const matchingGuidance = messages.filter((entry) => typeof entry.text === "string" && entry.text.includes(diagnostic.params.message));
    assert.equal(matchingGuidance.length, 1, JSON.stringify(messages));
    assert.equal(matchingGuidance[0].mode, "followUp");
    const launchesBeforeColdCancel = nativeSpawnRequests;
    bridgeRpc.dispose();
    nativeRpc.dispose();
    state.asyncJobs.clear();
    state.foregroundControls.clear();
    state.workflowControllers.clear();
    ctx.sessionManager.getSessionId = () => "cold-restored-session";
    state.currentSessionId = "cold-restored-session";
    nativeRpc = native.registerSubagentRpcBridge({ events, state, asyncDirRoot: fixture.ASYNC_DIR, getContext: () => ctx,
      execute: (...args) => executor.execute(...args) });
    bridgeRpc = bridge.registerPlanExecRpc(events, { timeoutMs: 12000, journalPath });
    const coldLookupClient = mainModule ? new mainModule.BridgeClient(events, 12000) : undefined;
    const coldLookup = coldLookupClient
      ? await coldLookupClient.operation(stoppingBody.operationId, stoppingBody.owner)
      : await request("operation", stoppingBody);
    assert.equal(coldLookup.success, true, JSON.stringify(coldLookup));
    assert.equal(coldLookup.data.runId, live.data.runId);
    assert.equal(coldLookup.data.operationId, stoppingBody.operationId);
    assert.equal(coldLookup.data.requestDigest, requestDigest);
    const coldCancelClient = mainModule ? new mainModule.BridgeClient(events, 12000) : undefined;
    const stop = coldCancelClient
      ? await coldCancelClient.cancelOperation(stoppingBody.operationId, stoppingBody.owner)
      : await request("stop", { params: { runId: live.data.runId } });
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
    assert.equal(stopped.data.runId, live.data.runId);
    assert.deepEqual(stopped.data.processTerminalProof.nativeOperation, originalLiveIdentity);
    assert.equal(nativeSpawnRequests, launchesBeforeColdCancel);
    const childId = stopped.data.runId;
    const childStatus = JSON.parse(fs.readFileSync(path.join(fixture.ASYNC_DIR, childId, "status.json"), "utf8"));
    assert.equal(childStatus.stopped, true, JSON.stringify(childStatus));
    assert.equal(stopped.data.cancellationRequested, true);
    assert.equal((await request("diagnoseOperation", diagnostic)).data.state, "cancelled");
    if (mainProofValidator) assert.equal(mainProofValidator(stopped.data, stopped.data.runId, { operationId: stoppingBody.operationId, requestDigest }), true);
    assert.throws(() => process.kill(escapedPid, 0), { code: "ESRCH" });
    cleanupBody = undefined;
    assert.equal(fixture.mockPi.callCount(), 2);
    t.diagnostic("Fresh main clients without capability negotiation recovered and cancelled the original worker after Bridge/native RPC recreation");
    t.diagnostic("Confirmed tool-error guidance queued once across a lost reply and Bridge restart; unconfirmed tools and stopped runs rejected");
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
    fs.rmSync(marker);
    fixture.mockPi.onCall({ delay: 120000, output: "must not outlive the explicit bounded lifetime" });
    const expiryParams = { ...params, executionLifetime: { mode: "bounded", timeoutMs: 10000 } };
    const expiryDigest = `sha256:${createHash("sha256").update(canonical({ cwd: fixture.tempDir, params: expiryParams })).digest("hex")}`;
    const expiryBody = { ...body, params: expiryParams, operationId: "bounded-gate-death",
      owner: { ...body.owner, key: "bounded-gate-death", requestDigest: expiryDigest } };
    cleanupBody = expiryBody;
    const expiryClient = mainModule ? new mainModule.BridgeClient(events, 12000) : undefined;
    if (expiryClient) await expiryClient.capabilities();
    const expiryLaunch = expiryClient
      ? await expiryClient.spawn(expiryBody.operationId, { ...expiryParams, cwd: fixture.tempDir }, expiryBody.owner)
      : await request("spawn", expiryBody);
    assert.equal(expiryLaunch.success, true, JSON.stringify(expiryLaunch));
    await fixture.waitForMockPiRuntime(fixture.mockPi, 3, 30_000);
    const expiryMarkerDeadline = Date.now() + 10_000;
    while (!fs.existsSync(marker) && Date.now() < expiryMarkerDeadline) await new Promise((resolve) => setTimeout(resolve, 50));
    escapedPid = JSON.parse(fs.readFileSync(marker, "utf8")).pid;
    assert.doesNotThrow(() => process.kill(escapedPid, 0));
    const expiryIntent = fs.readdirSync(fixture.ASYNC_DIR, { recursive: true })
      .filter((entry) => entry.endsWith("intent.json"))
      .map((entry) => path.join(fixture.ASYNC_DIR, entry))
      .find((file) => JSON.parse(fs.readFileSync(file, "utf8")).operationId === expiryBody.operationId);
    assert.ok(expiryIntent);
    const ownedDirectory = path.join(path.dirname(expiryIntent), "owned");
    const activeKernel = await kernel.observeKernelOwnedProcess(ownedDirectory);
    assert.equal(activeKernel.status, "active");
    const prepared = JSON.parse(fs.readFileSync(path.join(ownedDirectory, "request.json"), "utf8"));
    const workload = activeKernel.workloadIdentity;
    assert.ok(workload);
    execFileSync(prepared.request.nativeExecutable, ["signal", String(workload.pid), String(workload.pidVersion),
      workload.uniqueId, activeKernel.identity.coalitionId, String(osConstants.signals.SIGSTOP)], { timeout: 2000 });
    const leader = activeKernel.identity.leader;
    execFileSync(prepared.request.nativeExecutable, ["signal", String(leader.pid), String(leader.pidVersion),
      leader.uniqueId, activeKernel.identity.coalitionId, "9"], { timeout: 2000 });
    assert.equal(JSON.parse(fs.readFileSync(path.join(fixture.ASYNC_DIR, expiryLaunch.data.runId, "status.json"), "utf8")).state, "running");
    const observeExpiry = () => expiryClient
      ? expiryClient.operation(expiryBody.operationId, expiryBody.owner)
      : request("operation", expiryBody);
    let expired = await observeExpiry();
    const expiryDeadline = Date.now() + 30_000;
    while ((!childHasClosed(expired) || expired.data.status !== "failed") && Date.now() < expiryDeadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      expired = await observeExpiry();
    }
    assertOwnedProof(expired);
    assert.equal(expired.data.status, "failed");
    assert.equal(expired.data.statusPayload.state, "failed");
    assert.equal(expired.data.statusPayload.timedOut, true);
    assert.equal(expired.data.terminationReason, "execution_lifetime_expired");
    assert.equal(expired.data.cancellationRequested, false);
    assert.equal(fs.existsSync(path.join(ownedDirectory, "exit.json")), false);
    assert.throws(() => process.kill(escapedPid, 0), { code: "ESRCH" });
    if (mainProofValidator) assert.equal(mainProofValidator(expired.data, expired.data.runId,
      { operationId: expiryBody.operationId, requestDigest: expiryDigest }), true);
    cleanupBody = undefined;
    assert.equal(fixture.mockPi.callCount(), 4);
    t.diagnostic("Bounded expiry after helper death reaches the main client as failed with a retirement proof despite missing exit metadata");
    const childCalls = fs.readdirSync(fixture.mockPi.dir).filter((name) => name.startsWith("call-") && name.endsWith(".json"))
      .map((name) => JSON.parse(fs.readFileSync(path.join(fixture.mockPi.dir, name), "utf8")));
    assert.equal(childCalls.length, 4);
    for (const call of childCalls) assert.equal(call.cwd, fixture.tempDir);
    assert.equal(execFileSync("git", ["-C", fixture.tempDir, "rev-parse", "HEAD"], { encoding: "utf8" }).trim(), originalHead);
    assert.equal(fs.existsSync(worktreeBaseDir), false);
    t.diagnostic("Explicit worktree false kept every owned child in the original caller cwd and HEAD despite ambient worktree true");
  });
});
