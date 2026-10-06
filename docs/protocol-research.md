# Protocol research

This file records the reverse-engineered protocol facts used by `pi-subagents-bridge`.
Update it incrementally when either upstream package changes.

## Scope and versions checked

- Pi extension API contract tested with `@earendil-works/pi-coding-agent 1.0.4`; supported Pi range is `^1.0.2`.
- `pi-subagents` tested runtime: `0.76.1`; supported range: `>=0.76.0 <0.77.0`.
- The bridge v2 capability probe checks async spawn and process-terminal support; upstream ping does not attest workflow-proof availability or the version of the active Pi extension.
- Packed files matched installed files for:
  - `src/extension/rpc.ts`
  - `src/runs/background/result-watcher.ts`
  - `src/shared/types.ts`
  - `src/agents/agent-selection.ts`
- `@tintinweb/pi-tasks` supported runtime contract: `0.9.0`.

## pi-tasks v2 RPC contract

Source: `~/.pi/agent/npm/node_modules/@tintinweb/pi-tasks/src/index.ts`.

Evidence:

- Reply envelope is `{ success: true, data?: T } | { success: false, error: string }`: lines `96-101`.
- RPC reply channel is `<channel>:reply:<requestId>`: lines `103-119`.
- Spawn calls `subagents:rpc:spawn` with `{ type, prompt, options }` and expects `{ id }`: lines `126-131`.
- Stop calls `subagents:rpc:stop` with `{ agentId }`: lines `133-134`.
- `PROTOCOL_VERSION` is hardcoded to `2`: line `137`.
- Version check requires exact equality with `2`; lower or higher versions are treated as incompatible: lines `142-157`.
- Completion event consumed: `subagents:completed` with `{ id, result? }`: lines `207-214`.
- Failure event consumed: `subagents:failed` with `{ id, error?, result?, status }`: lines `249-260`.
- `status === "stopped"` marks the task completed and keeps partial `result`: lines `249-260`.
- Auto-cascade uses completed tasks to spawn dependent pending tasks with `agentType`: lines `218-240`.
- `TaskExecute` requires task metadata `agentType`, marks task `in_progress`, calls `spawnSubagent`, then stores returned agent id in task owner/metadata: lines `890-984`.

Bridge decisions:

- Answer `subagents:rpc:ping` locally with `{ success: true, data: { version: 2 } }`.
- Reply to pi-tasks on `<channel>:reply:<requestId>`.
- Return spawn success as `{ success: true, data: { id: runId } }`.
- Translate stopped/paused pi-subagents runs to `subagents:failed` with `status: "stopped"`.
- Use the RPC `requestId` as a durable replay key, but bind it to a semantic request digest and the current Pi session ID because the request has no task/list/attempt identity.
- Bind accepted completion delivery to `ctx.sessionManager.getSessionId()`. A same-session process can recover it; a foreign session cannot consume it.

## plan-exec bridge v2 contract

- Request channel is `plan-exec:bridge:v2:request`; reply prefix is `plan-exec:bridge:v2:reply:`.
- Version 1 remains supported without changing its request or reply shapes.
- Spawn identity is `{ operationId, owner: { kind: "pi-plan-exec", runId, key, requestDigest }, cwd, params }`.
- `requestDigest` is `sha256:` plus SHA-256 of canonical JSON `{ cwd: effectiveTopLevelCwd, params: originalParams }`.
- A journal record is persisted before native dispatch. `dispatching` records that cannot be proven bound are `unknown` and never retried automatically.
- Version 2 ping negotiates native `pi-subagents` capabilities. `processTerminalProof: { version: 1 }` is advertised only when upstream advertises version 1.
- Targeted v2 status, result, and adopt validate native `details.lifecycleStatus.processTerminal` proofs. `observed` proofs require a finite `observedAt` and an `instances` runner record whose `processInstanceId` matches `runnerProcessInstanceId`; pending, unknown, and not-started states may be forwarded diagnostically but are not observed writer-exit evidence. Only validated observed `subagent:process-terminal` events enter the cache, and a valid native status proof takes precedence over that cache.
- In `0.71.0`, targeted status returns native `details.workflowTerminalProof` for workflows (`runs/background/run-status.js` calls `readWorkflowTerminalProof` from `runs/background/workflow-terminal-proof.js`). The native ping advertises `processTerminalProof` but no separate workflow-proof capability; workflow proof is an optional targeted-status detail. The bridge forwards only a version 1 proof matching the requested run, with `state: "observed"`, `dispatchClosed: true`, a finite `observedAt`, and children whose native process-terminal states are `observed` or `not-started`. Pending, unknown, or malformed proofs are not completion evidence. If a closed terminal `workflowChildren` inventory has no proof field at all, v2 status, result, and adopt return a diagnostic to check the active runtime and durable async status/proof artifacts instead of assuming exit. `workflowChildren` summaries and child `process-terminal.json` files are never used to synthesize a workflow proof.
- Owner and digest mismatches fail before native dispatch. Operation lookup is durable and never starts a child.

## pi-subagents v1 RPC contract

Source: `~/.pi/agent/npm/node_modules/pi-subagents/src/extension/rpc.ts`.

Evidence:

- Protocol version is `1`: line `13`.
- Request channel is `subagents:rpc:v1:request`: line `14`.
- Ready event is `subagents:rpc:v1:ready`: line `15`.
- Reply prefix is `subagents:rpc:v1:reply:`: line `16`.
- Supported methods are `ping`, `status`, `spawn`, `interrupt`, `stop`: line `18`.
- Request envelope is `{ version, requestId, method, params?, source? }`: lines `21-30`.
- Reply envelope includes `version`, `requestId`, optional `method`, and either `success: true, data` or `success: false, error: { code, message }`: lines `32-47`.
- `dataFromToolResult` exposes text and `details` from the subagent tool result: lines `128-132`.
- Target params for status/interrupt/stop accept `id`, `runId`, `dir`, `index`: lines `141-147`.
- In released `src/extension/rpc.js:375-407`, `spawnParams` accepts inline text as `script`, rejects own `workflowScript` and `workflowScriptPath` with `invalid_params`, then maps `script` to the internal `workflowScript` carrier before validation and execution. The breaking public change shipped in 0.74.0; Bridge tests pin 0.76.1. Management actions, `async:false`, and the removed public `clarify` field are rejected.
- `stopAsyncRun` resolves the target async run, requires the run to be live/running in the active session, and returns `{ runId, asyncDir, previousState, state: "stopping", message }`: lines `207-267`.

Bridge decisions:

- Forward both TaskExecute and plan-exec spawn as v1 RPC on `subagents:rpc:v1:request` with `script`. Keep the bridge capability `workflowScriptSpawn` and native internal carriers unchanged.
- A correlated native `spawn/invalid_params` reply is persisted as typed prelaunch rejection before lookup reports no-start. Generic errors, missing method/identity and timeouts leave unbound operations unknown. Never infer no-start merely from an error string or missing run ID.
- `requestNativeCancel` returns whether its transaction created a new fence. Only that result proves cancellation preceded dispatch; an earlier read outside the transaction cannot prove it.
- Use v1 reply channel `subagents:rpc:v1:reply:<requestId>`.
- Read async spawn id from `data.details.runId` first, then `data.details.asyncId`, with top-level fallbacks for resilience.
- Override spawn acceptance to `{ level: "none", reason: ... }` because pi-tasks has no structured acceptance-report channel and should not inherit pi-subagents' async acceptance gate.
- Override spawn control to `{ enabled: false }` because pi-tasks TaskExecute is queue-style background orchestration, not interactive subagent supervision.
- Forward stop fire-and-forget using `{ id: agentId }`; pi-tasks ignores stop failures and expects local success.

## Cancellation and advisory observations (0.76.1)

Released `src/extension/rpc.js` still rejects `status.state !== "running"`
in `stopAsyncRun`. The separate model-facing paused-stop implementation is not
called by RPC. `test/upstream-rpc.test.ts` exercises real RPC refusal for paused
and queued fixture statuses. No protocol fallback is authorized by a refusal.
Workflow control callbacks also remain process-local and session-scoped.

Bridge advertises explicit delivery status and persists exact whole-run stop
receipts in journal schema 7. The caller retries pending delivery against the
same identity. Receipt replay is not process or workflow termination evidence.
See the [cancellation contract](../README.md#generic-plan-exec-rpc).

Native status adds `asyncSnapshot` from
`src/runs/background/async-status-snapshot.js`. It selects active-session jobs
and projects them with `src/runs/shared/async-status-projection.js`.
The snapshot has version/kind, generation time, caps, omission counters and
root run IDs, but no session ID. Materialized child steps can replace native IDs
with workflow keys. Bridge therefore accepts only an exact root-run match and
keeps the bounded activity separate from terminal proof. See the
[advisory DTO](../README.md#advisory-activity).

Pi's factory lifecycle forbids session timers before `session_start`.
Bridge now registers its session resources there and releases listeners and
timers on shutdown. Legacy in-flight spawn replies remain recoverable across
registration disposal; disposed proof subscriptions no longer receive events.

## pi-subagents async completion payload

Sources:

- `~/.pi/agent/npm/node_modules/pi-subagents/src/shared/types.ts`
- `~/.pi/agent/npm/node_modules/pi-subagents/src/runs/background/result-watcher.ts`
- `~/.pi/agent/npm/node_modules/pi-subagents/src/runs/background/subagent-runner.ts`

Evidence:

- Async completion event constant is `subagent:async-complete`: `shared/types.ts:903-905`.
- Result file data fields include `id`, `runId`, `agent`, `success`, `state`, `mode`, `summary`, `results`, `sessionId`, `cwd`, `sessionFile`, `asyncDir`: `result-watcher.ts:49-62`.
- Child result fields include `agent`, `output`, `error`, `success`, `sessionFile`, `artifactPaths.outputPath`, `intercomTarget`, `children`: `result-watcher.ts:38-47`.
- Watcher resolves run id from `data.runId ?? data.id ?? file basename`: `result-watcher.ts:120-124`.
- Watcher builds child output from `result.output ?? data.summary`: `result-watcher.ts:141-155`.
- Watcher emits `subagent:async-complete` with `...data`, resolved `runId`, optional `nestedChildren`, and normalized `results[]`: `result-watcher.ts:193-211`.
- Runner writes terminal result file with `success`, `state`, `summary`, `error`, `results`, `exitCode`, `timestamp`, `durationMs`, `asyncDir`, `sessionId`, `sessionFile`: `subagent-runner.ts:3066-3126`.
- Runner terminal states are:
  - `complete` when all child results succeeded.
  - `failed` on timeout, turn budget exceeded, or failed child result.
  - `paused` on interrupt.
    Evidence: `subagent-runner.ts:3071-3073`.

Bridge decisions:

- Use event fields directly. The watcher already reads and normalizes the result file before emitting the event.
- Completion result text order: `summary`, then top-level `output`, then joined child `results[].output/error`.
- Stopped/paused result text order: joined child `results[].output/error`, then top-level `output`, then `summary`, because `summary` is often the generic paused message.
- Failure error text order: top-level `error`, then first child `results[].error`, else `Agent failed`.
- Ignore events for run IDs not spawned by this bridge.
- Deduplicate completion events by run ID.

## Agent type mapping

Sources:

- `~/.pi/agent/npm/node_modules/pi-subagents/src/agents/agents.ts`
- `~/.pi/agent/npm/node_modules/pi-subagents/src/agents/agent-selection.ts`

Evidence:

- Builtin agent names are `context-builder`, `delegate`, `oracle`, `planner`, `researcher`, `reviewer`, `scout`, `worker`: `agents.ts:31-40`.
- Agent merge/discovery uses exact `agent.name` keys across builtin, package, user, and project agents: `agent-selection.ts:4-19`.
- Searches for `general-purpose` and `Explore` in pi-subagents source returned no builtin aliases.

Bridge decisions:

- Map pi-tasks examples to available nicobailon builtins:
  - `general-purpose` → `delegate`
  - `Explore` / `explore` → `scout`
- Pass every other `agentType` through unchanged.

## Maintenance checklist

When adapting to upstream changes:

1. Check installed versions:

   ```bash
   node -p 'require("~/.pi/agent/npm/node_modules/pi-subagents/package.json").version'
   node -p 'require("~/.pi/agent/npm/node_modules/@tintinweb/pi-tasks/package.json").version'
   ```

2. Run `npm pack pi-subagents` and compare the relevant packed files to installed source.
3. Re-read these files and update this doc:
   - `pi-subagents/src/extension/rpc.ts`
   - `pi-subagents/src/runs/background/result-watcher.ts`
   - `pi-subagents/src/runs/background/subagent-runner.ts`
   - `pi-subagents/src/shared/types.ts`
   - `pi-subagents/src/agents/agents.ts`
   - `pi-subagents/src/agents/agent-selection.ts`
   - `@tintinweb/pi-tasks/src/index.ts`
4. Update `docs/design.md` if a bridge behavior changes.
5. Add or update behavior tests before changing the bridge.
