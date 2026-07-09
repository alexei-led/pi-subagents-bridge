# Protocol research

This file records the reverse-engineered protocol facts used by `pi-subagents-bridge`.
Update it incrementally when either upstream package changes.

## Scope and versions checked

- `pi-subagents` installed package: `0.34.0`.
- `npm pack pi-subagents` package: `0.34.0`.
- Packed files matched installed files for:
  - `src/extension/rpc.ts`
  - `src/runs/background/result-watcher.ts`
  - `src/shared/types.ts`
  - `src/agents/agent-selection.ts`
- `@tintinweb/pi-tasks` installed package: `0.7.1`.

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
- `spawnParams` rejects management actions, `async:false`, and `clarify:true`, then forces `{ async: true, clarify: false }`: lines `193-204`.
- `stopAsyncRun` resolves the target async run, requires the run to be live/running in the active session, and returns `{ runId, asyncDir, previousState, state: "stopping", message }`: lines `207-267`.

Bridge decisions:

- Forward spawn as v1 RPC on `subagents:rpc:v1:request`.
- Use v1 reply channel `subagents:rpc:v1:reply:<requestId>`.
- Read async spawn id from `data.details.runId` first, then `data.details.asyncId`, with top-level fallbacks for resilience.
- Override spawn acceptance to `{ level: "none", reason: ... }` because pi-tasks has no structured acceptance-report channel and should not inherit pi-subagents' async acceptance gate.
- Override spawn control to `{ enabled: false }` because pi-tasks TaskExecute is queue-style background orchestration, not interactive subagent supervision.
- Forward stop fire-and-forget using `{ id: agentId }`; pi-tasks ignores stop failures and expects local success.

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
