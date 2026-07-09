# Bridge design

## Goal

Make `@tintinweb/pi-tasks` `TaskExecute` work with `nicobailon/pi-subagents`.

Only this path is in scope:

- pi-tasks RPC v2 `ping` / `spawn` / `stop`
- pi-subagents RPC v1 `ping` / `spawn` / `stop`
- pi-subagents async completion back to pi-tasks completion/failure events

## Non-goals

- Do not support `@tintinweb/pi-subagents`.
- Do not arbitrate multiple subagent providers loaded at once.
- Do not bridge every pi-subagents RPC method.
- Do not rewrite task orchestration logic that belongs in pi-tasks.
- Do not inspect or mutate unrelated pi-subagents runs.

## Architecture

Single extension file: `src/index.ts`.

State:

- `ownedRunIds`: run IDs spawned through this bridge only.
- `completedRunIds`: dedupe set for async completion events.

Inputs:

- `subagents:rpc:ping`
- `subagents:rpc:spawn`
- `subagents:rpc:stop`
- `subagent:async-complete`

Outputs:

- `subagents:rpc:ping:reply:<requestId>`
- `subagents:rpc:spawn:reply:<requestId>`
- `subagents:rpc:stop:reply:<requestId>`
- `subagents:completed`
- `subagents:failed`
- `subagents:ready`

## Request flow

### Ping

1. pi-tasks emits `subagents:rpc:ping`.
2. Bridge replies locally with protocol version `2`.
3. No pi-subagents RPC call is needed.

Reason:

- pi-tasks only uses ping to confirm exact protocol version compatibility.

### Spawn

1. pi-tasks emits `subagents:rpc:spawn` with `{ type, prompt, options }`.
2. Bridge maps the task agent name:
   - `general-purpose` → `delegate`
   - `Explore` / `explore` → `scout`
   - everything else passes through unchanged
3. Bridge forwards a v1 RPC request to pi-subagents with:
   - `agent`
   - `task`
   - `async: true`
   - `clarify: false`
   - `context: "fresh"`
   - optional `model`
   - optional `turnBudget.maxTurns`
4. Bridge reads the returned run ID from `data.details.runId`, with `details.asyncId` and top-level fallbacks as backup.
5. Bridge stores the run ID in `ownedRunIds` and replies to pi-tasks with `{ id: runId }`.

### Stop

1. pi-tasks emits `subagents:rpc:stop` with `{ agentId }`.
2. If the run ID is one this bridge owns, the bridge emits a pi-subagents v1 `stop` request with `{ id: agentId }`.
3. Bridge always replies success to pi-tasks.

Reason:

- pi-tasks already ignores stop errors.
- stop should not block task cleanup on best-effort cancellation.

## Completion flow

1. pi-subagents emits `subagent:async-complete` after its result watcher has already normalized the result file.
2. Bridge ignores events whose run IDs are not in `ownedRunIds`.
3. Bridge ignores duplicate completions using `completedRunIds`.
4. Bridge maps the event to pi-tasks events:
   - `state=complete` or `success=true` → `subagents:completed`
   - `state=failed` or `state=aborted` or `success=false` → `subagents:failed`
   - `state=paused` or `state=stopped` → `subagents:failed` with `status: "stopped"`
5. On stopped runs, bridge includes the partial result text so pi-tasks can mark the task completed with preserved output.

## Result text rules

Completed result preference:

1. `summary`
2. top-level `output`
3. joined child `results[].output` or `results[].error`

Stopped result preference:

1. joined child `results[].output` or `results[].error`
2. top-level `output`
3. `summary`

Failure error preference:

1. top-level `error`
2. first child `results[].error`
3. `Agent failed`

Reason:

- pi-subagents uses a generic paused summary string for interrupted runs.
- child output is usually the useful partial result.

## Ownership boundary

The bridge must only translate runs it spawned.

This avoids false positives for:

- `pi-fusion`
- pi-subagents chains
- direct `subagent(...)` calls
- any other extension using pi-subagents RPC

This is why `ownedRunIds` exists.

## Maintenance rules

When upstream changes:

1. Update `docs/protocol-research.md` first.
2. Adjust `src/index.ts` to match the new facts.
3. Add or update behavior tests in `test/bridge.test.ts`.
4. If packaging, workflows, or release expectations change, update README and this file too.

## Current release constraints

- First npm release is manual local `npm publish --access public`.
- Trusted publishing can only be configured after the package exists on npm.
- Later releases should use the GitHub Actions workflow in `.github/workflows/release.yml`.
