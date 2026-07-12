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
- in-flight and short-lived spawn reply maps: coalesce repeated request IDs.
- terminal-result deadlines: bound retries when a terminal result file is not readable yet.

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
3. Bridge rejects the request if two bridge-owned runs are already active.
4. Bridge forwards a v1 RPC request to pi-subagents with:
   - `agent`
   - `task`
   - `async: true`
   - `clarify: false`
   - `context: "fresh"`
   - `acceptance: { level: "none", reason: ... }`
   - `control: { enabled: false }`
   - optional `model`
   - `turnBudget.maxTurns`, defaulting to `12`
5. Bridge reads the returned run ID from `data.details.runId`, with `details.asyncId` and top-level fallbacks as backup.
6. Bridge stores the run ID in `ownedRunIds` and replies to pi-tasks with `{ id: runId }`.

Repeated copies of the same request ID share one spawn promise and replay one cached reply. Capacity errors are not cached, so pi-tasks can retry after a run finishes.

Reason:

- pi-tasks already owns task completion and result storage.
- pi-subagents' async acceptance gate expects a structured `acceptance-report` that pi-tasks never asked for and does not consume.
- pi-subagents' live `needs_attention` / `active_long_running` notices are useful in direct subagent supervision, but they are noisy and misleading in TaskExecute's fire-and-forget queue model.

### Stop

1. pi-tasks emits `subagents:rpc:stop` with `{ agentId }`.
2. If the run ID is one this bridge owns and is not already stopping, the bridge emits one pi-subagents v1 `stop` request with `{ id: agentId }`.
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
6. If status polling sees a terminal result path before its file is readable, bridge retries for at most five seconds before emitting a warning result.

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
4. append up to 4,000 characters of top-level or child partial output when present

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
