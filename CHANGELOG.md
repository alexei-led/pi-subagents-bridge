# Changelog

## 0.3.0 - 2026-08-30

- Add resilient v2 plan-exec RPC integration, session-fenced legacy run recovery, SQLite durability, replay identity checks, and retryable persistence.
- Add durable launch bindings, native terminal proof, capability negotiation, and fail-closed unknown-launch recovery.
- Retry transient accepted-run and known-run binding persistence failures.

## Unreleased

## 0.2.3 - 2026-08-08

- Disable pi-subagents control notices for bridge-owned child runs. This avoids
  stale control-event crashes after a Pi reload while plan-exec polls run status.

## 0.2.2 - 2026-08-08

- Translate `TaskExecute` and plan-exec child launches into the one-child
  `workflowScript` public API required by `pi-subagents` 0.43.0.
- Stop sending the removed public `clarify` field while retaining compatibility
  with plan-exec callers that still pass `clarify: false`.
- Advertise the `workflowScriptSpawn` plan-exec capability so clients reject
  older incompatible bridge releases before launching work.
- Return the child output rather than the outer workflow summary through the
  pi-tasks completion API.

## 0.2.1 - 2026-07-26

- Bumped `@earendil-works/pi-coding-agent` dev dependency from `^0.80.3` to `^0.82.1`
  to match the installed version and pick up updated type definitions.

## 0.2.0 - 2026-07-16

- Added the plan-exec `operation` RPC for safe reconciliation of durable spawn
  operation IDs after an unknown launch result. It reports `absent`, `pending`,
  `found`, or `unknown` without starting another child.
- Preserved successful and failed operation lookup outcomes across bridge
  re-registration during one Pi process lifetime.
- Documented operation-lifetime limits and the accepted `timeout` / `timeoutMs`
  spawn aliases.

## 0.1.6 - 2026-07-12

- Added the generic plan-exec v1 RPC with cwd forwarding, durable spawn-operation idempotency, and normalized status/result/stop/adopt responses over the pi-subagents v1 RPC.
- Preserved in-flight durable spawn operations across bridge re-registration and added contract validation for request and upstream reply envelopes.
- Documented the plan-exec protocol and included its runtime module in the npm package.

## 0.1.5 - 2026-07-12

- Limited bridge-owned task runs to two concurrent agents and applied a 12-turn default when TaskExecute does not provide maxTurns.
- Coalesced duplicate spawn and stop requests to avoid duplicate work and token spend.
- Retried terminal result-file reads for up to five seconds, retained bounded partial failure output, and made bridge registration idempotent.
- Added live Pi/HERDR validation and regression coverage for capacity, deduplication, result hydration, reload survival, and npm pack JSON compatibility.

## 0.1.4 - 2026-07-09

- Added a fallback completion poll so bridge-owned `TaskExecute` runs still update pi-tasks when the `subagent:async-complete` event is missed.
- Read pi-subagents `status` result files to translate polled run completion into the same `subagents:completed` / `subagents:failed` events used by the normal async-complete path.
- Added a regression test that exercises the status-poll fallback without relying on the async-complete event.

## 0.1.3 - 2026-07-09

- Disabled pi-subagents acceptance gating for bridge-spawned `TaskExecute` runs so pi-tasks jobs no longer pause on missing `acceptance-report` output.
- Disabled pi-subagents live control nudges for bridge-spawned `TaskExecute` runs to keep queue-style background tasks from surfacing misleading `needs attention` prompts.
- Added tests and docs for the bridge spawn overrides and execution-capable agent guidance.

## 0.1.2 - 2026-07-09

- No code changes from `0.1.1`.
- Release retry after configuring npm trusted publishing for GitHub Actions.

## 0.1.1 - 2026-07-09

- Kept the npm tarball to runtime package files only: `src/index.ts`, `package.json`, `README.md`, and `LICENSE`.
- Preserved active run ownership across bridge re-registration so in-flight tasks can still stop and complete.
- Cancelled in-flight spawn RPC listeners on bridge disposal and ignored late replies.

## 0.1.0 - 2026-07-09

### Added

- `@alexeiled/pi-subagents-bridge` package metadata, npm publish config, CI, and release workflow.
- Protocol bridge from `@tintinweb/pi-tasks` TaskExecute RPC v2 to `nicobailon/pi-subagents` RPC v1.
- Behavior tests for ping, spawn, stop, completion translation, ownership filtering, deduplication, and timeout handling.
- Front-page README, banner image, and release workflow docs.

### Changed

- Spawn requests map pi-tasks' `general-purpose` to `delegate` and `Explore` / `explore` to `scout`; other agent names pass through unchanged.
- Interrupted async completions are translated to `subagents:failed` with `status="stopped"` and the partial result text.
