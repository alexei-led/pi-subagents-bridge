# Changelog

## Unreleased

- Forward validated native workflow terminal proofs from pi-subagents 0.71.0 instead of synthesizing proofs from child inventories or disk records; accept observed and not-started workflow children.
- Validate observed process-terminal proofs against a matching runner instance, reject invalid event-cache entries, and prefer native status proofs over cached events.
- Require pi-subagents >=0.71.0 and Pi >=0.86.1 to match the published runtime contract. Report an actionable runtime/status diagnostic when a closed workflow has no native proof field; keep a pending proof nonterminal.

## 0.4.2 - 2026-09-22

- Synthesize a workflow terminal proof for released pi-subagents async runs: a persistent workflow host publishes no exit of its own, so a closed `workflowChildren` inventory plus every child's attested writer-exit proof is now the terminal evidence.
- Validate every child proof through `attestUpstreamTerminalProof`, accept the upstream terminal workflow states (`completed`, `failed`, `stopped`), and fall back to the child's `process-terminal.json` when the in-process proof cache missed it.

## 0.4.1 - 2026-09-22

- Modernize tooling: TypeScript 7, Biome (replacing ESLint), Vitest (replacing node:test), npm 12, and the latest stable GitHub Actions.
- Consume pi-subagents from the npm registry (^0.70.1) instead of a private Git fork; the lockfile resolves the released tarball and npm ci works without allow-git.
- Read upstream terminal-proof events from the ping reply's top level, so process-terminal proofs are subscribed with the released runtime.
- Remove the obsolete kernel-contract native test and simplify the development and release workflows.

## 0.4.0 - 2026-09-22

- Accept the released pi-subagents runtime: detached async spawn, stop control, writer-exit terminal proofs, and deadlines.
- Replace the kernel/lifetime capability contract with bridge-supervised lifetimes: a bounded lifetime is forwarded as the upstream timeout, and the bridge owns the durable operation identity, lookup, and cancellation fence.
- Capture upstream process-terminal events and expose them with operation lookups, so recovery confirms terminal state after a bridge restart.
- Keep the durable operation journal for lost-reply recovery and binding retries.

## 0.3.2 - 2026-09-22

- Add explicit execution-lifetime contracts to plan-exec RPC and owned native dispatch, including bounded or unbounded selection and a verified effective lifetime on every reply.
- Add durable native operation recovery: journal schema 5, persisted native params, owner-bound lookups, cold-client cancellation, no-start admission fences, and dispatch arbitration.
- Route owned native launches with journal-bound kernel proofs and keep terminal evidence consistent across recovery.
- Keep accepted-run ownership with a live owner PID; an expired heartbeat alone no longer transfers ownership.
- Preserve the caller workspace unless the caller explicitly requests an isolation override.
- Validate against Pi 0.87.0 and the npm 12 root Git dependency policy.

## 0.3.1 - 2026-09-22

- Validate against Pi 0.87.0.
- Open known schema-4/5 operation journals without changing their version or extra fields.
- Reject unknown journal versions before database changes and include recovery guidance.

## 0.3.0 - 2026-08-30

- Add resilient v2 plan-exec RPC integration, session-fenced legacy run recovery, SQLite durability, replay identity checks, and retryable persistence.
- Add durable launch bindings, native terminal proof, capability negotiation, and fail-closed unknown-launch recovery.
- Retry transient accepted-run and known-run binding persistence failures.

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
