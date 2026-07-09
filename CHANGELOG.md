# Changelog

## Unreleased

- None.

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
