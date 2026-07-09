# Changelog

## Unreleased

- None.

## 0.1.0 - 2026-07-09

### Added

- `@alexeiled/pi-subagents-bridge` package metadata, npm publish config, CI, and release workflow.
- Protocol bridge from `@tintinweb/pi-tasks` TaskExecute RPC v2 to `nicobailon/pi-subagents` RPC v1.
- Behavior tests for ping, spawn, stop, completion translation, ownership filtering, deduplication, and timeout handling.
- Front-page README, banner image, and release workflow docs.

### Changed

- Spawn requests map pi-tasks' `general-purpose` to `delegate` and `Explore` / `explore` to `scout`; other agent names pass through unchanged.
- Interrupted async completions are translated to `subagents:failed` with `status="stopped"` and the partial result text.
