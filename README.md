# pi-subagents-bridge

Bridge for `@tintinweb/pi-tasks` `TaskExecute` → `nicobailon/pi-subagents`.

`pi-tasks` speaks v2 RPC. `pi-subagents` speaks v1. This extension answers the ping handshake as v2, forwards spawn/stop to pi-subagents, and turns `subagent:async-complete` back into `subagents:completed` / `subagents:failed`.

## Install

```bash
pi install npm:@alexeiled/pi-subagents-bridge
```

Needs `@tintinweb/pi-tasks` and `nicobailon/pi-subagents` already installed.

## Use

```text
TaskCreate(..., agentType="general-purpose", ...)
TaskExecute(task_ids=["1"])
```

`agentType` mapping:

- `general-purpose` → `delegate`
- `Explore` / `explore` → `scout`
- anything else passes through unchanged

## Behavior

- tracks only run IDs spawned through this bridge
- ignores unrelated pi-subagents runs
- keeps completion text from async-complete payloads
- stop is best-effort cancel

## Details

- [`docs/protocol-research.md`](./docs/protocol-research.md) — upstream protocol facts
- [`docs/design.md`](./docs/design.md) — bridge flow and maintenance rules

## Constraint

Do not load `@tintinweb/pi-subagents` at the same time.
