# pi-subagents-bridge

Connects [nicobailon/pi-subagents](https://github.com/nicobailon/pi-subagents) with [@tintinweb/pi-tasks](https://github.com/tintinweb/pi-tasks).

These two packages are incompatible out of the box: pi-tasks speaks the tintinweb v2 RPC protocol (`subagents:rpc:spawn`), nicobailon speaks v1 (`subagents:rpc:v1:request`). `TaskExecute` silently fails without this bridge.

## What it does

- Answers pi-tasks' protocol handshake (`subagents:rpc:ping`)
- Forwards `TaskExecute` spawns → nicobailon's agent runner
- Translates nicobailon's `subagent:async-complete` → `subagents:completed` / `subagents:failed`
- Tracks only its own spawns — nicobailon chain/parallel runs (e.g. pi-fusion) pass through untouched

## Install

```bash
pi install npm:pi-subagents-bridge
```

Requires both `nicobailon/pi-subagents` and `@tintinweb/pi-tasks` already installed.

## Usage

```
TaskCreate(title="...", agentType="general-purpose", ...)
TaskExecute(task_ids=["1"])   ← now works through nicobailon
```

`agentType` maps directly to nicobailon agent names (`"general-purpose"`, `"Explore"`, or any custom agent in `.pi/agents/<name>.md`).

## Constraint

Do not load `@tintinweb/pi-subagents` alongside this bridge — they answer the same RPC channels.
