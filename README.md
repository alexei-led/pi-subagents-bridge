# pi-subagents-bridge

[![npm version](https://img.shields.io/npm/v/%40alexeiled%2Fpi-subagents-bridge?style=flat-square&logo=npm&logoColor=white)](https://www.npmjs.com/package/@alexeiled/pi-subagents-bridge)
[![CI](https://img.shields.io/github/actions/workflow/status/alexei-led/pi-subagents-bridge/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/alexei-led/pi-subagents-bridge/actions/workflows/ci.yml?query=branch%3Amain)
[![node](https://img.shields.io/badge/node-%3E%3D22.19.0-5fa04e?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](./LICENSE)

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

- [`docs/protocol-research.md`](https://github.com/alexei-led/pi-subagents-bridge/blob/main/docs/protocol-research.md) — upstream protocol facts
- [`docs/design.md`](https://github.com/alexei-led/pi-subagents-bridge/blob/main/docs/design.md) — bridge flow and maintenance rules

## Constraint

Do not load `@tintinweb/pi-subagents` at the same time.
