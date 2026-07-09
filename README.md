# pi-subagents-bridge

[![npm version](https://img.shields.io/npm/v/%40alexeiled%2Fpi-subagents-bridge?style=flat-square&logo=npm&logoColor=white)](https://www.npmjs.com/package/@alexeiled/pi-subagents-bridge)
[![CI](https://img.shields.io/github/actions/workflow/status/alexei-led/pi-subagents-bridge/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/alexei-led/pi-subagents-bridge/actions/workflows/ci.yml?query=branch%3Amain)
[![node](https://img.shields.io/badge/node-%3E%3D22.19.0-5fa04e?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](./LICENSE)

> Make `@tintinweb/pi-tasks` TaskExecute work with `nicobailon/pi-subagents`.

![pi-subagents-bridge](https://raw.githubusercontent.com/alexei-led/pi-subagents-bridge/main/assets/bridge.svg)

`pi-subagents-bridge` is a protocol adapter.
It speaks the v2 RPC that `@tintinweb/pi-tasks` expects and forwards the work to `nicobailon/pi-subagents` v1 RPC.

## Install

```bash
pi install npm:@alexeiled/pi-subagents-bridge
```

Requires both `@tintinweb/pi-tasks` and `nicobailon/pi-subagents` to be installed.

## Use

```text
TaskCreate(title="Write a short plan", agentType="general-purpose", ...)
TaskExecute(task_ids=["1"])
```

The bridge keeps custom `agentType` names unchanged.
It only maps pi-tasks' common examples to nicobailon builtins:

| pi-tasks `agentType`  | pi-subagents agent |
| --------------------- | ------------------ |
| `general-purpose`     | `delegate`         |
| `Explore` / `explore` | `scout`            |

Use exact Pi agent names for everything else.

## Protocol

| Direction             | Channel                              | Payload                                                                                                                                                                                     |
| --------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pi-tasks → bridge     | `subagents:rpc:ping`                 | reply `{ success: true, data: { version: 2 } }`                                                                                                                                             |
| Pi-tasks → bridge     | `subagents:rpc:spawn`                | forwards `prompt` → `task`, `options.model` → `model`, `options.maxTurns` → `turnBudget.maxTurns`; maps `general-purpose` → `delegate` and `Explore` → `scout`; sets async/fresh/no-clarify |
| bridge → pi-subagents | `subagents:rpc:v1:request`           | `{ version: 1, requestId, method, params }`                                                                                                                                                 |
| bridge ← pi-subagents | `subagents:rpc:v1:reply:<requestId>` | spawn reply is read from `data.details.runId` / `data.details.asyncId`                                                                                                                      |
| pi-subagents → bridge | `subagent:async-complete`            | `state=complete` → `subagents:completed`; `state=failed` → `subagents:failed`; `state=paused` → `subagents:failed` with `status="stopped"` and the partial result                           |

## Constraint

Do not load `@tintinweb/pi-subagents` at the same time.
The bridge only tracks its own spawned run IDs, so unrelated `pi-subagents` runs pass through untouched.

## Release

See [`DEVELOPMENT.md`](./DEVELOPMENT.md) for the local validation gate and release steps.
