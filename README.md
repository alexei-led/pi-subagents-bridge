# pi-subagents-bridge

[![npm version](https://img.shields.io/npm/v/%40alexeiled%2Fpi-subagents-bridge?style=flat-square&logo=npm&logoColor=white)](https://www.npmjs.com/package/@alexeiled/pi-subagents-bridge)
[![CI](https://img.shields.io/github/actions/workflow/status/alexei-led/pi-subagents-bridge/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/alexei-led/pi-subagents-bridge/actions/workflows/ci.yml?query=branch%3Amain)
[![node](https://img.shields.io/badge/node-%3E%3D22.19.0-5fa04e?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![license](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](./LICENSE)

`pi-subagents-bridge` is a protocol adapter between two Pi extensions:

- [`@tintinweb/pi-tasks`](https://www.npmjs.com/package/@tintinweb/pi-tasks)
- [`pi-subagents`](https://www.npmjs.com/package/pi-subagents)

It makes `TaskExecute` work with `pi-subagents` by translating the `pi-tasks` subagent RPC into the RPC and completion events that `pi-subagents` exposes. It also exposes a separate, generic plan-exec RPC for direct execution clients.

## What it does

`@tintinweb/pi-tasks` expects a v2 subagent protocol.
`pi-subagents` exposes a v1 RPC plus async completion events.
This bridge sits between them and handles the mismatch.

Specifically, it:

- answers the `pi-tasks` v2 ping handshake
- forwards `spawn` and `stop` requests to `pi-subagents`
- translates `pi-subagents` completion events back into `pi-tasks` task updates
- falls back to polling `pi-subagents` run `status` if an async completion event is missed
- keeps the generic plan-exec RPC separate from all `pi-tasks` channels

### Request and completion flow

```mermaid
sequenceDiagram
  autonumber
  participant Tasks as @tintinweb/pi-tasks
  participant Bridge as pi-subagents-bridge
  participant Subs as pi-subagents

  Tasks->>Bridge: ping (v2)
  Bridge-->>Tasks: version 2

  Tasks->>Bridge: spawn(type, prompt, options)
  Bridge->>Subs: spawn(script, async: true)
  Subs-->>Bridge: runId
  Bridge-->>Tasks: id = runId

  Subs-->>Bridge: subagent:async-complete
  Bridge-->>Tasks: subagents:completed / subagents:failed

  alt async completion event is missed
    Bridge->>Subs: status(runId)
    Subs-->>Bridge: state + result path
    Bridge-->>Tasks: subagents:completed / subagents:failed
  end
```

## Install

Install the two extensions being bridged, then install the bridge:

```bash
pi install npm:@tintinweb/pi-tasks
pi install npm:pi-subagents
pi install npm:@alexeiled/pi-subagents-bridge
```

Requirements:

- Node `>= 22.19.0`
- Pi `^1.0.2` with extension loading enabled; tested with Pi `1.0.2`
- `pi-subagents >= 0.76.0 < 0.77.0`; tested with `0.76.0` for RPC `script` input and native terminal proofs

The npm peer requirement does not verify which Pi extension is active. If a
closed workflow status has no native proof field, the bridge reports an error
to check the active runtime and durable async status/proof artifacts instead of
claiming that the worker exited.

The bridge uses Pi's public extension event API. Future Pi releases still need
validation if that API or the upstream subagent protocol changes.

The operation journal supports schema versions 1–5. Versions 4 and 5 retain their
version and extra fields when opened by this bridge. Unknown versions are rejected
before database changes; update the bridge instead of deleting or resetting the journal.

## Usage

Create tasks with `TaskCreate`, then execute them with `TaskExecute`.

```text
TaskCreate(subject="Write a short plan", agentType="general-purpose", ...)
TaskExecute(task_ids=["1"])
```

### Agent type mapping

The bridge preserves task agent names except for the compatibility aliases that `pi-tasks` examples commonly use.

| `TaskCreate(..., agentType=...)` | `pi-subagents` agent     |
| -------------------------------- | ------------------------ |
| `general-purpose`                | `delegate`               |
| `Explore`                        | `scout`                  |
| `explore`                        | `scout`                  |
| anything else                    | passed through unchanged |

Use an execution-capable agent for tasks that need shell commands, builds, or tests.
A read-only agent can still be useful for read-only tasks such as review or metadata inspection.

## Bridge behavior

### Request flow

| `pi-tasks` input      | Bridge behavior                                   |
| --------------------- | ------------------------------------------------- |
| `subagents:rpc:ping`  | replies locally with protocol version `2`         |
| `subagents:rpc:spawn` | sends a `pi-subagents` v1 `spawn` request         |
| `subagents:rpc:stop`  | sends a best-effort `pi-subagents` `stop` request |

Spawned runs are always forwarded as:

- one-child workflow execution through RPC `script`
- `async: true`
- `context: "fresh"`
- `control: { enabled: false }` on the outer workflow and its child

The bridge does not send the removed public `workflowScript` or `clarify` fields.
The plan-exec capability name `workflowScriptSpawn` is unchanged; it is not an RPC input field.

The bridge returns the spawned run ID back to `pi-tasks` and tracks that run as bridge-owned state. It runs at most **two** bridge-owned tasks at once. A task without an explicit `maxTurns` receives a **12-turn** budget.

### Completion flow

For runs the bridge spawned itself, it converts `pi-subagents` outcomes into `pi-tasks` events:

- success → `subagents:completed`
- failure → `subagents:failed`
- stopped or paused run → `subagents:failed` with `status: "stopped"`

If `subagent:async-complete` does not arrive, the bridge polls `pi-subagents` `status`, reads the result file path from that status output, and emits the same completion event that `pi-tasks` expects. If a terminal status arrives before its result file is readable, the bridge retries for up to five seconds instead of silently dropping the result.

## Generic plan-exec RPC

This protocol is independent of `pi-tasks`. Version 1 remains available on `plan-exec:bridge:v1:request` with replies on `plan-exec:bridge:v1:reply:<requestId>`.

Version 2 uses `plan-exec:bridge:v2:request` and `plan-exec:bridge:v2:reply:<requestId>`. It supports `ping`, `spawn`, `operation`, `status`, `result`, `stop`, `adopt`, `cancelOperation`, and optional `diagnoseOperation` with durable launch identity and native process-terminal proof.

- `ping` verifies the live `pi-subagents` RPC before advertising `workflowScriptSpawn`, `durableOperationLookup`, and `processTerminalProof` capabilities.
- `spawn` requires `operationId`, `cwd` when needed, `params.agent`, `params.task`, and an owner `{ kind: "pi-plan-exec", runId, key, requestDigest }`. The digest is SHA-256 over canonical `{ cwd, params }`. The bridge rejects mismatches before dispatch.
- The durable journal is written before the native spawn is emitted. A bound operation survives a full Pi restart. A legacy dispatch with no durable native reply becomes `unknown`; the bridge never launches it again automatically.
- `operation` never starts work. Version 2 returns `operationId`, `requestDigest`, and `absent`, `pending`, `found`, `cancelled`, or `unknown` binding state. It redelivers a durable cancellation intent when needed.
- `status` and `adopt` include a validated native `processTerminal` value when `pi-subagents` returns one. Only an `observed` proof with the matching run ID proves process termination.
- `result` uses the native status RPC because `pi-subagents` has no separate result RPC. `stop` delegates to the native stop RPC.
- `adopt` is observational. It does not silently transfer session ownership. Status and stop use native run IDs; the native runtime still enforces its session restrictions.

Explicit execution lifetimes require v2. Set `params.executionLifetime` to
`{ mode: "unbounded" }` or `{ mode: "bounded", timeoutMs: 1800000 }`.
Do not combine this field with legacy `timeout` or `timeoutMs`. Both paths wrap
the agent and task in a one-child async workflow sent through RPC `script`.
A bounded lifetime becomes `timeoutMs`; an unbounded request forwards no timeout.
The lifetime is included in the caller digest and echoed as
`effectiveExecutionLifetime`; this is bridge intent, not native attestation.
Explicit lifetimes do not accept caller workflow scripts.

`ping` advertises lifetime support when native async spawn and stop are available.
Ownership is bridge-supervised with best-effort escaped-descendant handling,
not kernel containment. The bridge journal owns operation identity and replay
protection; the released upstream RPC has no operation-ID lookup. A bound run
can be recovered from the journal. A lost spawn reply without a binding remains
`unknown`, including after restart.

`cancelOperation` records cancellation intent. It reports `neverStarted: true`
only when its atomic journal transaction creates a new fence before any dispatch
record exists. An existing unbound operation returns `unknown` and
`neverStarted: false`, including repeat cancellation of an old fence.
It is not safe to infer non-start from a missing run ID or a timeout.
A known run is stopped through native RPC; a stop request is not exit proof.

Reconcile native `processTerminalProof` (also exposed as `processTerminal`)
before starting replacement work. A workflow uses native `workflowTerminalProof`:
dispatch must be closed and each child observed or explicitly not-started.
Pending, unknown, malformed, or absent proofs do not establish completion.
The workflow's hosting Pi process can remain alive.

When native `diagnosticGuidance` advertises durable, idempotent `follow_up`
guidance for confirmed tool failures, `diagnoseOperation` accepts
`{ operationId, owner, params: { diagnosticId, toolCallId, message } }`.
The native runtime checks the referenced failed tool and queues guidance into the
existing live session. Reuse the same diagnostic ID and payload after an uncertain
reply: durable native receipts prevent a second enqueue. Replies bind the caller
operation, request digest, diagnostic ID, and tool-call ID, with
`guidanceOnly: true` and `queued`, `pending`, `cancelled`, or `rejected` state.
An enqueue receipt does not confirm a repair. Cancellation fences late guidance;
this method does not start or revive a worker.

The Vitest suite checks the released RPC validator, not only capability mocks.
An isolated Pi integration test completes both Bridge paths against real
pi-subagents workflows and child runtimes, using only a local fixture HTTP model.
It verifies lookup, replay identity, native workflow proof, and task completion.

### Upgrade and unresolved launches

Update pi-subagents to the supported range before installing Bridge 0.5.2, then
reload Pi. The fix applies to new launches; it does not rewrite old journal rows.
A previous `invalid_params` failure still looks `unknown` if no authoritative
binding or durable no-start evidence was stored. Do not delete the journal,
cancel to manufacture no-start proof, or launch a replacement worker.
Use the owning controller's diagnostic/recovery path; `/exec resume` alone may
remain blocked until that operation is reconciled.

The journal defaults to `~/.pi/pi-subagents-bridge/plan-exec-operations.sqlite`. SQLite transactions provide crash recovery and cross-process serialization without a stale application lock. Version 1 clients retain their existing response shape and also benefit from durable bound-operation lookup. Operation identity rows are retained as idempotency records; automatic pruning could make an old operation ID dispatch again. Remove the database only after all referenced plan runs are permanently retired and duplicate-launch protection is no longer needed. Existing v1 accepted-run rows migrate fail-closed with no session identity; they require explicit manual recovery rather than unsafe cross-session delivery.

Failures use `{ success: false, error: { code, message } }`. `operation_capacity` means the in-process bridge has 128 unresolved active operation IDs and will not evict one to accept another spawn.

## TaskExecute-specific safeguards

`TaskExecute` is queue-style orchestration, not direct subagent supervision.
Because of that, the bridge also applies two execution defaults to bridge-spawned runs:

- disables `pi-subagents` acceptance gating
- disables `pi-subagents` live control nudges

This avoids false pauses on missing acceptance reports and avoids misleading background `needs attention` notices for normal task runs. Repeated copies of one live request are coalesced only after their session and request digest match. The request ID is also journaled before native dispatch, so replay after the in-memory reply cache expires returns the existing run or fails closed as unknown instead of dispatching again. Transient persistence failures for known accepted runs and launch bindings are retried while the bridge process remains active.

Accepted run IDs are tied to the originating Pi session ID and a leased process owner. A foreign Pi session cannot claim or delete them. The same resumed session can reclaim them after the owner exits. An expired heartbeat alone cannot prove exit; a reused or still-live PID leaves ownership unresolved. The owner renews its fence before emitting completion, and failed reconciliation is retried periodically. If another session wins ownership, the bridge stops polling and emits `subagents:warning` with code `accepted_run_ownership_lost` instead of silently dropping the run.

The legacy `pi-tasks` spawn request does not contain task ID, list ID, or attempt generation. Therefore the bridge cannot recover a task binding after a crash that occurs after native dispatch but before the run ID is received. The durable request becomes unknown and is not launched again automatically. The bridge does not use prompt matching.

If the native run ID is known but local accepted-run persistence fails, the bridge acknowledges that known run instead of returning an error that could trigger a duplicate launch. It keeps completion ownership for the current process and logs the durability loss; a subsequent process restart then requires manual completion recovery.

## Scope and limits

This package is intentionally narrow.

It does:

- bridge `TaskExecute` task launches to `pi-subagents`
- track only runs spawned through this bridge
- ignore unrelated `pi-subagents` runs

It does not:

- replace `pi-tasks` task orchestration
- act as a generic adapter for `pi-subagents` methods beyond the documented plan-exec protocol
- support loading `@tintinweb/pi-subagents` alongside this bridge

Do not load `@tintinweb/pi-subagents` at the same time as this package.

## More detail

- [`docs/design.md`](./docs/design.md) — bridge behavior and maintenance rules
- [`docs/protocol-research.md`](./docs/protocol-research.md) — upstream protocol notes
- [`DEVELOPMENT.md`](./DEVELOPMENT.md) — local validation and release workflow
- [`CHANGELOG.md`](./CHANGELOG.md) — release history and RPC compatibility changes
