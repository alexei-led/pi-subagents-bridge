import * as fs from "node:fs";
import { randomUUID } from "node:crypto";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// Protocol evidence (installed sources verified against npm pack pi-subagents@0.34.0):
// - @tintinweb/pi-tasks src/index.ts:103-119 reply channel/envelope,
//   126-133 spawn/stop params, 137-157 strict PROTOCOL_VERSION=2,
//   207-260 completed/failed/stopped fields.
// - pi-subagents src/extension/rpc.ts:13-18 v1 channel/methods,
//   32-45 reply envelope, 128-132 spawn details, 141-147 id/runId stop target,
//   193-204 async-only spawn/no clarify.
// - pi-subagents src/runs/background/result-watcher.ts:49-57 result file fields,
//   141-164 child output/status normalization, 193-204 async-complete payload;
//   subagent-runner.ts:3066-3073 complete/failed/paused state values.
// - pi-subagents src/agents/agents.ts:31-40 builtin names and
//   src/agents/agent-selection.ts:4-19 exact-name merge; pi-tasks examples need aliases.
const PING_CHANNEL = "subagents:rpc:ping";
const SPAWN_CHANNEL = "subagents:rpc:spawn";
const STOP_CHANNEL = "subagents:rpc:stop";
const COMPLETED_EVENT = "subagents:completed";
const FAILED_EVENT = "subagents:failed";
const READY_EVENT = "subagents:ready";
const NB_REQUEST_CHANNEL = "subagents:rpc:v1:request";
const NB_COMPLETE_EVENT = "subagent:async-complete";
const NB_REPLY_PREFIX = "subagents:rpc:v1:reply:";
const DEFAULT_SPAWN_TIMEOUT_MS = 24_000;
const DEFAULT_COMPLETION_POLL_INTERVAL_MS = 2_000;

const AGENT_TYPE_ALIASES = new Map<string, string>([
  ["general-purpose", "delegate"],
  ["Explore", "scout"],
  ["explore", "scout"],
]);

const BRIDGE_ACCEPTANCE_CONFIG = {
  level: "none",
  reason:
    "pi-tasks bridge manages task lifecycle and result propagation; do not require pi-subagents acceptance reports.",
} as const;

const BRIDGE_CONTROL_CONFIG = {
  enabled: false,
} as const;

interface BridgeOptions {
  spawnTimeoutMs?: number;
  completionPollIntervalMs?: number;
}

type BridgeHost = Pick<ExtensionAPI, "events">;
type Unsubscribe = () => void;
type RpcReply<T> =
  { success: true; data: T } | { success: false; error: string };
type CompletionKind = "completed" | "failed" | "stopped";

interface BridgeState {
  ownedRunIds: Set<string>;
  completedRunIds: Set<string>;
}

const bridgeStates = new WeakMap<BridgeHost["events"], BridgeState>();

interface SpawnOptionsRaw {
  model?: unknown;
  maxTurns?: unknown;
}

interface AsyncCompleteRaw {
  runId?: unknown;
  asyncId?: unknown;
  id?: unknown;
  success?: unknown;
  state?: unknown;
  summary?: unknown;
  output?: unknown;
  error?: unknown;
  results?: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function replyChannel(channel: string, requestId: string): string {
  return `${channel}:reply:${requestId}`;
}

function nbReplyChannel(requestId: string): string {
  return `${NB_REPLY_PREFIX}${requestId}`;
}

function emitReply<T>(
  events: BridgeHost["events"],
  channel: string,
  requestId: string,
  reply: RpcReply<T>,
): void {
  events.emit(replyChannel(channel, requestId), reply);
}

function extractSpawnRunId(reply: unknown): string | undefined {
  if (!isRecord(reply)) return undefined;

  const details = isRecord(reply.details) ? reply.details : undefined;
  return (
    text(details?.runId) ??
    text(details?.asyncId) ??
    text(reply.runId) ??
    text(reply.asyncId)
  );
}

function extractRunId(payload: AsyncCompleteRaw): string | undefined {
  return text(payload.runId) ?? text(payload.asyncId) ?? text(payload.id);
}

function extractChildOutputs(payload: AsyncCompleteRaw): string[] {
  if (!Array.isArray(payload.results)) return [];

  const outputs: string[] = [];
  for (const result of payload.results) {
    if (!isRecord(result)) continue;
    const output = text(result.output) ?? text(result.error);
    if (output) outputs.push(output);
  }
  return outputs;
}

function extractCompletedResult(payload: AsyncCompleteRaw): string | undefined {
  return (
    text(payload.summary) ??
    text(payload.output) ??
    (extractChildOutputs(payload).join("\n\n") || undefined)
  );
}

function extractStoppedResult(payload: AsyncCompleteRaw): string | undefined {
  return (
    (extractChildOutputs(payload).join("\n\n") || undefined) ??
    text(payload.output) ??
    text(payload.summary)
  );
}

function extractFailureError(payload: AsyncCompleteRaw): string {
  const error = text(payload.error);
  if (error) return error;
  if (Array.isArray(payload.results)) {
    for (const result of payload.results) {
      if (!isRecord(result)) continue;
      const error = text(result.error);
      if (error) return error;
    }
  }
  return "Agent failed";
}

function classifyCompleteEvent(payload: AsyncCompleteRaw): CompletionKind {
  const state = text(payload.state);
  if (state === "paused" || state === "stopped") return "stopped";
  if (state === "failed" || state === "aborted" || payload.success === false)
    return "failed";
  if (state === "complete" || payload.success === true) return "completed";
  return "failed";
}

function resolveAgentType(type: string): string {
  return AGENT_TYPE_ALIASES.get(type) ?? type;
}

function requestNicobailonRpc<T>(
  events: BridgeHost["events"],
  method: "spawn" | "status",
  params: Record<string, unknown>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<T> {
  const requestId = randomUUID();

  return new Promise<T>((resolve, reject) => {
    let settled = false;

    const cleanup = (): void => {
      if (settled) return;
      settled = true;
      unsubscribe();
      clearTimeout(timeout);
      signal?.removeEventListener("abort", onAbort);
    };

    const fail = (error: Error): void => {
      if (settled) return;
      cleanup();
      reject(error);
    };

    const succeed = (value: T): void => {
      if (settled) return;
      cleanup();
      resolve(value);
    };

    const onAbort = (): void => {
      fail(new Error("Bridge disposed"));
    };

    const timeout = setTimeout(() => {
      fail(
        new Error(`nicobailon "${method}" RPC timed out after ${timeoutMs}ms`),
      );
    }, timeoutMs);

    const unsubscribe = events.on(nbReplyChannel(requestId), (raw: unknown) => {
      if (!isRecord(raw) || typeof raw.success !== "boolean") {
        fail(new Error("Malformed nicobailon RPC reply."));
        return;
      }

      if (raw.success) {
        succeed(raw.data as T);
        return;
      }

      const message = isRecord(raw.error)
        ? text(raw.error.message)
        : text(raw.error);
      fail(new Error(message ?? "nicobailon RPC error"));
    });

    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) {
      onAbort();
      return;
    }

    events.emit(NB_REQUEST_CHANNEL, {
      version: 1,
      requestId,
      method,
      params,
    });
  });
}

function normalizeSpawnOptions(raw: unknown): SpawnOptionsRaw | undefined {
  return isRecord(raw) ? raw : undefined;
}

function extractRpcText(reply: unknown): string | undefined {
  return isRecord(reply) ? text(reply.text) : undefined;
}

function parseStatusState(statusText: string): string | undefined {
  const match = /^State:\s+(.+)$/im.exec(statusText);
  return match?.[1]?.trim().toLowerCase();
}

function parseResultPath(statusText: string): string | undefined {
  const match = /^Result:\s+(.+)$/im.exec(statusText);
  return match?.[1]?.trim();
}

function classifyStatusText(statusText: string): CompletionKind | undefined {
  const state = parseStatusState(statusText);
  if (!state) return undefined;
  if (state === "paused" || state === "stopped") return "stopped";
  if (state === "failed" || state === "aborted") return "failed";
  if (state === "complete") return "completed";
  return undefined;
}

function readResultPayload(
  resultPath: string | undefined,
): AsyncCompleteRaw | undefined {
  if (!resultPath || !fs.existsSync(resultPath)) return undefined;
  try {
    const parsed: unknown = JSON.parse(fs.readFileSync(resultPath, "utf8"));
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function getBridgeState(events: BridgeHost["events"]): BridgeState {
  const existing = bridgeStates.get(events);
  if (existing) return existing;

  const created: BridgeState = {
    ownedRunIds: new Set<string>(),
    completedRunIds: new Set<string>(),
  };
  bridgeStates.set(events, created);
  return created;
}

export function registerBridge(
  pi: BridgeHost,
  options: BridgeOptions = {},
): { dispose: () => void } {
  const requestedSpawnTimeoutMs = options.spawnTimeoutMs;
  const spawnTimeoutMs =
    requestedSpawnTimeoutMs !== undefined && requestedSpawnTimeoutMs > 0
      ? requestedSpawnTimeoutMs
      : DEFAULT_SPAWN_TIMEOUT_MS;
  const requestedCompletionPollIntervalMs = options.completionPollIntervalMs;
  const completionPollIntervalMs =
    requestedCompletionPollIntervalMs !== undefined &&
    requestedCompletionPollIntervalMs > 0
      ? requestedCompletionPollIntervalMs
      : DEFAULT_COMPLETION_POLL_INTERVAL_MS;

  const { ownedRunIds, completedRunIds } = getBridgeState(pi.events);
  const pendingRpcControllers = new Set<AbortController>();
  const completionPollTimers = new Map<string, ReturnType<typeof setTimeout>>();
  const unsubscribes: Unsubscribe[] = [];
  let disposed = false;

  const track = (unsubscribe: Unsubscribe | void): void => {
    if (typeof unsubscribe === "function") unsubscribes.push(unsubscribe);
  };

  const clearCompletionPoll = (runId: string): void => {
    const timer = completionPollTimers.get(runId);
    if (timer) clearTimeout(timer);
    completionPollTimers.delete(runId);
  };

  const shouldStopPolling = (runId: string): boolean =>
    disposed || completedRunIds.has(runId) || !ownedRunIds.has(runId);

  const emitCompletion = (
    runId: string,
    kind: CompletionKind,
    payload?: AsyncCompleteRaw,
  ): void => {
    if (!ownedRunIds.has(runId) || completedRunIds.has(runId)) return;

    completedRunIds.add(runId);
    ownedRunIds.delete(runId);
    clearCompletionPoll(runId);

    if (kind === "stopped") {
      const result = payload ? extractStoppedResult(payload) : undefined;
      pi.events.emit(FAILED_EVENT, {
        id: runId,
        ...(result ? { result } : {}),
        status: "stopped",
      });
      return;
    }

    if (kind === "failed") {
      pi.events.emit(FAILED_EVENT, {
        id: runId,
        error: payload ? extractFailureError(payload) : "Agent failed",
        status: "failed",
      });
      return;
    }

    const result = payload ? extractCompletedResult(payload) : undefined;
    pi.events.emit(COMPLETED_EVENT, {
      id: runId,
      ...(result ? { result } : {}),
    });
  };

  const pollRunCompletion = async (runId: string): Promise<void> => {
    completionPollTimers.delete(runId);
    if (shouldStopPolling(runId)) {
      clearCompletionPoll(runId);
      return;
    }

    const controller = new AbortController();
    pendingRpcControllers.add(controller);

    try {
      const reply = await requestNicobailonRpc<unknown>(
        pi.events,
        "status",
        { id: runId },
        spawnTimeoutMs,
        controller.signal,
      );
      if (shouldStopPolling(runId)) {
        clearCompletionPoll(runId);
        return;
      }

      const statusText = extractRpcText(reply);
      const kind = statusText ? classifyStatusText(statusText) : undefined;
      if (kind && statusText) {
        const payload = readResultPayload(parseResultPath(statusText));
        emitCompletion(runId, kind, payload);
        return;
      }
    } catch {
      if (disposed) return;
    } finally {
      pendingRpcControllers.delete(controller);
    }

    if (shouldStopPolling(runId)) {
      clearCompletionPoll(runId);
      return;
    }

    const timer = setTimeout(() => {
      void pollRunCompletion(runId);
    }, completionPollIntervalMs);
    timer.unref();
    completionPollTimers.set(runId, timer);
  };

  const ensureCompletionPoll = (runId: string): void => {
    if (completionPollTimers.has(runId)) return;
    const timer = setTimeout(() => {
      void pollRunCompletion(runId);
    }, completionPollIntervalMs);
    timer.unref();
    completionPollTimers.set(runId, timer);
  };

  // pi-subagents/src/agents/agents.ts exposes exact runtime names and has no
  // general-purpose/Explore builtins. Keep only these pi-tasks compatibility aliases.
  track(
    pi.events.on(PING_CHANNEL, (raw: unknown) => {
      if (!isRecord(raw)) return;
      const requestId = text(raw.requestId);
      if (!requestId) return;

      emitReply(pi.events, PING_CHANNEL, requestId, {
        success: true,
        data: { version: 2 },
      } satisfies RpcReply<{ version: number }>);
    }),
  );

  const handleSpawn = async (raw: unknown): Promise<void> => {
    if (!isRecord(raw)) return;
    const requestId = text(raw.requestId);
    if (!requestId) return;

    const agentType = text(raw.type);
    const prompt = text(raw.prompt);
    if (!agentType || !prompt) {
      emitReply(pi.events, SPAWN_CHANNEL, requestId, {
        success: false,
        error: "spawn requires string type and prompt",
      });
      return;
    }

    const optionsRaw = normalizeSpawnOptions(raw.options);
    const model = text(optionsRaw?.model);
    const maxTurns =
      typeof optionsRaw?.maxTurns === "number" &&
      Number.isInteger(optionsRaw.maxTurns) &&
      optionsRaw.maxTurns > 0
        ? optionsRaw.maxTurns
        : undefined;
    const spawnParams: Record<string, unknown> = {
      agent: resolveAgentType(agentType),
      task: prompt,
      async: true,
      clarify: false,
      context: "fresh",
      // pi-tasks has its own task/result contract and no channel for pi-subagents'
      // structured acceptance gate. Without this override, async write-capable runs
      // infer reviewed/checked acceptance and can pause on a missing
      // `acceptance-report` even when the task itself succeeded.
      acceptance: BRIDGE_ACCEPTANCE_CONFIG,
      // TaskExecute is fire-and-forget orchestration, not an interactive subagent
      // supervisor. Disable pi-subagents live control nudges so background tasks
      // do not surface misleading 60s "needs attention" prompts through the bridge.
      control: BRIDGE_CONTROL_CONFIG,
      ...(model ? { model } : {}),
      ...(maxTurns ? { turnBudget: { maxTurns } } : {}),
    };

    const controller = new AbortController();
    pendingRpcControllers.add(controller);

    try {
      // pi-subagents rpc.ts: dataFromToolResult keeps async spawn metadata under details,
      // including details.runId/details.asyncId for the launched async run.
      const reply = await requestNicobailonRpc<unknown>(
        pi.events,
        "spawn",
        spawnParams,
        spawnTimeoutMs,
        controller.signal,
      );
      if (disposed) return;

      const runId = extractSpawnRunId(reply);
      if (!runId)
        throw new Error("nicobailon spawn reply did not include a run id");

      ownedRunIds.add(runId);
      ensureCompletionPoll(runId);
      emitReply(pi.events, SPAWN_CHANNEL, requestId, {
        success: true,
        data: { id: runId },
      } satisfies RpcReply<{ id: string }>);
    } catch (error: unknown) {
      if (disposed || controller.signal.aborted) return;

      emitReply(pi.events, SPAWN_CHANNEL, requestId, {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      pendingRpcControllers.delete(controller);
    }
  };

  track(
    pi.events.on(SPAWN_CHANNEL, (raw: unknown) => {
      void handleSpawn(raw);
    }),
  );

  track(
    pi.events.on(STOP_CHANNEL, (raw: unknown) => {
      if (!isRecord(raw)) return;
      const requestId = text(raw.requestId);
      if (!requestId) return;

      const agentId = text(raw.agentId) ?? text(raw.id) ?? text(raw.runId);
      if (agentId && ownedRunIds.has(agentId)) {
        try {
          // pi-subagents rpc.ts normalizes stop targets from id/runId; use canonical id here.
          pi.events.emit(NB_REQUEST_CHANNEL, {
            version: 1,
            requestId: randomUUID(),
            method: "stop",
            params: { id: agentId },
          });
        } catch {
          // pi-tasks ignores stop failures; still acknowledge success locally.
        }
      }

      emitReply(pi.events, STOP_CHANNEL, requestId, {
        success: true,
        data: undefined,
      } satisfies RpcReply<void>);
    }),
  );

  track(
    pi.events.on(NB_COMPLETE_EVENT, (raw: unknown) => {
      if (!isRecord(raw)) return;
      const payload = raw as AsyncCompleteRaw;
      const runId = extractRunId(payload);
      if (!runId || !ownedRunIds.has(runId) || completedRunIds.has(runId))
        return;

      emitCompletion(runId, classifyCompleteEvent(payload), payload);
    }),
  );

  for (const runId of ownedRunIds) ensureCompletionPoll(runId);

  pi.events.emit(READY_EVENT, {});

  return {
    dispose() {
      disposed = true;

      for (const controller of pendingRpcControllers) {
        controller.abort();
      }
      pendingRpcControllers.clear();

      for (const timer of completionPollTimers.values()) {
        clearTimeout(timer);
      }
      completionPollTimers.clear();

      while (unsubscribes.length > 0) {
        const unsubscribe = unsubscribes.pop();
        try {
          unsubscribe?.();
        } catch {
          // Best effort cleanup.
        }
      }
    },
  };
}

export default function bridgeExtension(pi: ExtensionAPI): void {
  registerBridge(pi);
}
