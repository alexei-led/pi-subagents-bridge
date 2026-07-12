import { randomUUID } from "node:crypto";

export const PLAN_EXEC_REQUEST_EVENT = "plan-exec:bridge:v1:request";
export const PLAN_EXEC_REPLY_PREFIX = "plan-exec:bridge:v1:reply:";

const SUBAGENTS_REQUEST_EVENT = "subagents:rpc:v1:request";
const SUBAGENTS_REPLY_PREFIX = "subagents:rpc:v1:reply:";
const PROTOCOL_VERSION = 1;
const METHODS = ["ping", "spawn", "status", "result", "stop", "adopt"] as const;

type Method = (typeof METHODS)[number];
type UpstreamMethod = "spawn" | "status" | "stop";
type Unsubscribe = () => void;

type EventBus = {
  on(event: string, handler: (payload: unknown) => void): Unsubscribe | void;
  emit(event: string, payload: unknown): void;
};

type Failure = {
  success: false;
  error: { code: "invalid_request" | "upstream_error"; message: string };
};
type Reply<T> = { success: true; data: T } | Failure;

interface SpawnRequest {
  operationId: string;
  fingerprint: string;
  params: Record<string, unknown>;
}

interface SpawnResult {
  runId: string;
  asyncDir?: string;
}

interface RunRequest {
  runId: string;
  asyncDir?: string;
}

interface Observation {
  runId: string;
  observed?: true;
  state?: string;
  asyncDir?: string;
  resultPath?: string;
  text?: string;
}

interface StopResult {
  runId: string;
  asyncDir?: string;
  state: string;
}

interface PlanExecOptions {
  timeoutMs: number;
}

interface Operation {
  fingerprint: string;
  reply: Promise<Reply<SpawnResult>>;
}

interface PlanExecState {
  operations: Map<string, Operation>;
  spawnControllers: Set<AbortController>;
  registration?: { dispose(): void };
}

const planExecStates = new WeakMap<EventBus, PlanExecState>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function replyEvent(requestId: string): string {
  return `${PLAN_EXEC_REPLY_PREFIX}${requestId}`;
}

function upstreamReplyEvent(requestId: string): string {
  return `${SUBAGENTS_REPLY_PREFIX}${requestId}`;
}

function failure(
  code: "invalid_request" | "upstream_error",
  message: string,
): Failure {
  return { success: false, error: { code, message } };
}

function isMethod(value: string): value is Method {
  return (METHODS as readonly string[]).includes(value);
}

function isFailure(value: unknown): value is Failure {
  return isRecord(value) && value.success === false && isRecord(value.error);
}

function extractSpawnRunId(reply: unknown): string | undefined {
  if (!isRecord(reply)) return undefined;
  const details = isRecord(reply.details) ? reply.details : undefined;
  return (
    nonEmptyString(details?.runId) ??
    nonEmptyString(details?.asyncId) ??
    nonEmptyString(reply.runId) ??
    nonEmptyString(reply.asyncId)
  );
}

function extractSpawnAsyncDir(reply: unknown): string | undefined {
  if (!isRecord(reply)) return undefined;
  const details = isRecord(reply.details) ? reply.details : undefined;
  return nonEmptyString(details?.asyncDir) ?? nonEmptyString(reply.asyncDir);
}

function parseStatusLine(
  text: string,
  name: "State" | "Result" | "Dir",
): string | undefined {
  const match = new RegExp(`^${name}:\\s+(.+)$`, "im").exec(text);
  return nonEmptyString(match?.[1]);
}

function normalizeState(value: unknown): string | undefined {
  return nonEmptyString(value)?.toLowerCase();
}

function validateOptionalString(
  value: Record<string, unknown>,
  key: string,
): string | undefined | Failure {
  if (!(key in value)) return undefined;
  return (
    nonEmptyString(value[key]) ??
    failure("invalid_request", `spawn ${key} must be a non-empty string`)
  );
}

function validateOptionalTimeout(
  params: Record<string, unknown>,
  key: "timeout" | "timeoutMs",
): number | undefined | Failure {
  if (!(key in params)) return undefined;
  const value = params[key];
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? value
    : failure("invalid_request", `spawn ${key} must be a positive number`);
}

function operationFingerprint(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) {
    return `[${value.map((item) => operationFingerprint(item)).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) => `${JSON.stringify(key)}:${operationFingerprint(value[key])}`,
      )
      .join(",")}}`;
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "bigint") return `bigint:${value.toString()}`;
  if (typeof value === "symbol") return `symbol:${value.description ?? ""}`;
  if (typeof value === "undefined") return "undefined";
  return "function";
}

function validateSpawn(raw: Record<string, unknown>): SpawnRequest | Failure {
  const operationId = nonEmptyString(raw.operationId);
  if (!operationId) {
    return failure("invalid_request", "spawn requires a non-empty operationId");
  }

  const params = raw.params;
  if (!isRecord(params)) {
    return failure("invalid_request", "spawn requires an object params");
  }

  const agent = nonEmptyString(params.agent);
  const task = nonEmptyString(params.task);
  if (!agent || !task) {
    return failure(
      "invalid_request",
      "spawn requires non-empty string agent and task",
    );
  }

  const topLevelCwd = validateOptionalString(raw, "cwd");
  const paramsCwd = validateOptionalString(params, "cwd");
  const timeout = validateOptionalTimeout(params, "timeout");
  const timeoutMs = validateOptionalTimeout(params, "timeoutMs");
  for (const value of [topLevelCwd, paramsCwd, timeout, timeoutMs]) {
    if (isFailure(value)) return value;
  }
  if (topLevelCwd && paramsCwd && topLevelCwd !== paramsCwd) {
    return failure(
      "invalid_request",
      "spawn cwd must match when supplied at both the request and params levels",
    );
  }
  if (
    timeout !== undefined &&
    timeoutMs !== undefined &&
    timeout !== timeoutMs
  ) {
    return failure(
      "invalid_request",
      "spawn timeout and timeoutMs must match when both are supplied",
    );
  }
  if (params.async === false) {
    return failure(
      "invalid_request",
      "spawn only supports detached async execution",
    );
  }
  if (params.clarify === true) {
    return failure("invalid_request", "spawn cannot set clarify to true");
  }

  const cwd = topLevelCwd ?? paramsCwd;
  const forwarded: Record<string, unknown> = {
    ...params,
    agent,
    task,
    async: true,
    clarify: false,
  };
  delete forwarded.timeout;
  if (cwd !== undefined) forwarded.cwd = cwd;
  if (timeout !== undefined || timeoutMs !== undefined) {
    forwarded.timeoutMs = timeout ?? timeoutMs;
  }

  return {
    operationId,
    fingerprint: operationFingerprint(forwarded),
    params: forwarded,
  };
}

function validateRunRequest(
  method: "status" | "result" | "stop" | "adopt",
  raw: Record<string, unknown>,
): RunRequest | Failure {
  const params = raw.params;
  if (!isRecord(params)) {
    return failure("invalid_request", `${method} requires an object params`);
  }

  const runId = nonEmptyString(params.runId);
  if (!runId) {
    return failure("invalid_request", `${method} requires a non-empty runId`);
  }

  if (!("asyncDir" in params)) return { runId };
  const asyncDir = nonEmptyString(params.asyncDir);
  return asyncDir
    ? { runId, asyncDir }
    : failure(
        "invalid_request",
        `${method} asyncDir must be a non-empty string`,
      );
}

function normalizeObservation(
  request: RunRequest,
  upstream: unknown,
  observed = false,
): Observation {
  const text = isRecord(upstream) ? nonEmptyString(upstream.text) : undefined;
  const state = text
    ? normalizeState(parseStatusLine(text, "State"))
    : undefined;
  const asyncDir =
    request.asyncDir ?? (text ? parseStatusLine(text, "Dir") : undefined);
  const resultPath = text ? parseStatusLine(text, "Result") : undefined;
  return {
    runId: request.runId,
    ...(observed ? { observed: true } : {}),
    ...(state ? { state } : {}),
    ...(asyncDir ? { asyncDir } : {}),
    ...(resultPath ? { resultPath } : {}),
    ...(text ? { text } : {}),
  };
}

function normalizeStop(request: RunRequest, upstream: unknown): StopResult {
  const data = isRecord(upstream) ? upstream : undefined;
  const runId = nonEmptyString(data?.runId) ?? request.runId;
  const asyncDir = nonEmptyString(data?.asyncDir) ?? request.asyncDir;
  const state = normalizeState(data?.state) ?? "stopping";
  return {
    runId,
    ...(asyncDir ? { asyncDir } : {}),
    state,
  };
}

function requestSubagents(
  events: EventBus,
  method: UpstreamMethod,
  params: Record<string, unknown>,
  timeoutMs: number,
  signal: AbortSignal,
): Promise<unknown> {
  const requestId = randomUUID();

  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = (): void => {
      if (settled) return;
      settled = true;
      if (typeof unsubscribe === "function") unsubscribe();
      clearTimeout(timeout);
      signal.removeEventListener("abort", onAbort);
    };
    const rejectWith = (error: Error): void => {
      if (settled) return;
      cleanup();
      reject(error);
    };
    const resolveWith = (value: unknown): void => {
      if (settled) return;
      cleanup();
      resolve(value);
    };
    const onAbort = (): void => rejectWith(new Error("Bridge disposed"));
    const timeout = setTimeout(() => {
      rejectWith(
        new Error(`pi-subagents ${method} RPC timed out after ${timeoutMs}ms`),
      );
    }, timeoutMs);
    const unsubscribe = events.on(
      upstreamReplyEvent(requestId),
      (raw: unknown) => {
        if (
          !isRecord(raw) ||
          raw.version !== PROTOCOL_VERSION ||
          raw.requestId !== requestId ||
          typeof raw.success !== "boolean" ||
          (raw.method !== undefined && raw.method !== method)
        ) {
          rejectWith(new Error("Malformed pi-subagents RPC reply"));
          return;
        }
        if (raw.success) {
          resolveWith(raw.data);
          return;
        }
        const message = isRecord(raw.error)
          ? nonEmptyString(raw.error.message)
          : nonEmptyString(raw.error);
        rejectWith(new Error(message ?? "pi-subagents RPC error"));
      },
    );

    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) {
      onAbort();
      return;
    }
    events.emit(SUBAGENTS_REQUEST_EVENT, {
      version: PROTOCOL_VERSION,
      requestId,
      method,
      params,
    });
  });
}

function getPlanExecState(events: EventBus): PlanExecState {
  const existing = planExecStates.get(events);
  if (existing) return existing;

  const created: PlanExecState = {
    operations: new Map(),
    spawnControllers: new Set(),
  };
  planExecStates.set(events, created);
  return created;
}

/**
 * Registers the plan-exec protocol without touching the legacy pi-tasks events.
 * Adoption is observational only: it does not make a foreign run locally stoppable.
 */
export function registerPlanExecRpc(
  events: EventBus,
  options: PlanExecOptions,
): { dispose(): void } {
  const state = getPlanExecState(events);
  if (state.registration) return state.registration;

  const transientControllers = new Set<AbortController>();
  let disposed = false;

  const emit = (requestId: string, reply: Reply<object>): void => {
    if (!disposed) events.emit(replyEvent(requestId), reply);
  };

  const startOperation = (
    request: SpawnRequest,
  ): Promise<Reply<SpawnResult>> => {
    const existing = state.operations.get(request.operationId);
    if (existing) {
      return existing.fingerprint === request.fingerprint
        ? existing.reply
        : Promise.resolve(
            failure(
              "invalid_request",
              "spawn operationId was already used with different parameters",
            ),
          );
    }

    const controller = new AbortController();
    state.spawnControllers.add(controller);
    const operation = requestSubagents(
      events,
      "spawn",
      request.params,
      options.timeoutMs,
      controller.signal,
    )
      .then((reply): Reply<SpawnResult> => {
        const runId = extractSpawnRunId(reply);
        const asyncDir = extractSpawnAsyncDir(reply);
        return runId
          ? {
              success: true,
              data: { runId, ...(asyncDir ? { asyncDir } : {}) },
            }
          : failure(
              "upstream_error",
              "pi-subagents spawn reply did not include a runId",
            );
      })
      .catch((error: unknown): Reply<SpawnResult> =>
        failure(
          "upstream_error",
          error instanceof Error ? error.message : String(error),
        ),
      )
      .finally(() => state.spawnControllers.delete(controller));
    state.operations.set(request.operationId, {
      fingerprint: request.fingerprint,
      reply: operation,
    });
    return operation;
  };

  const invoke = async (
    method: Method,
    raw: Record<string, unknown>,
  ): Promise<Reply<object>> => {
    if (method === "ping") {
      return {
        success: true,
        data: { version: PROTOCOL_VERSION, methods: [...METHODS] },
      };
    }

    if (method === "spawn") {
      const request = validateSpawn(raw);
      if (isFailure(request)) return request;
      return startOperation(request);
    }

    const request = validateRunRequest(method, raw);
    if (isFailure(request)) return request;
    const controller = new AbortController();
    transientControllers.add(controller);
    try {
      if (method === "stop") {
        const upstream = await requestSubagents(
          events,
          "stop",
          {
            id: request.runId,
            ...(request.asyncDir ? { dir: request.asyncDir } : {}),
          },
          options.timeoutMs,
          controller.signal,
        );
        return { success: true, data: normalizeStop(request, upstream) };
      }

      // pi-subagents exposes terminal result metadata through its status RPC.
      const upstream = await requestSubagents(
        events,
        "status",
        {
          id: request.runId,
          ...(request.asyncDir ? { dir: request.asyncDir } : {}),
        },
        options.timeoutMs,
        controller.signal,
      );
      return {
        success: true,
        data: normalizeObservation(request, upstream, method === "adopt"),
      };
    } catch (error: unknown) {
      return failure(
        "upstream_error",
        error instanceof Error ? error.message : String(error),
      );
    } finally {
      transientControllers.delete(controller);
    }
  };

  const unsubscribe = events.on(PLAN_EXEC_REQUEST_EVENT, (raw: unknown) => {
    if (!isRecord(raw)) return;
    const requestId = nonEmptyString(raw.requestId);
    if (!requestId || /[\r\n]/.test(requestId)) return;
    if (raw.version !== PROTOCOL_VERSION) {
      emit(
        requestId,
        failure(
          "invalid_request",
          `unsupported plan-exec RPC version: ${String(raw.version)}`,
        ),
      );
      return;
    }
    const methodName = nonEmptyString(raw.method);
    if (!methodName) {
      emit(requestId, failure("invalid_request", "request requires a method"));
      return;
    }
    if (!isMethod(methodName)) {
      emit(
        requestId,
        failure("invalid_request", `unsupported method: ${methodName}`),
      );
      return;
    }

    void invoke(methodName, raw).then((reply) => emit(requestId, reply));
  });

  const registration = {
    dispose(): void {
      if (disposed) return;
      disposed = true;
      unsubscribe?.();
      if (state.registration === registration) {
        delete state.registration;
      }
      for (const controller of transientControllers) controller.abort();
      transientControllers.clear();
      // Keep spawned-operation listeners and replies alive through extension reloads.
      // A retry with the same operationId can then recover its original launch.
    },
  };
  state.registration = registration;
  return registration;
}
