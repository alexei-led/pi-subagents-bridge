import { createHash, randomUUID } from "node:crypto";
import {
  OperationJournal,
  type OperationJournalRecord,
} from "./operation-journal.js";
import { singleChildWorkflowScript } from "./workflow-spawn.js";

export const PLAN_EXEC_REQUEST_EVENT = "plan-exec:bridge:v1:request";
export const PLAN_EXEC_REPLY_PREFIX = "plan-exec:bridge:v1:reply:";
export const PLAN_EXEC_V2_REQUEST_EVENT = "plan-exec:bridge:v2:request";
export const PLAN_EXEC_V2_REPLY_PREFIX = "plan-exec:bridge:v2:reply:";

const SUBAGENTS_REQUEST_EVENT = "subagents:rpc:v1:request";
const SUBAGENTS_REPLY_PREFIX = "subagents:rpc:v1:reply:";
const PROTOCOL_VERSION = 1;
const MAX_COMPLETED_OPERATION_HISTORY = 128;
const METHODS = [
  "ping",
  "spawn",
  "operation",
  "status",
  "result",
  "stop",
  "adopt",
] as const;

type Method = (typeof METHODS)[number];
type ProtocolVersion = 1 | 2;
type UpstreamMethod = "ping" | "spawn" | "status" | "stop";
type Unsubscribe = () => void;

type EventBus = {
  on(event: string, handler: (payload: unknown) => void): Unsubscribe | void;
  emit(event: string, payload: unknown): void;
};

type Failure = {
  success: false;
  error: {
    code: "invalid_request" | "upstream_error" | "operation_capacity";
    message: string;
  };
};
type Reply<T> = { success: true; data: T } | Failure;

interface SpawnRequest {
  protocolVersion: ProtocolVersion;
  operationId: string;
  fingerprint: string;
  ownerRunId?: string;
  params: Record<string, unknown>;
}

interface SpawnResult {
  runId: string;
  asyncDir?: string;
  requestDigest: string;
}

interface OperationRequest {
  operationId: string;
  ownerRunId?: string;
  requestDigest?: string;
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
  processTerminal?: Record<string, unknown>;
}

interface StopResult {
  runId: string;
  asyncDir?: string;
  state: string;
}

interface PlanExecOptions {
  timeoutMs: number;
  journalPath?: string;
}

interface Operation {
  fingerprint: string;
  ownerRunId?: string;
  reply: Promise<Reply<SpawnResult>>;
  outcome?: Reply<SpawnResult>;
}

interface PlanExecState {
  operations: Map<string, Operation>;
  spawnControllers: Set<AbortController>;
  journal?: OperationJournal;
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

function replyEvent(version: ProtocolVersion, requestId: string): string {
  const prefix =
    version === 2 ? PLAN_EXEC_V2_REPLY_PREFIX : PLAN_EXEC_REPLY_PREFIX;
  return `${prefix}${requestId}`;
}

function upstreamReplyEvent(requestId: string): string {
  return `${SUBAGENTS_REPLY_PREFIX}${requestId}`;
}

function failure(
  code: "invalid_request" | "upstream_error" | "operation_capacity",
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

function canonicalJson(value: unknown): string {
  if (value === null) return "null";
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
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

function operationFingerprint(value: unknown): string {
  return `sha256:${createHash("sha256").update(canonicalJson(value)).digest("hex")}`;
}

function validateOwner(
  raw: Record<string, unknown>,
  operationId: string,
  requestDigest: string,
): { runId: string } | Failure {
  const owner = raw.owner;
  if (!isRecord(owner)) {
    return failure("invalid_request", "spawn requires an object owner");
  }
  const runId = nonEmptyString(owner.runId);
  if (
    owner.kind !== "pi-plan-exec" ||
    !runId ||
    nonEmptyString(owner.key) !== operationId
  ) {
    return failure(
      "invalid_request",
      "spawn owner must identify the pi-plan-exec run and operation",
    );
  }
  if (nonEmptyString(owner.requestDigest) !== requestDigest) {
    return failure(
      "invalid_request",
      "spawn owner requestDigest does not match cwd and params",
    );
  }
  return { runId };
}

function validateSpawn(
  raw: Record<string, unknown>,
  protocolVersion: ProtocolVersion,
): SpawnRequest | Failure {
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
  if (
    "completionGuard" in params &&
    typeof params.completionGuard !== "boolean"
  ) {
    return failure(
      "invalid_request",
      "spawn completionGuard must be a boolean",
    );
  }

  const cwd = topLevelCwd ?? paramsCwd;
  const digestParams = { ...params };
  delete digestParams.cwd;
  const fingerprint = operationFingerprint({
    ...(cwd !== undefined ? { cwd } : {}),
    params: digestParams,
  });
  const owner =
    protocolVersion === 2
      ? validateOwner(raw, operationId, fingerprint)
      : undefined;
  if (isFailure(owner)) return owner;
  const {
    agent: _agent,
    task: _task,
    async: _async,
    clarify: _clarify,
    completionGuard,
    ...workflowDefaults
  } = params;
  const forwarded: Record<string, unknown> = {
    ...workflowDefaults,
    workflowScript: singleChildWorkflowScript(
      agent,
      task,
      completionGuard === undefined ? {} : { completionGuard },
    ),
    async: true,
  };
  delete forwarded.timeout;
  if (cwd !== undefined) forwarded.cwd = cwd;
  if (timeout !== undefined || timeoutMs !== undefined) {
    forwarded.timeoutMs = timeout ?? timeoutMs;
  }

  return {
    protocolVersion,
    operationId,
    fingerprint,
    ...(owner ? { ownerRunId: owner.runId } : {}),
    params: forwarded,
  };
}

function validateOperationRequest(
  raw: Record<string, unknown>,
  protocolVersion: ProtocolVersion,
): OperationRequest | Failure {
  const operationId = nonEmptyString(raw.operationId);
  if (!operationId) {
    return failure("invalid_request", "operation requires a non-empty operationId");
  }
  if (protocolVersion === 1) return { operationId };

  const owner = raw.owner;
  if (!isRecord(owner)) {
    return failure("invalid_request", "operation requires an object owner");
  }
  const ownerRunId = nonEmptyString(owner.runId);
  const requestDigest = nonEmptyString(owner.requestDigest);
  if (
    owner.kind !== "pi-plan-exec" ||
    !ownerRunId ||
    nonEmptyString(owner.key) !== operationId ||
    !requestDigest
  ) {
    return failure(
      "invalid_request",
      "operation owner must identify the pi-plan-exec run, operation, and request digest",
    );
  }
  return { operationId, ownerRunId, requestDigest };
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

function extractProcessTerminal(
  upstream: unknown,
  expectedRunId: string,
): Record<string, unknown> | undefined {
  if (!isRecord(upstream) || !isRecord(upstream.details)) return undefined;
  const lifecycleStatus = upstream.details.lifecycleStatus;
  if (!isRecord(lifecycleStatus) || !isRecord(lifecycleStatus.processTerminal)) {
    return undefined;
  }
  const proof = lifecycleStatus.processTerminal;
  const state = proof.state;
  if (
    proof.version !== 1 ||
    proof.runId !== expectedRunId ||
    !nonEmptyString(proof.runnerProcessInstanceId) ||
    (state !== "pending" &&
      state !== "not-started" &&
      state !== "observed" &&
      state !== "unknown")
  ) {
    return undefined;
  }
  if (
    state === "observed" &&
    (typeof proof.observedAt !== "number" ||
      !Number.isFinite(proof.observedAt) ||
      !Array.isArray(proof.instances))
  ) {
    return undefined;
  }
  if (state === "unknown" && !nonEmptyString(proof.reason)) return undefined;
  return { ...proof };
}

function normalizeObservation(
  request: RunRequest,
  upstream: unknown,
  observed = false,
  includeProcessTerminal = false,
): Observation {
  const text = isRecord(upstream) ? nonEmptyString(upstream.text) : undefined;
  const state = text
    ? normalizeState(parseStatusLine(text, "State"))
    : undefined;
  const asyncDir =
    request.asyncDir ?? (text ? parseStatusLine(text, "Dir") : undefined);
  const resultPath = text ? parseStatusLine(text, "Result") : undefined;
  const processTerminal = includeProcessTerminal
    ? extractProcessTerminal(upstream, request.runId)
    : undefined;
  return {
    runId: request.runId,
    ...(observed ? { observed: true } : {}),
    ...(state ? { state } : {}),
    ...(asyncDir ? { asyncDir } : {}),
    ...(resultPath ? { resultPath } : {}),
    ...(text ? { text } : {}),
    ...(processTerminal ? { processTerminal } : {}),
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

function pruneCompletedOperations(state: PlanExecState): void {
  for (const [operationId, operation] of state.operations) {
    if (state.operations.size < MAX_COMPLETED_OPERATION_HISTORY) return;
    if (operation.outcome) state.operations.delete(operationId);
  }
}

function getPlanExecState(
  events: EventBus,
  journalPath?: string,
): PlanExecState {
  const existing = planExecStates.get(events);
  if (existing) {
    if (
      journalPath &&
      existing.journal &&
      existing.journal.filePath !== new OperationJournal(journalPath).filePath
    ) {
      throw new Error("plan-exec RPC was already registered with a different operation journal");
    }
    if (journalPath && !existing.journal) {
      existing.journal = new OperationJournal(journalPath);
    }
    return existing;
  }

  const created: PlanExecState = {
    operations: new Map(),
    spawnControllers: new Set(),
    ...(journalPath ? { journal: new OperationJournal(journalPath) } : {}),
  };
  planExecStates.set(events, created);
  return created;
}

function durableSpawnReply(
  record: OperationJournalRecord,
): Reply<SpawnResult> {
  if (record.binding === "bound" && record.runId) {
    return {
      success: true,
      data: {
        runId: record.runId,
        ...(record.asyncDir ? { asyncDir: record.asyncDir } : {}),
        requestDigest: record.requestDigest,
      },
    };
  }
  return failure(
    "upstream_error",
    record.error ??
      "pi-subagents spawn outcome is unknown after bridge restart",
  );
}

function validateOperationIdentity(
  request: OperationRequest,
  requestDigest: string,
  ownerRunId?: string,
): Failure | undefined {
  if (!request.requestDigest && !request.ownerRunId) return undefined;
  if (
    request.requestDigest !== requestDigest ||
    request.ownerRunId !== ownerRunId
  ) {
    return failure(
      "invalid_request",
      "operation owner does not match the durable operation",
    );
  }
  return undefined;
}

function durableLookup(
  record: OperationJournalRecord,
): Record<string, unknown> {
  if (record.binding === "bound" && record.runId) {
    return {
      state: "found",
      requestDigest: record.requestDigest,
      runId: record.runId,
      ...(record.asyncDir ? { asyncDir: record.asyncDir } : {}),
    };
  }
  return {
    state: "unknown",
    requestDigest: record.requestDigest,
    error:
      record.error ??
      "pi-subagents spawn outcome is unknown after bridge restart",
  };
}

/**
 * Registers the plan-exec protocol without touching the legacy pi-tasks events.
 * Adoption is observational only: it does not make a foreign run locally stoppable.
 */
export function registerPlanExecRpc(
  events: EventBus,
  options: PlanExecOptions,
): { dispose(): void } {
  const state = getPlanExecState(events, options.journalPath);
  if (state.registration) return state.registration;

  const transientControllers = new Set<AbortController>();
  let disposed = false;

  const emit = (
    protocolVersion: ProtocolVersion,
    requestId: string,
    reply: Reply<object>,
  ): void => {
    if (!disposed) events.emit(replyEvent(protocolVersion, requestId), reply);
  };

  const startOperation = (
    request: SpawnRequest,
  ): Promise<Reply<SpawnResult>> => {
    const existing = state.operations.get(request.operationId);
    if (existing) {
      return existing.fingerprint === request.fingerprint &&
        existing.ownerRunId === request.ownerRunId
        ? existing.reply
        : Promise.resolve(
            failure(
              "invalid_request",
              "spawn operationId was already used with different parameters",
            ),
          );
    }

    if (state.journal) {
      try {
        const durable = state.journal.get(request.operationId);
        if (durable) {
          return durable.requestDigest === request.fingerprint &&
            durable.ownerRunId === request.ownerRunId
            ? Promise.resolve(durableSpawnReply(durable))
            : Promise.resolve(
                failure(
                  "invalid_request",
                  "spawn operationId was already used with different parameters",
                ),
              );
        }
        const begun = state.journal.begin(
          request.operationId,
          request.fingerprint,
          request.ownerRunId,
        );
        if (!begun.created) {
          return begun.record.requestDigest === request.fingerprint &&
            begun.record.ownerRunId === request.ownerRunId
            ? Promise.resolve(durableSpawnReply(begun.record))
            : Promise.resolve(
                failure(
                  "invalid_request",
                  "spawn operationId was already used with different parameters",
                ),
              );
        }
      } catch (error: unknown) {
        return Promise.resolve(
          failure(
            "upstream_error",
            error instanceof Error ? error.message : String(error),
          ),
        );
      }
    }

    pruneCompletedOperations(state);
    if (state.operations.size >= MAX_COMPLETED_OPERATION_HISTORY)
      return Promise.resolve(
        failure(
          "operation_capacity",
          "plan-exec operation history is full of active operations",
        ),
      );

    const controller = new AbortController();
    state.spawnControllers.add(controller);
    const operation: Operation = {
      fingerprint: request.fingerprint,
      ...(request.ownerRunId ? { ownerRunId: request.ownerRunId } : {}),
      reply: requestSubagents(
        events,
        "spawn",
        request.params,
        options.timeoutMs,
        controller.signal,
      )
        .then((reply): Reply<SpawnResult> => {
          const runId = extractSpawnRunId(reply);
          const asyncDir = extractSpawnAsyncDir(reply);
          if (!runId) {
            throw new Error("pi-subagents spawn reply did not include a runId");
          }
          try {
            state.journal?.bind(
              request.operationId,
              request.fingerprint,
              runId,
              asyncDir,
            );
          } catch (error: unknown) {
            // The native run ID is stronger than a failed local persistence step.
            // Return it so plan-exec can durably attach instead of losing a known
            // launch and risking a replacement worker.
            console.error(
              `Failed to persist bridge operation '${request.operationId}' binding:`,
              error,
            );
          }
          return {
            success: true,
            data: {
              runId,
              ...(asyncDir ? { asyncDir } : {}),
              requestDigest: request.fingerprint,
            },
          };
        })
        .catch((error: unknown): Reply<SpawnResult> => {
          const message = error instanceof Error ? error.message : String(error);
          try {
            state.journal?.markUnknown(
              request.operationId,
              request.fingerprint,
              message,
            );
          } catch (journalError: unknown) {
            return failure(
              "upstream_error",
              `pi-subagents spawn outcome is unknown and the operation journal could not be updated: ${journalError instanceof Error ? journalError.message : String(journalError)}`,
            );
          }
          return failure("upstream_error", message);
        })
        .then((outcome) => {
          operation.outcome = outcome;
          pruneCompletedOperations(state);
          return outcome;
        })
        .finally(() => state.spawnControllers.delete(controller)),
    };
    state.operations.set(request.operationId, operation);
    return operation.reply;
  };

  const invoke = async (
    method: Method,
    raw: Record<string, unknown>,
    protocolVersion: ProtocolVersion,
  ): Promise<Reply<object>> => {
    if (method === "ping") {
      if (protocolVersion === 1) {
        return {
          success: true,
          data: {
            version: 1,
            capabilities: { workflowScriptSpawn: true },
            methods: [...METHODS],
          },
        };
      }

      const controller = new AbortController();
      transientControllers.add(controller);
      try {
        const upstream = await requestSubagents(
          events,
          "ping",
          {},
          options.timeoutMs,
          controller.signal,
        );
        const capabilities = isRecord(upstream) && isRecord(upstream.capabilities)
          ? upstream.capabilities
          : undefined;
        const terminalCapability = capabilities?.processTerminalProof;
        return {
          success: true,
          data: {
            version: 2,
            protocol: "plan-exec-bridge",
            capabilities: {
              workflowScriptSpawn: capabilities?.asyncSpawn === true,
              durableOperationLookup: state.journal
                ? { version: 1 }
                : false,
              processTerminalProof:
                isRecord(terminalCapability) && terminalCapability.version === 1
                  ? { version: 1 }
                  : false,
            },
            methods: [...METHODS],
          },
        };
      } catch (error: unknown) {
        return failure(
          "upstream_error",
          error instanceof Error ? error.message : String(error),
        );
      } finally {
        transientControllers.delete(controller);
      }
    }

    if (method === "spawn") {
      if (protocolVersion === 2 && !state.journal) {
        return failure(
          "upstream_error",
          "plan-exec bridge v2 requires a durable operation journal",
        );
      }
      const request = validateSpawn(raw, protocolVersion);
      if (isFailure(request)) return request;
      const outcome = await startOperation(request);
      if (!outcome.success || protocolVersion === 2) return outcome;
      const { requestDigest: _requestDigest, ...data } = outcome.data;
      return { success: true, data };
    }

    if (method === "operation") {
      if (protocolVersion === 2 && !state.journal) {
        return failure(
          "upstream_error",
          "plan-exec bridge v2 requires a durable operation journal",
        );
      }
      const request = validateOperationRequest(raw, protocolVersion);
      if (isFailure(request)) return request;
      const operation = state.operations.get(request.operationId);
      if (operation) {
        const identityFailure = validateOperationIdentity(
          request,
          operation.fingerprint,
          operation.ownerRunId,
        );
        if (identityFailure) return identityFailure;
        const operationData = !operation.outcome
          ? { state: "pending" }
          : operation.outcome.success
            ? { state: "found", ...operation.outcome.data }
            : {
                state: "unknown",
                requestDigest: operation.fingerprint,
                error: operation.outcome.error.message,
              };
        if (protocolVersion === 2) {
          return {
            success: true,
            data: { operationId: request.operationId, ...operationData },
          };
        }
        const { requestDigest: _requestDigest, ...legacyData } = operationData;
        return { success: true, data: legacyData };
      }
      try {
        const durable = state.journal?.get(request.operationId);
        if (durable) {
          const identityFailure = validateOperationIdentity(
            request,
            durable.requestDigest,
            durable.ownerRunId,
          );
          if (identityFailure) return identityFailure;
        }
        const operationData = durable
          ? durableLookup(durable)
          : { state: "absent" };
        if (protocolVersion === 2) {
          return {
            success: true,
            data: { operationId: request.operationId, ...operationData },
          };
        }
        if (!durable) return { success: true, data: operationData };
        const {
          requestDigest: _requestDigest,
          ...legacyData
        } = durableLookup(durable);
        return { success: true, data: legacyData };
      } catch (error: unknown) {
        return failure(
          "upstream_error",
          error instanceof Error ? error.message : String(error),
        );
      }
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
        data: normalizeObservation(
          request,
          upstream,
          method === "adopt",
          protocolVersion === 2,
        ),
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

  const subscribe = (
    event: string,
    protocolVersion: ProtocolVersion,
  ): Unsubscribe | void =>
    events.on(event, (raw: unknown) => {
      if (!isRecord(raw)) return;
      const requestId = nonEmptyString(raw.requestId);
      if (!requestId || /[\r\n]/.test(requestId)) return;
      if (raw.version !== protocolVersion) {
        emit(
          protocolVersion,
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
        emit(
          protocolVersion,
          requestId,
          failure("invalid_request", "request requires a method"),
        );
        return;
      }
      if (!isMethod(methodName)) {
        emit(
          protocolVersion,
          requestId,
          failure("invalid_request", `unsupported method: ${methodName}`),
        );
        return;
      }

      void invoke(methodName, raw, protocolVersion).then((reply) =>
        emit(protocolVersion, requestId, reply),
      );
    });
  const unsubscribes = [
    subscribe(PLAN_EXEC_REQUEST_EVENT, 1),
    subscribe(PLAN_EXEC_V2_REQUEST_EVENT, 2),
  ];

  const registration = {
    dispose(): void {
      if (disposed) return;
      disposed = true;
      for (const unsubscribe of unsubscribes) unsubscribe?.();
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
