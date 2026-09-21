import type { OperationJournalRecord } from "./operation-journal.js";

type RecordValue = Record<string, unknown>;

function record(value: unknown): value is RecordValue {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function text(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/** Older journals used the caller identity directly, but cannot attest kernel proofs. */
export function nativeOperationIdentity(operation: OperationJournalRecord): { operationId: string; digest: string } {
  const params = operation.nativeParams;
  if (params && ("operationId" in params || "digest" in params)) {
    if (!text(params.operationId) || !text(params.digest)) throw new Error("Incomplete durable native operation mapping");
    return { operationId: params.operationId, digest: params.digest };
  }
  return { operationId: operation.operationId, digest: operation.requestDigest };
}

function sameBinding(value: unknown, expected: RecordValue): value is RecordValue {
  return record(value) && ["operationId", "requestDigest", "hostId", "bootId"].every((key) =>
    text(expected[key]) && value[key] === expected[key]);
}

function sameIdentity(value: unknown, expected: RecordValue, binding: RecordValue): boolean {
  if (!record(value) || !sameBinding(value, binding) || !sameBinding(expected, binding) ||
    value.version !== 1 || expected.version !== 1 || value.backend !== "darwin-resource-coalition-v1" ||
    expected.backend !== value.backend || !text(expected.coalitionId) || value.coalitionId !== expected.coalitionId) return false;
  const leader = value.leader;
  const expectedLeader = expected.leader;
  return record(leader) && record(expectedLeader) && Number.isSafeInteger(leader.pid) &&
    typeof leader.pid === "number" && leader.pid > 0 && leader.pid === expectedLeader.pid &&
    text(leader.uniqueId) && leader.uniqueId === expectedLeader.uniqueId &&
    Number.isSafeInteger(leader.pidVersion) && typeof leader.pidVersion === "number" &&
    leader.pidVersion >= 0 && leader.pidVersion === expectedLeader.pidVersion;
}

/** Attest only the caller-to-native mapping; the native provider validates kernel evidence. */
export function attestNativeTerminalProof(
  proof: unknown,
  operation: OperationJournalRecord,
  runId: string,
): RecordValue | undefined {
  const params = operation.nativeParams;
  if (!params || !text(params.operationId) || !text(params.digest) ||
    !record(params.executionOwnership) || params.executionOwnership.mode !== "kernel" ||
    !record(proof) || proof.version !== 1 || proof.state !== "observed" || proof.runId !== runId ||
    (operation.runId !== undefined && operation.runId !== runId) || !text(proof.runnerProcessInstanceId) ||
    typeof proof.observedAt !== "number" || !Number.isFinite(proof.observedAt) || !Array.isArray(proof.instances)) return undefined;
  const native = proof.nativeOperation;
  if (!record(native) || native.operationId !== params.operationId || native.digest !== params.digest) return undefined;
  const ownership = proof.processTreeOwnership;
  if (!record(ownership) || ownership.version !== 1 || ownership.scope !== "owned-process-tree" ||
    ownership.escapedDescendants !== "contained") return undefined;
  const binding = proof.kernelBinding;
  const observation = proof.kernelProof;
  if (!record(binding) || !record(observation) || observation.status !== "retired" ||
    !sameBinding(observation.binding, binding) || !record(observation.identity) ||
    !sameIdentity(observation.identity, observation.identity, binding)) return undefined;
  const terminal = observation.proof;
  if (!sameBinding(terminal, binding) || terminal.kind !== "darwin-coalition-retired" ||
    !sameIdentity(terminal.identity, observation.identity, binding) || !text(terminal.observedAt) ||
    Date.parse(terminal.observedAt) !== proof.observedAt) return undefined;
  return { ...proof, callerBinding: { operationId: operation.operationId, requestDigest: operation.requestDigest } };
}
