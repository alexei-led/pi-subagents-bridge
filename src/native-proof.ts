import type { OperationJournalRecord } from './operation-journal.js';

type RecordValue = Record<string, unknown>;

function record(value: unknown): value is RecordValue {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

/** Older journals used the caller identity directly; keep reading that mapping. */
export function nativeOperationIdentity(operation: OperationJournalRecord): {
  operationId: string;
  digest: string;
} {
  const params = operation.nativeParams;
  if (params && ('operationId' in params || 'digest' in params)) {
    if (!text(params.operationId) || !text(params.digest))
      throw new Error('Incomplete durable native operation mapping');
    return { operationId: params.operationId, digest: params.digest };
  }
  return {
    operationId: operation.operationId,
    digest: operation.requestDigest,
  };
}

/**
 * The released pi-subagents runtime publishes a writer-exit terminal proof for
 * a run. Accept it when it is a versioned observation of that run; the bridge
 * owns the caller identity, and the provider owns the process evidence.
 */
export function attestUpstreamTerminalProof(
  proof: unknown,
  runId: string,
): RecordValue | undefined {
  if (
    !record(proof) ||
    proof.version !== 1 ||
    proof.state !== 'observed' ||
    proof.runId !== runId
  )
    return undefined;
  if (
    typeof proof.observedAt !== 'number' ||
    !Number.isFinite(proof.observedAt)
  )
    return undefined;
  if (!text(proof.runnerProcessInstanceId)) return undefined;
  if (!Array.isArray(proof.instances) && !record(proof.writers))
    return undefined;
  return proof;
}
