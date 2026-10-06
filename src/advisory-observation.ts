const SOURCE = 'pi-subagents.async-status-snapshot';
const MAX_AGE_MS = 30_000;
const MAX_RUNS = 20;
const MAX_TEXT_LENGTH = 160;

export interface AdvisoryObservation {
  version: 1;
  source: typeof SOURCE;
  runId: string;
  generatedAt: number;
  activity?: {
    state?: string;
    currentTool?: string;
    lastActivityAt?: number;
    currentToolStartedAt?: number;
    turnCount?: number;
    toolCount?: number;
  };
  omitted: { runs: number; children: number; byteLimitExceeded: boolean };
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function count(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function displayText(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.trim().length > 0 &&
    value.length <= MAX_TEXT_LENGTH &&
    !/[\p{Cc}\p{Cf}]/u.test(value)
  );
}

// Native RPC builds this snapshot from active-session jobs. Only its exact root
// run ID is usable here: projected workflow steps can replace child IDs with keys.
export function advisoryObservation(
  snapshot: unknown,
  runId: string,
  requestedAt: number,
  receivedAt: number,
): AdvisoryObservation | undefined {
  if (
    !record(snapshot) ||
    snapshot.kind !== SOURCE ||
    snapshot.version !== 1 ||
    !count(snapshot.generatedAt) ||
    snapshot.generatedAt < requestedAt ||
    snapshot.generatedAt > receivedAt ||
    receivedAt - snapshot.generatedAt > MAX_AGE_MS ||
    !Array.isArray(snapshot.runs) ||
    snapshot.runs.length > MAX_RUNS
  )
    return undefined;
  const omitted = snapshot.omitted;
  if (
    !record(omitted) ||
    !count(omitted.runs) ||
    !count(omitted.children) ||
    typeof omitted.byteLimitExceeded !== 'boolean'
  )
    return undefined;
  const matches = snapshot.runs.filter(
    (node) => record(node) && node.id === runId,
  );
  if (matches.length !== 1) return undefined;
  const node: unknown = matches[0];
  if (!record(node) || (node.kind !== 'workflow' && node.kind !== 'subagent'))
    return undefined;

  let activity: AdvisoryObservation['activity'];
  if (node.activity !== undefined) {
    if (!record(node.activity)) return undefined;
    activity = {};
    for (const key of ['state', 'currentTool'] as const) {
      const value = node.activity[key];
      if (value === undefined) continue;
      if (!displayText(value)) return undefined;
      activity[key] = value;
    }
    for (const key of [
      'lastActivityAt',
      'currentToolStartedAt',
      'turnCount',
      'toolCount',
    ] as const) {
      const value = node.activity[key];
      if (value === undefined) continue;
      if (
        !count(value) ||
        ((key === 'lastActivityAt' || key === 'currentToolStartedAt') &&
          value > snapshot.generatedAt)
      )
        return undefined;
      activity[key] = value;
    }
    if (!Object.keys(activity).length) activity = undefined;
  }
  return {
    version: 1,
    source: SOURCE,
    runId,
    generatedAt: snapshot.generatedAt,
    ...(activity ? { activity } : {}),
    omitted: {
      runs: omitted.runs,
      children: omitted.children,
      byteLimitExceeded: omitted.byteLimitExceeded,
    },
  };
}
