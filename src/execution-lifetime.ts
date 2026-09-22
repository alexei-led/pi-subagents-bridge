export type ExecutionLifetime =
  | { mode: 'unbounded' }
  | { mode: 'bounded'; timeoutMs: number };

export function parseExecutionLifetime(
  value: unknown,
): ExecutionLifetime | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value))
    return undefined;
  if (!('mode' in value)) return undefined;
  if (value.mode === 'unbounded' && Object.keys(value).length === 1) {
    return { mode: 'unbounded' };
  }
  if (
    value.mode === 'bounded' &&
    'timeoutMs' in value &&
    typeof value.timeoutMs === 'number' &&
    Number.isSafeInteger(value.timeoutMs) &&
    value.timeoutMs > 0 &&
    value.timeoutMs <= 2_147_483_647 &&
    Object.keys(value).length === 2
  ) {
    return { mode: 'bounded', timeoutMs: value.timeoutMs };
  }
  return undefined;
}
