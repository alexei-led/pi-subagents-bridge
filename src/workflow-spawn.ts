export function singleChildWorkflowScript(
  agent: string,
  task: string,
  childParams: Record<string, unknown> = {},
): string {
  return `return runs.run("main", ${JSON.stringify({ agent, task, ...childParams })})`;
}
