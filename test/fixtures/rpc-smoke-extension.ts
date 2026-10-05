import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';

export default function smoke(pi: ExtensionAPI): void {
  const request = (
    channel: string,
    prefix: string,
    fields: object,
  ): Promise<Record<string, unknown>> => {
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error('Smoke RPC timed out'));
      }, 10_000);
      const off = pi.events.on(prefix + requestId, (raw: unknown) => {
        clearTimeout(timer);
        off();
        assert.ok(isRecord(raw));
        resolve(raw);
      });
      pi.events.emit(channel, { requestId, ...fields });
    });
  };
  const native = (method: string, params: object) =>
    request('subagents:rpc:v1:request', 'subagents:rpc:v1:reply:', {
      version: 1,
      method,
      params,
    });
  const plan = (method: string, fields: object) =>
    request('plan-exec:bridge:v2:request', 'plan-exec:bridge:v2:reply:', {
      version: 2,
      method,
      ...fields,
    });

  pi.registerCommand('bridge-smoke', {
    description: 'Isolated RPC integration fixture',
    handler: async (_args, ctx) => {
      const report: Record<string, unknown> = {};
      try {
        const rejected = await native('spawn', {
          workflowScript: 'return 1',
          async: true,
        });
        assert.equal(rejected.success, false);
        assert.ok(isRecord(rejected.error));
        assert.equal(rejected.error.code, 'invalid_params');
        report.obsoleteField = rejected.error.code;

        const params = {
          agent: 'bridge-smoke',
          executionLifetime: { mode: 'bounded', timeoutMs: 30_000 },
          task: 'Return BRIDGE_SMOKE_OK.',
        };
        const operationId = randomUUID();
        const requestDigest =
          'sha256:' +
          createHash('sha256').update(JSON.stringify({ params })).digest('hex');
        const owner = {
          kind: 'pi-plan-exec',
          runId: 'isolated-smoke',
          key: operationId,
          requestDigest,
        };
        const fields = { operationId, owner, params };
        const spawned = await plan('spawn', fields);
        assert.equal(spawned.success, true, JSON.stringify(spawned));
        assert.ok(isRecord(spawned.data));
        assert.deepEqual(
          spawned.data.effectiveExecutionLifetime,
          params.executionLifetime,
        );
        const runId = spawned.data.runId;
        assert.equal(typeof runId, 'string');
        const lookup = await plan('operation', { operationId, owner });
        assert.ok(isRecord(lookup.data));
        assert.equal(lookup.data.state, 'found');
        assert.equal(lookup.data.runId, runId);
        const replay = await plan('spawn', fields);
        assert.ok(isRecord(replay.data));
        assert.equal(replay.data.runId, runId);
        let terminal: Record<string, unknown> | undefined;
        const deadline = Date.now() + 40_000;
        while (Date.now() < deadline) {
          const status = await plan('status', { params: { runId } });
          assert.equal(status.success, true, JSON.stringify(status));
          assert.ok(isRecord(status.data));
          if (
            isRecord(status.data.workflowTerminalProof) &&
            status.data.workflowTerminalProof.state === 'observed'
          ) {
            terminal = status.data;
            break;
          }
          await delay(100);
        }
        assert.ok(terminal, 'Missing native workflow terminal proof');
        assert.match(String(terminal.text), /completed/i);
        report.plan = {
          runId,
          lookup: 'found',
          replay: 'same-run',
          proof: terminal.workflowTerminalProof,
        };

        const completion = new Promise<unknown>((resolve, reject) => {
          const timer = setTimeout(() => {
            done();
            failed();
            reject(new Error('Task completion timed out'));
          }, 40_000);
          const done = pi.events.on('subagents:completed', (data) => {
            clearTimeout(timer);
            done();
            failed();
            resolve(data);
          });
          const failed = pi.events.on('subagents:failed', (data) => {
            clearTimeout(timer);
            done();
            failed();
            reject(new Error(JSON.stringify(data)));
          });
        });
        // Observe rejection immediately while spawn is in flight.
        const observedCompletion = completion.then(
          (data) => ({ data }),
          (error: unknown) => ({ error }),
        );
        const task = await request(
          'subagents:rpc:spawn',
          'subagents:rpc:spawn:reply:',
          {
            type: 'bridge-smoke',
            prompt: 'Return BRIDGE_SMOKE_OK.',
            options: { maxTurns: 1 },
          },
        );
        assert.equal(task.success, true, JSON.stringify(task));
        const completed = await observedCompletion;
        assert.ok('data' in completed, JSON.stringify(completed));
        assert.ok(isRecord(completed.data));
        assert.match(String(completed.data.result), /BRIDGE_SMOKE_OK/);
        report.task = completed.data;
        report.success = true;
      } catch (error) {
        report.success = false;
        report.error = error instanceof Error ? error.stack : String(error);
      }
      fs.writeFileSync(
        path.join(ctx.cwd, 'smoke-result.json'),
        JSON.stringify(report, null, 2),
      );
      ctx.shutdown();
    },
  });
}
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
