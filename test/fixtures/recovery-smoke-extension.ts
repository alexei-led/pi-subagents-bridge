import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { OperationJournal } from '../../src/operation-journal.js';

function record(value: unknown): Record<string, unknown> {
  assert.ok(
    typeof value === 'object' && value !== null && !Array.isArray(value),
  );
  return value as Record<string, unknown>;
}
function fields(operationId: string) {
  const params = {
    agent: 'bridge-smoke',
    executionLifetime: { mode: 'unbounded' },
    task: 'Never execute',
    workflowScriptPath: './removed.js',
  };
  const requestDigest =
    'sha256:' +
    createHash('sha256').update(JSON.stringify({ params })).digest('hex');
  return {
    operationId,
    params,
    owner: {
      kind: 'pi-plan-exec',
      runId: 'recovery-fixture',
      key: operationId,
      requestDigest,
    },
  };
}
export default function recoverySmoke(pi: ExtensionAPI): void {
  let spawns = 0;
  pi.events.on('subagents:rpc:v1:request', (raw) => {
    if (record(raw).method === 'spawn') spawns++;
  });
  const call = (
    method: string,
    fields: object,
  ): Promise<Record<string, unknown>> => {
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error('recovery RPC timed out'));
      }, 10000);
      const off = pi.events.on(
        `plan-exec:bridge:v2:reply:${requestId}`,
        (raw) => {
          clearTimeout(timer);
          off();
          resolve(record(raw));
        },
      );
      pi.events.emit('plan-exec:bridge:v2:request', {
        version: 2,
        requestId,
        method,
        ...fields,
      });
    });
  };
  pi.registerCommand('bridge-rejection-seed', {
    description:
      'Seed isolated correlated rejection and legacy unknown fixture',
    handler: async (_args, ctx) => {
      assert.ok(
        process.env.BRIDGE_AGTERM_SMOKE_ROOT,
        'requires isolated smoke launcher',
      );
      const rejected = fields('correlated-rejection');
      const reply = await call('spawn', rejected);
      assert.equal(record(reply.error).upstreamCode, 'invalid_params');
      const lookup = record((await call('operation', rejected)).data);
      assert.equal(lookup.neverStarted, true);
      const legacy = fields('legacy-unknown');
      const journal = new OperationJournal(
        path.join(
          os.homedir(),
          '.pi',
          'pi-subagents-bridge',
          'plan-exec-operations.sqlite',
        ),
      );
      journal.begin(
        legacy.operationId,
        legacy.owner.requestDigest,
        legacy.owner.runId,
        { mode: 'unbounded' },
      );
      assert.equal(spawns, 1);
      fs.writeFileSync(
        path.join(ctx.cwd, 'recovery-seed.json'),
        JSON.stringify({ rejected, legacy, spawns, lookup }, null, 2),
      );
      ctx.ui.notify(
        'Correlated rejection persisted; legacy no-proof row fenced. Restarting host.',
        'info',
      );
      ctx.shutdown();
    },
  });
  pi.registerCommand('bridge-rejection-verify', {
    description:
      'Verify rejection recovery and legacy fencing after actual host restart',
    handler: async (_args, ctx) => {
      assert.ok(
        process.env.BRIDGE_AGTERM_SMOKE_ROOT,
        'requires isolated smoke launcher',
      );
      const saved = record(
        JSON.parse(
          fs.readFileSync(path.join(ctx.cwd, 'recovery-seed.json'), 'utf8'),
        ),
      );
      const rejected = record(saved.rejected);
      const legacy = record(saved.legacy);
      const lookup = record((await call('operation', rejected)).data);
      assert.equal(lookup.state, 'not_started');
      const cancelled = record((await call('cancelOperation', rejected)).data);
      assert.equal(cancelled.neverStarted, true);
      assert.equal(cancelled.cancellationRequested, true);
      assert.equal((await call('spawn', rejected)).success, false);
      const unknown = record((await call('operation', legacy)).data);
      assert.equal(unknown.state, 'unknown');
      assert.match(
        String(unknown.text),
        /repeated resume cannot establish absence/,
      );
      assert.equal((await call('spawn', legacy)).success, false);
      const cancelUnknown = record(
        (await call('cancelOperation', legacy)).data,
      );
      assert.equal(cancelUnknown.neverStarted, false);
      assert.equal((await call('spawn', legacy)).success, false);
      assert.equal(spawns, 0);
      fs.writeFileSync(
        path.join(ctx.cwd, 'recovery-verified.json'),
        JSON.stringify(
          { lookup, cancelled, unknown, cancelUnknown, spawns },
          null,
          2,
        ),
      );
      ctx.ui.notify(
        'PASS: durable rejection fenced; legacy remains unknown; zero replay spawns. Now run /bridge-smoke.',
        'info',
      );
    },
  });
}
