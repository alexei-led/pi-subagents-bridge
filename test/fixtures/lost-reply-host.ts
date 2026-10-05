import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import bridge from '../../src/index.js';

function rec(value: unknown): Record<string, unknown> {
  assert.ok(
    typeof value === 'object' && value !== null && !Array.isArray(value),
  );
  return value as Record<string, unknown>;
}
export default function lostReplyHost(pi: ExtensionAPI): void {
  const root = process.env.BRIDGE_LOST_REPLY_ROOT;
  assert.ok(root, 'requires isolated lost-reply server');
  const requests = new Map<string, string>();
  const events: ExtensionAPI['events'] = {
    emit(channel, data) {
      if (
        channel === 'subagents:rpc:v1:request' &&
        rec(data).method === 'spawn'
      ) {
        const req = rec(data);
        const phase = fs.readFileSync(path.join(root, 'phase.txt'), 'utf8');
        requests.set(String(req.requestId), phase);
        fs.appendFileSync(
          path.join(root, 'dispatches.jsonl'),
          `${JSON.stringify({ phase, requestId: req.requestId })}\n`,
        );
      }
      pi.events.emit(channel, data);
    },
    on(channel, handler) {
      return pi.events.on(channel, (raw: unknown) => {
        const requestId = channel.replace('subagents:rpc:v1:reply:', '');
        const phase = requests.get(requestId);
        if (phase && rec(raw).success === true) {
          requests.delete(requestId);
          fs.writeFileSync(
            path.join(root, `lost-${phase}.json`),
            JSON.stringify({ requestId, reply: raw }, null, 2),
          );
          return;
        }
        handler(raw);
      });
    },
  };
  bridge({ ...pi, events });
  const rpc = (
    method: string,
    fields: object,
  ): Promise<Record<string, unknown>> => {
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error('fixture bridge call timed out'));
      }, 40000);
      const off = pi.events.on(
        `plan-exec:bridge:v2:reply:${requestId}`,
        (raw) => {
          clearTimeout(timer);
          off();
          resolve(rec(raw));
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
  const until = async (predicate: () => boolean) => {
    const deadline = Date.now() + 30000;
    while (!predicate()) {
      assert.ok(Date.now() < deadline, 'fixture condition timed out');
      await delay(100);
    }
  };
  pi.events.on('subagent:async-complete', (raw) => {
    const data = rec(raw);
    if (
      typeof data.toolCallId === 'string' &&
      data.toolCallId.startsWith('rpc-spawn-')
    )
      fs.writeFileSync(
        path.join(root, 'completion.json'),
        JSON.stringify(data, null, 2),
      );
  });
  pi.registerCommand('lost-seed', {
    description:
      'Start one real child but drop its native spawn reply; live or complete',
    handler: async (args, ctx) => {
      const phase = args.trim() === 'complete' ? 'complete' : 'live';
      fs.writeFileSync(path.join(root, 'phase.txt'), phase);
      const params = {
        agent: 'lost-fixture',
        executionLifetime: { mode: 'unbounded' },
        task: 'Return LOST_REPLY_OK.',
      };
      const operationId = randomUUID();
      const requestDigest =
        'sha256:' +
        createHash('sha256').update(JSON.stringify({ params })).digest('hex');
      const fields = {
        operationId,
        params,
        owner: {
          kind: 'pi-plan-exec',
          runId: `fixture-${phase}`,
          key: operationId,
          requestDigest,
        },
      };
      fs.writeFileSync(
        path.join(root, `fields-${phase}.json`),
        JSON.stringify(fields, null, 2),
      );
      const pending = rpc('spawn', fields).then(
        (result) => {
          fs.writeFileSync(
            path.join(root, `spawn-outcome-${phase}.json`),
            JSON.stringify(result, null, 2),
          );
        },
        (error) => {
          fs.writeFileSync(
            path.join(root, `spawn-outcome-${phase}.json`),
            JSON.stringify({ error: String(error) }),
          );
        },
      );
      await until(
        () =>
          fs.existsSync(path.join(root, `lost-${phase}.json`)) &&
          fs.existsSync(path.join(root, `held-${phase}.json`)),
      );
      if (phase === 'complete') {
        await fetch(
          fs.readFileSync(path.join(root, 'server-url.txt'), 'utf8') +
            '/release',
        );
        const requestId = rec(
          JSON.parse(
            fs.readFileSync(path.join(root, `lost-${phase}.json`), 'utf8'),
          ),
        ).requestId;
        await until(() => {
          if (!fs.existsSync(path.join(root, 'completion.json'))) return false;
          return (
            rec(
              JSON.parse(
                fs.readFileSync(path.join(root, 'completion.json'), 'utf8'),
              ),
            ).toolCallId === `rpc-spawn-${requestId}`
          );
        });
      }
      fs.writeFileSync(
        path.join(root, `ready-${phase}.json`),
        JSON.stringify({
          phase,
          pid: process.pid,
          sessionId: ctx.sessionManager.getSessionId(),
        }),
      );
      ctx.ui.notify(
        `READY ${phase}: one real child; native launch reply dropped. Terminate ONLY fixture host PID ${process.pid}, then restart it and run /lost-verify ${phase}.`,
        'info',
      );
      await pending;
    },
  });
  pi.registerCommand('lost-verify', {
    description:
      'After full host restart, recover same child without another spawn',
    handler: async (args, ctx) => {
      const phase = args.trim() === 'complete' ? 'complete' : 'live';
      const fields = rec(
        JSON.parse(
          fs.readFileSync(path.join(root, `fields-${phase}.json`), 'utf8'),
        ),
      );
      const lost = rec(
        JSON.parse(
          fs.readFileSync(path.join(root, `lost-${phase}.json`), 'utf8'),
        ),
      );
      const spawned = rec(rec(lost.reply).data);
      const details = rec(spawned.details);
      const nativeRunId = String(details.runId ?? details.asyncId);
      const found = rec((await rpc('operation', fields)).data);
      assert.equal(found.state, 'found', JSON.stringify(found));
      assert.equal(found.runId, nativeRunId);
      assert.deepEqual(found.effectiveExecutionLifetime, { mode: 'unbounded' });
      for (let i = 0; i < 2; i++)
        assert.equal(rec((await rpc('spawn', fields)).data).runId, nativeRunId);
      const dispatches = fs
        .readFileSync(path.join(root, 'dispatches.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => rec(JSON.parse(line)));
      assert.equal(dispatches.filter((row) => row.phase === phase).length, 1);
      let terminal: Record<string, unknown> | undefined;
      if (phase === 'complete') {
        terminal = rec(
          (await rpc('status', { params: { runId: nativeRunId } })).data,
        );
        assert.equal(rec(terminal.workflowTerminalProof).state, 'observed');
        assert.match(String(terminal.text), /LOST_REPLY_OK/);
      }
      if (phase === 'live')
        await fetch(
          fs.readFileSync(path.join(root, 'server-url.txt'), 'utf8') +
            '/release',
        );
      fs.writeFileSync(
        path.join(root, `verified-${phase}.json`),
        JSON.stringify(
          {
            phase,
            found,
            ...(terminal ? { terminal } : {}),
            nativeRunId,
            dispatches: 1,
            pid: process.pid,
            sessionId: ctx.sessionManager.getSessionId(),
          },
          null,
          2,
        ),
      );
      ctx.ui.notify(
        `PASS ${phase}: same native run ${nativeRunId}; one dispatch; two replays created no worker.`,
        'info',
      );
    },
  });
}
