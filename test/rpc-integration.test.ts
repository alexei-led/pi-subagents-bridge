import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { onTestFinished, test } from 'vitest';

test('isolated Pi and released subagents complete both Bridge RPC paths', {
  timeout: 100_000,
}, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-real-rpc-'));
  const home = path.join(root, 'home');
  const agentDir = path.join(root, 'agent');
  const cwd = path.join(root, 'cwd');
  for (const dir of [home, agentDir, cwd, path.join(agentDir, 'agents')])
    fs.mkdirSync(dir, { recursive: true });
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  let childModelRequests = 0;
  // Only the model HTTP boundary is fake. Pi, Bridge, RPC, workflow and child runtimes are real.
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (data: Buffer) => {
      body += data.toString();
    });
    req.on('end', () => {
      // Native completion notices also wake the parent; count only child prompts.
      if (body.includes('"text":"Task: Return BRIDGE_SMOKE_OK."'))
        childModelRequests++;
    });
    req.resume();
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    const chunk = {
      id: 'fixture',
      object: 'chat.completion.chunk',
      created: 1,
      model: 'fixture',
      choices: [
        {
          index: 0,
          delta: { role: 'assistant', content: 'BRIDGE_SMOKE_OK' },
          finish_reason: null,
        },
      ],
    };
    res.write(`data: ${JSON.stringify(chunk)}\n\n`);
    res.write(
      'data: ' +
        JSON.stringify({
          ...chunk,
          choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        }) +
        '\n\n',
    );
    res.end('data: [DONE]\n\n');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  onTestFinished(() => {
    server.close();
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  fs.writeFileSync(
    path.join(agentDir, 'models.json'),
    JSON.stringify({
      providers: {
        smoke: {
          api: 'openai-completions',
          apiKey: 'fixture-not-a-secret',
          baseUrl: `http://127.0.0.1:${address.port}/v1`,
          models: [
            {
              id: 'fixture',
              reasoning: false,
              contextWindow: 128000,
              maxTokens: 1024,
            },
          ],
        },
      },
    }),
  );
  fs.writeFileSync(
    path.join(agentDir, 'settings.json'),
    JSON.stringify({
      defaultProvider: 'smoke',
      defaultModel: 'fixture',
      defaultThinkingLevel: 'off',
    }),
  );
  fs.writeFileSync(
    path.join(agentDir, 'agents', 'bridge-smoke.md'),
    '---\nname: bridge-smoke\ndescription: Isolated read-only RPC fixture\nmodel: smoke/fixture\nthinking: off\ntools: read\nsystemPromptMode: replace\ninheritProjectContext: false\ninheritSkills: false\n---\nReturn BRIDGE_SMOKE_OK.\n',
  );
  const child = spawn(
    process.execPath,
    [
      path.resolve('node_modules/@earendil-works/pi-coding-agent/dist/cli.js'),
      '--mode',
      'rpc',
      '--no-extensions',
      '--no-skills',
      '--no-prompt-templates',
      '--no-themes',
      '--no-context-files',
      '--extension',
      path.resolve('node_modules/pi-subagents/index.js'),
      '--extension',
      path.resolve('src/index.ts'),
      '--extension',
      path.resolve('test/fixtures/rpc-smoke-extension.ts'),
    ],
    {
      cwd,
      env: {
        PATH: process.env.PATH,
        HOME: home,
        TMPDIR: root,
        PI_CODING_AGENT_DIR: agentDir,
        PI_OFFLINE: '1',
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    },
  );
  let stderr = '';
  let stdout = '';
  child.stderr.on('data', (data: Buffer) => {
    stderr += data.toString();
  });
  child.stdout.on('data', (data: Buffer) => {
    stdout += data.toString();
  });
  const timer = setTimeout(() => child.kill('SIGTERM'), 90_000);
  onTestFinished(() => {
    clearTimeout(timer);
    if (child.exitCode === null) child.kill('SIGTERM');
  });
  child.stdin.write(
    JSON.stringify({ id: 'smoke', type: 'prompt', message: '/bridge-smoke' }) +
      '\n',
  );
  const [code, signal] = await once(child, 'close');
  clearTimeout(timer);
  assert.equal(code, 0, stderr + stdout);
  assert.equal(signal, null, stderr + stdout);
  const report = JSON.parse(
    fs.readFileSync(path.join(cwd, 'smoke-result.json'), 'utf8'),
  );
  assert.equal(report.success, true, JSON.stringify(report) + stderr);
  assert.equal(report.obsoleteField, 'invalid_params');
  assert.equal(report.plan.lookup, 'found');
  assert.equal(report.plan.proof.state, 'observed');
  assert.match(report.task.result, /BRIDGE_SMOKE_OK/);
  assert.equal(childModelRequests, 2);
  assert.equal(report.plan.proof.children.length, 1);
});
