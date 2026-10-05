// Launch from agterm with session new --command "node /absolute/path/to/this-file".
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../..',
);
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-agterm-'));
const home = path.join(root, 'home');
const agent = path.join(root, 'agent');
const cwd = path.join(root, 'cwd');
for (const dir of [home, agent, cwd, path.join(agent, 'agents')])
  fs.mkdirSync(dir, { recursive: true });
let childRequests = 0;
const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (data) => {
    body += data.toString();
  });
  req.on('end', () => {
    if (body.includes('"text":"Task: Return BRIDGE_SMOKE_OK."'))
      childRequests++;
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
      `data: ${JSON.stringify({ ...chunk, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`,
    );
    res.end('data: [DONE]\n\n');
  });
});
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const port = server.address().port;
fs.writeFileSync(
  path.join(agent, 'models.json'),
  JSON.stringify({
    providers: {
      smoke: {
        api: 'openai-completions',
        apiKey: 'fixture-not-a-secret',
        baseUrl: `http://127.0.0.1:${port}/v1`,
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
  path.join(agent, 'settings.json'),
  JSON.stringify({
    defaultProvider: 'smoke',
    defaultModel: 'fixture',
    defaultThinkingLevel: 'off',
  }),
);
fs.writeFileSync(
  path.join(agent, 'agents/bridge-smoke.md'),
  '---\nname: bridge-smoke\ndescription: Isolated read-only fixture\nmodel: smoke/fixture\nthinking: off\ntools: read\nsystemPromptMode: replace\ninheritProjectContext: false\ninheritSkills: false\n---\nReturn BRIDGE_SMOKE_OK.\n',
);
const extensions = [
  path.join(repo, 'node_modules/pi-subagents/index.js'),
  path.join(repo, 'src/index.ts'),
  path.join(repo, 'test/fixtures/rpc-smoke-extension.ts'),
  path.join(repo, 'test/fixtures/recovery-smoke-extension.ts'),
];
const env = {
  PATH: process.env.PATH,
  TERM: process.env.TERM ?? 'xterm-256color',
  HOME: home,
  TMPDIR: root,
  PI_CODING_AGENT_DIR: agent,
  PI_OFFLINE: '1',
  BRIDGE_AGTERM_SMOKE_ROOT: root,
};
const cli = path.join(
  repo,
  'node_modules/@earendil-works/pi-coding-agent/dist/cli.js',
);
const evidence = {
  root,
  extensions,
  bridgeVersion: JSON.parse(fs.readFileSync(path.join(repo, 'package.json')))
    .version,
  subagentsVersion: JSON.parse(
    fs.readFileSync(path.join(repo, 'node_modules/pi-subagents/package.json')),
  ).version,
  piVersion: JSON.parse(
    fs.readFileSync(
      path.join(
        repo,
        'node_modules/@earendil-works/pi-coding-agent/package.json',
      ),
    ),
  ).version,
};
fs.writeFileSync(
  path.join(root, 'loaded-sources.json'),
  JSON.stringify(evidence, null, 2),
);
console.log('Isolated evidence directory:', root);
try {
  for (const command of [
    '/bridge-rejection-seed',
    '/bridge-rejection-verify then /bridge-smoke',
  ]) {
    console.log('Run in this real Pi terminal:', command);
    const child = spawn(
      process.execPath,
      [
        cli,
        '--no-extensions',
        '--no-skills',
        '--no-prompt-templates',
        '--no-themes',
        '--no-context-files',
        '--verbose',
        ...extensions.flatMap((p) => ['--extension', p]),
      ],
      { cwd, env, stdio: 'inherit' },
    );
    const [code] = await once(child, 'exit');
    assert.equal(code, 0);
  }
  assert.equal(
    JSON.parse(fs.readFileSync(path.join(cwd, 'recovery-verified.json')))
      .spawns,
    0,
  );
  assert.equal(
    JSON.parse(fs.readFileSync(path.join(cwd, 'smoke-result.json'))).success,
    true,
  );
  assert.equal(childRequests, 2);
  fs.writeFileSync(
    path.join(root, 'PASS.json'),
    JSON.stringify({ ...evidence, childRequests, success: true }, null, 2),
  );
  console.log(
    'PASS: actual Pi restarted; rejected/legacy replay count zero; exactly two fresh child requests.',
    root,
  );
} finally {
  server.close();
}
