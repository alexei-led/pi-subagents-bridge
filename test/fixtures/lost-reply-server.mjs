import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const requestedRoot = process.argv[2];
assert.ok(
  requestedRoot,
  'An existing empty private fixture directory is required',
);
const rootStat = fs.lstatSync(requestedRoot);
assert.ok(
  rootStat.isDirectory() && !rootStat.isSymbolicLink(),
  'Fixture root must be a directory, not a symlink',
);
const root = fs.realpathSync(requestedRoot);
assert.equal(fs.readdirSync(root).length, 0, 'Fixture root must be empty');
if (process.getuid) {
  assert.equal(
    rootStat.uid,
    process.getuid(),
    'Fixture root must be owned by the current user',
  );
  assert.equal(
    rootStat.mode & 0o077,
    0,
    'Fixture root must be private (mode 0700)',
  );
}
const repo = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../..',
);
for (const name of ['home', 'agent/agents', 'cwd', 'native'])
  fs.mkdirSync(path.join(root, name), { recursive: true });
const pending = new Set();
function respond(res) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream' });
  const chunk = {
    id: 'fixture',
    object: 'chat.completion.chunk',
    created: 1,
    model: 'fixture',
    choices: [
      {
        index: 0,
        delta: { role: 'assistant', content: 'LOST_REPLY_OK' },
        finish_reason: null,
      },
    ],
  };
  res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  res.write(
    `data: ${JSON.stringify({ ...chunk, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`,
  );
  res.end('data: [DONE]\n\n');
}
const server = http.createServer((req, res) => {
  if (req.url === '/release') {
    for (const held of pending) respond(held);
    pending.clear();
    res.end('released');
    return;
  }
  let body = '';
  req.on('data', (chunk) => {
    body += chunk.toString();
  });
  req.on('end', () => {
    const worker = body.includes('"text":"Task: Return LOST_REPLY_OK."');
    const phase = fs.existsSync(path.join(root, 'phase.txt'))
      ? fs.readFileSync(path.join(root, 'phase.txt'), 'utf8')
      : 'none';
    fs.appendFileSync(
      path.join(root, 'model-calls.jsonl'),
      `${JSON.stringify({ phase, worker })}\n`,
    );
    if (worker) {
      pending.add(res);
      fs.writeFileSync(
        path.join(root, `held-${phase}.json`),
        JSON.stringify({ phase, worker: true }),
      );
      console.log('HELD child request:', phase);
    } else respond(res);
  });
});
server.listen(0, '127.0.0.1', () => {
  const addr = server.address();
  assert.ok(addr && typeof addr !== 'string');
  const url = `http://127.0.0.1:${addr.port}`;
  fs.writeFileSync(path.join(root, 'server-url.txt'), url);
  fs.writeFileSync(
    path.join(root, 'agent/models.json'),
    JSON.stringify({
      providers: {
        lost: {
          api: 'openai-completions',
          apiKey: 'fixture-not-a-secret',
          baseUrl: `${url}/v1`,
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
    path.join(root, 'agent/settings.json'),
    JSON.stringify({
      defaultProvider: 'lost',
      defaultModel: 'fixture',
      defaultThinkingLevel: 'off',
    }),
  );
  fs.writeFileSync(
    path.join(root, 'agent/agents/lost-fixture.md'),
    '---\nname: lost-fixture\ndescription: Isolated read-only held worker\nmodel: lost/fixture\nthinking: off\ntools: read\nsystemPromptMode: replace\ninheritProjectContext: false\ninheritSkills: false\n---\nReturn LOST_REPLY_OK.\n',
  );
  const manifest = {
    repo,
    root,
    cli: path.join(
      repo,
      'node_modules/@earendil-works/pi-coding-agent/dist/cli.js',
    ),
    native: path.join(repo, 'node_modules/pi-subagents/index.js'),
    bridgeWrapper: path.join(repo, 'test/fixtures/lost-reply-host.ts'),
    actualBridge: path.join(repo, 'src/index.ts'),
    piVersion: JSON.parse(
      fs.readFileSync(
        path.join(
          repo,
          'node_modules/@earendil-works/pi-coding-agent/package.json',
        ),
      ),
    ).version,
    subagentsVersion: JSON.parse(
      fs.readFileSync(
        path.join(repo, 'node_modules/pi-subagents/package.json'),
      ),
    ).version,
  };
  fs.writeFileSync(
    path.join(root, 'sources.json'),
    JSON.stringify(manifest, null, 2),
  );
  console.log('READY fixture root:', root, 'server:', url);
});
