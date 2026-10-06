# Development

## Local setup

```bash
npm install
npm run test:all
pi install /absolute/path/to/pi-subagents-bridge
```

Reload Pi after local Bridge source changes. Restart Pi after updating an
already-loaded Pi or pi-subagents package; `/reload` can retain mixed upstream
module versions.

For local edits:

```text
/reload
```

## Validation

```bash
npm run check          # Biome (format + lint) and TypeScript
npm test               # Vitest
npm run format         # rewrite formatting
npm run lint:fix       # apply safe lint fixes
npm run pack:dry
npm run publish:dry
```

`npm run test:all` is the local pre-release gate.

The pinned development stack is Pi 1.0.4 and pi-subagents 0.76.1.
`test/upstream-rpc.test.ts` loads the pinned pi-subagents RPC validator directly,
including its paused/queued stop refusal. Cancellation-delivery tests cover
late binding, retries and durable receipts; session-lifecycle tests cover
factory loading without a session and repeated start/shutdown.
`test/rpc-integration.test.ts` starts local Pi with Bridge and pi-subagents in
isolated HOME, agent, workspace, and temporary directories. It uses a local HTTP
model fixture, not credentials or globally installed extensions, and completes
both launch paths with native proof and replay checks.

For a visible, isolated restart/recovery check in agterm, launch:

```bash
agtermctl session new --cwd "$PWD" --name "Bridge recovery test" \
  --command "$(command -v node) $PWD/test/fixtures/agterm-smoke.mjs" --wait
```

Use the returned session ID for every terminal operation. The fixture prints its
temporary evidence directory and opens real Pi with only local Bridge,
pi-subagents, and two fixture command extensions. Run
`/bridge-rejection-seed`. After the host restarts, run
`/bridge-rejection-verify`, then `/bridge-smoke`. The final `PASS.json`
asserts no rejection/legacy replays and exactly two fresh child model requests.
The node driver hosts the deterministic HTTP model; its Pi child owns the TUI.
No global package, journal, credential, or live plan is changed. Evidence stays
in the printed temporary directory for inspection.

### Lost native reply and full host restart

`test/fixtures/lost-reply-server.mjs <empty-private-directory>` requires a
current-user-owned, non-symlink root (mode 0700 on POSIX) and refuses non-empty
roots before writing. Use `mktemp -d` to create it. The server creates an isolated
HOME/agent/project/native root and starts a deterministic loopback model. Run it
in its own agterm session. The generated `sources.json` records the exact local
CLI, upstream extension and Bridge fixture paths.

Start a separate direct Pi CLI session with the generated HOME,
PI_CODING_AGENT_DIR, PI_SUBAGENTS_TEMP_ROOT and BRIDGE_LOST_REPLY_ROOT. Use
`--no-extensions` and load only the local pi-subagents entry and
`test/fixtures/lost-reply-host.ts`. The wrapper loads the actual local Bridge
once and drops only the matching native spawn reply at the event boundary.
Workers and native status/proof handling remain real.

1. Run `/lost-seed live`. After READY, terminate only the fixture host PID printed
   by the command; the deterministic model keeps the real detached child active.
2. Restart the same isolated CLI and run `/lost-verify live`. It must reattach
   the same native run and replay twice with only one dispatch, then release the
   held model request.
3. Run `/lost-seed complete`. Wait for READY after native completion, terminate
   only that fixture host, restart again and run `/lost-verify complete`.
   The durable binding must survive native result delivery, with observed native
   workflow terminal proof and the fixture output.
4. Inspect `verified-live.json`, `verified-complete.json`, `dispatches.jsonl`
   and `model-calls.jsonl`: two native dispatches and two worker model requests
   total, one per scenario. Preserve terminal/tree evidence before closing the
   fixture sessions.

Never point these fault-injection fixtures at a real run, journal or agent
directory. They do not test or authorize termination of an unknown old worker.

## Release

Target package:

```text
@alexeiled/pi-subagents-bridge
```

For each release, decide patch versus minor, update `package.json`,
`package-lock.json`, and `CHANGELOG.md`, then commit and tag the chosen version:

```bash
npm run test:all
git commit -am "chore: release <version>"
git tag v<version>
git push origin main --follow-tags
```

Use `npm version patch` or `npm version minor` only when it makes the intended
version change; do not bump an already versioned release a second time.

The GitHub release workflow runs on pushed `v*` tags.
It verifies the tag matches `package.json`, checks it is on `main`, runs the validation gate, then publishes with npm provenance. The same workflow creates the GitHub release
with the tag as its title and the matching `CHANGELOG.md` section as its notes.

Configure npm trusted publishing after the first package publish:

```bash
npm trust github @alexeiled/pi-subagents-bridge \
  --repo alexei-led/pi-subagents-bridge \
  --file release.yml \
  --allow-publish \
  -y
```

`--file` must be just the workflow file name, not `.github/workflows/release.yml`.
