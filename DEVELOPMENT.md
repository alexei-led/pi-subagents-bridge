# Development

## Local setup

```bash
npm install
npm run test:all
pi install /absolute/path/to/pi-subagents-bridge
```

Reload Pi after changing the extension:

```text
/reload
```

## Validation

```bash
npm run lint
npm run check
npm test
npm run pack:dry
npm run publish:dry
```

`npm run test:all` is the local pre-release gate.

## Release

Target package:

```text
@alexeiled/pi-subagents-bridge
```

Cut v0.1.0:

```bash
npm run test:all
npm version 0.1.0
git push origin main --follow-tags
```

The GitHub release workflow runs on pushed `v*` tags.
It verifies the tag matches `package.json`, checks it is on `main`, runs the validation gate, then publishes with npm provenance.

Configure npm trusted publishing after the first package publish:

```bash
npm trust github @alexeiled/pi-subagents-bridge \
  --repo alexei-led/pi-subagents-bridge \
  --file release.yml \
  --allow-publish \
  -y
```

`--file` must be just the workflow file name, not `.github/workflows/release.yml`.
