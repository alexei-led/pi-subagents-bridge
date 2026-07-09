import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

interface PackFile {
  path: string;
}

interface PackResult {
  filename: string;
  name: string;
  version: string;
  size: number;
  files: PackFile[];
}

interface PackageManifest {
  name: string;
  version: string;
  pi: { extensions?: string[]; image?: string };
  publishConfig?: { access?: string };
}

test("npm pack includes only release package assets", async (t) => {
  const packDir = await mkdtemp(join(tmpdir(), "pi-subagents-bridge-pack-"));
  t.after(async () => {
    await rm(packDir, { recursive: true, force: true });
  });

  const { stdout } = await execFileAsync(
    "npm",
    ["pack", "--json", "--pack-destination", packDir],
    { maxBuffer: 1024 * 1024 },
  );
  const result = parsePackResult(stdout);
  const manifest = parsePackageManifest(await readFile("package.json", "utf8"));
  const files = new Set(result.files.map((file) => file.path));

  assert.equal(result.name, manifest.name);
  assert.equal(result.version, manifest.version);
  assert.equal(
    result.filename,
    `${manifest.name.replaceAll("/", "-")}-${manifest.version}.tgz`.replace(
      /^@/,
      "",
    ),
  );
  assert.ok(result.size < 75_000);
  assert.ok(files.has("package.json"));
  assert.ok(files.has("README.md"));
  assert.ok(files.has("CHANGELOG.md"));
  assert.ok(files.has("LICENSE"));
  assert.ok(files.has("src/index.ts"));
  assert.ok(files.has("assets/bridge.svg"));
  assert.ok(files.has("docs/design.md"));
  assert.ok(files.has("docs/protocol-research.md"));
  assert.equal(files.has("tsconfig.json"), false);
  assert.equal(files.has("eslint.config.js"), false);
  assert.equal(
    [...files].some((file) => file.startsWith("test/")),
    false,
  );
  assert.equal(
    [...files].some((file) => file.startsWith(".github/")),
    false,
  );

  assert.deepEqual(manifest.pi.extensions, ["./src/index.ts"]);
  assert.match(manifest.pi.image ?? "", /^https:\/\//);
  assert.equal(manifest.publishConfig?.access, "public");
});

function parsePackageManifest(raw: string): PackageManifest {
  const value: unknown = JSON.parse(raw);
  assert.ok(isPackageManifest(value));
  return value;
}

function parsePackResult(stdout: string): PackResult {
  const value: unknown = JSON.parse(stdout);
  assert.ok(Array.isArray(value));
  const items: unknown[] = value;
  assert.equal(items.length, 1);
  const result = items[0];
  assert.ok(isPackResult(result));
  return result;
}

function isPackResult(value: unknown): value is PackResult {
  return (
    isRecord(value) &&
    typeof value.filename === "string" &&
    typeof value.name === "string" &&
    typeof value.version === "string" &&
    typeof value.size === "number" &&
    Array.isArray(value.files) &&
    value.files.every((file) => isRecord(file) && typeof file.path === "string")
  );
}

function isPackageManifest(value: unknown): value is PackageManifest {
  return (
    isRecord(value) &&
    typeof value.name === "string" &&
    typeof value.version === "string" &&
    isRecord(value.pi) &&
    (value.pi.extensions === undefined ||
      (Array.isArray(value.pi.extensions) &&
        value.pi.extensions.every((item) => typeof item === "string"))) &&
    (value.pi.image === undefined || typeof value.pi.image === "string") &&
    (value.publishConfig === undefined ||
      (isRecord(value.publishConfig) &&
        (value.publishConfig.access === undefined ||
          typeof value.publishConfig.access === "string")))
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
