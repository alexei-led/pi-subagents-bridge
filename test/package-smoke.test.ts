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
  files?: string[];
  pi: { extensions?: string[]; image?: string };
  publishConfig?: { access?: string };
}

test("npm pack includes only the runtime extension package", async (t) => {
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
  assert.deepEqual([...files].sort(), [
    "LICENSE",
    "README.md",
    "package.json",
    "src/index.ts",
  ]);

  assert.deepEqual(manifest.files, ["src/index.ts"]);
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
  const items: unknown[] = Array.isArray(value)
    ? value
    : isRecord(value)
      ? Object.values(value)
      : [];
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
    (value.files === undefined ||
      (Array.isArray(value.files) &&
        value.files.every((item) => typeof item === "string"))) &&
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
