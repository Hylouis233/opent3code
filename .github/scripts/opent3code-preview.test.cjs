const { test } = require("node:test");
const assert = require("node:assert/strict");
const { mkdtempSync, readFileSync, writeFileSync, rmSync, symlinkSync, mkdirSync } = require("node:fs");
const path = require("node:path");
const { tmpdir } = require("node:os");

test("preview creates supervised settings and preserves existing settings", async () => {
  const { initializePreview } = await import("../../scripts/opent3code-preview.mjs");
  const root = mkdtempSync(path.join(tmpdir(), "opent3code-init-"));
  try {
    const result = initializePreview(path.join(root, "preview"), root);
    const settings = JSON.parse(readFileSync(result.settings));
    assert.equal(settings.defaultRuntimeMode, "approval-required");
    assert.equal(settings.providerInstances.mcode.driver, "mcode");
    writeFileSync(result.settings, '{"user":"preserved"}\n');
    initializePreview(result.home, root);
    assert.equal(readFileSync(result.settings, "utf8"), '{"user":"preserved"}\n');
  } finally { rmSync(root, { recursive: true, force: true }); }
});
test("preview rejects production namespaces and metadata links", async () => {
  const { initializePreview } = await import("../../scripts/opent3code-preview.mjs");
  const root = mkdtempSync(path.join(tmpdir(), "opent3code-init-"));
  try {
    for (const folder of [".t3", ".dsh", ".minimax"]) assert.throws(() => initializePreview(path.join(root, folder, "child"), root), /namespace/);
    writeFileSync(path.join(root, "keep"), "important");
    assert.throws(() => initializePreview(root, root), /not an OpenT3Code/);
    assert.throws(() => initializePreview("relative", root), /absolute/);
    if (process.platform !== "win32") {
      mkdirSync(path.join(root, ".t3"));
      symlinkSync(path.join(root, ".t3"), path.join(root, "linked"));
      assert.throws(() => initializePreview(path.join(root, "linked", "child"), root), /namespace/);
      const result = initializePreview(path.join(root, "preview"), root);
      rmSync(result.settings);
      symlinkSync(path.join(root, "keep"), result.settings);
      assert.throws(() => initializePreview(result.home, root), /regular file/);
      assert.equal(readFileSync(path.join(root, "keep"), "utf8"), "important");
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});
test("DSH setup uses explicit build approvals and refuses changed or existing profiles", async () => {
  const { prepareProfile } = await import("../../integrations/dsh-opent3code/prepare-profile.mjs");
  const root = mkdtempSync(path.join(tmpdir(), "opent3code-dsh-"));
  try {
    const dir = prepareProfile(root);
    const file = path.join(dir, "pnpm-workspace.yaml");
    const policy = readFileSync(file, "utf8");
    assert.match(policy, /nodeLinker: hoisted/);
    assert.match(policy, /dsh-subprocess-local@0\.1\.5-rc\.2/);
    assert.doesNotMatch(policy, /dangerouslyAllowAllBuilds|strictDepBuilds: false/);
    assert.equal(prepareProfile(root), dir);
    writeFileSync(file, "user-owned\n");
    assert.throws(() => prepareProfile(root), /no files were overwritten/);
    assert.equal(readFileSync(file, "utf8"), "user-owned\n");
    assert.throws(() => prepareProfile("relative"), /absolute/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
