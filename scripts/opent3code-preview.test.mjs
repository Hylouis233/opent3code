import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, rmSync, symlinkSync, mkdirSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { initializePreview } from "./opent3code-preview.mjs";

test("preview creates supervised settings and never rewrites existing settings", () => {
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
test("preview refuses production namespaces, relative paths and unrelated directories", () => {
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
