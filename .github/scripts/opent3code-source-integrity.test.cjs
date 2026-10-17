const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const { mkdtempSync, rmSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { test } = require("node:test");
const { check } = require("./opent3code-source-integrity.cjs");

const REAL =
  "export function score(a: string[], b: string[]) {\n  return a.length + b.length;\n}\n".repeat(5);

function repo(t) {
  const dir = mkdtempSync(join(tmpdir(), "source-integrity-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const git = (...args) => execFileSync("git", args, { cwd: dir, stdio: "pipe" });
  const commit = (content, message = "change") => {
    writeFileSync(join(dir, "mod.ts"), content);
    git("add", "mod.ts");
    git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", message);
  };
  git("init", "-q");
  commit(REAL, "init");
  return { dir, commit };
}

test("normal edits pass", (t) => {
  const { dir, commit } = repo(t);
  commit(`${REAL}// more\n`);
  assert.deepEqual(check("HEAD~1", "HEAD", { cwd: dir }), []);
});

test("whole-file placeholders fail even in placeholder-only mode or with a trailer", (t) => {
  const { dir, commit } = repo(t);
  commit("PLACEHOLDER\n", "stub\n\nSource-Integrity-Allow: mod.ts");
  assert.equal(check("HEAD~1", "HEAD", { cwd: dir }).length, 1);
  assert.equal(check("HEAD~1", "HEAD", { cwd: dir, placeholderOnly: true }).length, 1);
});

test("shrinking by more than 80% fails unless allowed by trailer or placeholder-only mode", (t) => {
  const { dir, commit } = repo(t);
  commit(REAL.slice(0, 40));
  assert.match(check("HEAD~1", "HEAD", { cwd: dir })[0], /shrank/);
  assert.deepEqual(check("HEAD~1", "HEAD", { cwd: dir, placeholderOnly: true }), []);
  commit(REAL);
  commit(REAL.slice(0, 40), "shrink\n\nSource-Integrity-Allow: mod.ts");
  assert.deepEqual(check("HEAD~1", "HEAD", { cwd: dir }), []);
});
