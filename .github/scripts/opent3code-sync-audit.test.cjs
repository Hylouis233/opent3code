const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");
const { assertSha, nulFields, parseDiff, parseMerge, sensitive, analyze, summary } = require("./opent3code-sync-audit.cjs");
const sha = "a".repeat(40);
const header = `:100644 100644 ${sha} ${"b".repeat(40)} M\0`;

test("audit requires exact SHAs, not refs or Git options", () => {
  assert.equal(assertSha(sha), sha);
  for (const invalid of ["main", "--all", sha + "\n", "a".repeat(39), null]) assert.throws(() => assertSha(invalid));
});
test("raw Git inventory preserves unusual paths and does not truncate at 3000", () => {
  const names = Array.from({ length: 3101 }, (_, i) => `folder/file-${i}`);
  names.push("tabs\tand\nnewlines", "--option", "README.md");
  const rows = parseDiff(names.map((name) => header + name + "\0").join(""));
  assert.equal(rows.length, names.length);
  assert.deepEqual(rows.map((row) => row.filename), names);
  assert.equal(rows.at(-1).newBlob, "b".repeat(40));
});
test("partial, duplicate and unknown diff records fail closed", () => {
  for (const invalid of [header, header + "x", header + "x\0" + header + "x\0", header.replace(" M", " R100") + "x\0", header + "\0"]) assert.throws(() => parseDiff(invalid));
  assert.deepEqual(nulFields(""), []);
});
test("a computed tree is not a validated candidate", () => {
  assert.deepEqual(parseMerge(sha + "\0", 0), { clean: true, tree: sha, conflicts: [] });
  assert.deepEqual(parseMerge(sha + "\0README.md\0", 1).conflicts, ["README.md"]);
  for (const [text, status] of [[sha, 0], [sha + "\0", 1], [sha + "\0x\0", 0], [sha + "\0", 2]]) assert.throws(() => parseMerge(text, status));
});
test("diagnostics flag release policy and independent integrations for review", () => {
  for (const name of ["README.md", ".github/SECURITY.md", "integrations/dsh-opent3code/package.json", "apps/server/src/provider/Drivers/ExternalAcpDriver.ts", "scripts/opent3code-preview.mjs"]) assert.equal(sensitive(name), true);
  assert.equal(sensitive("apps/web/src/simple.ts"), false);
});
test("real Git recovery inventory is complete and leaves branches and files untouched", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "opent3code-audit-test-"));
  const work = path.join(root, "work");
  const bare = path.join(root, "objects.git");
  fs.mkdirSync(work);
  const g = (...args) => execFileSync("git", ["-C", work, ...args], { encoding: "utf8", stdio: ["pipe", "pipe", "pipe"] }).trim();
  try {
    g("init", "-q"); g("config", "user.name", "Audit fixture"); g("config", "user.email", "audit@example.invalid");
    fs.writeFileSync(path.join(work, "README.md"), "base\n"); g("add", "."); g("commit", "-qm", "base");
    const ancestor = g("rev-parse", "HEAD");
    fs.writeFileSync(path.join(work, "README.md"), "fork\n");
    fs.writeFileSync(path.join(work, "opent3code-only.txt"), "preserve\n"); g("add", "."); g("commit", "-qm", "fork");
    const base = g("rev-parse", "HEAD");
    g("checkout", "-q", ancestor);
    fs.mkdirSync(path.join(work, "many"));
    for (let i = 0; i < 3101; i++) fs.writeFileSync(path.join(work, "many", String(i)), "fixture\n");
    fs.writeFileSync(path.join(work, "README.md"), "upstream\n"); g("add", "."); g("commit", "-qm", "upstream");
    const head = g("rev-parse", "HEAD");
    execFileSync("git", ["clone", "--bare", "--quiet", work, bare]);
    const refs = execFileSync("git", ["-C", bare, "show-ref"], { encoding: "utf8" });
    const report = analyze(bare, base, head);
    assert.equal(report.mergeBase, ancestor);
    assert.equal(report.upstreamFiles.length, 3102);
    assert.equal(report.forkFiles.length, 2);
    assert.deepEqual(report.mergeComputation.conflicts, ["README.md"]);
    assert.deepEqual(report.overlap, ["README.md"]);
    assert.equal(report.validationSucceeded, false);
    assert.equal(report.mergeAuthorized, false);
    assert.equal(report.diagnosticOnly, true);
    assert.equal(execFileSync("git", ["-C", bare, "show-ref"], { encoding: "utf8" }), refs);
    assert.equal(fs.readFileSync(path.join(work, "README.md"), "utf8"), "upstream\n");
    assert.equal(fs.existsSync(path.join(bare, "README.md")), false);
    assert.equal(analyze(bare, ancestor, head).mergeComputation.clean, true);
    assert.throws(() => analyze(work, base, head), /bare repository/);
    report.overlap.push("<script>alert(1)</script>");
    assert.doesNotMatch(summary(report), /<script>/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
