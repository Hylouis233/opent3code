const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { execFileSync } = require("node:child_process");
const {
  assertSha,
  decodeGitOutput,
  nulFields,
  parseDiff,
  parseMerge,
  sensitive,
  analyze,
  summary,
} = require("./opent3code-sync-audit.cjs");
const sha = "a".repeat(40);
const header = `:100644 100644 ${sha} ${"b".repeat(40)} M\0`;

test("audit requires exact SHAs, not refs or Git options", () => {
  assert.equal(assertSha(sha), sha);
  for (const invalid of ["main", "--all", sha + "\n", "a".repeat(39), null])
    assert.throws(() => assertSha(invalid));
});
test("Git output decoding preserves UTF-8 and rejects malformed bytes", () => {
  const text = "folder/中文\tname\nfile\0";
  assert.equal(decodeGitOutput(Buffer.from(text)), text);
  assert.equal(decodeGitOutput(Buffer.alloc(0)), "");
  for (const bytes of [
    [0xff, 0],
    [0xc0, 0xaf, 0],
    [0xe2, 0x82],
    [0xed, 0xa0, 0x80],
  ])
    assert.throws(() => decodeGitOutput(Buffer.from(bytes)), /not valid UTF-8/);
});
test("raw Git inventory preserves unusual paths and does not truncate at 3000", () => {
  const names = Array.from({ length: 3101 }, (_, i) => `folder/file-${i}`);
  names.push("tabs\tand\nnewlines", "--option", "README.md");
  const rows = parseDiff(names.map((name) => header + name + "\0").join(""));
  assert.equal(rows.length, names.length);
  assert.deepEqual(
    rows.map((row) => row.filename),
    names,
  );
  assert.equal(rows.at(-1).newBlob, "b".repeat(40));
});
test("partial, duplicate and unknown diff records fail closed", () => {
  for (const invalid of [
    header,
    header + "x",
    header + "x\0" + header + "x\0",
    header.replace(" M", " R100") + "x\0",
    header + "\0",
  ])
    assert.throws(() => parseDiff(invalid));
  assert.deepEqual(nulFields(""), []);
});
test("a computed tree is not a validated candidate", () => {
  assert.deepEqual(parseMerge(sha + "\0", 0), { clean: true, tree: sha, conflicts: [] });
  assert.deepEqual(parseMerge(sha + "\0README.md\0", 1).conflicts, ["README.md"]);
  for (const [text, status] of [
    [sha, 0],
    [sha + "\0", 1],
    [sha + "\0x\0", 0],
    [sha + "\0", 2],
  ])
    assert.throws(() => parseMerge(text, status));
});
test("diagnostics flag release policy and independent integrations for review", () => {
  for (const name of [
    "README.md",
    ".github/SECURITY.md",
    "integrations/dsh-opent3code/package.json",
    "apps/server/src/provider/Drivers/ExternalAcpDriver.ts",
    "scripts/opent3code-preview.mjs",
  ])
    assert.equal(sensitive(name), true);
  assert.equal(sensitive("apps/web/src/simple.ts"), false);
});
test("real Git recovery inventory is complete and leaves branches and files untouched", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "opent3code-audit-test-"));
  const work = path.join(root, "work");
  const bare = path.join(root, "objects.git");
  fs.mkdirSync(work);
  // Fixture commits must not depend on a hosted runner's Git account configuration.
  const fixtureEnv = {
    ...process.env,
    GIT_AUTHOR_NAME: "Audit fixture",
    GIT_AUTHOR_EMAIL: "audit@example.invalid",
    GIT_COMMITTER_NAME: "Audit fixture",
    GIT_COMMITTER_EMAIL: "audit@example.invalid",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: process.platform === "win32" ? "NUL" : "/dev/null",
    GIT_CONFIG_COUNT: "0",
  };
  const g = (...args) =>
    execFileSync("git", ["-C", work, ...args], {
      encoding: "utf8",
      env: fixtureEnv,
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
  try {
    g("init", "-q");
    fs.writeFileSync(path.join(work, "README.md"), "base\n");
    g("add", ".");
    g("commit", "-qm", "base");
    assert.equal(
      g("log", "-1", "--format=%an <%ae>|%cn <%ce>"),
      "Audit fixture <audit@example.invalid>|Audit fixture <audit@example.invalid>",
    );
    const ancestor = g("rev-parse", "HEAD");
    fs.writeFileSync(path.join(work, "README.md"), "fork\n");
    fs.writeFileSync(path.join(work, "opent3code-only.txt"), "preserve\n");
    g("add", ".");
    g("commit", "-qm", "fork");
    const base = g("rev-parse", "HEAD");
    g("checkout", "-q", ancestor);
    fs.mkdirSync(path.join(work, "many"));
    for (let i = 0; i < 3101; i++)
      fs.writeFileSync(path.join(work, "many", String(i)), "fixture\n");
    fs.writeFileSync(path.join(work, "README.md"), "upstream\n");
    g("add", ".");
    g("commit", "-qm", "upstream");
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
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
test("NUL-delimited evidence preserves every non-NUL ASCII character and Unicode path", () => {
  const names = Array.from(
    { length: 127 },
    (_, index) => `folder/prefix${String.fromCodePoint(index + 1)}suffix`,
  );
  names.push("folder/中文", "folder/cafe\u0301", "folder/\u{1f9ea}");
  const wire = names.map((name) => header + name + "\0").join("");
  const decoded = decodeGitOutput(Buffer.from(wire));
  assert.equal(decoded, wire);
  assert.deepEqual(
    parseDiff(decoded).map((entry) => entry.filename),
    names,
  );
  assert.deepEqual(parseMerge(sha + "\0" + names.join("\0") + "\0", 1).conflicts, names);
});
test("NUL remains a record delimiter rather than part of a filename", () => {
  assert.throws(() => parseDiff(header + "prefix\0suffix\0"), /Incomplete raw diff/);
  assert.throws(() => parseDiff(header + "prefix\0\0"), /Incomplete raw diff/);
  assert.throws(() => parseMerge(sha + "\0prefix\0\0", 1), /empty conflict paths/);
  assert.throws(() => parseMerge(sha + "\0prefix\0prefix\0", 1), /Duplicate conflict paths/);
});
