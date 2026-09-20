"use strict";

// Read-only recovery evidence. Never checks out, executes, pushes, or approves upstream code.
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { isUtf8 } = require("node:buffer");
const SHA = /^[a-f0-9]{40}$/;

// This standalone trusted tool cannot load application workspace helpers.
// Reject invalid path bytes instead of silently replacing them in audit evidence.
function decodeGitOutput(bytes) {
  if (!isUtf8(bytes)) throw Error("Git output is not valid UTF-8; no audit can be trusted.");
  // oxlint-disable-next-line t3code/no-new-text-encoder-decoder -- Standalone CI audit cannot load workspace helpers; invalid UTF-8 was rejected above.
  return Buffer.from(bytes).toString("utf8");
}

function assertSha(value) {
  if (!SHA.test(value || "")) throw Error("An exact 40-character commit SHA is required.");
  return value;
}
function git(repo, args, allowed = [0]) {
  const env = {
    PATH: process.env.PATH,
    HOME: path.dirname(repo),
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: process.platform === "win32" ? "NUL" : "/dev/null",
    GIT_CONFIG_COUNT: "0",
    GIT_TERMINAL_PROMPT: "0",
    GIT_NO_REPLACE_OBJECTS: "1",
    LC_ALL: "C",
  };
  const result = spawnSync(
    "git",
    [
      "-c",
      "core.hooksPath=/dev/null",
      "-c",
      "protocol.ext.allow=never",
      "-c",
      "protocol.file.allow=never",
      "-c",
      "submodule.recurse=false",
      "-C",
      repo,
      ...args,
    ],
    { env, maxBuffer: 128 * 1024 * 1024, timeout: 300000 },
  );
  if (result.error || !allowed.includes(result.status)) {
    throw Error(
      `Git ${args[0]} failed: ${result.error?.message || decodeGitOutput(result.stderr).slice(0, 4000)}`,
    );
  }
  return { status: result.status, text: decodeGitOutput(result.stdout) };
}
function nulFields(text) {
  if (text === "") return [];
  if (!text.endsWith("\0")) throw Error("Truncated Git output; no audit can be trusted.");
  return text.slice(0, -1).split("\0");
}
function parseDiff(text) {
  const fields = nulFields(text);
  if (fields.length % 2) throw Error("Incomplete raw diff record.");
  const seen = new Set();
  const files = [];
  for (let i = 0; i < fields.length; i += 2) {
    const match = /^:([0-7]{6}) ([0-7]{6}) ([a-f0-9]{40}) ([a-f0-9]{40}) ([AMDT])$/.exec(fields[i]);
    const filename = fields[i + 1];
    if (!match || !filename || seen.has(filename))
      throw Error("Invalid or duplicate raw diff record.");
    seen.add(filename);
    files.push({
      filename,
      status: match[5],
      oldMode: match[1],
      newMode: match[2],
      oldBlob: match[3],
      newBlob: match[4],
    });
  }
  return files;
}
function parseMerge(text, status) {
  if (status !== 0 && status !== 1) throw Error("Merge computation failed.");
  const fields = nulFields(text);
  const tree = assertSha(fields.shift());
  if (status === 0) {
    if (fields.length) throw Error("Unexpected clean-merge output.");
    return { clean: true, tree, conflicts: [] };
  }
  if (fields.length === 0 || fields.some((field) => !field)) {
    throw Error("Missing or empty conflict paths.");
  }
  const conflicts = fields;
  if (new Set(conflicts).size !== conflicts.length) throw Error("Duplicate conflict paths.");
  return { clean: false, tree, conflicts };
}
function sensitive(filename) {
  return (
    /^(README\.md|CONTRIBUTING\.md|AGENTS\.md|LICENSE|NOTICE)$/.test(filename) ||
    filename.startsWith(".github/") ||
    filename.startsWith("integrations/dsh-opent3code/") ||
    filename.includes("opent3code") ||
    filename.includes("ExternalAcp") ||
    filename.endsWith("provider/builtInDrivers.ts") ||
    filename.endsWith("providerDriverMeta.ts")
  );
}
function analyze(repo, base, head) {
  assertSha(base);
  assertSha(head);
  if (git(repo, ["rev-parse", "--is-bare-repository"]).text.trim() !== "true") {
    throw Error("Use a disposable bare repository, never a working checkout.");
  }
  for (const sha of [base, head]) {
    if (git(repo, ["rev-parse", `${sha}^{commit}`]).text.trim() !== sha)
      throw Error("Commit identity mismatch.");
  }
  const ancestors = git(repo, ["merge-base", "--all", base, head]).text.trim().split("\n");
  if (ancestors.length !== 1) throw Error("Multiple merge bases require separate review.");
  const ancestor = assertSha(ancestors[0]);
  const diff = (from, to) =>
    parseDiff(
      git(repo, [
        "diff",
        "--raw",
        "--no-abbrev",
        "--no-renames",
        "--no-ext-diff",
        "--no-textconv",
        "-z",
        from,
        to,
        "--",
      ]).text,
    );
  const upstreamFiles = diff(ancestor, head);
  const forkFiles = diff(ancestor, base);
  const upstreamNames = new Set(upstreamFiles.map((file) => file.filename));
  const overlap = forkFiles
    .filter((file) => upstreamNames.has(file.filename))
    .map((file) => file.filename);
  const output = git(
    repo,
    ["merge-tree", "--write-tree", "--name-only", "--no-messages", "-z", base, head],
    [0, 1],
  );
  const merge = parseMerge(output.text, output.status);
  const groups = Object.create(null);
  for (const file of upstreamFiles) {
    const group = file.filename.split("/").slice(0, 2).join("/");
    groups[group] = (groups[group] || 0) + 1;
  }
  return {
    format: 1,
    diagnosticOnly: true,
    validationSucceeded: false,
    mergeAuthorized: false,
    base,
    head,
    mergeBase: ancestor,
    baseTree: git(repo, ["rev-parse", `${base}^{tree}`]).text.trim(),
    headTree: git(repo, ["rev-parse", `${head}^{tree}`]).text.trim(),
    upstreamCommitCount: Number(git(repo, ["rev-list", "--count", `${base}..${head}`]).text.trim()),
    forkCommitCount: Number(git(repo, ["rev-list", "--count", `${head}..${base}`]).text.trim()),
    renameDetection: false,
    upstreamFiles,
    forkFiles,
    overlap,
    sensitiveUpstreamFiles: upstreamFiles
      .filter((file) => sensitive(file.filename))
      .map((file) => file.filename),
    upstreamPathGroups: groups,
    mergeComputation: merge,
    // A conflicted merge-tree tree contains conflict markers. It is NOT a candidate commit.
    note: "Git object evidence only. No application tests, candidate commit, check approval, merge or publication.",
  };
}
function summary(report) {
  const escape = (value) =>
    String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  return [
    "# OpenT3Code synchronization diagnostics",
    "",
    "**Diagnostic evidence only — not candidate validation or merge approval.**",
    "",
    `Fork: ${report.base}`,
    `Upstream: ${report.head}`,
    `Merge base: ${report.mergeBase}`,
    "",
    `Upstream commits: ${report.upstreamCommitCount}; fork-only commits: ${report.forkCommitCount}.`,
    `Complete upstream changed paths: ${report.upstreamFiles.length}; fork changed paths: ${report.forkFiles.length}.`,
    "Counts use Git paths without rename heuristics; a rename is an addition and a deletion, unlike the GitHub UI.",
    `Overlapping paths: ${report.overlap.length}; conflicted paths: ${report.mergeComputation.conflicts.length}.`,
    "",
    "## Conflicts",
    `<pre>${escape(JSON.stringify(report.mergeComputation.conflicts, null, 2))}</pre>`,
    "",
    "## Largest upstream path groups",
    `<pre>${escape(
      JSON.stringify(
        Object.entries(report.upstreamPathGroups)
          .sort((a, b) => b[1] - a[1])
          .slice(0, 20),
        null,
        2,
      ),
    )}</pre>`,
    "",
    "## Fork/upstream overlap",
    `<pre>${escape(JSON.stringify(report.overlap, null, 2))}</pre>`,
    "",
    "Full path, mode and blob identities are in manifest.json. Sensitive paths still require review.",
    "Existing API limits, PR Size failures, approval requirements and branch protection remain authoritative.",
    "",
  ].join("\n");
}
function main() {
  const base = assertSha(process.env.AUDIT_BASE);
  const head = assertSha(process.env.AUDIT_HEAD);
  const root = process.env.AUDIT_ROOT;
  if (!root || !path.isAbsolute(root) || fs.existsSync(root) || process.argv.length !== 2)
    throw Error("AUDIT_ROOT must be a new absolute directory; positional arguments are not accepted.");
  fs.mkdirSync(root, { mode: 0o700 });
  const repo = path.join(root, "objects.git");
  fs.mkdirSync(repo);
  git(repo, ["init", "--bare", "--quiet"]);
  // Fetch public objects only. No credentials, checkout, submodules, dependencies or hooks.
  git(repo, [
    "fetch",
    "--quiet",
    "--no-tags",
    "--no-recurse-submodules",
    "https://github.com/Hylouis233/opent3code.git",
    base,
  ]);
  git(repo, [
    "fetch",
    "--quiet",
    "--no-tags",
    "--no-recurse-submodules",
    "https://github.com/pingdotgg/t3code.git",
    head,
  ]);
  const report = analyze(repo, base, head);
  report.auditToolSha = assertSha(process.env.AUDIT_TOOL_SHA);
  const output = path.join(root, "report");
  fs.mkdirSync(output);
  fs.writeFileSync(path.join(output, "manifest.json"), JSON.stringify(report, null, 2) + "\n", {
    flag: "wx",
  });
  const text = summary(report);
  fs.writeFileSync(path.join(output, "summary.md"), text, { flag: "wx" });
  if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, text);
  console.log(text);
}
module.exports = {
  assertSha,
  decodeGitOutput,
  nulFields,
  parseDiff,
  parseMerge,
  sensitive,
  analyze,
  summary,
};
if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
