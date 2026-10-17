// Fail when a tracked source file becomes a placeholder or shrinks by more than 80%.
//
// Usage: node .github/scripts/opent3code-source-integrity.cjs [--placeholder-only] [BASE] [HEAD]
// BASE defaults to HEAD~1 and HEAD defaults to HEAD. Every source file added, modified, or
// renamed between BASE and HEAD is compared; deleted files are ignored. An intentional large
// shrink can be allowed with a commit message trailer in the range:
//   Source-Integrity-Allow: path/to/file.ts
// --placeholder-only skips the shrink rule (CI uses it for upstream sync and integration PRs).
const { execFileSync } = require("node:child_process");

const SOURCE_SUFFIXES = [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py", ".sh", ".ps1", ".rs"];
const PLACEHOLDER = /^(placeholder[\w-]*|todo|tbd|fixme|\.\.\.)$/i;
const MAX_SHRINK = 0.8;
const MIN_OLD_BYTES = 200;
const ALLOW_TRAILER = /^Source-Integrity-Allow:\s*(\S+)\s*$/gm;
const NULL_SHA = "0".repeat(40);

function git(args, cwd, input) {
  return execFileSync("git", args, {
    cwd,
    input,
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
    stdio: ["pipe", "pipe", "ignore"],
  });
}

function check(base, head, { cwd, placeholderOnly = false } = {}) {
  const log = git(["log", "--format=%B", `${base}..${head}`], cwd);
  const allowed = new Set([...log.matchAll(ALLOW_TRAILER)].map((match) => match[1]));
  const pathspecs = SOURCE_SUFFIXES.map((suffix) => `*${suffix}`);
  // In a blobless CI checkout this content diff fetches every needed blob in one request.
  git(["diff", "--numstat", "-M", base, head, "--", ...pathspecs], cwd);
  const changes = [];
  const raw = git(["diff", "--raw", "--no-abbrev", "-M", base, head, "--", ...pathspecs], cwd);
  for (const line of raw.split("\n")) {
    const match = /^:\d+ \d+ ([0-9a-f]+) ([0-9a-f]+) (\S+)\t([^\t]+)(?:\t(.+))?$/.exec(line);
    if (!match || match[3].startsWith("D")) continue;
    const [, oldBlob, newBlob, status, firstPath, secondPath] = match;
    changes.push({
      oldBlob,
      newBlob,
      added: status.startsWith("A"),
      path: secondPath ?? firstPath,
    });
  }
  if (changes.length === 0) return [];
  const blobs = changes.flatMap((c) => (c.added ? [c.newBlob] : [c.oldBlob, c.newBlob]));
  const sizes = new Map(
    git(["cat-file", "--batch-check=%(objectname) %(objectsize)"], cwd, `${blobs.join("\n")}\n`)
      .trim()
      .split("\n")
      .map((row) => row.split(" "))
      .map(([oid, size]) => [oid, Number(size)]),
  );
  const errors = [];
  for (const { oldBlob, newBlob, added, path } of changes) {
    const newSize = sizes.get(newBlob);
    if (newSize <= 64) {
      const content = git(["cat-file", "blob", newBlob], cwd).trim();
      if (PLACEHOLDER.test(content)) {
        errors.push(`${path}: whole file content is the placeholder ${JSON.stringify(content)}`);
        continue;
      }
    }
    if (placeholderOnly || added || allowed.has(path)) continue;
    const oldSize = sizes.get(oldBlob);
    if (oldSize >= MIN_OLD_BYTES && newSize < oldSize * (1 - MAX_SHRINK)) {
      const pct = Math.round((1 - newSize / oldSize) * 100);
      errors.push(`${path}: shrank from ${oldSize} to ${newSize} bytes (${pct}% smaller)`);
    }
  }
  return errors;
}

function main(argv, cwd) {
  const placeholderOnly = argv.includes("--placeholder-only");
  const args = argv.filter((arg) => arg !== "--placeholder-only");
  const base = args[0] && args[0] !== NULL_SHA ? args[0] : "HEAD~1";
  const head = args[1] || "HEAD";
  const errors = check(base, head, { cwd, placeholderOnly });
  const scope = placeholderOnly ? " (placeholder rule only)" : "";
  if (errors.length > 0) {
    console.log(`Source integrity check failed for ${base}..${head}${scope}:`);
    for (const error of errors) console.log(`- ${error}`);
    console.log(
      `Fix: restore the real file content (for example \`git checkout ${base} -- <path>\`). ` +
        "If the shrink is intentional, add the commit trailer `Source-Integrity-Allow: <path>`.",
    );
    return 1;
  }
  console.log(`Source integrity check passed for ${base}..${head}${scope}.`);
  return 0;
}

module.exports = { check, main };

if (require.main === module) process.exitCode = main(process.argv.slice(2));
