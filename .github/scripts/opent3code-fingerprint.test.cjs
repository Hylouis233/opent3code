const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const vm = require("node:vm");
const { execFileSync, spawnSync } = require("node:child_process");

const workflow = fs.readFileSync(
  path.join(__dirname, "../workflows/mobile-fingerprint-check.yml"),
  "utf8",
);
const merge = "a".repeat(40);
const base = "b".repeat(40);
const head = "c".repeat(40);
const fullName = "Hylouis233/opent3code";

// Exercise the actual checked-in shell and API-only publisher, not copies.
function block(source, key, spaces) {
  const marker = " ".repeat(spaces) + key + ": |\n";
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `Missing workflow block: ${key}`);
  const lines = source.slice(start + marker.length).split("\n");
  const prefix = " ".repeat(spaces + 2);
  const result = [];
  for (const line of lines) {
    if (line && !line.startsWith(prefix)) break;
    result.push(line.slice(prefix.length));
  }
  return result.join("\n");
}
const detection = block(workflow.slice(workflow.indexOf("      - id: changes\n")), "run", 8);
const publisher = block(workflow.slice(workflow.indexOf("  label:\n")), "script", 10);

test("fingerprinting keeps PR code read-only and the publisher API-only", () => {
  const [calculation, label] = workflow.split("  label:\n");
  assert.match(calculation, /permissions:\n  contents: read/);
  assert.doesNotMatch(calculation, /(?:issues|pull-requests|contents): write/);
  assert.doesNotMatch(workflow, /pull_request_target/);
  assert.doesNotMatch(calculation, /secrets\./);
  assert.match(label, /needs: fingerprint/);
  assert.match(label, /if: github.event.pull_request.head.repo.full_name == github.repository/);
  assert.doesNotMatch(label, /uses: (?:actions\/checkout|voidzero-dev\/setup-vp)/);
  assert.doesNotMatch(label, /^\s+run: /m);
  assert.doesNotMatch(label, /require\(|import\(/);
});
test("event coverage clears stale labels without installing irrelevant dependencies", () => {
  const events = workflow.slice(workflow.indexOf("on:\n"), workflow.indexOf("permissions:\n"));
  assert.match(events, /types: \[opened, synchronize, reopened, edited\]/);
  assert.doesNotMatch(events, /paths:/);
  for (const name of [
    "Setup Vite+",
    "Expose pnpm",
    "Fingerprint merge result",
    "Checkout fingerprint base",
    "Setup Vite+ for base",
    "Expose base pnpm",
    "Fingerprint base",
    "Upload fingerprint evidence",
  ]) {
    assert.ok(
      workflow.includes(`name: ${name}\n        if: steps.changes.outputs.relevant == 'true'`),
    );
  }
  assert.doesNotMatch(workflow, /github.event.pull_request.base.sha/);
  assert.match(workflow, /--depth=2/);
  assert.match(workflow, /BASE_SHA: \$\{\{ steps.changes.outputs.base_sha \}\}/);
  assert.match(workflow, /jq -er/);
});

function fixture(native) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "opent3code-fingerprint-test-"));
  const env = {
    ...process.env,
    GIT_AUTHOR_NAME: "Fingerprint fixture",
    GIT_AUTHOR_EMAIL: "fixture@example.invalid",
    GIT_COMMITTER_NAME: "Fingerprint fixture",
    GIT_COMMITTER_EMAIL: "fixture@example.invalid",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: os.devNull,
    GIT_CONFIG_COUNT: "0",
  };
  const git = (...args) => execFileSync("git", args, { cwd: root, env, encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  fs.writeFileSync(path.join(root, "package.json"), "{}\n");
  git("add", ".");
  git("commit", "-qm", "base");
  const oldBase = git("rev-parse", "HEAD");
  git("checkout", "-qb", "feature");
  fs.writeFileSync(path.join(root, native ? "package.json" : "notes.md"), "changed\n");
  git("add", ".");
  git("commit", "-qm", "feature");
  const feature = git("rev-parse", "HEAD");
  git("checkout", "-q", "main");
  fs.writeFileSync(path.join(root, "base-moved.md"), "base advanced\n");
  git("add", ".");
  git("commit", "-qm", "advance base");
  const parent = git("rev-parse", "HEAD");
  git("merge", "--no-ff", "--quiet", "feature", "-m", "candidate");
  return { root, env, git, oldBase, feature, parent, candidate: git("rev-parse", "HEAD") };
}
function detect(f, overrides = {}) {
  const output = path.join(f.root, "workflow-output");
  fs.writeFileSync(output, "");
  const result = spawnSync("bash", ["-c", detection], {
    cwd: f.root,
    env: {
      ...f.env,
      EXPECTED_MERGE: f.candidate,
      EXPECTED_HEAD: f.feature,
      BASE_SHA: f.oldBase,
      GITHUB_OUTPUT: output,
      GITHUB_STEP_SUMMARY: path.join(f.root, "summary"),
      ...overrides,
    },
    encoding: "utf8",
  });
  return { ...result, output: fs.readFileSync(output, "utf8") };
}
for (const native of [true, false]) {
  test(`real merge uses its current first parent and detects relevant=${native}`, () => {
    const f = fixture(native);
    try {
      const result = detect(f);
      assert.equal(result.status, 0, result.stderr);
      assert.match(result.output, new RegExp(`base_sha=${f.parent}`));
      assert.match(result.output, new RegExp(`merge_sha=${f.candidate}`));
      assert.match(result.output, new RegExp(`relevant=${native}`));
      assert.notEqual(f.parent, f.oldBase);
      assert.equal(f.git("rev-parse", "HEAD"), f.candidate);
    } finally {
      fs.rmSync(f.root, { recursive: true, force: true });
    }
  });
}
test("wrong head, wrong candidate and non-merge commits fail before relevance output", () => {
  const f = fixture(true);
  try {
    for (const overrides of [{ EXPECTED_HEAD: head }, { EXPECTED_MERGE: merge }]) {
      const result = detect(f, overrides);
      assert.notEqual(result.status, 0);
      assert.doesNotMatch(result.output, /relevant=/);
    }
    f.git("checkout", "--quiet", "--detach", f.feature);
    const result = detect(f, { EXPECTED_MERGE: f.feature });
    assert.notEqual(result.status, 0);
    assert.doesNotMatch(result.output, /relevant=/);
  } finally {
    fs.rmSync(f.root, { recursive: true, force: true });
  }
});

async function publish(options = {}) {
  const writes = [];
  let reads = 0;
  const pull = {
    state: "open",
    mergeable: true,
    merge_commit_sha: merge,
    head: { sha: head, repo: { full_name: fullName } },
    base: { ref: "main", repo: { full_name: fullName } },
    labels: options.hasLabel ? [{ name: "📱 Native Change" }] : [],
  };
  const context = {
    sha: merge,
    repo: { owner: "Hylouis233", repo: "opent3code" },
    payload: { pull_request: { number: 7, head: { sha: head }, base: { ref: "main" } } },
  };
  const github = {
    rest: {
      pulls: {
        get: async () => {
          reads += 1;
          options.reads?.push(reads);
          const current = structuredClone(pull);
          if (options.changePull) options.changePull(current, reads);
          return { data: current };
        },
      },
      repos: {
        getBranch: async () => ({ data: { commit: { sha: options.branchSha?.(reads) ?? base } } }),
      },
      issues: {
        getLabel: async () => ({ data: { name: "📱 Native Change" } }),
        createLabel: async (value) => writes.push(["create", value]),
        addLabels: async (value) => writes.push(["add", value]),
        removeLabel: async (value) => writes.push(["remove", value]),
      },
    },
  };
  const env = {
    CHANGED_PLATFORMS: "ios",
    FINGERPRINT_RELEVANT: "true",
    CHECKED_BASE: base,
    CHECKED_MERGE: merge,
    ...options.env,
  };
  await vm.runInNewContext(`(async () => {\n${publisher}\n})()`, {
    github,
    context,
    core: { notice() {} },
    process: { env },
    setTimeout(callback, milliseconds) {
      options.delays?.push(milliseconds);
      callback();
    },
  });
  return writes;
}
test("a current checked candidate can add an advisory native label", async () => {
  const writes = await publish();
  assert.equal(writes.length, 1);
  assert.equal(writes[0][0], "add");
});
test("a current irrelevant candidate clears a stale label without Expo output", async () => {
  const writes = await publish({
    hasLabel: true,
    env: { FINGERPRINT_RELEVANT: "false", CHANGED_PLATFORMS: "" },
  });
  assert.equal(writes.length, 1);
  assert.equal(writes[0][0], "remove");
});
for (const reason of ["head", "merge", "closed", "base", "external", "unknown"]) {
  test(`stale or untrusted ${reason} evidence cannot write labels`, async () => {
    const writes = await publish({
      changePull(pull) {
        if (reason === "head") pull.head.sha = "d".repeat(40);
        if (reason === "merge") pull.merge_commit_sha = "d".repeat(40);
        if (reason === "closed") pull.state = "closed";
        if (reason === "base") pull.base.ref = "other";
        if (reason === "external") pull.head.repo.full_name = "other/fork";
        if (reason === "unknown") pull.mergeable = null;
      },
    });
    assert.equal(writes.length, 0);
  });
}
test("base advancement before or during publication leaves labels untouched", async () => {
  for (const when of [1, 2]) {
    const writes = await publish({ branchSha: (reads) => (reads >= when ? "d".repeat(40) : base) });
    assert.equal(writes.length, 0);
  }
});
test("candidate regeneration during publication leaves labels untouched", async () => {
  const writes = await publish({
    changePull(pull, reads) {
      if (reads === 2) pull.merge_commit_sha = "d".repeat(40);
    },
  });
  assert.equal(writes.length, 0);
});
test("missing and malformed outputs never become unchanged fingerprint evidence", async () => {
  for (const env of [
    { CHECKED_BASE: "" },
    { CHECKED_MERGE: "" },
    { CHECKED_MERGE: "d".repeat(40) },
    { FINGERPRINT_RELEVANT: "" },
    { FINGERPRINT_RELEVANT: "false", CHANGED_PLATFORMS: "ios" },
    { CHANGED_PLATFORMS: "ios\nandroid" },
  ])
    await assert.rejects(() => publish({ env }), /Invalid fingerprint output/);
});

test("indeterminate mergeability is retried before publishing a current result", async () => {
  const reads = [];
  const delays = [];
  const writes = await publish({
    reads,
    delays,
    changePull(pull, count) {
      if (count < 3) {
        pull.mergeable = null;
        pull.merge_commit_sha = null;
      }
    },
  });
  assert.equal(writes.length, 1);
  assert.equal(writes[0][0], "add");
  assert.deepEqual(delays, [3000, 3000]);
  assert.deepEqual(reads, [1, 2, 3, 4]);
});
test("exhausted unknown mergeability cannot clear a stale label", async () => {
  const reads = [];
  const delays = [];
  const writes = await publish({
    reads,
    delays,
    hasLabel: true,
    env: { FINGERPRINT_RELEVANT: "false", CHANGED_PLATFORMS: "" },
    changePull(pull) {
      pull.mergeable = null;
      pull.merge_commit_sha = null;
    },
  });
  assert.equal(writes.length, 0);
  assert.equal(reads.length, 8);
  assert.deepEqual(delays, Array(7).fill(3000));
});
test("a head change during mergeability retries stops publication", async () => {
  const delays = [];
  const writes = await publish({
    delays,
    changePull(pull, count) {
      if (count === 1) pull.mergeable = null;
      else pull.head.sha = "d".repeat(40);
    },
  });
  assert.equal(writes.length, 0);
  assert.deepEqual(delays, [3000]);
});
test("the final publication recheck also waits for determinate mergeability", async () => {
  const delays = [];
  const writes = await publish({
    delays,
    changePull(pull, count) {
      if (count === 2) pull.mergeable = null;
    },
  });
  assert.equal(writes.length, 1);
  assert.deepEqual(delays, [3000]);
});

test("the base toolchain is selected after checkout and before base fingerprinting", () => {
  const names = [
    "Fingerprint merge result",
    "Checkout fingerprint base",
    "Setup Vite+ for base",
    "Expose base pnpm",
    "Fingerprint base",
  ];
  const positions = names.map((name) => workflow.indexOf(`      - name: ${name}\n`));
  assert.ok(
    positions.every(
      (position, index) => position >= 0 && (!index || position > positions[index - 1]),
    ),
  );
  const setup = workflow.slice(positions[2], positions[3]);
  assert.match(setup, /uses: voidzero-dev\/setup-vp@v1/);
  assert.match(setup, /node-version-file: package.json/);
  assert.match(setup, /cache: false/);
  assert.match(setup, /run-install: \|/);
  assert.match(setup, /--filter=@t3tools\/mobile\.\.\./);
  assert.doesNotMatch(workflow.slice(positions[1], positions[2]), /pnpm install/);
});
test("base pnpm exposure selects its declared version, never the head PATH binary", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "opent3code-pnpm-test-"));
  const baseStep = workflow.slice(workflow.indexOf("      - name: Expose base pnpm\n"));
  const expose = block(baseStep, "run", 8);
  const bin = (version) => path.join(root, ".vite-plus/package_manager/pnpm", version, "pnpm/bin");
  const output = path.join(root, "github-path");
  const manifest = path.join(root, "package.json");
  try {
    for (const version of ["10.24.0", "10.23.0"]) {
      fs.mkdirSync(bin(version), { recursive: true });
      const executable = path.join(bin(version), "pnpm");
      fs.writeFileSync(executable, `#!/bin/sh\necho ${version}\n`, { mode: 0o700 });
    }
    const env = {
      ...process.env,
      HOME: root,
      GITHUB_PATH: output,
      PATH: `${bin("10.24.0")}${path.delimiter}${process.env.PATH}`,
    };
    fs.writeFileSync(manifest, JSON.stringify({ packageManager: "pnpm@10.23.0" }));
    fs.writeFileSync(output, "");
    const result = spawnSync("bash", ["-e", "-c", expose], { cwd: root, env, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout.trim(), "10.23.0");
    assert.equal(fs.readFileSync(output, "utf8").trim(), bin("10.23.0"));
    const next = execFileSync("pnpm", ["--version"], {
      cwd: root,
      env: { ...env, PATH: `${bin("10.23.0")}${path.delimiter}${env.PATH}` },
      encoding: "utf8",
    });
    assert.equal(next.trim(), "10.23.0");
    fs.writeFileSync(manifest, JSON.stringify({ packageManager: "pnpm@10.22.0" }));
    const missing = spawnSync("bash", ["-e", "-c", expose], { cwd: root, env, encoding: "utf8" });
    assert.notEqual(missing.status, 0);
    assert.doesNotMatch(missing.stdout, /10\.24\.0/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
