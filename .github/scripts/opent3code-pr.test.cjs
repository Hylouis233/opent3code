const assert = require("node:assert/strict");
const { test } = require("node:test");
const {
  classifySize,
  classifySizeWithCoverage,
  classifyTrust,
  validatePolicy,
  syncLabel,
  labelPullRequests,
} = require("./opent3code-pr.cjs");

for (const [lines, expected] of [
  [0, "XS"],
  [9, "XS"],
  [10, "S"],
  [29, "S"],
  [30, "M"],
  [99, "M"],
  [100, "L"],
  [499, "L"],
  [500, "XL"],
  [999, "XL"],
  [1000, "XXL"],
]) {
  test(`upstream size boundary ${lines}`, () => {
    assert.equal(
      classifySize([{ filename: "src/a.ts", additions: lines, deletions: 0 }]).label,
      `size:${expected}`,
    );
  });
}

test("mixed PR excludes tests but test-only PR still counts them", () => {
  const tests = { filename: "src/a.test.ts", additions: 500, deletions: 500 };
  assert.equal(classifySize([tests]).label, "size:XXL");
  assert.equal(
    classifySize([tests, { filename: "src/a.ts", additions: 10, deletions: 0 }]).label,
    "size:S",
  );
});
test("renaming production code into tests is not a free exclusion", () => {
  assert.equal(
    classifySize([
      { filename: "tests/a.ts", previous_filename: "src/a.ts", additions: 100, deletions: 0 },
    ]).nonTest,
    100,
  );
});
test("invalid diff data fails closed", () => {
  assert.throws(() => classifySize([{ filename: "a", additions: -1, deletions: 0 }]));
  assert.throws(() => classifySize([{ filename: "a", additions: "9", deletions: 0 }]));
});
const policy = { trusted: ["Hylouis233"], denounced: ["blocked"] };
test("local policy is case insensitive and explicit denouncement wins", () => {
  assert.equal(classifyTrust("hylouis233", "none", policy), "vouch:trusted");
  assert.equal(classifyTrust("blocked", "admin", policy), "vouch:denounced");
});
test("write collaborators are trusted; bots and upstream names are not implicitly trusted", () => {
  assert.equal(classifyTrust("teammate", "write", policy), "vouch:trusted");
  for (const login of ["upstream-maintainer", "arbitrary[bot]"]) {
    assert.equal(classifyTrust(login, "read", policy), "vouch:unvouched");
  }
});
test("malformed trusted policy is rejected", () => {
  assert.throws(() => validatePolicy({ trusted: "*", denounced: [] }));
  assert.throws(() => validatePolicy({ trusted: ["any\nuser"], denounced: [] }));
  assert.equal(validatePolicy(policy), policy);
});
test("a stale PR head cannot update labels", async () => {
  const calls = [];
  await syncLabel({
    github: {
      rest: {
        pulls: { get: async () => ({ data: { state: "open", head: { sha: "new" } } }) },
        issues: {
          addLabels: async () => calls.push("add"),
          removeLabel: async () => calls.push("remove"),
        },
      },
    },
    repo: { owner: "owner", repo: "repo" },
    pull: { number: 1, head: { sha: "old" } },
    label: "size:S",
    managed: ["size:S", "size:M"],
    core: { info() {} },
  });
  assert.deepEqual(calls, []);
});
test("only managed stale labels are removed", async () => {
  const calls = [];
  await syncLabel({
    github: {
      rest: {
        pulls: {
          get: async () => ({
            data: {
              state: "open",
              head: { sha: "same" },
              labels: [{ name: "bug" }, { name: "size:M" }],
            },
          }),
        },
        issues: {
          addLabels: async (arg) => calls.push(arg.labels),
          removeLabel: async (arg) => calls.push(arg.name),
        },
      },
    },
    repo: { owner: "owner", repo: "repo" },
    pull: { number: 1, head: { sha: "same" } },
    label: "size:S",
    managed: ["size:S", "size:M"],
    core: { info() {} },
  });
  assert.deepEqual(calls, ["size:M", ["size:S"]]);
});

function cappedFiles(nonTest = 0) {
  const files = Array.from({ length: 3000 }, (_, index) => ({
    filename: `tests/${index}.ts`,
    additions: 1,
    deletions: 0,
  }));
  if (nonTest) files[0] = { filename: "src/a.ts", additions: nonTest, deletions: 0 };
  return files;
}

test("complete lists retain exact size semantics, including exactly 3000 files", () => {
  for (const files of [
    [],
    [{ filename: "src/a.ts", additions: 10, deletions: 0 }],
    cappedFiles(),
  ]) {
    assert.deepEqual(classifySizeWithCoverage(files, files.length), {
      ...classifySize(files),
      incomplete: false,
    });
  }
});

test("the API cap permits XXL only when observed non-test changes prove it", () => {
  for (const changedFiles of [3001, 5068]) {
    const result = classifySizeWithCoverage(cappedFiles(1000), changedFiles);
    assert.equal(result.label, "size:XXL");
    assert.equal(result.nonTest, 1000);
    assert.equal(result.incomplete, true);
    assert.equal(result.effective, null);
  }
});

test("a large tests-only partial list is unknown, not a proven XXL", () => {
  const files = cappedFiles();
  assert.equal(classifySize(files).label, "size:XXL");
  assert.equal(classifySizeWithCoverage(files, 5068).label, "size:unknown");
  assert.equal(
    classifySize([...files, { filename: "src/new.ts", additions: 1, deletions: 0 }]).label,
    "size:XS",
  );
});

test("999 observed non-test lines cannot determine a capped PR size", () => {
  const result = classifySizeWithCoverage(cappedFiles(999), 5068);
  assert.equal(result.label, "size:unknown");
  assert.equal(result.effective, null);
});

test("incomplete pagination below the documented cap still fails closed", () => {
  for (const [returned, expected] of [
    [0, 1],
    [99, 100],
    [2999, 3000],
    [2999, 5068],
  ]) {
    assert.throws(
      () => classifySizeWithCoverage(cappedFiles().slice(0, returned), expected),
      /Incomplete file list/,
    );
  }
});

test("invalid changed-file metadata and diff counts remain errors", () => {
  for (const count of [-1, 0.5, "3000", undefined, NaN, Infinity]) {
    assert.throws(() => classifySizeWithCoverage([], count), /Invalid changed-file count/);
  }
  assert.throws(() => classifySizeWithCoverage(cappedFiles(), 2999), /Invalid changed-file count/);
  const files = cappedFiles();
  files[0].additions = -1;
  assert.throws(() => classifySizeWithCoverage(files, 5068), /Invalid diff statistics/);
});

function labelHarness(files, changedFiles, existingLabel = "size:S", currentSha = "same") {
  const calls = { added: [], removed: [], info: [], warnings: [], definitions: [] };
  const pull = {
    number: 4,
    state: "open",
    head: { sha: "same" },
    changed_files: changedFiles,
    labels: [{ name: existingLabel }, { name: "bug" }],
  };
  let reads = 0;
  const listFiles = () => {};
  const github = {
    paginate: async (method, options) => {
      assert.equal(method, listFiles);
      assert.equal(options.pull_number, 4);
      assert.equal(options.per_page, 100);
      return files;
    },
    rest: {
      pulls: {
        listFiles,
        get: async () => ({
          data: ++reads === 1 ? pull : { ...pull, head: { sha: currentSha } },
        }),
      },
      issues: {
        getLabel: async () => ({ data: { color: "", description: "" } }),
        updateLabel: async (arg) => calls.definitions.push(arg.name),
        addLabels: async (arg) => calls.added.push(...arg.labels),
        removeLabel: async (arg) => calls.removed.push(arg.name),
      },
    },
  };
  const args = {
    github,
    context: {
      repo: { owner: "owner", repo: "repo" },
      eventName: "pull_request_target",
      payload: { pull_request: pull },
    },
    core: {
      info: (message) => calls.info.push(message),
      warning: (message) => calls.warnings.push(message),
    },
    kind: "size",
  };
  return { calls, args };
}

test("size labeling handles 5068 files and reports the non-test lower bound", async () => {
  const { calls, args } = labelHarness(cappedFiles(1000), 5068);
  await labelPullRequests(args);
  assert.deepEqual(calls.added, ["size:XXL"]);
  assert.deepEqual(calls.removed, ["size:S"]);
  assert.match(calls.warnings[0], /3000\/5068.*lower bound 1000/);
  assert.ok(!calls.info.some((line) => line.includes("effective")));
});

test("unknown coverage replaces a stale size label without removing unrelated labels", async () => {
  const { calls, args } = labelHarness(cappedFiles(), 5068, "size:XXL");
  await labelPullRequests(args);
  assert.ok(calls.definitions.includes("size:unknown"));
  assert.deepEqual(calls.added, ["size:unknown"]);
  assert.deepEqual(calls.removed, ["size:XXL"]);
  assert.equal(calls.warnings.length, 1);
});

test("a complete list removes size:unknown and restores exact totals", async () => {
  const files = [{ filename: "src/a.ts", additions: 10, deletions: 0 }];
  const { calls, args } = labelHarness(files, 1, "size:unknown");
  await labelPullRequests(args);
  assert.deepEqual(calls.added, ["size:S"]);
  assert.deepEqual(calls.removed, ["size:unknown"]);
  assert.deepEqual(calls.warnings, []);
  assert.ok(calls.info.some((line) => line.includes("10 effective")));
});

test("capped results still cannot update a stale PR head", async () => {
  const { calls, args } = labelHarness(cappedFiles(1000), 5068, "size:S", "new");
  await labelPullRequests(args);
  assert.deepEqual(calls.added, []);
  assert.deepEqual(calls.removed, []);
});

test("unexpected truncation fails before changing PR labels", async () => {
  const { calls, args } = labelHarness(cappedFiles().slice(0, 100), 5068);
  await assert.rejects(labelPullRequests(args), /Incomplete file list/);
  assert.deepEqual(calls.added, []);
  assert.deepEqual(calls.removed, []);
});

test("file-list API failures propagate instead of creating an unknown label", async () => {
  const { calls, args } = labelHarness([], 5068);
  args.github.paginate = async () => {
    throw new Error("API unavailable");
  };
  await assert.rejects(labelPullRequests(args), /API unavailable/);
  assert.deepEqual(calls.added, []);
  assert.deepEqual(calls.removed, []);
});
