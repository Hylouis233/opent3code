const assert = require("node:assert/strict");
const { test } = require("node:test");
const { classifySize, classifyTrust, validatePolicy, syncLabel } = require("./opent3code-pr.cjs");

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
