"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const {
  alreadyContainsUpstream,
  isUpstreamPr,
  reviewRequired,
  outstandingChangeRequest,
  assertMergeable,
  prepare,
} = require("./opent3code-sync.cjs");
const head = "a".repeat(40),
  base = "b".repeat(40),
  merge = "c".repeat(40);
const expected = { head, base, merge };
const candidate = () => ({
  state: "open",
  draft: false,
  merged: false,
  mergeable: true,
  head: { sha: head },
  base: { sha: base },
  merge_commit_sha: merge,
});

test("upstream ancestry is interpreted in the correct direction", () => {
  assert.equal(alreadyContainsUpstream("ahead"), true);
  assert.equal(alreadyContainsUpstream("identical"), true);
  for (const state of ["behind", "diverged", "unknown"])
    assert.equal(alreadyContainsUpstream(state), false);
});
test("application-only changes are eligible for validation, not automatically trusted", () => {
  assert.equal(reviewRequired([{ filename: "apps/server/src/provider/Example.ts" }]), false);
});
test("branding, policy, automation and renamed protected paths require review", () => {
  for (const filename of [
    "README.md",
    "LICENSE",
    "AGENTS.md",
    ".github/workflows/ci.yml",
    ".github/actions/example/action.yml",
    ".github/scripts/opent3code-sync.cjs",
    "docs/assets/opent3code-hero.svg",
    "docs/operations/opent3code.md",
  ]) {
    assert.equal(reviewRequired([{ filename }]), true, filename);
    assert.equal(
      reviewRequired([{ filename: "moved.txt", previous_filename: filename }]),
      true,
      filename,
    );
  }
});
test("missing or truncated comparison fails closed", () => {
  assert.equal(reviewRequired(undefined), true);
  assert.equal(
    reviewRequired(Array.from({ length: 300 }, (_, i) => ({ filename: `apps/${i}.ts` }))),
    true,
  );
});
test("exact validated candidate can proceed to API and protection checks", () => {
  assert.doesNotThrow(() => assertMergeable(expected, candidate(), base, true));
});
test("head movement, base movement and regenerated merge require revalidation", () => {
  for (const field of ["head", "base"]) {
    const pr = candidate();
    pr[field].sha = "d".repeat(40);
    assert.throws(() => assertMergeable(expected, pr, base, true), /moved/);
  }
  const pr = candidate();
  pr.merge_commit_sha = "d".repeat(40);
  assert.throws(() => assertMergeable(expected, pr, base, true), /moved/);
  assert.throws(() => assertMergeable(expected, candidate(), "d".repeat(40), true), /moved/);
});
test("failed, missing, draft, closed and conflicting candidates are rejected", () => {
  assert.throws(() => assertMergeable(expected, candidate(), base, false), /validation/);
  assert.throws(() => assertMergeable({ ...expected, head: "" }, candidate(), base, true), /SHA/);
  for (const patch of [
    { state: "closed" },
    { draft: true },
    { merged: true },
    { mergeable: false },
    { mergeable: null },
  ]) {
    assert.throws(
      () => assertMergeable(expected, { ...candidate(), ...patch }, base, true),
      /candidate/,
    );
  }
});
test("comments do not erase change requests; approval or dismissal does", () => {
  const review = (login, state) => ({ user: { login }, state });
  assert.equal(
    outstandingChangeRequest([review("a", "CHANGES_REQUESTED"), review("a", "COMMENTED")]),
    true,
  );
  assert.equal(
    outstandingChangeRequest([review("a", "CHANGES_REQUESTED"), review("a", "APPROVED")]),
    false,
  );
  assert.equal(
    outstandingChangeRequest([review("a", "CHANGES_REQUESTED"), review("a", "DISMISSED")]),
    false,
  );
  assert.equal(
    outstandingChangeRequest([review("a", "CHANGES_REQUESTED"), review("b", "APPROVED")]),
    true,
  );
});
test("prepare does not write when the fork already contains upstream", async () => {
  const summary = {
    addRaw() {
      return this;
    },
    async write() {},
  };
  const github = {
    rest: {
      repos: {
        get: async () => ({
          data: { id: 1341460159, owner: { login: "Hylouis233" }, default_branch: "main" },
        }),
        getBranch: async (args) => ({
          data: { commit: { sha: args.owner === "pingdotgg" ? head : base } },
        }),
        compareCommits: async (args) => {
          assert.equal(args.base, head);
          assert.equal(args.head, base);
          return { data: { status: "ahead" } };
        },
      },
    },
  };
  await prepare({
    github,
    context: { repo: { owner: "Hylouis233", repo: "opent3code" } },
    core: { summary },
  });
});
test("prepare refuses a different repository even with the same name", async () => {
  const github = {
    rest: {
      repos: {
        get: async () => ({
          data: { id: 1, owner: { login: "Hylouis233" }, default_branch: "main" },
        }),
      },
    },
  };
  await assert.rejects(
    prepare({ github, context: { repo: { owner: "Hylouis233", repo: "opent3code" } }, core: {} }),
    /authorized fork/,
  );
});

test("upstream PR identity requires the exact repository and branch", () => {
  const pr = {
    head: { repo: { id: 1153130349, full_name: "pingdotgg/t3code" }, ref: "main" },
  };
  assert.equal(isUpstreamPr(pr), true);
  pr.head.repo.id = 1;
  assert.equal(isUpstreamPr(pr), false);
  pr.head.repo.id = 1153130349;
  pr.head.ref = "unreviewed-feature";
  assert.equal(isUpstreamPr(pr), false);
});

test("prepare creates a cross-repository PR without writing Git refs", async () => {
  const outputs = {};
  const summary = {
    addRaw() {
      return this;
    },
    async write() {},
  };
  const pr = {
    ...candidate(),
    number: 1,
    html_url: "https://github.com/Hylouis233/opent3code/pull/1",
    head: { sha: head, ref: "main", repo: { id: 1153130349, full_name: "pingdotgg/t3code" } },
    base: { sha: base, ref: "main" },
  };
  const github = {
    paginate: async () => [],
    rest: {
      repos: {
        get: async () => ({
          data: { id: 1341460159, owner: { login: "Hylouis233" }, default_branch: "main" },
        }),
        getBranch: async (args) => ({
          data: { commit: { sha: args.owner === "pingdotgg" ? head : base } },
        }),
        compareCommits: async (args) => ({
          data:
            args.base === head ? { status: "diverged" } : { files: [{ filename: "apps/a.ts" }] },
        }),
      },
      pulls: {
        list() {},
        create: async (args) => {
          assert.equal(args.head, "pingdotgg:main");
          assert.equal(args.base, "main");
          assert.equal(args.maintainer_can_modify, false);
          return { data: pr };
        },
        get: async () => ({ data: pr }),
      },
    },
  };
  await prepare({
    github,
    context: { repo: { owner: "Hylouis233", repo: "opent3code" } },
    core: {
      summary,
      setOutput(name, value) {
        outputs[name] = value;
      },
    },
  });
  assert.equal(outputs.head, head);
  assert.equal(outputs.base, base);
  assert.equal(outputs.merge, merge);
  assert.equal(outputs.auto_merge, "true");
});
