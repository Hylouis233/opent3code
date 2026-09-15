const assert = require("node:assert/strict");
const { test } = require("node:test");
const { matchingPulls, previewBody } = require("./opent3code-pr-report.cjs");
const sha = "a".repeat(40);
const run = {
  head_sha: sha,
  head_branch: "feature",
  head_repository: { full_name: "contributor/fork" },
};
const pull = {
  state: "open",
  base: { repo: { full_name: "owner/repo" } },
  head: { sha, ref: "feature", repo: { full_name: "contributor/fork" } },
};
test("fork PR resolution requires matching repository, branch, SHA and open state", () => {
  assert.equal(matchingPulls([pull], run, "owner/repo").length, 1);
  assert.equal(matchingPulls([pull], { ...run, head_sha: "b".repeat(40) }, "owner/repo").length, 0);
  assert.equal(matchingPulls([{ ...pull, state: "closed" }], run, "owner/repo").length, 0);
  assert.equal(matchingPulls([pull], run, "other/repo").length, 0);
  assert.equal(matchingPulls([pull], { ...run, head_branch: "other" }, "owner/repo").length, 0);
  assert.equal(
    matchingPulls([pull], { ...run, head_repository: { full_name: "attacker/fork" } }, "owner/repo")
      .length,
    0,
  );
});
test("successful artifacts are clearly unsigned and not release binaries", () => {
  const body = previewBody({
    fullName: "owner/repo",
    sha,
    runId: 1,
    conclusion: "success",
    artifactId: 2,
  });
  assert.match(body, /runs\/1\/artifacts\/2/);
  assert.match(body, /not a signed\/notarized macOS app/);
});
test("missing artifacts and failed runs never claim preview success", () => {
  for (const input of [{ conclusion: "failure", artifactId: 2 }, { conclusion: "success" }]) {
    const body = previewBody({ fullName: "owner/repo", sha, runId: 1, ...input });
    assert.match(body, /No downloadable preview/);
    assert.doesNotMatch(body, /Download unsigned/);
  }
});
test("untrusted identifiers cannot inject URLs or markup into comments", () => {
  for (const input of [
    { fullName: "owner/repo](bad)" },
    { sha: "bad" },
    { runId: -1 },
    { artifactId: "2" },
  ]) {
    assert.throws(() =>
      previewBody({ fullName: "owner/repo", sha, runId: 1, conclusion: "success", ...input }),
    );
  }
});
