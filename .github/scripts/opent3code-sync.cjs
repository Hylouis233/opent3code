"use strict";

const REPOSITORY_ID = 1341460159;
const UPSTREAM = { owner: "pingdotgg", repo: "t3code", branch: "main" };
const SHA = /^[a-f0-9]{40}$/;

function alreadyContainsUpstream(status) {
  return status === "ahead" || status === "identical";
}

function protectedPath(path) {
  return /^(README\.md|CONTRIBUTING\.md|AGENTS\.md|LICENSE|NOTICE)$/.test(path) ||
    path.startsWith(".github/workflows/") ||
    path.startsWith(".github/actions/") ||
    path.startsWith(".github/scripts/opent3code-") ||
    path.startsWith("docs/assets/opent3code-") ||
    path === "docs/operations/opent3code.md";
}

function reviewRequired(files) {
  // The comparison API returns at most 300 files. At the boundary, fail closed.
  return !Array.isArray(files) || files.length >= 300 || files.some((file) =>
    protectedPath(file.filename) || (file.previous_filename && protectedPath(file.previous_filename)));
}

function outstandingChangeRequest(reviews) {
  const latest = new Map();
  for (const review of reviews) {
    if (review.user && review.state !== "COMMENTED" && review.state !== "PENDING") {
      latest.set(review.user.login, review.state);
    }
  }
  return [...latest.values()].includes("CHANGES_REQUESTED");
}

function assertMergeable(expected, pr, branchSha, validationSucceeded) {
  for (const value of [expected.head, expected.base, expected.merge]) {
    if (!SHA.test(value || "")) throw new Error("Missing or invalid validated SHA.");
  }
  if (!validationSucceeded) throw new Error("Candidate validation did not succeed.");
  if (pr.state !== "open" || pr.draft || pr.merged || pr.mergeable !== true) {
    throw new Error("Pull request is not an open, non-draft, mergeable candidate.");
  }
  if (pr.head.sha !== expected.head || pr.base.sha !== expected.base ||
      pr.merge_commit_sha !== expected.merge || branchSha !== expected.base) {
    throw new Error("Candidate or base moved after validation; revalidate on the next run.");
  }
}

async function repository(github, context) {
  const { data } = await github.rest.repos.get(context.repo);
  if (data.id !== REPOSITORY_ID || data.owner.login !== "Hylouis233") {
    throw new Error("Refusing to mutate a repository other than the authorized fork.");
  }
  return { ...context.repo, branch: data.default_branch };
}

async function prepare({ github, context, core }) {
  const repo = await repository(github, context);
  const { data: base } = await github.rest.repos.getBranch({ ...repo });
  const { data: upstream } = await github.rest.repos.getBranch(UPSTREAM);
  const head = upstream.commit.sha;
  const baseSha = base.commit.sha;
  const { data: comparison } = await github.rest.repos.compareCommits({
    ...context.repo, base: head, head: baseSha,
  });
  if (alreadyContainsUpstream(comparison.status)) {
    await core.summary.addRaw(`Upstream ${head} is already contained in ${repo.branch}.`).write();
    return;
  }
  const branch = `sync/upstream-${head.slice(0, 12)}`;
  const open = await github.paginate(github.rest.pulls.list, {
    ...context.repo, state: "open", base: repo.branch, per_page: 100,
  });
  const pending = open.find((pr) => pr.head.repo?.id === REPOSITORY_ID &&
    pr.head.ref.startsWith("sync/upstream-") && pr.head.ref !== branch);
  if (pending) {
    core.warning(`Resolve existing sync PR #${pending.number} before importing a newer snapshot.`);
    await core.summary.addRaw(`Existing sync PR needs attention: ${pending.html_url}`).write();
    return;
  }
  const prs = await github.paginate(github.rest.pulls.list, {
    ...context.repo, state: "all", head: `${context.repo.owner}:${branch}`, per_page: 100,
  });
  const existing = prs.find((pr) => pr.head.ref === branch && pr.head.repo?.id === REPOSITORY_ID);
  if (existing && existing.state !== "open") {
    core.warning(`Sync PR #${existing.number} was closed; it will not be reopened automatically.`);
    await core.summary.addRaw(`Previously closed sync PR: ${existing.html_url}`).write();
    return;
  }
  try {
    const { data: ref } = await github.rest.git.getRef({ ...context.repo, ref: `heads/${branch}` });
    if (ref.object.sha !== head) throw new Error("Snapshot branch moved; refusing to overwrite it.");
  } catch (error) {
    if (error.status !== 404) throw error;
    await github.rest.git.createRef({ ...context.repo, ref: `refs/heads/${branch}`, sha: head });
  }
  const pr = existing || (await github.rest.pulls.create({
    ...context.repo, base: repo.branch, head: branch,
    title: `chore: sync upstream ${head.slice(0, 12)}`,
    body: `Import pingdotgg/t3code:main at ${head}.\n\nThis snapshot includes upstream commits and PRs already merged there, not arbitrary open PRs. Fork history is preserved through a merge commit. Conflicts, maintenance-file changes, failed validation, review objections, or repository protection rules block automatic merging.\n\nPrepared by the OpenT3Code upstream sync workflow.`,
  })).data;
  let candidate;
  for (let attempt = 0; attempt < 8; attempt++) {
    candidate = (await github.rest.pulls.get({ ...context.repo, pull_number: pr.number })).data;
    if (candidate.mergeable !== null) break;
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }
  if (candidate.mergeable !== true || !SHA.test(candidate.merge_commit_sha || "") ||
      candidate.head.sha !== head || candidate.base.sha !== baseSha || candidate.base.ref !== repo.branch) {
    core.warning(`PR #${pr.number} needs conflict resolution or a fresh base snapshot.`);
    await core.summary.addRaw(`Review required: ${pr.html_url}`).write();
    return;
  }
  const { data: diff } = await github.rest.repos.compareCommits({
    ...context.repo, base: baseSha, head: candidate.merge_commit_sha,
  });
  const manual = reviewRequired(diff.files);
  for (const [name, value] of Object.entries({
    pr: pr.number, head, base: baseSha, merge: candidate.merge_commit_sha, auto_merge: !manual,
  })) core.setOutput(name, String(value));
  await core.summary.addRaw(`${pr.html_url}\n\nValidated candidate: ${candidate.merge_commit_sha}\n\n${manual ? "Manual review required: protected paths or incomplete comparison." : "Eligible for validation; not merged yet."}`).write();
}

async function merge({ github, context, core, expected }) {
  const repo = await repository(github, context);
  const pull_number = Number(expected.pr);
  if (!Number.isSafeInteger(pull_number) || pull_number < 1) throw new Error("Invalid PR number.");
  const { data: pr } = await github.rest.pulls.get({ ...context.repo, pull_number });
  const { data: branch } = await github.rest.repos.getBranch(repo);
  assertMergeable(expected, pr, branch.commit.sha, expected.validationSucceeded === true);
  if (pr.head.repo?.id !== REPOSITORY_ID || pr.base.ref !== repo.branch ||
      pr.head.ref !== `sync/upstream-${expected.head.slice(0, 12)}`) {
    throw new Error("Unexpected sync PR origin or target.");
  }
  const { data: diff } = await github.rest.repos.compareCommits({
    ...context.repo, base: expected.base, head: expected.merge,
  });
  if (reviewRequired(diff.files)) throw new Error("Protected files or incomplete diff require review.");
  const reviews = await github.paginate(github.rest.pulls.listReviews, { ...context.repo, pull_number, per_page: 100 });
  if (outstandingChangeRequest(reviews)) throw new Error("A reviewer still requests changes.");
  const checks = await github.paginate(github.rest.checks.listForRef, {
    ...context.repo, ref: expected.head, filter: "latest", per_page: 100,
  });
  if (checks.some((check) => check.status !== "completed" ||
      !["success", "skipped", "neutral"].includes(check.conclusion))) {
    throw new Error("An observed head check failed, is pending, or needs approval.");
  }
  const { data: status } = await github.rest.repos.getCombinedStatusForRef({ ...context.repo, ref: expected.head });
  if (status.total_count > 0 && status.state !== "success") throw new Error("Commit status is not successful.");
  // Recheck immediately before the merge API call; branch protection remains authoritative.
  const { data: latest } = await github.rest.repos.getBranch(repo);
  if (latest.commit.sha !== expected.base) throw new Error("Base moved; revalidation required.");
  const { data: result } = await github.rest.pulls.merge({
    ...context.repo, pull_number, sha: expected.head, merge_method: "merge",
    commit_title: `chore: merge upstream ${expected.head.slice(0, 12)}`,
  });
  if (!result.merged) throw new Error(result.message || "GitHub declined the merge.");
  await core.summary.addRaw(`Merged ${pr.html_url}\n\nResult: ${result.sha}`).write();
}

module.exports = { alreadyContainsUpstream, protectedPath, reviewRequired, outstandingChangeRequest, assertMergeable, prepare, merge };
