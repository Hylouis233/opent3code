const PREVIEW_LABELS = ["preview:mac", "preview:bundle"];
const MARKER = "<!-- opent3code-desktop-preview -->";

function matchingPulls(pulls, run, fullName) {
  return pulls.filter(
    (pull) =>
      pull.state === "open" &&
      pull.base.repo.full_name === fullName &&
      pull.head.sha === run.head_sha &&
      pull.head.ref === run.head_branch &&
      pull.head.repo?.full_name === run.head_repository?.full_name,
  );
}

function previewBody({ fullName, sha, runId, conclusion, artifactId }) {
  if (
    !/^[a-z0-9_.-]+\/[a-z0-9_.-]+$/i.test(fullName) ||
    !/^[a-f0-9]{40}$/i.test(sha) ||
    !Number.isSafeInteger(runId) ||
    runId <= 0 ||
    (artifactId != null && (!Number.isSafeInteger(artifactId) || artifactId <= 0))
  ) {
    throw new Error("Invalid preview identity.");
  }
  const runUrl = `https://github.com/${fullName}/actions/runs/${runId}`;
  const available = conclusion === "success" && artifactId != null;
  return [
    MARKER,
    "## OpenT3Code desktop preview",
    "",
    `Commit: \`${sha}\``,
    "",
    available
      ? `[Download unsigned JS bundle](${runUrl}/artifacts/${artifactId}) (expires after 7 days).`
      : `No downloadable preview was produced. [Inspect the build](${runUrl}).`,
    "",
    "This is PR-produced JavaScript, not a signed/notarized macOS app, DMG, or an official release. Review the changes before executing it.",
    "",
    "The request applies only to this commit. Apply `preview:bundle` (or the upstream-compatible `preview:mac`) again to request a new build.",
  ].join("\n");
}

async function publishPreview({ github, context, core }) {
  const run = context.payload.workflow_run;
  const repo = context.repo;
  const fullName = `${repo.owner}/${repo.repo}`;
  if (
    run.event !== "pull_request" ||
    run.repository?.full_name !== fullName ||
    run.path !== ".github/workflows/desktop-macos-preview.yml"
  )
    return;
  const candidates = await github.paginate(github.rest.repos.listPullRequestsAssociatedWithCommit, {
    ...repo,
    commit_sha: run.head_sha,
    per_page: 100,
  });
  const matches = matchingPulls(candidates, run, fullName);
  if (matches.length !== 1) {
    core.info("No unique current open PR for this run; skipping.");
    return;
  }
  const pullNumber = matches[0].number;
  const { data: pull } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
  if (
    matchingPulls([pull], run, fullName).length !== 1 ||
    !pull.labels.some((label) => PREVIEW_LABELS.includes(label.name))
  )
    return;
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
    ...repo,
    run_id: run.id,
    filter: "latest",
    per_page: 100,
  });
  // An unrelated label completes the workflow with its only job skipped.
  if (!jobs.some((job) => job.name === "Build preview JS bundle" && job.conclusion !== "skipped"))
    return;
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
    ...repo,
    run_id: run.id,
    per_page: 100,
  });
  const bundles = artifacts.filter(
    (item) => item.name === "opent3code-preview-bundle" && !item.expired,
  );
  const body = previewBody({
    fullName,
    sha: run.head_sha,
    runId: run.id,
    conclusion: run.conclusion,
    artifactId: bundles.length === 1 ? bundles[0].id : undefined,
  });
  // Recheck after API reads so an old build does not consume a new commit's request.
  const { data: latest } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
  if (matchingPulls([latest], run, fullName).length !== 1) return;
  const labels = latest.labels.filter((label) => PREVIEW_LABELS.includes(label.name));
  if (labels.length === 0) return;
  const comments = await github.paginate(github.rest.issues.listComments, {
    ...repo,
    issue_number: pullNumber,
    per_page: 100,
  });
  const existing = comments.find(
    (comment) => comment.user?.login === "github-actions[bot]" && comment.body?.startsWith(MARKER),
  );
  if (existing) {
    await github.rest.issues.updateComment({ ...repo, comment_id: existing.id, body });
  } else {
    await github.rest.issues.createComment({ ...repo, issue_number: pullNumber, body });
  }
  for (const label of labels) {
    try {
      await github.rest.issues.removeLabel({ ...repo, issue_number: pullNumber, name: label.name });
    } catch (error) {
      if (error.status !== 404) throw error;
    }
  }
}

module.exports = { matchingPulls, previewBody, publishPreview };
