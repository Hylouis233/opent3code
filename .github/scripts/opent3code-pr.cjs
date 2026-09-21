const { Buffer } = require("node:buffer");

const SIZE_NAMES = ["size:XS", "size:S", "size:M", "size:L", "size:XL", "size:XXL"];
const MANAGED_SIZE_NAMES = [...SIZE_NAMES, "size:unknown"];
const PR_FILES_API_LIMIT = 3000;
const TRUST_NAMES = ["vouch:trusted", "vouch:unvouched", "vouch:denounced"];
const LABELS = [
  ["size:XS", "0e8a16", "0-9 effective changed lines."],
  ["size:S", "5ebd3e", "10-29 effective changed lines."],
  ["size:M", "fbca04", "30-99 effective changed lines."],
  ["size:L", "fe7d37", "100-499 effective changed lines."],
  ["size:XL", "d93f0b", "500-999 effective changed lines."],
  ["size:XXL", "b60205", "1,000+ effective changed lines."],
  ["size:unknown", "ededed", "File-list limit reached; size cannot be determined safely."],
  ["vouch:trusted", "1f883d", "Author has write access or is explicitly trusted by OpenT3Code."],
  ["vouch:unvouched", "fbca04", "New or unvouched contributor; contributions are welcome."],
  ["vouch:denounced", "d1242f", "Author is listed for maintainer review; no automatic closure."],
  ["preview:bundle", "5319e7", "Request an unsigned desktop JS bundle for this PR commit."],
  [
    "preview:mac",
    "5319e7",
    "Upstream-compatible alias for an unsigned JS bundle, not a signed macOS app.",
  ],
].map(([name, color, description]) => ({ name, color, description }));

function isTestPath(path) {
  return (
    /(^|\/)(__tests__|test|tests)\//.test(path) ||
    /^apps\/server\/integration\//.test(path) ||
    /\.(test|spec|browser|integration)\./.test(path)
  );
}

function classifySize(files) {
  let test = 0;
  let nonTest = 0;
  for (const file of files) {
    for (const count of [file.additions, file.deletions]) {
      if (!Number.isSafeInteger(count) || count < 0) throw new Error("Invalid diff statistics.");
    }
    const lines = file.additions + file.deletions;
    const testOnly =
      isTestPath(file.filename) && (!file.previous_filename || isTestPath(file.previous_filename));
    if (testOnly) test += lines;
    else nonTest += lines;
  }
  const effective = nonTest === 0 ? test : nonTest;
  const index = [10, 30, 100, 500, 1000].findIndex((limit) => effective < limit);
  return { test, nonTest, effective, label: SIZE_NAMES[index === -1 ? 5 : index] };
}

function classifySizeWithCoverage(files, changedFiles) {
  if (!Number.isSafeInteger(changedFiles) || changedFiles < 0 || files.length > changedFiles) {
    throw new Error("Invalid changed-file count.");
  }
  const incomplete = files.length !== changedFiles;
  if (incomplete && (files.length !== PR_FILES_API_LIMIT || changedFiles <= PR_FILES_API_LIMIT)) {
    throw new Error(`Incomplete file list (${files.length}/${changedFiles}); refusing to guess size.`);
  }
  const result = classifySize(files);
  if (!incomplete) return { ...result, incomplete };
  // Tests-only totals may shrink when unseen production changes are included.
  return {
    ...result,
    effective: null,
    label: result.nonTest >= 1000 ? "size:XXL" : "size:unknown",
    incomplete,
  };
}

function validatePolicy(policy) {
  for (const key of ["trusted", "denounced"]) {
    if (
      !Array.isArray(policy?.[key]) ||
      policy[key].length > 1000 ||
      policy[key].some(
        (login) => typeof login !== "string" || !/^[a-z0-9-]+(?:\[bot\])?$/i.test(login),
      )
    ) {
      throw new Error(`Invalid ${key} list in OpenT3Code vouch policy.`);
    }
  }
  return policy;
}

function classifyTrust(login, permission, policy) {
  const matches = (names) => names.some((name) => name.toLowerCase() === login.toLowerCase());
  if (matches(policy.denounced)) return "vouch:denounced";
  if (matches(policy.trusted) || ["admin", "maintain", "write"].includes(permission))
    return "vouch:trusted";
  return "vouch:unvouched";
}

async function ensureLabels(github, repo) {
  for (const label of LABELS) {
    try {
      const { data } = await github.rest.issues.getLabel({ ...repo, name: label.name });
      if (data.color !== label.color || (data.description ?? "") !== label.description) {
        await github.rest.issues.updateLabel({ ...repo, ...label });
      }
    } catch (error) {
      if (error.status !== 404) throw error;
      try {
        await github.rest.issues.createLabel({ ...repo, ...label });
      } catch (createError) {
        if (createError.status !== 422) throw createError;
        // Another PR may have created the label concurrently. Verify it exists.
        await github.rest.issues.getLabel({ ...repo, name: label.name });
      }
    }
  }
}

async function syncLabel({ github, repo, pull, label, managed, core }) {
  const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pull.number });
  if (current.state !== "open" || current.head.sha !== pull.head.sha) {
    core.info(`PR #${pull.number} changed while being classified; skipping stale result.`);
    return;
  }
  for (const existing of current.labels) {
    if (!managed.includes(existing.name) || existing.name === label) continue;
    try {
      await github.rest.issues.removeLabel({
        ...repo,
        issue_number: pull.number,
        name: existing.name,
      });
    } catch (error) {
      if (error.status !== 404) throw error;
    }
  }
  if (!current.labels.some((existing) => existing.name === label)) {
    await github.rest.issues.addLabels({ ...repo, issue_number: pull.number, labels: [label] });
  }
  core.info(`PR #${pull.number}: ${label}`);
}

async function labelPullRequests({ github, context, core, kind }) {
  if (!["size", "trust"].includes(kind)) throw new Error("Unknown label operation.");
  const repo = context.repo;
  // No PR checkout or dependencies: this module is loaded from the trusted default branch.
  await ensureLabels(github, repo);
  let policy;
  if (kind === "trust") {
    const { data } = await github.rest.repos.getContent({
      ...repo,
      path: ".github/opent3code-vouched.json",
      ref: context.payload.repository.default_branch,
    });
    if (Array.isArray(data) || data.encoding !== "base64" || data.size > 65536) {
      throw new Error("Cannot read the trusted OpenT3Code vouch policy.");
    }
    policy = validatePolicy(JSON.parse(Buffer.from(data.content, "base64").toString("utf8")));
  }
  const pulls =
    context.eventName === "pull_request_target"
      ? [context.payload.pull_request]
      : await github.paginate(github.rest.pulls.list, { ...repo, state: "open", per_page: 100 });
  for (const candidate of pulls) {
    const { data: pull } = await github.rest.pulls.get({ ...repo, pull_number: candidate.number });
    if (pull.state !== "open") continue;
    if (kind === "size") {
      const files = await github.paginate(github.rest.pulls.listFiles, {
        ...repo,
        pull_number: pull.number,
        per_page: 100,
      });
      const result = classifySizeWithCoverage(files, pull.changed_files);
      if (result.incomplete) {
        core.warning(
          `PR #${pull.number}: GitHub returned ${files.length}/${pull.changed_files} files; ` +
            `observed non-test lower bound ${result.nonTest} lines; ${result.label}.`,
        );
      }
      await syncLabel({ github, repo, pull, label: result.label, managed: MANAGED_SIZE_NAMES, core });
      if (!result.incomplete) {
        core.info(
          `${result.nonTest} non-test + ${result.test} test lines; ${result.effective} effective.`,
        );
      }
    } else {
      let permission =
        pull.user.login.toLowerCase() === repo.owner.toLowerCase() ? "admin" : "none";
      if (permission === "none") {
        try {
          const { data } = await github.rest.repos.getCollaboratorPermissionLevel({
            ...repo,
            username: pull.user.login,
          });
          permission = data.permission;
        } catch (error) {
          if (![403, 404].includes(error.status)) throw error;
          core.info(
            `No collaborator permission available for ${pull.user.login}; using local policy only.`,
          );
        }
      }
      const label = classifyTrust(pull.user.login, permission, policy);
      await syncLabel({ github, repo, pull, label, managed: TRUST_NAMES, core });
    }
  }
}

module.exports = {
  classifySize,
  classifySizeWithCoverage,
  classifyTrust,
  validatePolicy,
  syncLabel,
  labelPullRequests,
};
