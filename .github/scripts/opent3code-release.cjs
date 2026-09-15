const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const OWNER = "Hylouis233";
const REPO_ID = 1341460159;
const MARKER = "<!-- opent3code-source-prerelease -->";
const SHA = /^[a-f0-9]{40}$/;
function validateManifest(value) {
  if (value?.enabled !== true || value.sourceOnly !== true || !/^0\.1\.0-alpha\.[1-9][0-9]*$/.test(value.version || "")) throw Error("Only explicitly enabled source Alpha releases are allowed.");
  return { ...value, tag: `opent3code-v${value.version}` };
}
function assertRun(run, sha, jobs) {
  if (!SHA.test(sha || "") || run.repository?.id !== REPO_ID || run.head_repository?.id !== REPO_ID || run.head_branch !== "main" || run.head_sha !== sha || run.path !== ".github/workflows/ci.yml" || run.event !== "push" || run.status !== "completed" || run.conclusion !== "success") throw Error("Not a successful exact-main CI run.");
  for (const name of ["PR Gate", "OpenT3Code Maintenance", "Check", "Test", "Test Server 1", "Test Server 2", "Test Server 3", "Rust", "Release Smoke"]) {
    const matches = jobs.filter(job => job.name === name);
    if (matches.length !== 1 || matches[0].status !== "completed" || matches[0].conclusion !== "success") throw Error(`Required job is missing or unsuccessful: ${name}`);
  }
}
async function textAt(github, repo, file, ref) {
  const { data } = await github.rest.repos.getContent({ ...repo, path: file, ref });
  if (data.type !== "file" || data.encoding !== "base64" || data.size > 100000) throw Error("Invalid trusted release input.");
  return Buffer.from(data.content, "base64").toString("utf8");
}
async function verify(github, repo, runId, sha) {
  const { data: metadata } = await github.rest.repos.get(repo);
  if (metadata.id !== REPO_ID || metadata.owner.login !== OWNER || metadata.default_branch !== "main") throw Error("Wrong repository.");
  const { data: main } = await github.rest.repos.getBranch({ ...repo, branch: "main" });
  if (main.commit.sha !== sha) throw Error("Main moved; use its own successful CI.");
  const { data: run } = await github.rest.actions.getWorkflowRun({ ...repo, run_id: Number(runId) });
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, { ...repo, run_id: Number(runId), filter: "latest", per_page: 100 });
  assertRun(run, sha, jobs);
}
async function prepare({ github, context, core }) {
  const run = context.payload.workflow_run;
  if (!run || context.eventName !== "workflow_run") throw Error("Only completed CI events are accepted.");
  await verify(github, context.repo, run.id, run.head_sha);
  const manifest = validateManifest(JSON.parse(await textAt(github, context.repo, ".github/opent3code-release.json", run.head_sha)));
  try {
    const { data: existing } = await github.rest.repos.getReleaseByTag({ ...context.repo, tag: manifest.tag });
    if (existing.draft) throw Error("A draft with this tag already exists; inspect it rather than overwriting it.");
    core.notice(`Release ${manifest.tag} already exists; immutable release left unchanged.`);
    return;
  } catch (error) { if (error.status !== 404) throw error; }
  core.setOutput("sha", run.head_sha);
  core.setOutput("version", manifest.version);
  core.setOutput("tag", manifest.tag);
  core.setOutput("run_id", String(run.id));
}
function verifyPayload(root, version, sha, runId) {
  const names = [`opent3code-source-${version}.zip`, `opent3code-dsh-plugin-${version}.tgz`, "RELEASE.json", "SHA256SUMS"];
  if (fs.readdirSync(root).sort().join("\n") !== [...names].sort().join("\n")) throw Error("Unexpected release payload entries.");
  for (const name of names) {
    const stat = fs.lstatSync(path.join(root, name));
    if (!stat.isFile() || stat.size === 0 || stat.size > 250000000) throw Error("Invalid release asset.");
  }
  const receipt = JSON.parse(fs.readFileSync(path.join(root, "RELEASE.json"), "utf8"));
  if (receipt.sha !== sha || receipt.version !== version || receipt.ciRun !== Number(runId) || receipt.sourceOnly !== true) throw Error("Release receipt mismatch.");
  const rows = fs.readFileSync(path.join(root, "SHA256SUMS"), "utf8").trim().split(/\r?\n/);
  if (rows.length !== 3) throw Error("Missing checksums.");
  const expected = new Set(names.filter(name => name !== "SHA256SUMS"));
  for (const row of rows) {
    const match = /^([a-f0-9]{64})  ([a-zA-Z0-9_.-]+)$/.exec(row);
    if (!match || !expected.delete(match[2])) throw Error("Invalid checksum entry.");
    const digest = crypto.createHash("sha256").update(fs.readFileSync(path.join(root, match[2]))).digest("hex");
    if (digest !== match[1]) throw Error("Asset checksum mismatch.");
  }
  return names;
}
async function publish({ github, context, core, sha, version, runId, root }) {
  await verify(github, context.repo, runId, sha);
  const manifest = validateManifest(JSON.parse(await textAt(github, context.repo, ".github/opent3code-release.json", sha)));
  if (manifest.version !== version) throw Error("Release version changed.");
  const names = verifyPayload(root, version, sha, runId);
  const notes = await textAt(github, context.repo, `docs/releases/${version}.md`, sha);
  try {
    await github.rest.repos.getReleaseByTag({ ...context.repo, tag: manifest.tag });
    throw Error("Release already exists; refusing to overwrite.");
  } catch (error) { if (error.status !== 404) throw error; }
  try {
    const { data: tag } = await github.rest.git.getRef({ ...context.repo, ref: `tags/${manifest.tag}` });
    if (tag.object.type !== "commit" || tag.object.sha !== sha) throw Error("Existing tag does not point at this candidate.");
  } catch (error) {
    if (error.status !== 404) throw error;
    await github.rest.git.createRef({ ...context.repo, ref: `refs/tags/${manifest.tag}`, sha });
  }
  const { data: release } = await github.rest.repos.createRelease({ ...context.repo, tag_name: manifest.tag, target_commitish: sha, name: `OpenT3Code ${version} — source preview`, body: `${MARKER}\n${notes}\n\nSource: \`${sha}\`. Validated by CI run ${runId}.`, draft: true, prerelease: true, make_latest: "false" });
  for (const name of names) {
    const bytes = fs.readFileSync(path.join(root, name));
    await github.rest.repos.uploadReleaseAsset({ ...context.repo, release_id: release.id, name, data: bytes, headers: { "content-type": "application/octet-stream", "content-length": bytes.length } });
  }
  await verify(github, context.repo, runId, sha);
  const assets = await github.paginate(github.rest.repos.listReleaseAssets, { ...context.repo, release_id: release.id, per_page: 100 });
  if (assets.length !== names.length || names.some(name => !assets.some(asset => asset.name === name && asset.state === "uploaded" && asset.size === fs.statSync(path.join(root, name)).size))) throw Error("Uploaded assets are incomplete; draft preserved.");
  await github.rest.repos.updateRelease({ ...context.repo, release_id: release.id, draft: false, prerelease: true, make_latest: "false" });
  await core.summary.addRaw(`Published source-only prerelease: ${release.html_url}\n`).write();
}
module.exports = { validateManifest, assertRun, verifyPayload, prepare, publish };
