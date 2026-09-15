# Maintaining OpenT3Code

OpenT3Code is an independent, source-first fork of `pingdotgg/t3code`. Its repository has stable GitHub ID `1341460159` and owner `Hylouis233`. Preserve upstream authorship, license notices, fork-specific changes, and Git history.

## Repository name and release identity

The intended repository name is `Hylouis233/opent3code`. The initial content migration was committed under `Hylouis233/t3code`; changing a README does not rename a GitHub repository. A repository administrator must complete the rename in GitHub Settings, or with an authenticated GitHub CLI:

```bash
gh repo rename opent3code --repo Hylouis233/t3code --yes
```

Verify the new repository URL before updating the clone command in README. Existing clones can then use:

```bash
git remote set-url origin https://github.com/Hylouis233/opent3code.git
```

The sync workflow resolves its repository from GitHub's event context and verifies the stable repository ID, so the intended rename does not require hard-coding a new destination in its script.

Repository branding is separate from runtime and release identity. The inherited `t3` and `@t3tools` package names, application IDs, protocol names, data directories, and update/distribution endpoints have not been comprehensively migrated. Do not publish these inherited packages under upstream ownership or advertise upstream installers as OpenT3Code releases. A separately tested release migration is required before publishing independent binaries or mobile applications.

## Scheduled upstream synchronization

[OpenT3Code upstream sync](../../.github/workflows/opent3code-sync.yml) runs daily at **02:23 UTC**, subject to GitHub scheduling delays, and supports manual dispatch. Changes to the maintenance script or workflow also trigger a preparation run on `main`.

The workflow imports only `pingdotgg/t3code:main`, including pull requests already merged into that branch. It does not import every open upstream PR. Open PRs need a separate, explicitly selected review; do not automatically reopen closed upstream proposals.

The preparation job opens or reuses a cross-repository PR from `pingdotgg:main` to the fork's default branch. It never writes an upstream branch, resets `main`, or creates local snapshots of upstream workflow files. The source branch can advance: every run captures and validates the exact head, base, and candidate merge SHAs, and any movement blocks merging until a fresh validation. Existing legacy `sync/upstream-*` PRs must be resolved first. A previously closed, unmerged PR for the same upstream SHA is not recreated automatically.

GitHub creates a candidate merge commit. Validation checks out its exact SHA without persisted Git credentials and verifies both parents. The validation job has read-only repository permissions, receives no deployment or provider credentials, and runs maintenance tests, lint/format checks, typechecking, workspace tests, the desktop build/preload check, and Rust crate tests. This is not a claim of live provider authentication testing, a signed release, mobile-device testing, or a complete cross-platform end-to-end certification.

A separate API-only job checks the candidate and branch SHAs again, rejects outstanding change requests and failed or unfinished observed head checks, and requests a normal merge through GitHub's PR API. Repository protection rules remain authoritative; the workflow does not bypass them. An absent head check is not proof of validation: the separate candidate-validation job must have succeeded.

## Automatic-merge boundaries

The following require review rather than automatic merging:

- A conflict, unknown mergeability, changed candidate, changed base, or failed validation.
- Changes to README, contribution/agent policy, root license/notice files, workflow/action definitions, OpenT3Code maintenance scripts, or OpenT3Code documentation assets and operations guidance.
- A missing comparison or a comparison reaching GitHub's 300-file response boundary; incomplete evidence fails closed.
- Outstanding change requests, unsuccessful or unfinished observed head checks, or a repository rule that rejects the merge.

The path guard runs in trusted, API-only jobs before and after candidate validation. Candidate code is not executed in a job with repository write permissions. Changed maintenance files therefore cannot silently rewrite their own automatic-approval policy.

Inspect the workflow summary and PR when a gate stops the run. Resolve actual conflicts on a working branch, review the complete diff, and revalidate the resulting candidate. Never use `reset --hard upstream/main` or a forced update of `main` to remove fork changes. Do not label missing, pending, approval-required, or cancelled tests as successful.

## GitHub setup and operational limits

Actions must be enabled for this fork. Its policy must allow the workflow's declared `contents: write` and `pull-requests: write` permissions and allow GitHub Actions to create pull requests. These are repository/organization settings, not permissions that a workflow can grant itself. No personal access token is required by the checked-in implementation.

If GitHub rejects PR creation, inspect **Settings > Actions > General > Workflow permissions**, enable **Allow GitHub Actions to create and approve pull requests** when permitted, and check the organization policy. Keep the failed run visible; do not imply that synchronization completed. GitHub-token-created PR workflows can require approval. The dedicated validation job is included so a merely missing PR-triggered run is never treated as a green build; observed unfinished checks still block automatic merging.

The initial local-snapshot design was rejected with HTTP 403 by the default Actions token when creating a reference containing different workflow files. The current cross-repository PR design avoids that reference write; it does not bypass PR-creation settings or branch protection.

The general CI uses GitHub-hosted runners in this fork instead of requiring upstream's Blacksmith runner pool. The upstream production-relay deployment job is restricted to `pingdotgg/t3code`; the fork must not deploy upstream infrastructure. Other inherited packaging/release configuration is not an independently configured OpenT3Code release service.

Run the maintenance policy tests locally without installing application dependencies:

```bash
node --test .github/scripts/opent3code-sync.test.cjs
```

A scheduled ChatGPT maintenance task is a separate follow-up mechanism, not a repository credential or a guarantee that an unavailable connector can execute a merge. Repository Actions summaries, PR state, and actual commit ancestry are the completion evidence.
