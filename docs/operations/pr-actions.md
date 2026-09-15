# OpenT3Code pull request automation

This fork follows `pingdotgg/t3code`'s PR workflow structure, with GitHub-hosted runners and an independent contribution policy. The starting point for this configuration is OpenT3Code commit `91011c8775d7807a2f1f2a433d7882e4d1c3bd4e` and the upstream workflow definitions inspected on September 15, 2026. A checked-in workflow is not evidence that a run passed; inspect its exact commit and Actions result.

## Workflow map

| Workflow | Trigger | Behavior in this fork |
| --- | --- | --- |
| [CI](../../.github/workflows/ci.yml) | PR, main push, manual dispatch, merge queue | Maintains upstream lint/format, unused-code analysis, typechecking, desktop build/preload verification, workspace tests, three server shards, Rust tests, conditional mobile native lint, and release smoke checks. Adds fork automation tests and the `PR Gate` aggregate. |
| [PR Size](../../.github/workflows/pr-size.yml) | PR metadata events, policy push, manual dispatch | Initializes labels and updates `size:XS` through `size:XXL`; policy changes also refresh open PRs. |
| [PR Vouch](../../.github/workflows/pr-vouch.yml) | PR metadata events, policy push, manual dispatch | Labels trust using repository write access and the independent OpenT3Code policy. Never closes PRs or grants permissions. |
| [Mobile Fingerprint Check](../../.github/workflows/mobile-fingerprint-check.yml) | Mobile-related PR changes | Compares iOS/Android native fingerprints against the base. Stores evidence for all PRs and labels same-repository PRs in a separate API-only job. Advisory, not an unconditional required check. |
| [Thread Transfer Report](../../.github/workflows/thread-transfer-report.yml) | Completed PR CI | Retains upstream's trusted publisher and baseline/performance comparison. |
| [Desktop macOS Preview](../../.github/workflows/desktop-macos-preview.yml) | A maintainer applies `preview:bundle` or `preview:mac` | Builds the exact PR head with read-only permissions and uploads an unsigned JavaScript bundle. No signed application is produced. |
| [Desktop Preview Report](../../.github/workflows/desktop-macos-preview-publish.yml) | Completed preview build | Links the artifact on the matching current PR without downloading or executing it. Consumes the one-shot request label. |
| [Windows Tests](../../.github/workflows/windows-tests.yml) | Manual dispatch | Uses `windows-2025`, preserving upstream's on-demand diagnostic role. Not a claim that the entire Windows suite passes. |

Core Linux work uses `ubuntu-24.04`, and conditional native lint uses `macos-latest`. The existing upstream synchronization workflow is preserved; this change does not enable any upstream production deployment or independent package publication.

## Contribution labels

The size boundaries match upstream: XS below 10 effective lines, S below 30, M below 100, L below 500, XL below 1,000, and XXL otherwise. Test-only PRs count test lines; mixed PRs exclude test lines. Unlike upstream's Git whitespace-filtered diff, this fork uses GitHub's additions/deletions API statistics, including whitespace-only changes. Renaming production code into a test path does not exclude those changes. An incomplete file listing stops classification instead of publishing a guessed label.

Edit [opent3code-vouched.json](../../.github/opent3code-vouched.json) on the default branch to manage explicit trust. Initially only `Hylouis233` is listed. `admin`, `maintain`, and `write` collaborators are also trusted. Upstream's `VOUCHED.td` remains in the repository for upstream compatibility, but is not used for this fork's PR labels. Bot account type alone is not a trust grant. Explicit denouncement takes precedence, but it is still only a review signal.

`vouch:unvouched` does not mean a contribution is unwanted. Neither trust nor size labels authorize merging, disable tests, or automatically close PRs. Use **Actions > PR Vouch > Run workflow** to refresh labels manually; the upstream `/recheck-vouch` comment command is not enabled here.

## Preview requests

Apply `preview:bundle` to an open PR. `preview:mac` is retained as an upstream-compatible alias. Both currently request the same JS bundle containing server/web output and the Electron JavaScript layer, not a standalone `.app`, DMG, installer, or signed release. Review the PR before running its code. Artifacts expire after seven days and are not copied into GitHub Releases or upstream hosting.

The reporter checks repository, branch, current head SHA, open state, build job, and artifact identity. An unrelated label, closed PR, or stale build cannot publish a current preview. A failed build is reported as unavailable. Later commits require removing/reapplying the preview label; it is not a standing build subscription.

The inherited upstream signing/publishing implementation has been replaced for this fork. Apple signing credentials, notarization, Expo/EAS credentials, public app IDs, update channels, and distribution infrastructure require a separately reviewed setup. Do not put these credentials in pull-request build jobs.

## Repository settings

Workflow files cannot enable repository-level settings themselves. After these jobs have run, configure a branch ruleset or branch protection for `main` to require **PR Gate** from GitHub Actions. Require PRs and resolved conversations as appropriate for the maintainers. Do not require advisory, path-filtered, or manual-only workflows unconditionally. The merge-queue trigger is present, but no queue or protection rule is enabled merely by adding YAML.

Allow the referenced Actions and the declared per-job permissions. Keep the default token read-only; only metadata publishers request write permissions. The separate upstream-sync workflow needs **Allow GitHub Actions to create and approve pull requests** if it creates PRs using `GITHUB_TOKEN`; basic PR tests and label updates do not require that option. Organization policy may restrict these settings.

These controls are configured in **Settings > Rules** (or **Branches**) and **Settings > Actions > General**. This configuration does not add a PAT, modify secrets, or change repository-level auto-merge settings. A green gate is not automatic merge authorization for arbitrary PRs.

## External review services

The existing `.coderabbit.yaml` requests automatic CodeRabbit review, but YAML does not install or authorize the CodeRabbit GitHub App. Likewise, the inherited Cursor hygiene webhook skips when its URL/auth secrets are absent; it is not a credential-free AI review service. Any upstream Macroscope or other bot configuration requires a separately connected service. None is claimed to be active just because its configuration was inherited, and none is required by `PR Gate`.

## Verification and security boundaries

Run the new focused tests without installing application dependencies:

```bash
node --test .github/scripts/opent3code-pr.test.cjs .github/scripts/opent3code-pr-report.test.cjs
```

`PR Gate` fails for missing, failed, cancelled, or unexpectedly skipped required jobs. Only the mobile native-lint job may be skipped when change detection explicitly says it is unnecessary. Full application tests and platform builds run in GitHub CI, not in the API-only label/report jobs.

`pull_request_target` labelers load only default-branch scripts and policy. The privileged preview reporter also loads only default-branch code and treats build artifacts as untrusted; it never executes or downloads them. PR code runs separately with a read-only token and without persisted checkout credentials. Keep these boundaries when importing future upstream changes.

GitHub references: [workflow security](https://docs.github.com/en/actions/reference/security/secure-use), [repository Actions settings](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository), and [workflow events](https://docs.github.com/actions/using-workflows/events-that-trigger-workflows).
