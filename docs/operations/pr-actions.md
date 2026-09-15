# OpenT3Code pull request automation

This fork follows `pingdotgg/t3code`'s PR workflow structure, with GitHub-hosted runners and an independent contribution policy. The starting point is OpenT3Code commit `91011c8775d7807a2f1f2a433d7882e4d1c3bd4e` and the upstream definitions inspected on September 15, 2026. A checked-in workflow is not evidence that a run passed; inspect its exact commit and Actions result.

## Core CI and merge gate

[CI](../../.github/workflows/ci.yml) runs on pull requests, main pushes, manual dispatch and merge-queue events. It retains upstream lint/format checks, unused-code analysis, typechecking, desktop build/preload verification, workspace tests, three server test shards, Rust tests, conditional mobile native lint and release smoke checks. It adds fork automation tests and the aggregate `PR Gate` job.

`PR Gate` fails for missing, failed, cancelled or unexpectedly skipped required jobs. Only mobile native lint may be skipped when change detection explicitly says it is unnecessary. Linux jobs use `ubuntu-24.04`; conditional native lint uses `macos-latest`. Public credential-free sparse checkouts avoid orphaned vendored gitlinks without modifying upstream source or persisting Git credentials.

The existing upstream synchronization workflow is preserved. This configuration does not enable upstream production deployment, independent package publication, branch protection or automatic merging of arbitrary PRs.

## Contribution labels

[PR Size](../../.github/workflows/pr-size.yml) initializes label definitions and updates `size:XS` through `size:XXL`. [PR Vouch](../../.github/workflows/pr-vouch.yml) adds advisory author-trust labels. Both run on PR metadata events, relevant policy pushes and manual dispatch; policy pushes and manual runs refresh open PRs.

Size boundaries match upstream: XS below 10 effective lines, S below 30, M below 100, L below 500, XL below 1,000 and XXL otherwise. Test-only PRs count test lines; mixed PRs exclude them. Unlike upstream's whitespace-filtered Git diff, this fork uses GitHub's additions/deletions statistics, including whitespace-only changes. Renaming production code into a test path does not exclude those changes. An incomplete file listing stops classification rather than publishing a guessed label.

Edit [opent3code-vouched.json](../../.github/opent3code-vouched.json) on the default branch to manage explicit trust. Initially only `Hylouis233` is listed. Collaborators with `admin`, `maintain` or `write` permissions are also trusted. Upstream's `VOUCHED.td` remains for upstream compatibility but is not used for these labels. Bot account type alone is not a trust grant. Explicit denouncement takes precedence but is still only a review signal.

`vouch:unvouched` does not mean a contribution is unwanted. Neither trust nor size labels authorize merging, grant permissions, disable tests or automatically close PRs. Use **Actions > PR Vouch > Run workflow** for manual refresh; the upstream `/recheck-vouch` comment command is not enabled here.

## Mobile, Windows and performance reports

[Mobile Fingerprint Check](../../.github/workflows/mobile-fingerprint-check.yml) compares iOS/Android native fingerprints against the PR base when mobile-related paths change. All PRs receive an Actions summary and fingerprint artifacts. Same-repository PRs also receive the native-change label through a separate API-only job. This is advisory, not an unconditional required check.

[Windows Tests](../../.github/workflows/windows-tests.yml) uses `windows-2025` and preserves upstream's manual diagnostic role. Select an optional workspace and relative test files through workflow dispatch. Inputs are passed through environment variables and validated, not interpolated into PowerShell code. This is not a claim that the entire Windows suite passes.

[Thread Transfer Report](../../.github/workflows/thread-transfer-report.yml) retains upstream's trusted publisher and baseline/performance comparison after PR CI completes.

## Preview requests

Apply `preview:bundle` to an open PR to run [Desktop macOS Preview](../../.github/workflows/desktop-macos-preview.yml). The upstream name and `preview:mac` label remain as compatibility aliases. Both labels currently request the same unsigned JavaScript bundle containing server/web output and the Electron JavaScript layer, not a standalone `.app`, DMG, installer or signed release. Review the PR before running its code. Artifacts expire after seven days and are not copied into GitHub Releases or upstream hosting.

[Desktop Preview Report](../../.github/workflows/desktop-macos-preview-publish.yml) links the artifact on the matching current PR without downloading or executing it. The reporter checks repository, branch, current head SHA, open state, build job and artifact identity. An unrelated label, closed PR or stale build cannot publish a current preview. A failed build is reported as unavailable. The request label is consumed; later commits require applying it again rather than receiving automatic preview builds.

The inherited signing/publishing implementation has been replaced for this fork. Apple signing credentials, notarization, Expo/EAS credentials, app IDs, update channels and distribution infrastructure require a separately reviewed setup. Do not put these credentials in pull-request build jobs.

## Repository settings

Workflow files cannot enable repository-level settings themselves. After the jobs have run, configure a branch ruleset or branch protection for `main` to require **PR Gate** from GitHub Actions. Require PRs and resolved conversations as appropriate for the maintainers. Do not require advisory, path-filtered or manual-only workflows unconditionally. The merge-queue trigger is present, but no queue or protection rule is enabled merely by adding YAML.

Allow the referenced Actions and declared per-job permissions. Keep the default token read-only; only metadata publishers request write permissions. The separate upstream-sync workflow needs **Allow GitHub Actions to create and approve pull requests** when it creates PRs using `GITHUB_TOKEN`; basic tests and label updates do not require that option. Organization policy may restrict these settings.

These controls live in **Settings > Rules** (or **Branches**) and **Settings > Actions > General**. This configuration does not add a PAT, modify secrets or change repository-level auto-merge settings. A green gate is not automatic merge authorization for arbitrary PRs.

## External review services

The existing `.coderabbit.yaml` requests automatic review, but YAML does not install or authorize the CodeRabbit GitHub App. The inherited Cursor hygiene webhook skips when its URL/auth secrets are absent; it is not a credential-free AI review service. Any inherited Macroscope or other bot configuration also requires a separately connected service. None is claimed to be active just because its configuration was inherited, and none is required by `PR Gate`.

## Verification and security boundaries

Run the new focused tests without installing application dependencies:

```bash
node --test .github/scripts/opent3code-pr.test.cjs .github/scripts/opent3code-pr-report.test.cjs
```

Full application tests and platform builds run in CI, not API-only label/report jobs. The `Check` job keeps its formatter gate and uploads public maintained-file diagnostics when it fails, so canonical formatting can be inspected without weakening validation.

`pull_request_target` labelers load only default-branch scripts and policy. The privileged preview reporter also loads only default-branch code and treats build artifacts as untrusted; it never executes or downloads them. PR code runs separately with a read-only token and no persisted checkout credentials. Keep these boundaries when importing future upstream changes.

GitHub references: [workflow security](https://docs.github.com/en/actions/reference/security/secure-use), [repository Actions settings](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/enabling-features-for-your-repository/managing-github-actions-settings-for-a-repository), and [workflow events](https://docs.github.com/actions/using-workflows/events-that-trigger-workflows).
