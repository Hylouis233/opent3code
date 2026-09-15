# Contributing to OpenT3Code

OpenT3Code welcomes focused contributions across CLI, TUI, desktop, web, mobile, and remote agent workflows. This is the contribution policy for this independent fork, not the policy of upstream T3 Code.

## Start with a concrete integration or problem

Bug fixes, reliability, performance, accessibility, documentation, and new provider or client integrations are in scope. For a substantial feature, open a draft PR against this fork with the problem, proposed boundary, compatibility limits, and validation plan before building a large implementation. Do not direct fork proposals to upstream's support channels. A proposal is not a guarantee of acceptance.

## Development

Use the [first-checkout runbook](docs/operations/development.md#first-checkout) and the source-first instructions in [README.md](README.md#run-this-fork). Read [AGENTS.md](AGENTS.md) for engineering conventions. This file overrides its inherited upstream contribution restrictions, but does not waive tests, security, or review.

Keep development state separate from live installations. Do not commit credentials, account data, personal transcripts, or generated scratch files. Keep patches small enough to review and avoid unrelated rewrites or dependency changes.

## Adding an agent or interface

Document what works and what does not: discovery, authentication, session lifecycle, streaming, cancellation, approvals, resume, working directories, and recovery. Reuse the existing provider and shared-contract boundaries. Raw terminal access is not equivalent to a structured agent integration. Arbitrary desktop automation and a standalone TUI are not already supported just because this fork welcomes them.

Preserve each provider's authentication and permission controls. Do not log tokens or silently grant permissions. Check the implications for web, desktop, mobile, local, and remote clients; explicitly identify any unsupported surface.

## Evidence and review

Use a conventional commit title and explain the problem and result in plain English. Add focused behavior tests for changed logic. Include the exact commands run, their results, and anything not tested. Visible changes need screenshots; interaction changes may need a short recording. Upload PR-only evidence rather than committing it; maintained documentation assets belong under `docs/assets`.

Run the smallest relevant local checks. CI owns the full integration suite. The fork's maintenance-script tests can be run without installing the application:

```bash
node --test .github/scripts/opent3code-sync.test.cjs
```

Never replace failed or missing evidence with an unsupported compatibility claim. Maintainers may request a smaller change or defer an integration until its contract is clear.

## Upstream work and attribution

Retain original authorship and licensing when importing upstream commits. Upstream changes already merged to `main` follow the [sync policy](docs/operations/opent3code.md). Unmerged upstream PRs need individual review and selection; do not bulk-import them or reopen previously closed upstream proposals automatically.
