<div align="center">
  <img src="./docs/assets/opent3code-hero.svg" alt="OpenT3Code — your agents, your tools, your interface" width="100%" />

# OpenT3Code

**An open, community-driven home for coding agents — across CLI, desktop, web, mobile, and terminal workflows.**

[Get started](#run-this-fork) · [Compatibility](#what-works-today) · [Contribute](./CONTRIBUTING.md) · [Upstream maintenance](./docs/operations/opent3code.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-4f46e5.svg)](./LICENSE)
![Project direction](https://img.shields.io/badge/direction-agent%20agnostic-0f766e)
![Distribution](https://img.shields.io/badge/distribution-source%20first-334155)

</div>

## Your workflow should not depend on one interface

OpenT3Code is an independently maintained fork of [T3 Code](https://github.com/pingdotgg/t3code). It keeps the upstream foundation for running coding agents on your own machine, while opening the contribution scope to more tools and more ways of working.

**Our direction is broad compatibility, not a preferred-vendor list:** command-line agents, terminal user interfaces, desktop integrations, browser clients, mobile clients, and remote workflows should all have a place here. A tool's interface should not decide whether it belongs in the ecosystem.

> [!IMPORTANT]
> **Open to every tool does not mean every tool is already integrated.** This fork includes the inherited adapters and the explicit source-preview integrations listed below. A universal CLI adapter, a standalone OpenT3Code TUI, and arbitrary desktop-app control are extension goals, not shipped features. Launching a process is not the same as supporting structured sessions, permissions, streaming, or recovery.

## What makes this fork different?

|                 | T3 Code foundation                                   | OpenT3Code direction                                                                      |
| --------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Agent execution | Provider-specific adapters and orchestration         | Keep those adapters; welcome additional runtimes and protocols                            |
| Interfaces      | Web, Electron desktop, and mobile clients            | Preserve them; make terminal-first and other client contributions welcome                 |
| Contributions   | Governed by the upstream contribution policy         | Accept focused provider, CLI, TUI, desktop, accessibility, and interoperability proposals |
| Maintenance     | Upstream development and releases                    | Traceable upstream merges, independent review, and preservation of fork-specific changes  |
| Distribution    | Upstream packages, applications, and hosted services | Source-first fork; do not represent upstream binaries or services as our own              |

This is a difference in **project scope and maintenance policy**, not a claim that the new integrations already exist. We credit the upstream project and do not imply its endorsement.

## What works today

### OpenT3Code source-preview integrations

**MiniMax Code and DeepSeek Harness are now native ACP provider options.** Both support supervised text sessions, assistant/reasoning/tool events, one-shot permission requests, cancellation and instance/workspace-bound native resume. They remain opt-in preview integrations; account authentication and model selection stay with the official CLI.

MiniMax Code launches `mcode acp`. DeepSeek Harness launches a dedicated `opent3code` profile provided by the [installable DSH companion bundle](./integrations/dsh-opent3code/README.md). No terminal scraping or account proxy is involved. Unsupported attachment, plan-mode, steering, rollback and background text-generation operations are rejected explicitly. DSH resume does not replay native history.

Start with the [source-preview guide](./docs/user/opent3code-preview.md). This release is source-only, not a signed installer or certification of live paid-account generation on every platform.

### Agent integrations inherited from upstream

| Agent              | Integration available in the current source | Getting connected                                                        |
| ------------------ | ------------------------------------------- | ------------------------------------------------------------------------ |
| Codex              | Provider adapter                            | Install and authenticate its CLI                                         |
| Claude Code        | Provider adapter                            | Install and authenticate its CLI                                         |
| Cursor             | Provider adapter                            | Install and authenticate Cursor CLI                                      |
| Grok Build         | Provider adapter                            | Install and authenticate its CLI                                         |
| OpenCode           | Provider adapter                            | Install and authenticate OpenCode                                        |
| Google Antigravity | Provider adapter                            | Enable it in Settings, then use its installation and Google sign-in flow |

Availability depends on your operating system, installed provider version, account, and the individual adapter's capabilities. This table describes code present in the inherited implementation; it is not a fresh end-to-end certification of every provider.

### Interfaces and integration boundaries

| Surface or tool                         | Status              | What this means                                                                      |
| --------------------------------------- | ------------------- | ------------------------------------------------------------------------------------ |
| Local web interface                     | Available in source | Run the server and browser client from this checkout                                 |
| Desktop application                     | Available in source | Electron client; use the source development/build commands below                     |
| Server CLI                              | Available in source | Start and manage the inherited server; not a standalone conversational TUI           |
| Mobile clients                          | Available in source | Inherited React Native clients; branded store releases are not provided by this fork |
| Terminal / TUI-based agents             | Adapter-dependent   | A tool needs a compatible protocol or an implemented adapter for managed sessions    |
| Standalone OpenT3Code TUI               | Planned             | Contributions welcome; no standalone terminal client is shipped yet                  |
| Arbitrary CLI / third-party desktop app | Planned             | Requires an integration contract or bridge; not universal GUI automation             |

<img src="./docs/assets/opent3code-architecture.svg" alt="OpenT3Code architecture: existing provider adapters and web, desktop, and mobile clients; proposed CLI, TUI, and desktop bridges are shown separately" width="100%" />

## Run this fork

Use Node.js **24.13.1 or newer 24.x**, and install [Vite+](https://viteplus.dev/guide/). Use a fixed source-preview release or a specific commit rather than silently following a changing main branch.

```sh
git clone https://github.com/Hylouis233/opent3code.git
cd opent3code
vp i
node scripts/opent3code-preview.mjs
```

The preview launcher initializes separate `.opent3code-preview/userdata` state with **Supervised** permissions. It refuses existing native tool namespaces and does not overwrite existing preview settings. Open the complete **pairing URL** printed by the runner, not just the bare origin. Do not share its token in a group, issue or screenshot.

Install and authenticate your native CLI first. MiniMax Code is initially enabled in the new preview; install the DSH companion profile before enabling DeepSeek Harness. See the [preview guide](./docs/user/opent3code-preview.md) for versions, account ownership, installation, limits and removal.

Inherited developer commands such as `vp run dev` and `vp run dev:desktop` are still available, but do not automatically acquire the preview launcher's isolation policy. Normal main checkouts and linked worktrees have different defaults; consult the [development guide](./docs/operations/development.md#state-and-ports).

> [!WARNING]
> This release does not distribute independently branded desktop installers. Internal `t3` / `@t3tools` package names, desktop application IDs and native packaging identities remain inherited. `npx t3@latest` and official T3 installers install upstream T3 Code, not OpenT3Code. Do not use an inherited installer to replace or share an existing T3 profile.

## Extending the ecosystem

We welcome integrations for **CLI, TUI, desktop, web, mobile, and remote tools**, without restricting proposals to the existing provider list.

A managed integration should state its actual capabilities: executable or endpoint discovery, authentication, session creation and resume, streaming, cancellation, permission prompts, working-directory isolation, errors, and recovery. Unsupported capabilities must be explicit. A terminal process should not be presented as a fully managed agent merely because it can be launched.

Start with the existing [provider boundary](./apps/server/src/provider), [shared contracts](./packages/contracts), and [contribution guide](./CONTRIBUTING.md). Put runtime-specific complexity in an adapter rather than adding provider-specific behavior throughout the clients. Never bypass a tool's authentication or permission model.

## Staying current with upstream

The [upstream sync workflow](./.github/workflows/opent3code-sync.yml) checks `pingdotgg/t3code:main` daily and can also be run manually. New upstream commits — including PRs already merged there — are proposed in a dedicated sync PR. The candidate is validated without write credentials; only a passing, unchanged candidate can be merged automatically.

Conflicts, changed fork-maintenance files, failed validation, or repository protection rules stop automatic merging. **Open, unmerged upstream PRs are not batch-merged.** They need separate review and an explicit selection. See [maintenance and rename instructions](./docs/operations/opent3code.md) for permissions, review gates, and recovery.

## Documentation

[Installation](./docs/user/install.md) · [Permissions](./docs/user/permission-modes.md) · [Keyboard shortcuts](./docs/user/keybindings.md) · [Project settings](./docs/user/project-settings.md) · [Remote access](./docs/user/remote-access.md) · [Source control](./docs/user/source-control.md) · [Architecture](./docs/internals/overview.md)

Some inherited guides still describe upstream names, distribution channels, and hosted services. The fork-specific installation and compatibility notes on this page take precedence for OpenT3Code.

## Contributing

Focused fixes, new adapters, terminal-first workflows, desktop integrations, documentation, and accessibility improvements are welcome. Propose a larger feature in a **draft PR against this fork** before implementing it. Include compatibility limits, test evidence, and screenshots or recordings for visible changes. Do not send fork-specific support requests to upstream.

Read [CONTRIBUTING.md](./CONTRIBUTING.md) and the engineering guidance in [AGENTS.md](./AGENTS.md). The fork contribution policy takes precedence over inherited product-policy statements.

## License and attribution

[MIT](./LICENSE). OpenT3Code is derived from T3 Code by its upstream authors and contributors. Existing copyright, license, and third-party notices are retained. Names and logos of third-party products belong to their respective owners.
