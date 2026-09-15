# OpenT3Code source preview

This Alpha is for developers who can build from source. It is not a signed desktop installer or an authenticated certification of every provider. Internal T3 package names and desktop application IDs remain inherited; do not distribute the inherited desktop packaging as an isolated OpenT3Code product.

## Start without sharing T3 data

Use Node.js 24.13.1 or newer 24.x and [Vite+](https://viteplus.dev/guide/). Clone this repository, check out the release tag or exact commit you intend to test, then:

```sh
vp i
node scripts/opent3code-preview.mjs
```

The launcher creates a dedicated `.opent3code-preview/userdata` directory and initializes **Supervised** permissions. It never rewrites settings in an existing preview profile. Use `--home-dir /absolute/new/directory` to select another empty directory. Do not point it at T3, DSH or MCode state. This isolates OpenT3Code state, not the agent's filesystem access or native CLI account.

Open the **complete pairing URL** printed by the dev runner. Its token grants access to your environment: do not paste it into a group, issue or screenshot. Start with a disposable project. Stop using Ctrl+C. Keep or back up preview data before removing it; no automatic migration or deletion of an existing tool profile is provided.

## MiniMax Code

Install and authenticate the official CLI yourself:

```sh
npm install -g @minimax-ai/code@0.4.7
mcode login
# For a Global account instead: mcode login --region global
```

The preview initializes a MiniMax Code instance; the adapter runs `mcode acp`. Select **CLI configured model** and **Supervised**. Account login, BYOK providers and model selection remain under the official CLI's control. The OpenT3Code startup probe checks the protocol, not your subscription or model access. Authentication can therefore remain unknown until a real session is attempted.

An optional absolute CLI data directory maps to `MINIMAX_DATA_DIR`; authenticate using the same directory beforehand. Leaving it blank uses your normal CLI environment. Do not place tokens directly in a repository or attach private provider configuration to a bug report.

## DeepSeek Harness

Install the [companion profile bundle](../../integrations/dsh-opent3code/README.md) into a NEW `opent3code` DSH profile. Configure the selected provider using DSH's own configuration. Then enable/add **DeepSeek Harness** in Settings > Providers. The adapter runs `dsh --profile opent3code`, not a terminal GUI.

An optional absolute CLI data directory maps to `DSH_HOME`. Inspect the effective profile before use: later user patches can override bundle defaults. This is a local, trusted-user integration, not a hosted multi-user sandbox.

## Capability limits

Both preview adapters support text prompts, assistant/reasoning/tool events, one-shot permissions, cancellation and native resume tied to the same provider instance, workspace and CLI home. Long-lived blanket approvals are not granted. Unsupported attachment types, non-supervised runtime modes, plan-mode switching, mid-turn steering, background title/commit generation and rollback are rejected explicitly. Choose another capable provider for background text-generation features.

DSH's native ACP supports resume but not history replay. OpenT3Code keeps its own conversation history; this preview does not import arbitrary native histories. DSH notifications may be committed messages rather than token-by-token output. A model or custom plugin can have additional limitations.

Provider subprocess fixtures and official CLI handshake tests are not a substitute for testing your own authenticated account. Full real-account generation, all-platform GUI tests and signed binary upgrades are not claimed for this source Alpha.

## Feedback

Report the exact release/commit, operating system, Node version, native CLI version, integration and a minimal reproduction. Redact keys, pairing URLs, personal paths and prompt contents. If repository Issues are unavailable, use the maintainer's existing contact in the test group; do not send fork-specific bugs to upstream support.
