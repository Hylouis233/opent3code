# OpenT3Code companion bundle for DeepSeek Harness

This is an installable **DSH profile plugin bundle**, not a replacement for DSH's web UI. It composes the official DSH ACP server for OpenT3Code, with workspace-write sandbox policy, approval requests, and session telemetry disabled. It does not proxy account credentials.

The preview targets `@deepseek-ai/dsh@0.1.5-rc.1` with ACP/base/application bundles `0.1.5-rc.2`. A launcher version alone does not pin all DSH dependencies; keep the profile lockfile after installation. Do not copy another computer's credentials or databases.

## Install into a dedicated profile

Install Node.js 24 and pnpm, then the official CLI. Choose a separate `DSH_HOME` before installation when an isolated account configuration is desired. Use that same home in OpenT3Code's provider settings.

```sh
npm install -g @deepseek-ai/dsh@0.1.5-rc.1
# From this source directory, create the local package; no npm publication is required.
npm pack --ignore-scripts
# Initialize a NEW profile with dsh-base, then add the companion bundle.
dsh plugin --profile opent3code add ./opent3code-dsh-plugin-0.1.0-alpha.1.tgz
dsh --profile opent3code --dump-config
```

Do not add this bundle on top of the existing `acp` application bundle: that would register two ACP application rows. Configure credentials with DSH's own settings. Nothing here signs you in or includes an API key.

In OpenT3Code, add **DeepSeek Harness** under Settings > Providers. Select **Supervised** permissions and **CLI configured model**. The driver runs `dsh --profile opent3code` over stdio. Instance environment variables `OPENT3CODE_DSH_PROVIDER` and `OPENT3CODE_DSH_MODEL` select an already configured provider/model. Treat all DSH plugin configuration as trusted executable code.

A later home/profile patch can override bundle defaults. Review effective `approval` and `sandbox-policy` rows after changes. Workspace-write is not a universal operating-system security guarantee; tools and the selected sandbox backend must enforce their policies. Never expose the host to untrusted users.

## Limits and removal

The adapter supports text prompts, assistant/reasoning/tool updates, one-shot approvals, cancellation and native session resume. DSH emits committed semantic messages, not necessarily token-by-token deltas. Native history replay, rollback, structured questions, attachments, plan-mode switching and mid-turn steering are not advertised. Unsupported operations fail explicitly.

Stopping a session terminates only its managed ACP process and does not delete DSH sessions. To remove this bundle, stop the instance, remove it from OpenT3Code's provider list, and run `dsh plugin --profile opent3code remove @opent3code/dsh-plugin`. Keep session data until you have exported anything needed.
