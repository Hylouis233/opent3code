from pathlib import Path
import re

root = Path.cwd()
def replace(file, before, after):
    p = root / file
    text = p.read_text()
    assert text.count(before) == 1, (file, before)
    p.write_text(text.replace(before, after))

p = root / '.gitignore'
p.write_text(p.read_text() + '\n# Independent source preview; never commit runtime state.\n.opent3code-preview/\n')
replace('knip.jsonc', '"entry": ["knip-schemas.ts", "mobile-native-client.ts"]', '"entry": ["knip-schemas.ts", "mobile-native-client.ts", "opent3code-preview.mjs"]')
replace('.github/workflows/ci.yml', "git sparse-checkout set --no-cone '/*' '!/*/' '/.github/scripts/'", "git sparse-checkout set --no-cone '/*' '!/.repos/'")
replace('.github/workflows/ci.yml', 'node --test .github/scripts/opent3code-sync.test.cjs .github/scripts/opent3code-pr.test.cjs .github/scripts/opent3code-pr-report.test.cjs', 'node --test .github/scripts/opent3code-*.test.cjs')
replace('docs/operations/development.md', '../../README.md#install-vp', '../../README.md#run-this-fork')
replace('integrations/dsh-opent3code/README.md', 'Install Node.js 24 and pnpm, then the official CLI.', 'Install Node.js 24 and pnpm 11.10.0, then the official CLI.')
replace('integrations/dsh-opent3code/README.md', 'npm install -g @deepseek-ai/dsh@0.1.5-rc.1\n', 'npm install -g @deepseek-ai/dsh@0.1.5-rc.1\n# From this source directory: initialize only a NEW dedicated profile build policy.\nnode prepare-profile.mjs\n')
replace('integrations/dsh-opent3code/README.md', 'Do not add this bundle on top of the existing', 'The setup helper approves only the observed native subprocess, node-pty and koffi versions; other native build scripts remain blocked. It refuses to overwrite an existing or changed profile. A new dependency version requires review, not disabling pnpm build checks. Keep the generated lockfile.\n\nDo not add this bundle on top of the existing')
replace('apps/server/src/provider/Drivers/ExternalAcpDriver.ts', 'readThread: (threadId) => get(threadId).pipe(Effect.as({ threadId, turns: [] })),', 'readThread: (threadId) => get(threadId).pipe(Effect.flatMap(() => Effect.fail(invalid("readThread", "Native transcript export is unavailable; use the OpenT3Code conversation history.")))),')
replace('.github/scripts/opent3code-release.cjs', 'value.sourceOnly !== true || !/^', 'value.sourceOnly !== true || typeof value.version !== "string" || !/^')

# Preserve every inherited job body, but prevent upstream publishing/deployment on the fork.
for name in ['release.yml', 'release-desktop.yml', 'publish-aur.yml', 'mobile-eas-production.yml', 'mobile-eas-preview.yml', 'web-preview.yml']:
    p = root / '.github/workflows' / name
    text = p.read_text()
    start = text.index('\njobs:\n')
    prefix, body = text[:start + len('\njobs:\n')], text[start + len('\njobs:\n'):]
    matches = list(re.finditer(r'^  [a-zA-Z0-9_-]+:\s*$', body, re.M))
    assert matches, name
    result = body[:matches[0].start()]
    for i, match in enumerate(matches):
        block = body[match.start():matches[i+1].start() if i+1 < len(matches) else len(body)]
        lines = block.splitlines(keepends=True)
        found = next((j for j, line in enumerate(lines) if line.startswith('    if:')), None)
        if found is None:
            lines.insert(1, "    if: github.repository == 'pingdotgg/t3code'\n")
        else:
            value = lines[found].split('if:', 1)[1].strip()
            end = found + 1
            if value in ('|', '>-', '>', '|-'):
                chunks = []
                while end < len(lines) and (lines[end].startswith('      ') or not lines[end].strip()):
                    chunks.append(lines[end].strip())
                    end += 1
                value = ' '.join(chunks).strip()
            if value.startswith('${{') and value.endswith('}}'):
                value = value[3:-2].strip()
            assert value and '\n' not in value, name
            lines[found:end] = ["    if: ${{ github.repository == 'pingdotgg/t3code' && (" + value + ") }}\n"]
        result += ''.join(lines)
    p.write_text(prefix + result)

p = root / 'README.md'
text = p.read_text()
text = text.replace('This fork currently inherits the adapters and clients listed below.', 'This fork includes the inherited adapters and the explicit source-preview integrations listed below.')
preview = '''## What works today

### OpenT3Code source-preview integrations

**MiniMax Code and DeepSeek Harness are now native ACP provider options.** Both support supervised text sessions, assistant/reasoning/tool events, one-shot permission requests, cancellation and instance/workspace-bound native resume. They remain opt-in preview integrations; account authentication and model selection stay with the official CLI.

MiniMax Code launches `mcode acp`. DeepSeek Harness launches a dedicated `opent3code` profile provided by the [installable DSH companion bundle](./integrations/dsh-opent3code/README.md). No terminal scraping or account proxy is involved. Unsupported attachment, plan-mode, steering, rollback and background text-generation operations are rejected explicitly. DSH resume does not replay native history.

Start with the [source-preview guide](./docs/user/opent3code-preview.md). This release is source-only, not a signed installer or certification of live paid-account generation on every platform.
'''
assert text.count('## What works today\n') == 1
text = text.replace('## What works today\n', preview)
a = text.index('## Run this fork\n'); b = text.index('## Extending the ecosystem\n', a)
text = text[:a] + '''## Run this fork

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

''' + text[b:]
p.write_text(text)
(root / '.github/SECURITY.md').write_text('''# OpenT3Code security reporting

This is an independently maintained source-preview fork. Its integration code and preview packaging are maintained by Hylouis233, not the upstream T3 team. No independent security certification is claimed.

Do not post API keys, pairing tokens, private prompts, account files or exploit details in a public issue. Use an existing private contact with the maintainer or privately request a reporting channel through the test group. A private GitHub reporting channel must be enabled and verified before it is advertised; this file alone does not enable it.

Issues reproduced in unmodified upstream T3 Code can also be reported through the upstream project's own security policy. Fork-specific integrations and releases are not covered by upstream support or its safe-harbor statements.
''')
(root / '.github/ISSUE_TEMPLATE/config.yml').write_text('''blank_issues_enabled: true
contact_links:
  - name: OpenT3Code integration and preview guide
    url: https://github.com/Hylouis233/opent3code/blob/main/docs/user/opent3code-preview.md
    about: Check installation and capability limits before reporting a fork-specific problem.
''')
