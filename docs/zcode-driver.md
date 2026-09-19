# ZCode provider driver — verified protocol notes and implementation plan

Status: **feasibility confirmed (2026-09-19)**; pure protocol codec committed at
`apps/server/src/provider/Drivers/ZcodeProtocol.ts` with unit tests. The runtime
Effect driver is the remaining work item.

## Why this is now possible

The ZCode CLI has no ACP support, which blocked the `ExternalAcpDriver` path.
It does, however, ship a first-class stdio agent server:

```
node /Applications/ZCode.app/Contents/Resources/glm/zcode.cjs app-server
```

speaking the **ZCode Protocol v1** — a rich, bidirectional JSON protocol. A
full live round trip was verified on 2026-09-19 (zcode.cjs 0.16.5, ZCode.app
3.12.3): `session/create` → `session/send` → `stream.chunk` → `usage.delta`
→ `model_request_completed` → `turn.terminal`, with the model replying.

## Bootstrapping notes

- The standalone CLI cannot locate its built-in provider config; it probes
  `<entrypointDir>/provider/zcode-builtin.json`. The app ships the real file at
  `Resources/config/provider/zcode-builtin.json`; copying it to
  `Resources/glm/provider/` makes `app-server` start standalone.
- Login state lives in the CLI's own storage (`zcode login`); the probe reused
  the desktop app's existing session.

## Frame format (NOT JSON-RPC)

Strict schemas from the bundle (`kKt` request schema, zod `.strict()`):

- Client request: `{"id": <string|int>, "method": "...", "params": {...}}` —
  a `jsonrpc` key is **rejected** (`unrecognized_keys`).
- Response: `{"id": ..., "result": ...}` or `{"id": ..., "error": {code, message, data?}}`.
- Server-origin request: has both `id` and `method` (ids like `"server-1"`).
- Notification: `method` + `params`, no `id`. NDJSON, one frame per line.

**Critical handshake detail:** during `session/create` the server sends
`session/requestRuntimePreferences` and *holds the create response* until the
client replies `{"id": <same>, "result": {...prefs}}`. Verified prefs shape:

```json
{
  "askUserQuestionAutoResolutionEnabled": false,
  "nativeSearchEnhancementsEnabled": false,
  "memoryEnabled": false
}
```

## Method surface (verified from the bundle)

`session/create|resume|list|read|messages|events|subscribe|send|stop|fork|
close|compact|setModel|setThoughtLevel|setMode|subagents|usage|
cancelBackgroundTask|goal|requestRuntimePreferences`, plus
`computer-use/operation-event`, `runtime/capabilities`, `v4/telemetry/event`
notifications and `startup/storageState` boot notifications.

- `session/create` params: `{"workspace": {"workspacePath": ..., "workspaceKey": ...}}`
  (`cwd` is rejected). Result carries a live `projection` snapshot and
  `protocol: {name: "ZCode Protocol", version: 1}`.
- `session/send` params: `{"sessionId": ..., "content": "..."}` → `{accepted, stateRevision}`.
- Turn lifecycle arrives as `v4/telemetry/event` notifications with `kind`:
  `turn.started`, `stream.chunk` (assistant text under `delta`/`text`),
  `usage.delta`, `model.request.status`, `turn.terminal`.

## Remaining implementation plan

1. `packages/contracts`: `ZcodeSettings = makeExternalAcpSettings("zcode.cjs")`-style
   schema (enabled, binaryPath, nodePath) + export.
2. `apps/server/src/provider/Drivers/ZcodeDriver.ts` mirroring `ExternalAcpDriver`:
   - one `app-server` process per thread, spawned via the
     `ChildProcessSpawner` service (same pattern as `AcpSessionRuntime`);
   - line framing with `ZcodeProtocol.dispatchZcodeMessage` (auto-answers
     runtime preferences, resolves/futures pending requests);
   - `session/create` on start (workspace = thread cwd), `session/send` on turn,
     `session/stop` on interrupt, `session/close` + dispose on stop;
   - telemetry mapping: `turn.started` → `turn.started`, `stream.chunk` →
     `content.delta` (reuse `makeAcpContentDeltaEvent`), `turn.terminal` →
     `turn.completed` / `turn.aborted`;
   - refresh/status probe via `session/list`; badge "Preview".
3. Register in `builtInDrivers.ts`, add web meta in `providerDriverMeta.ts`,
   enable the instance in server settings.
4. Approvals (pendingPermissions in the projection), `session/setModel` model
   switching, and `session/resume` continuation are v2 follow-ups; declare
   them unsupported in `ProviderAdapterCapabilities` for v1.
