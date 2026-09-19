/**
 * ZCode Protocol (v1) framing and event mapping — pure helpers.
 *
 * The ZCode CLI (`zcode app-server`) speaks newline-delimited JSON with its
 * own envelope — NOT JSON-RPC: requests are `{id, method, params?, trace?}`
 * (strict schema; a `jsonrpc` key is rejected), responses echo the id with
 * `result`/`error`, and both sides may originate requests. The server asks
 * the client for runtime preferences (`session/requestRuntimePreferences`)
 * mid-handshake and holds `session/create` until the client answers.
 *
 * Verified against zcode.cjs 0.16.5 (bundle request schema `kKt`) and a live
 * round trip on 2026-09-19: spawn → session/create → session/subscribe →
 * session/send → telemetry(turn.started → stream.chunk → usage.delta →
 * model_request_completed → turn.terminal). Session listing works the same
 * way. The runtime driver wiring lives in the follow-up plan documented in
 * `docs/zcode-driver.md`.
 *
 * @module provider/Drivers/ZcodeProtocol
 */

/** Strict request envelope; `jsonrpc` is rejected by the server schema. */
export interface ZcodeRequest {
  readonly id: number | string;
  readonly method: string;
  readonly params?: unknown;
}

export interface ZcodeResponse {
  readonly id: number | string;
  readonly result?: unknown;
  readonly error?: ZcodeProtocolError;
}

export interface ZcodeProtocolError {
  readonly code?: number;
  readonly message?: string;
  readonly data?: unknown;
}

export interface ZcodeNotification {
  readonly method: string;
  readonly params?: unknown;
}

export type ZcodeServerMessage = ZcodeResponse | ZcodeNotification | ZcodeServerRequest;

export interface ZcodeServerRequest {
  readonly id: number | string;
  readonly method: string;
  readonly params?: unknown;
}

export function isZcodeResponse(
  message: ZcodeServerMessage,
): message is ZcodeResponse {
  return (
    "id" in message &&
    message.id !== undefined &&
    ("result" in message || "error" in message) &&
    !("method" in message)
  );
}

export function isZcodeServerRequest(
  message: ZcodeServerMessage,
): message is ZcodeServerRequest {
  return "id" in message && message.id !== undefined && "method" in message;
}

export function isZcodeNotification(
  message: ZcodeServerMessage,
): message is ZcodeNotification {
  return !("id" in message) && "method" in message;
}

/** Preferences the engine asks the client for while materializing a session. */
export const ZCODE_RUNTIME_PREFERENCES = {
  askUserQuestionAutoResolutionEnabled: false,
  nativeSearchEnhancementsEnabled: false,
  memoryEnabled: false,
} as const;

/** Frame a client request as one NDJSON line. */
export function encodeZcodeRequest(request: ZcodeRequest): string {
  return `${JSON.stringify(request)}\n`;
}

/** Frame a reply to a server-origin request. */
export function encodeZcodeReply(id: number | string, result: unknown): string {
  return `${JSON.stringify({ id, result })}\n`;
}

/** Frame a protocol error reply. */
export function encodeZcodeErrorReply(
  id: number | string,
  code: number,
  message: string,
): string {
  return `${JSON.stringify({ id, error: { code, message } })}\n`;
}

export type ZcodeDispatch =
  | { readonly kind: "resolve"; readonly id: number | string; readonly value: unknown }
  | { readonly kind: "reject"; readonly id: number | string; readonly error: Error }
  | { readonly kind: "reply"; readonly line: string }
  | { readonly kind: "ignore" };

/**
 * Pure decision for one decoded server message given the set of server
 * requests already answered — keeps the (untestable) process plumbing thin.
 */
export function dispatchZcodeMessage(
  message: ZcodeServerMessage,
  answered: ReadonlySet<number | string>,
): ZcodeDispatch {
  if (isZcodeServerRequest(message)) {
    if (answered.has(message.id)) return { kind: "ignore" };
    if (message.method === "session/requestRuntimePreferences") {
      return { kind: "reply", line: encodeZcodeReply(message.id, ZCODE_RUNTIME_PREFERENCES) };
    }
    return { kind: "reply", line: encodeZcodeErrorReply(message.id, -32601, "Unsupported client method") };
  }
  if (isZcodeResponse(message)) {
    if (message.error !== undefined) {
      return {
        kind: "reject",
        id: message.id,
        error: new Error(
          `ZCode Protocol ${message.error.message ?? "request failed"} (${message.error.code ?? "?"})`,
        ),
      };
    }
    return { kind: "resolve", id: message.id, value: message.result };
  }
  return { kind: "ignore" };
}

/* --------------------------------------------------------------------------
 * Telemetry mapping (v4/telemetry/event notifications)
 * -------------------------------------------------------------------------- */

export interface ZcodeTelemetryEvent {
  readonly kind?: string;
  readonly turnId?: string;
  readonly sessionId?: string;
  readonly [key: string]: unknown;
}

export function isZcodeTelemetryNotification(message: ZcodeServerMessage): boolean {
  return isZcodeNotification(message) && message.method === "v4/telemetry/event";
}

export function zcodeTelemetryParams(message: ZcodeServerMessage): ZcodeTelemetryEvent | null {
  if (!isZcodeTelemetryNotification(message)) return null;
  const params = (message as ZcodeNotification).params;
  if (typeof params !== "object" || params === null) return null;
  return params as ZcodeTelemetryEvent;
}

/** Extract assistant text from a `stream.chunk` telemetry event, if any. */
export function zcodeStreamChunkText(event: ZcodeTelemetryEvent): string | null {
  if (event.kind !== "stream.chunk") return null;
  for (const key of ["delta", "text", "content", "chunk"] as const) {
    const value = event[key];
    if (typeof value === "string" && value.length > 0) return value;
  }
  return null;
}

export type ZcodeTurnOutcome =
  | { readonly status: "completed" }
  | { readonly status: "aborted"; readonly reason: string };

/** Terminal turn outcome; `turn.terminal` failures map to aborted. */
export function zcodeTurnOutcome(event: ZcodeTelemetryEvent): ZcodeTurnOutcome | null {
  if (event.kind !== "turn.terminal") return null;
  for (const key of ["failure", "error", "reason"] as const) {
    const value = event[key];
    if (typeof value === "string" && value.length > 0) {
      return { status: "aborted", reason: value };
    }
    if (value !== null && typeof value === "object") {
      const message = (value as { readonly message?: unknown }).message;
      if (typeof message === "string" && message.length > 0) {
        return { status: "aborted", reason: message };
      }
    }
  }
  return { status: "completed" };
}

/* --------------------------------------------------------------------------
 * session/list mapping
 * -------------------------------------------------------------------------- */

export interface ZcodeSessionListEntry {
  readonly sessionId?: string;
  readonly title?: string;
  readonly status?: string;
  readonly updatedAt?: number;
  readonly workspace?: { readonly workspacePath?: string };
}

export function zcodeSessionsFromListResult(result: unknown): ReadonlyArray<ZcodeSessionListEntry> {
  if (typeof result !== "object" || result === null) return [];
  const sessions = (result as { readonly sessions?: unknown }).sessions;
  if (!Array.isArray(sessions)) return [];
  return sessions.filter(
    (entry): entry is ZcodeSessionListEntry =>
      typeof entry === "object" &&
      entry !== null &&
      typeof (entry as ZcodeSessionListEntry).sessionId === "string",
  );
}
