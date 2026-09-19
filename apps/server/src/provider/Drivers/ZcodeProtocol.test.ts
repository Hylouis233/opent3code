import { describe, expect, it } from "@effect/vitest";

import {
  dispatchZcodeMessage,
  encodeZcodeReply,
  encodeZcodeRequest,
  isZcodeNotification,
  isZcodeServerRequest,
  zcodeSessionsFromListResult,
  zcodeStreamChunkText,
  zcodeTelemetryParams,
  zcodeTurnOutcome,
  ZCODE_RUNTIME_PREFERENCES,
  type ZcodeServerMessage,
} from "./ZcodeProtocol.ts";

const parseLine = (line: string): unknown => JSON.parse(line.trim());

describe("ZcodeProtocol framing", () => {
  it("encodes requests without a jsonrpc key", () => {
    const line = encodeZcodeRequest({ id: 7, method: "session/list", params: {} });
    expect(parseLine(line)).toEqual({ id: 7, method: "session/list", params: {} });
  });

  it("answers runtime preferences once, then ignores repeats", () => {
    const request: ZcodeServerMessage = {
      id: "server-1",
      method: "session/requestRuntimePreferences",
      params: { sessionId: "sess_x", scope: "runtime-materialization" },
    };
    const first = dispatchZcodeMessage(request, new Set());
    expect(first.kind).toBe("reply");
    if (first.kind === "reply") {
      expect(parseLine(first.line)).toEqual({
        id: "server-1",
        result: ZCODE_RUNTIME_PREFERENCES,
      });
    }
    const second = dispatchZcodeMessage(request, new Set(["server-1"]));
    expect(second.kind).toBe("ignore");
  });

  it("refuses unknown server requests so the engine does not hang", () => {
    const outcome = dispatchZcodeMessage(
      { id: 9, method: "session/somethingNew", params: {} },
      new Set(),
    );
    expect(outcome.kind).toBe("reply");
    if (outcome.kind === "reply") {
      expect(parseLine(outcome.line)).toEqual({
        id: 9,
        error: { code: -32601, message: "Unsupported client method" },
      });
    }
  });

  it("resolves responses and rejects error responses with a readable message", () => {
    const ok = dispatchZcodeMessage({ id: 2, result: { sessions: [] } }, new Set());
    expect(ok).toMatchObject({ kind: "resolve", id: 2, value: { sessions: [] } });

    const bad = dispatchZcodeMessage(
      { id: 3, error: { code: -32602, message: "Invalid params" } },
      new Set(),
    );
    expect(bad.kind).toBe("reject");
    if (bad.kind === "reject") {
      expect(bad.error.message).toContain("Invalid params");
    }
  });

  it("classifies notifications, server requests, and responses", () => {
    expect(isZcodeNotification({ method: "state.updated", params: {} })).toBe(true);
    expect(
      isZcodeServerRequest({ id: "s1", method: "session/requestRuntimePreferences" }),
    ).toBe(true);
    expect(isZcodeServerRequest({ id: 4, result: null })).toBe(false);
  });

  it("replies use the bare id+result envelope", () => {
    expect(parseLine(encodeZcodeReply("server-9", { ok: true }))).toEqual({
      id: "server-9",
      result: { ok: true },
    });
  });
});

describe("ZcodeProtocol telemetry mapping", () => {
  const telemetry = (event: Record<string, unknown>): ZcodeServerMessage => ({
    method: "v4/telemetry/event",
    params: { sessionId: "sess_a", ...event },
  });

  it("extracts assistant text from stream.chunk across payload spellings", () => {
    expect(zcodeStreamChunkText({ kind: "stream.chunk", delta: "你好" })).toBe("你好");
    expect(zcodeStreamChunkText({ kind: "stream.chunk", text: "world" })).toBe("world");
    expect(zcodeStreamChunkText({ kind: "stream.chunk" })).toBeNull();
    expect(zcodeStreamChunkText({ kind: "turn.started" })).toBeNull();
  });

  it("maps turn.terminal to completed or aborted-with-reason", () => {
    expect(zcodeTurnOutcome({ kind: "turn.terminal" })).toEqual({ status: "completed" });
    expect(
      zcodeTurnOutcome({ kind: "turn.terminal", failure: { message: "quota exceeded" } }),
    ).toEqual({ status: "aborted", reason: "quota exceeded" });
    expect(zcodeTurnOutcome({ kind: "turn.terminal", reason: "cancelled" })).toEqual({
      status: "aborted",
      reason: "cancelled",
    });
    expect(zcodeTurnOutcome({ kind: "stream.chunk" })).toBeNull();
  });

  it("reads telemetry params only from the v4 notification method", () => {
    const event = zcodeTelemetryParams(telemetry({ kind: "turn.started", turnId: "t1" }));
    expect(event?.kind).toBe("turn.started");
    expect(event?.turnId).toBe("t1");
    expect(zcodeTelemetryParams({ method: "state.updated", params: {} })).toBeNull();
  });
});

describe("ZcodeProtocol session list", () => {
  it("keeps entries with a string sessionId and drops the rest", () => {
    const result = zcodeSessionsFromListResult({
      sessions: [
        { sessionId: "sess_1", title: "接手会话", status: "idle", workspace: { workspacePath: "/w" } },
        { title: "no id" },
        null,
      ],
    });
    expect(result).toHaveLength(1);
    expect(result[0]?.sessionId).toBe("sess_1");
    expect(result[0]?.title).toBe("接手会话");
    expect(zcodeSessionsFromListResult(undefined)).toEqual([]);
    expect(zcodeSessionsFromListResult({ sessions: "nope" })).toEqual([]);
  });
});
