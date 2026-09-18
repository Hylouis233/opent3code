// @effect-diagnostics nodeBuiltinImport:off -- Sync temp-store fixtures mirror the reader under test.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { describe, expect, it } from "@effect/vitest";

import {
  mcodeRowToMessage,
  readMcodeCandidates,
  readMcodeThread,
  readZcodeCandidates,
  readZcodeThread,
} from "./SqliteAgentSessions.ts";

function tempDir(prefix: string): string {
  return NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix));
}

function prepare(db: NodeSqlite.DatabaseSync, sql: string): void {
  db.prepare(sql).run();
}

function seedMcodeStore(dbPath: string): void {
  const storeHome = NodePath.dirname(NodePath.dirname(NodePath.dirname(dbPath)));
  const syntheticWorkspace = NodePath.join(storeHome, "sessions", "mvs_0e1f2a3b4c5d6789", "workspace");
  const db = new NodeSqlite.DatabaseSync(dbPath);
  try {
    prepare(
      db,
      "CREATE TABLE local_runtime_sessions (session_id TEXT PRIMARY KEY, title TEXT," +
        " workspace_dir TEXT, created_at_ms INTEGER, updated_at_ms INTEGER," +
        " archived INTEGER NOT NULL DEFAULT 0)",
    );
    prepare(
      db,
      "CREATE TABLE local_runtime_pi_history_rows (id INTEGER PRIMARY KEY AUTOINCREMENT," +
        " session_id TEXT NOT NULL, role TEXT, created_at_ms INTEGER, data_json TEXT NOT NULL)",
    );
    db
      .prepare(
        "INSERT INTO local_runtime_sessions (session_id, title, workspace_dir," +
          " created_at_ms, updated_at_ms, archived) VALUES (?, ?, ?, ?, ?, 0)",
      )
      .run("mvs_keep", "保留会话", "/tmp/real-project", 1_000, 2_000);
    db
      .prepare(
        "INSERT INTO local_runtime_sessions (session_id, title, workspace_dir," +
          " created_at_ms, updated_at_ms, archived) VALUES (?, ?, ?, ?, ?, 0)",
      )
      .run("mvs_synth", "自动化", syntheticWorkspace, 1_000, 3_000);
    db
      .prepare(
        "INSERT INTO local_runtime_pi_history_rows (session_id, role, created_at_ms, data_json)" +
          " VALUES (?, ?, ?, ?)",
      )
      .run("mvs_keep", "user", 1_100, JSON.stringify({ role: "user", content: "看下报错" }));
    db
      .prepare(
        "INSERT INTO local_runtime_pi_history_rows (session_id, role, created_at_ms, data_json)" +
          " VALUES (?, ?, ?, ?)",
      )
      .run(
        "mvs_keep",
        "assistant",
        1_200,
        JSON.stringify({
          role: "assistant",
          content: [
            { type: "thinking", thinking: "内部推理" },
            { type: "text", text: "原因是 X" },
          ],
        }),
      );
  } finally {
    db.close();
  }
}

function seedZcodeStore(dbPath: string): void {
  const db = new NodeSqlite.DatabaseSync(dbPath);
  try {
    prepare(
      db,
      "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, title TEXT," +
        " time_created INTEGER, time_updated INTEGER, time_archived INTEGER)",
    );
    prepare(
      db,
      "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL," +
        " time_created INTEGER, sequence INTEGER, data TEXT NOT NULL)",
    );
    prepare(
      db,
      "CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL, session_id TEXT NOT NULL," +
        " sequence INTEGER, data TEXT NOT NULL)",
    );
    db
      .prepare(
        "INSERT INTO session (id, directory, title, time_created, time_updated, time_archived)" +
          " VALUES (?, ?, ?, ?, ?, NULL)",
      )
      .run("sess_1", "/tmp/real-project", "ZCode 会话", 5_000, 6_000);
    db
      .prepare("INSERT INTO message (id, session_id, time_created, sequence, data) VALUES (?, ?, ?, ?, ?)")
      .run(
        "msg_1",
        "sess_1",
        5_100,
        1,
        JSON.stringify({
          role: "user",
          semantics: { origin: "real_user" },
          metadata: { inputIntent: { text: "继续迁移" } },
        }),
      );
    db
      .prepare("INSERT INTO message (id, session_id, time_created, sequence, data) VALUES (?, ?, ?, ?, ?)")
      .run("msg_2", "sess_1", 5_200, 2, JSON.stringify({ role: "assistant" }));
    db
      .prepare("INSERT INTO part (id, message_id, session_id, sequence, data) VALUES (?, ?, ?, ?, ?)")
      .run("part_1", "msg_2", "sess_1", 1, JSON.stringify({ type: "text", text: "迁移完成一半" }));
    db
      .prepare("INSERT INTO part (id, message_id, session_id, sequence, data) VALUES (?, ?, ?, ?, ?)")
      .run("part_2", "msg_2", "sess_1", 2, JSON.stringify({ type: "reasoning", text: "隐藏思考" }));
  } finally {
    db.close();
  }
}

describe("SqliteAgentSessions mcode", () => {
  it("keeps real-workspace sessions and drops synthetic per-session workspaces", () => {
    const dir = tempDir("sqlite-agent-mcode-");
    try {
      // Real store layout: <home>/v2/sqlite/runtime-state.sqlite; the
      // synthetic filter keys off <home>/sessions/ next to it.
      const dbPath = NodePath.join(dir, "v2", "sqlite", "runtime-state.sqlite");
      NodeFS.mkdirSync(NodePath.dirname(dbPath), { recursive: true });
      seedMcodeStore(dbPath);
      const candidates = readMcodeCandidates(dbPath);
      expect(candidates.map((candidate) => candidate.providerSessionId)).toEqual(["mvs_keep"]);
      expect(candidates[0]?.cwd).toBe("/tmp/real-project");
    } finally {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("maps history to prose, dropping thinking blocks and tool rows", () => {
    const dir = tempDir("sqlite-agent-mcode-thread-");
    try {
      const dbPath = NodePath.join(dir, "runtime-state.sqlite");
      seedMcodeStore(dbPath);
      const thread = readMcodeThread(dbPath, "mvs_keep");
      expect(thread?.messages).toEqual([
        { role: "user", text: "看下报错", createdAtMs: 1_100 },
        { role: "assistant", text: "原因是 X", createdAtMs: 1_200 },
      ]);
      expect(readMcodeThread(dbPath, "mvs_synth")).toBeNull();
      expect(readMcodeThread(NodePath.join(dir, "missing.sqlite"), "mvs_keep")).toBeNull();
    } finally {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("strips the system-reminder agent-context blob from user turns", () => {
    const injected = `<system-reminder><agent-context>agent: Mavis</agent-context></system-reminder>\n\n不修改任何东西，帮我看看`;
    expect(
      mcodeRowToMessage("user", JSON.stringify({ role: "user", content: injected }), 1),
    ).toEqual({ role: "user", text: "不修改任何东西，帮我看看", createdAtMs: 1 });
    expect(
      mcodeRowToMessage("user", JSON.stringify({ role: "user", content: "<system-reminder>only" }), 2),
    ).toBeNull();
  });
});

describe("SqliteAgentSessions zcode", () => {
  it("reads user prompts and assistant text parts, skipping reasoning", () => {
    const dir = tempDir("sqlite-agent-zcode-");
    try {
      const dbPath = NodePath.join(dir, "db.sqlite");
      seedZcodeStore(dbPath);
      expect(readZcodeCandidates(dbPath).map((candidate) => candidate.providerSessionId)).toEqual([
        "sess_1",
      ]);
      const thread = readZcodeThread(dbPath, "sess_1");
      expect(thread?.title).toBe("ZCode 会话");
      expect(thread?.messages).toEqual([
        { role: "user", text: "继续迁移", createdAtMs: 5_100 },
        { role: "assistant", text: "迁移完成一半", createdAtMs: 5_200 },
      ]);
      expect(readZcodeThread(NodePath.join(dir, "missing.sqlite"), "sess_1")).toBeNull();
    } finally {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });
});
