// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { describe, expect, it } from "@effect/vitest";

import {
  listTranscriptFiles,
  probeMcodeUsageStore,
  readTranscriptRecords,
  statSqliteUsageStore,
  statUsageFile,
} from "./usageTranscriptReader.ts";

function createOpenCodexLedger(lines: readonly string[]): {
  readonly filePath: string;
  readonly cleanup: () => void;
} {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencodex-usage-"));
  const filePath = NodePath.join(dir, "usage.jsonl");
  NodeFS.writeFileSync(filePath, `${lines.join("\n")}\n`);
  return {
    filePath,
    cleanup: () => NodeFS.rmSync(dir, { recursive: true, force: true }),
  };
}

interface McodeRow {
  readonly id: number;
  readonly sessionId?: string;
  readonly model?: string | null;
  readonly timestampMs?: number;
  readonly inputTokens?: number;
  readonly outputTokens?: number;
  readonly reasoningTokens?: number;
  readonly cacheReadTokens?: number;
  readonly cacheWriteTokens?: number;
  readonly costUsd?: number | null;
}

function createMcodeDb(rows: readonly McodeRow[]): { dbPath: string; cleanup: () => void } {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-mcode-usage-"));
  const dbPath = NodePath.join(dir, "runtime-state.sqlite");
  const db = new NodeSqlite.DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE local_runtime_token_usage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      agent_name TEXT NOT NULL,
      framework_type TEXT NOT NULL,
      turn_id TEXT,
      model TEXT,
      ts INTEGER NOT NULL,
      input_tokens INTEGER NOT NULL,
      output_tokens INTEGER NOT NULL,
      reasoning_tokens INTEGER NOT NULL,
      cache_read_tokens INTEGER NOT NULL,
      cache_write_tokens INTEGER NOT NULL,
      cost_usd REAL,
      raw TEXT
    );
    CREATE INDEX idx_local_runtime_token_usage_ts
      ON local_runtime_token_usage(ts, id);
  `);
  const insert = db.prepare(`
    INSERT INTO local_runtime_token_usage (
      id, session_id, agent_name, framework_type, turn_id, model, ts,
      input_tokens, output_tokens, reasoning_tokens,
      cache_read_tokens, cache_write_tokens, cost_usd, raw
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  for (const row of rows) {
    insert.run(
      row.id,
      row.sessionId ?? "session-1",
      "general",
      "pi-agent",
      "turn-1",
      row.model === undefined ? "minimax/MiniMax-M3" : row.model,
      row.timestampMs ?? 1_786_000_000_000,
      row.inputTokens ?? 0,
      row.outputTokens ?? 0,
      row.reasoningTokens ?? 0,
      row.cacheReadTokens ?? 0,
      row.cacheWriteTokens ?? 0,
      row.costUsd ?? 0,
      "{}",
    );
  }
  db.close();
  return {
    dbPath,
    cleanup: () => NodeFS.rmSync(dir, { recursive: true, force: true }),
  };
}

describe("readTranscriptRecords for opencodex", () => {
  it("streams only measurable rows inside the requested window", async () => {
    const cutoffMs = 1_786_000_000_000;
    const before = JSON.stringify({
      requestId: "before",
      timestamp: cutoffMs - 1,
      provider: "openai",
      model: "gpt-5.4",
      usage: { inputTokens: 10, outputTokens: 5 },
    });
    const inside = JSON.stringify({
      requestId: "req-2",
      timestamp: cutoffMs,
      provider: "openai-p372059",
      model: "gpt-5.4",
      conversationId: "session-a",
      usage: {
        inputTokens: 1_050,
        outputTokens: 45,
        reasoningOutputTokens: 12,
        cachedInputTokens: 900,
        cacheReadInputTokens: 900,
        cacheCreationInputTokens: 30,
      },
    });
    const { filePath, cleanup } = createOpenCodexLedger([before, "{broken", inside]);

    try {
      expect(await readTranscriptRecords(filePath, "opencodex", cutoffMs)).toEqual([
        {
          provider: "opencodex",
          timestampMs: cutoffMs,
          model: "openai/gpt-5.4",
          sessionId: "session-a",
          totals: {
            uncachedInputTokens: 120,
            cachedInputTokens: 900,
            cacheCreationTokens: 30,
            outputTokens: 45,
            reasoningTokens: 12,
          },
          reportedCostUsd: null,
          dedupeKey: "opencodex:req-2",
  kimiSessionIdFromTranscriptPath,
  listTranscriptFiles,
  readTranscriptRecords,
  resolveKimiDesktopDataDir,
} from "./usageTranscriptReader.ts";

function createKimiTranscript(lines: readonly string[]): {
  readonly filePath: string;
  readonly cleanup: () => void;
} {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-kimi-usage-"));
  const filePath = NodePath.join(
    dir,
    "sessions",
    "wd_demo_123",
    "session-123",
    "agents",
    "main",
    "wire.jsonl",
  );
  NodeFS.mkdirSync(NodePath.dirname(filePath), { recursive: true });
  NodeFS.writeFileSync(filePath, `${lines.join("\n")}\n`);
  return { filePath, cleanup: () => NodeFS.rmSync(dir, { recursive: true, force: true }) };
}

function usageRecord(input: {
  readonly time: number;
  readonly scope?: string;
  readonly model?: string;
  readonly inputOther?: number;
  readonly inputCacheRead?: number;
  readonly inputCacheCreation?: number;
  readonly output?: number;
}): string {
  return JSON.stringify({
    type: "usage.record",
    model: input.model ?? "kimi-code/k3",
    usage: {
      inputOther: input.inputOther ?? 100,
      inputCacheRead: input.inputCacheRead ?? 200,
      inputCacheCreation: input.inputCacheCreation ?? 30,
      output: input.output ?? 40,
    },
    usageScope: input.scope ?? "turn",
    time: input.time,
  });
}

describe("readTranscriptRecords for Kimi Code", () => {
  it("reads turn-scoped usage and respects the requested coverage bound", async () => {
    const { filePath, cleanup } = createKimiTranscript([
      JSON.stringify({ type: "metadata", created_at: 1_000 }),
      usageRecord({ time: 1_500, inputOther: 999 }),
      usageRecord({ time: 2_000 }),
      usageRecord({ time: 2_500, scope: "session", inputOther: 999 }),
      "not-json",
    ]);
    try {
      expect(await readTranscriptRecords(filePath, "kimi", 2_000)).toEqual([
        {
          provider: "kimi",
          timestampMs: 2_000,
          model: "kimi-code/k3",
          sessionId: "session-123",
          totals: {
            uncachedInputTokens: 100,
            cachedInputTokens: 200,
            cacheCreationTokens: 30,
            outputTokens: 40,
            reasoningTokens: 0,
          },
          reportedCostUsd: null,
          dedupeKey: null,
        },
      ]);
    } finally {
      cleanup();
    }
  });

  it("does not turn read failures into cacheable empty usage", async () => {
    expect(
      await readTranscriptRecords(
        NodePath.join(NodeOS.tmpdir(), "t3-opencodex-no-such-dir", "usage.jsonl"),
        "opencodex",
        0,
      ),
    ).toBeNull();
  });

  it("fingerprints the append-only ledger", async () => {
    const { filePath, cleanup } = createOpenCodexLedger([]);
    try {
      const stats = NodeFS.statSync(filePath);
      expect(await statUsageFile(filePath, 0)).toEqual([
        { path: filePath, size: stats.size, mtimeMs: stats.mtimeMs },
      ]);
      expect(await statUsageFile(filePath, stats.mtimeMs + 1)).toEqual([]);
    } finally {
      cleanup();
    }
  });

  it("distinguishes a stat failure from an empty in-window ledger", async () => {
    expect(
      await statUsageFile(
        NodePath.join(NodeOS.tmpdir(), "t3-opencodex-missing-stat", "usage.jsonl"),
        0,
      ),
    ).toBeNull();
  });

  it("reports a transcript root that disappears before the walk", async () => {
    const missingRoot = NodePath.join(NodeOS.tmpdir(), "t3-usage-missing-transcript-root");
    expect(await listTranscriptFiles(missingRoot, 0)).toEqual({
      files: [],
      failedEntries: 1,
    });
  });
});

describe("readTranscriptRecords for mcode", () => {
  it("reads only token rows inside the requested window", async () => {
    const cutoffMs = 1_786_000_000_000;
    const { dbPath, cleanup } = createMcodeDb([
      { id: 1, timestampMs: cutoffMs - 1, outputTokens: 10 },
      {
        id: 2,
        sessionId: "session-a",
        timestampMs: cutoffMs,
        inputTokens: 120,
        outputTokens: 45,
        reasoningTokens: 12,
        cacheReadTokens: 900,
        cacheWriteTokens: 30,
      },
    ]);
    try {
      expect(await readTranscriptRecords(dbPath, "mcode", cutoffMs)).toEqual([
        {
          provider: "mcode",
          timestampMs: cutoffMs,
          model: "minimax/MiniMax-M3",
          sessionId: "session-a",
          totals: {
            uncachedInputTokens: 120,
            cachedInputTokens: 900,
            cacheCreationTokens: 30,
            outputTokens: 45,
            reasoningTokens: 12,
          },
          reportedCostUsd: null,
          dedupeKey: "mcode:2",
        },
      ]);
    } finally {
      cleanup();
    }
  });

  it("detects which compatibility database has canonical usage accounting", async () => {
    const { dbPath, cleanup } = createMcodeDb([]);
    const emptyPath = NodePath.join(NodePath.dirname(dbPath), "empty.sqlite");
    const incompatiblePath = NodePath.join(NodePath.dirname(dbPath), "incompatible.sqlite");
    const corruptPath = NodePath.join(NodePath.dirname(dbPath), "corrupt.sqlite");
    const missingPath = NodePath.join(NodePath.dirname(dbPath), "missing.sqlite");
    new NodeSqlite.DatabaseSync(emptyPath).close();
    const incompatible = new NodeSqlite.DatabaseSync(incompatiblePath);
    incompatible.exec("CREATE TABLE local_runtime_token_usage (id INTEGER PRIMARY KEY)");
    incompatible.close();
    NodeFS.writeFileSync(corruptPath, "not a sqlite database");
    try {
      expect(await probeMcodeUsageStore(dbPath)).toBe("ready");
      expect(await probeMcodeUsageStore(emptyPath)).toBe("absent");
      expect(await probeMcodeUsageStore(incompatiblePath)).toBe("absent");
      expect(await probeMcodeUsageStore(corruptPath)).toBe("failed");
      expect(await probeMcodeUsageStore(missingPath)).toBe("absent");
    } finally {
      cleanup();
    }
  });

  it("does not turn transient failures into cacheable empty usage", async () => {
    expect(
      await readTranscriptRecords(
        NodePath.join(NodeOS.tmpdir(), "t3-mcode-no-such-dir", "runtime-state.sqlite"),
        "mcode",
        0,
      ),
    ).toBeNull();
  });

  it("treats a pre-accounting database as an empty store", async () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-mcode-empty-"));
    const dbPath = NodePath.join(dir, "runtime-state.sqlite");
    new NodeSqlite.DatabaseSync(dbPath).close();
    try {
      expect(await readTranscriptRecords(dbPath, "mcode", 0)).toEqual([]);
    } finally {
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("includes the SQLite WAL in the scan fingerprint", async () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-mcode-wal-"));
    const dbPath = NodePath.join(dir, "runtime-state.sqlite");
    const db = new NodeSqlite.DatabaseSync(dbPath);
    try {
      db.exec("PRAGMA journal_mode = WAL; CREATE TABLE usage (id TEXT)");
      db.prepare("INSERT INTO usage VALUES (?)").run("new-row");

      const dbStats = NodeFS.statSync(dbPath);
      const walStats = NodeFS.statSync(`${dbPath}-wal`);
      expect(await statSqliteUsageStore(dbPath, 0)).toEqual([
        {
          path: dbPath,
          size: dbStats.size + walStats.size,
          mtimeMs: Math.max(dbStats.mtimeMs, walStats.mtimeMs),
        },
      ]);
    } finally {
      db.close();
      NodeFS.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("distinguishes a stat failure from an empty in-window store", async () => {
    expect(
      await statSqliteUsageStore(
        NodePath.join(NodeOS.tmpdir(), "t3-mcode-missing-stat", "runtime-state.sqlite"),
        0,
      ),
    ).toBeNull();
  });

  it("reports a transcript root that disappears before the walk", async () => {
    const missingRoot = NodePath.join(NodeOS.tmpdir(), "t3-usage-missing-transcript-root");
    expect(await listTranscriptFiles(missingRoot, 0)).toEqual({
      files: [],
      failedEntries: 1,
    });
  });
  it("returns null when a transcript cannot be read", async () => {
    expect(
      await readTranscriptRecords(
        NodePath.join(NodeOS.tmpdir(), "t3-kimi-no-such-dir", "wire.jsonl"),
        "kimi",
      ),
    ).toBeNull();
  });
});

describe("kimiSessionIdFromTranscriptPath", () => {
  it("extracts the session id from TUI, desktop, and subagent paths", () => {
    expect(
      kimiSessionIdFromTranscriptPath(
        NodePath.join("home", "sessions", "wd_demo", "session-a", "agents", "main", "wire.jsonl"),
      ),
    ).toBe("session-a");
    expect(
      kimiSessionIdFromTranscriptPath(
        NodePath.join(
          "kimi-desktop",
          "runtime",
          "kimi-code",
          "home",
          "sessions",
          "wd_demo",
          "conv-b",
          "agents",
          "agent-2",
          "wire.jsonl",
        ),
      ),
    ).toBe("conv-b");
  });
});

describe("resolveKimiDesktopDataDir", () => {
  it("prefers an explicit desktop data directory", () => {
    expect(
      resolveKimiDesktopDataDir(
        { KIMI_DESKTOP_DATA_DIR: "/custom/desktop" },
        "linux",
        "/home/user",
      ),
    ).toBe("/custom/desktop");
  });

  it("uses Electron's platform data roots", () => {
    expect(
      resolveKimiDesktopDataDir(
        { APPDATA: "C:\\Users\\me\\AppData\\Roaming" },
        "win32",
        "C:\\Users\\me",
      ),
    ).toBe(NodePath.join("C:\\Users\\me\\AppData\\Roaming", "kimi-desktop"));
    expect(resolveKimiDesktopDataDir({}, "darwin", "/Users/me")).toBe(
      NodePath.join("/Users/me", "Library", "Application Support", "kimi-desktop"),
    );
    expect(resolveKimiDesktopDataDir({ XDG_CONFIG_HOME: "/xdg" }, "linux", "/home/me")).toBe(
      NodePath.join("/xdg", "kimi-desktop"),
    );
  });

  it("falls back to the conventional Windows and Linux data roots", () => {
    expect(resolveKimiDesktopDataDir({}, "win32", "C:\\Users\\me")).toBe(
      NodePath.join("C:\\Users\\me", "AppData", "Roaming", "kimi-desktop"),
    );
    expect(resolveKimiDesktopDataDir({}, "linux", "/home/me")).toBe(
      NodePath.join("/home/me", ".config", "kimi-desktop"),
    );
  });
});

describe("listTranscriptFiles", () => {
  it("reports a missing root as a failed listing", async () => {
    const listing = await listTranscriptFiles(
      NodePath.join(NodeOS.tmpdir(), "t3-kimi-listing-missing"),
      0,
    );
    expect(listing).toEqual({ files: [], failedEntries: 1 });
  });

  it("lists recent wire transcripts and ignores non-jsonl files", async () => {
    const { filePath, cleanup } = createKimiTranscript([usageRecord({ time: 2_000 })]);
    NodeFS.writeFileSync(NodePath.join(NodePath.dirname(filePath), "state.json"), "{}");
    try {
      const root = NodePath.join(NodePath.dirname(filePath), "..", "..", "..");
      const listing = await listTranscriptFiles(root, 0);
      expect(listing.failedEntries).toBe(0);
      expect(listing.files.map((file) => file.path)).toEqual([filePath]);
    } finally {
      cleanup();
    }
  });
});
