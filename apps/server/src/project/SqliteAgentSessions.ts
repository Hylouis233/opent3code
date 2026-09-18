// @effect-diagnostics nodeBuiltinImport:off -- Read-only SQLite readers sit below the Effect service boundary, mirroring usageTranscriptReader.
/**
 * SQLite-backed agent session sources for the import scanner.
 *
 * mcode (MiniMax Code) and zcode keep their history in SQLite instead of the
 * per-session JSONL transcripts Claude Code and Codex write. This module owns
 * the read-only mapping from those stores to the scanner's thread model:
 *
 * - mcode: `~/.minimax/v2/sqlite/runtime-state.sqlite`
 *     local_runtime_sessions      one row per session (title, workspace_dir, ms)
 *     local_runtime_pi_history_rows  ordered messages as Anthropic-style JSON
 * - zcode: `~/.zcode/cli/db/db.sqlite`
 *     session  one row per session (title, directory, ms)
 *     message  message skeletons; user text lives in metadata.inputIntent.text
 *     part     content fragments keyed by message (type "text" carries prose)
 *
 * Every query is bounded and read-only. A missing, locked, or corrupt store
 * yields `null`/empty results so discovery degrades per source instead of
 * failing the scan. Imported threads from these sources are read-only history:
 * zcode has no ACP driver, and mcode resume cursors are not reconstructed
 * here (TODO(resume)).
 *
 * @module project/SqliteAgentSessions
 */
import * as NodeSqlite from "node:sqlite";
import * as NodePath from "node:path";

/** Matches the scanner-wide import caps; sqlite reads share the same ceilings. */
const MAX_SESSION_MESSAGES = 200;
const MAX_SESSION_CANDIDATES = 5000;
const SQLITE_BUSY_TIMEOUT_MS = 1_000;

export interface SqliteSessionCandidate {
  readonly providerSessionId: string;
  readonly cwd: string;
  readonly title: string;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
}

export interface SqliteSessionMessage {
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly createdAtMs: number;
}

export interface SqliteSessionThread {
  readonly providerSessionId: string;
  readonly title: string;
  readonly cwd: string;
  readonly createdAtMs: number;
  readonly updatedAtMs: number;
  readonly messages: ReadonlyArray<SqliteSessionMessage>;
}

export const MCODE_DEFAULT_DB = (home: string): string =>
  NodePath.join(home, "v2", "sqlite", "runtime-state.sqlite");

export const ZCODE_DEFAULT_DB = (home: string): string =>
  NodePath.join(home, ".zcode", "cli", "db", "db.sqlite");

function openReadOnly(dbPath: string): NodeSqlite.DatabaseSync | null {
  try {
    return new NodeSqlite.DatabaseSync(dbPath, {
      readOnly: true,
      timeout: SQLITE_BUSY_TIMEOUT_MS,
    });
  } catch {
    return null;
  }
}

function isoOrNull(ms: unknown): number {
  return typeof ms === "number" && Number.isFinite(ms) && ms > 0 ? ms : 0;
}

/* --------------------------------------------------------------------------
 * mcode
 * -------------------------------------------------------------------------- */

interface McodeContentBlock {
  readonly type?: unknown;
  readonly text?: unknown;
  readonly thinking?: unknown;
}

/** Extract prose from an Anthropic-style content field (string or block list). */
function mcodeText(content: unknown): string {
  if (typeof content === "string") return content.trim();
  if (!Array.isArray(content)) return "";
  const parts: string[] = [];
  for (const block of content as ReadonlyArray<McodeContentBlock>) {
    if (typeof block !== "object" || block === null) continue;
    if (block.type === "text" && typeof block.text === "string" && block.text.length > 0) {
      parts.push(block.text.trim());
    }
  }
  return parts.join("\n");
}

/**
 * mcode prepends a `<system-reminder>` agent-context blob to the user's turn;
 * the human's prompt follows the closing tag. Strip the injection and keep
 * only the real prompt; rows that carry nothing else are not user prose.
 */
function stripMcodeSystemReminder(text: string): string {
  const marker = "</system-reminder>";
  const index = text.lastIndexOf(marker);
  const stripped = index === -1 ? text : text.slice(index + marker.length);
  return stripped.trim();
}

export function mcodeRowToMessage(
  role: unknown,
  dataJson: unknown,
  createdAtMs: number,
): SqliteSessionMessage | null {
  if (typeof dataJson !== "string") return null;
  let parsed: { role?: unknown; content?: unknown };
  try {
    parsed = JSON.parse(dataJson) as { role?: unknown; content?: unknown };
  } catch {
    return null;
  }
  if (parsed.role !== "user" && parsed.role !== "assistant") return null;
  const text = mcodeText(parsed.content);
  if (text.length === 0) return null;
  const prose = parsed.role === "user" ? stripMcodeSystemReminder(text) : text;
  if (prose.length === 0) return null;
  return {
    role: parsed.role,
    text: prose,
    createdAtMs: isoOrNull(createdAtMs),
  };
}

/** Sessions grouped per workspace, newest first, bounded. */
export function readMcodeCandidates(dbPath: string): ReadonlyArray<SqliteSessionCandidate> {
  const db = openReadOnly(dbPath);
  if (db === null) return [];
  try {
    const rows = db
      .prepare(
        "SELECT session_id, title, workspace_dir, created_at_ms, updated_at_ms" +
          " FROM local_runtime_sessions" +
          " WHERE archived = 0 AND workspace_dir IS NOT NULL AND workspace_dir != ''" +
          " ORDER BY updated_at_ms DESC LIMIT ?",
      )
      .all(MAX_SESSION_CANDIDATES) as Array<Record<string, unknown>>;
    return rows.flatMap((row): SqliteSessionCandidate[] => {
      const id = row["session_id"];
      const cwd = row["workspace_dir"];
      if (typeof id !== "string" || id.length === 0 || typeof cwd !== "string") return [];
      return [
        {
          providerSessionId: id,
          cwd,
          title: typeof row["title"] === "string" && row["title"].length > 0 ? row["title"] : id,
          createdAtMs: isoOrNull(row["created_at_ms"]),
          updatedAtMs: isoOrNull(row["updated_at_ms"]),
        },
      ];
    });
  } catch {
    return [];
  } finally {
    db.close();
  }
}

export function readMcodeThread(
  dbPath: string,
  providerSessionId: string,
): SqliteSessionThread | null {
  const db = openReadOnly(dbPath);
  if (db === null) return null;
  try {
    const row = db
      .prepare(
        "SELECT session_id, title, workspace_dir, created_at_ms, updated_at_ms" +
          " FROM local_runtime_sessions WHERE session_id = ?",
      )
      .get(providerSessionId) as Record<string, unknown> | undefined;
    if (row === undefined || typeof row["workspace_dir"] !== "string") return null;
    const messageRows = db
      .prepare(
        "SELECT role, data_json, created_at_ms FROM local_runtime_pi_history_rows" +
          " WHERE session_id = ? ORDER BY id LIMIT ?",
      )
      .all(providerSessionId, MAX_SESSION_MESSAGES) as Array<Record<string, unknown>>;
    const messages: SqliteSessionMessage[] = [];
    for (const messageRow of messageRows) {
      const message = mcodeRowToMessage(
        messageRow["role"],
        messageRow["data_json"],
        isoOrNull(messageRow["created_at_ms"]),
      );
      if (message !== null) messages.push(message);
    }
    if (messages.length === 0) return null;
    return {
      providerSessionId,
      title: typeof row["title"] === "string" && row["title"].length > 0 ? row["title"] : providerSessionId,
      cwd: row["workspace_dir"],
      createdAtMs: isoOrNull(row["created_at_ms"]),
      updatedAtMs: isoOrNull(row["updated_at_ms"]),
      messages,
    };
  } catch {
    return null;
  } finally {
    db.close();
  }
}

/* --------------------------------------------------------------------------
 * zcode
 * -------------------------------------------------------------------------- */

interface ZcodeMessageData {
  readonly role?: unknown;
  readonly semantics?: { readonly origin?: unknown };
  readonly metadata?: {
    readonly inputIntent?: { readonly text?: unknown };
  };
}

export function zcodeRowToUserMessage(dataJson: unknown, createdAtMs: number): SqliteSessionMessage | null {
  if (typeof dataJson !== "string") return null;
  let parsed: ZcodeMessageData;
  try {
    parsed = JSON.parse(dataJson) as ZcodeMessageData;
  } catch {
    return null;
  }
  if (parsed.role !== "user") return null;
  // Only real prompts carry importable prose; steering and system rows do not.
  if (parsed.semantics?.origin !== "real_user") return null;
  const text = parsed.metadata?.inputIntent?.text;
  if (typeof text !== "string" || text.trim().length === 0) return null;
  return { role: "user", text: text.trim(), createdAtMs };
}

/** Assistant prose lives in `part` rows of type "text" attached to the message. */
export function zcodePartsToAssistantMessage(
  dataJson: unknown,
  partTexts: ReadonlyArray<string>,
  createdAtMs: number,
): SqliteSessionMessage | null {
  if (partTexts.length === 0) return null;
  if (typeof dataJson !== "string") return null;
  let parsed: ZcodeMessageData;
  try {
    parsed = JSON.parse(dataJson) as ZcodeMessageData;
  } catch {
    return null;
  }
  if (parsed.role !== "assistant") return null;
  const text = partTexts.map((part) => part.trim()).filter((part) => part.length > 0).join("\n");
  if (text.length === 0) return null;
  return { role: "assistant", text, createdAtMs };
}

export function readZcodeCandidates(dbPath: string): ReadonlyArray<SqliteSessionCandidate> {
  const db = openReadOnly(dbPath);
  if (db === null) return [];
  try {
    const rows = db
      .prepare(
        "SELECT id, title, directory, time_created, time_updated" +
          " FROM session WHERE time_archived IS NULL AND directory IS NOT NULL AND directory != ''" +
          " ORDER BY time_updated DESC LIMIT ?",
      )
      .all(MAX_SESSION_CANDIDATES) as Array<Record<string, unknown>>;
    return rows.flatMap((row): SqliteSessionCandidate[] => {
      const id = row["id"];
      const cwd = row["directory"];
      if (typeof id !== "string" || id.length === 0 || typeof cwd !== "string") return [];
      return [
        {
          providerSessionId: id,
          cwd,
          title: typeof row["title"] === "string" && row["title"].length > 0 ? row["title"] : id,
          createdAtMs: isoOrNull(row["time_created"]),
          updatedAtMs: isoOrNull(row["time_updated"]),
        },
      ];
    });
  } catch {
    return [];
  } finally {
    db.close();
  }
}

export function readZcodeThread(
  dbPath: string,
  providerSessionId: string,
): SqliteSessionThread | null {
  const db = openReadOnly(dbPath);
  if (db === null) return null;
  try {
    const row = db
      .prepare("SELECT id, title, directory, time_created, time_updated FROM session WHERE id = ?")
      .get(providerSessionId) as Record<string, unknown> | undefined;
    if (row === undefined || typeof row["directory"] !== "string") return null;
    const messageRows = db
      .prepare(
        "SELECT m.data AS data, m.time_created AS time_created," +
          " (SELECT GROUP_CONCAT(json_quote(json_extract(p.data, '$.text')), char(10))" +
          "    FROM part p WHERE p.message_id = m.id AND json_extract(p.data, '$.type') = 'text'" +
          "    ORDER BY p.sequence) AS text_parts" +
          " FROM message m WHERE m.session_id = ?" +
          " ORDER BY m.sequence, m.time_created LIMIT ?",
      )
      .all(providerSessionId, MAX_SESSION_MESSAGES) as Array<Record<string, unknown>>;
    const messages: SqliteSessionMessage[] = [];
    for (const messageRow of messageRows) {
      const data = messageRow["data"];
      const createdAtMs = isoOrNull(messageRow["time_created"]);
      const asUser = zcodeRowToUserMessage(data, createdAtMs);
      if (asUser !== null) {
        messages.push(asUser);
        continue;
      }
      let partTexts: string[] = [];
      const joined = messageRow["text_parts"];
      if (typeof joined === "string" && joined.length > 0) {
        try {
          const decoded = JSON.parse(`[${joined}]`) as unknown[];
          partTexts = decoded.filter((part): part is string => typeof part === "string");
        } catch {
          partTexts = [];
        }
      }
      const asAssistant = zcodePartsToAssistantMessage(data, partTexts, createdAtMs);
      if (asAssistant !== null) messages.push(asAssistant);
    }
    if (messages.length === 0) return null;
    return {
      providerSessionId,
      title:
        typeof row["title"] === "string" && row["title"].length > 0 ? row["title"] : providerSessionId,
      cwd: row["directory"],
      createdAtMs: isoOrNull(row["time_created"]),
      updatedAtMs: isoOrNull(row["time_updated"]),
      messages,
    };
  } catch {
    return null;
  } finally {
    db.close();
  }
}
