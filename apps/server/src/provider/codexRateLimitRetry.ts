/**
 * Bounds how long a Codex turn may sit in a provider HTTP 429 retry.
 *
 * The Codex CLI retries rate-limited model requests itself and only tells the
 * app server "Reconnecting... n/m" (`error` with `willRetry: true`). When the
 * upstream sends a long `Retry-After`, Codex sleeps for the full value without
 * any cap and emits nothing until it wakes, which leaves a turn (and any
 * delegated task waiting on it) "running" for hours. The adapter cannot
 * intercept the HTTP retry, so it applies this policy around it: every retry
 * notice gets an exponential-backoff (or Retry-After) budget, and when that
 * budget, the attempt cap, or the maximum delay is exceeded the turn ends as
 * an explicit usage-limit failure instead of hanging.
 */
export interface CodexRateLimitRetryPolicy {
  /** Highest retry attempt the adapter waits for. Later notices end the turn. */
  readonly maxAttempts: number;
  /** Expected wait for attempt 1 when no Retry-After is known; doubles per attempt. */
  readonly baseDelayMs: number;
  /** Longest single wait the adapter accepts, including a server Retry-After. */
  readonly maxDelayMs: number;
  /** Extra time after the expected wait for the retried request to show progress. */
  readonly progressGraceMs: number;
}

export const CODEX_RATE_LIMIT_RETRY_POLICY: CodexRateLimitRetryPolicy = {
  maxAttempts: 5,
  baseDelayMs: 5_000,
  maxDelayMs: 5 * 60_000,
  progressGraceMs: 60_000,
};

export type CodexRateLimitRetryDecision =
  | {
      readonly type: "wait";
      /** How long the provider is expected to wait before retrying. */
      readonly delayMs: number;
      /** How long the adapter waits for progress before ending the turn. */
      readonly deadlineMs: number;
    }
  | {
      readonly type: "give_up";
      readonly reason: "attempts_exhausted" | "retry_after_exceeds_max_delay";
    };

export function codexRateLimitBackoffMs(
  attempt: number,
  policy: CodexRateLimitRetryPolicy = CODEX_RATE_LIMIT_RETRY_POLICY,
): number {
  const exponent = Math.max(0, Math.floor(attempt) - 1);
  return Math.min(policy.baseDelayMs * 2 ** exponent, policy.maxDelayMs);
}

export function decideCodexRateLimitRetry(
  input: { readonly attempt: number; readonly retryAfterMs: number | null },
  policy: CodexRateLimitRetryPolicy = CODEX_RATE_LIMIT_RETRY_POLICY,
): CodexRateLimitRetryDecision {
  if (input.attempt > policy.maxAttempts) {
    return { type: "give_up", reason: "attempts_exhausted" };
  }
  if (input.retryAfterMs !== null && input.retryAfterMs > policy.maxDelayMs) {
    return { type: "give_up", reason: "retry_after_exceeds_max_delay" };
  }
  const delayMs = input.retryAfterMs ?? codexRateLimitBackoffMs(input.attempt, policy);
  return { type: "wait", delayMs, deadlineMs: delayMs + policy.progressGraceMs };
}

const UNIT_MS: Record<string, number> = {
  ms: 1,
  millisecond: 1,
  milliseconds: 1,
  s: 1_000,
  sec: 1_000,
  secs: 1_000,
  second: 1_000,
  seconds: 1_000,
  m: 60_000,
  min: 60_000,
  mins: 60_000,
  minute: 60_000,
  minutes: 60_000,
  h: 3_600_000,
  hr: 3_600_000,
  hrs: 3_600_000,
  hour: 3_600_000,
  hours: 3_600_000,
};

// RFC 9110 lets Retry-After carry an IMF-fixdate instead of delay-seconds.
const RETRY_AFTER_HTTP_DATE =
  /retry[-_ ]?after["']?\s*[:=]?\s*["']?((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+GMT)/iu;

/**
 * Reads a retry delay from provider error text: a `Retry-After` header or
 * field (seconds, or an HTTP date measured from `nowMs` when the caller
 * supplies it), a `reset_seconds` field, or "try again in N <unit>".
 */
export function parseCodexRetryAfterMs(
  text: string | null | undefined,
  nowMs?: number,
): number | null {
  if (text == null || text.length === 0) return null;
  if (nowMs !== undefined) {
    const httpDate = RETRY_AFTER_HTTP_DATE.exec(text)?.[1];
    const retryAt = httpDate === undefined ? Number.NaN : Date.parse(httpDate);
    if (Number.isFinite(retryAt)) return Math.max(0, retryAt - nowMs);
  }
  const seconds =
    /retry[-_ ]?after["']?\s*[:=]?\s*["']?(\d+(?:\.\d+)?)/iu.exec(text)?.[1] ??
    /reset_seconds["']?\s*[:=]\s*["']?(\d+(?:\.\d+)?)/iu.exec(text)?.[1];
  if (seconds !== undefined) {
    const value = Number.parseFloat(seconds);
    return Number.isFinite(value) ? Math.round(value * 1_000) : null;
  }
  const phrase =
    /(?:try again|retry) in\s+(\d+(?:\.\d+)?)\s*(milliseconds?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h)\b/iu.exec(
      text,
    );
  if (phrase?.[1] === undefined || phrase[2] === undefined) return null;
  const unit = UNIT_MS[phrase[2].toLowerCase()];
  const value = Number.parseFloat(phrase[1]);
  return unit === undefined || !Number.isFinite(value) ? null : Math.round(value * unit);
}

/** The HTTP status Codex attached to a structured error, whatever its variant. */
export function codexErrorHttpStatusCode(errorInfo: unknown): number | null {
  if (typeof errorInfo !== "object" || errorInfo === null) return null;
  for (const detail of Object.values(errorInfo)) {
    if (typeof detail === "object" && detail !== null && "httpStatusCode" in detail) {
      const status = (detail as { readonly httpStatusCode?: unknown }).httpStatusCode;
      if (typeof status === "number") return status;
    }
  }
  return null;
}

const TEXTUAL_HTTP_429 =
  /\btoo many requests\b|\b(?:http|status(?:[ _]?code)?)["']?\s*[:=]?\s*["']?429\b|^\s*429\b/imu;

/**
 * Whether provider error text reports HTTP 429. Codex's structured status is
 * optional, so the text is the fallback evidence when it is missing.
 */
export function codexErrorTextIndicatesHttp429(text: string | null | undefined): boolean {
  return text != null && TEXTUAL_HTTP_429.test(text);
}
