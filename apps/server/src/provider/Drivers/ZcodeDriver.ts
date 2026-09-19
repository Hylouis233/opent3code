/**
 * ZcodeDriver - ZCode as a supervised preview provider over its own stdio
 * agent protocol (NOT ACP and NOT JSON-RPC; see ZcodeProtocol.ts).
 *
 * One `zcode app-server` process backs one thread. The protocol handshake is
 * bidirectional: the server holds `session/create` until the client answers
 * `session/requestRuntimePreferences`, which the forked stdout pump
 * auto-replies. Assistant output streams as `v4/telemetry/event`
 * notifications and is mapped onto the shared ProviderRuntimeEvent
 * vocabulary (turn.started / content.delta / turn.completed).
 *
 * v1 preview scope: approvals, model switching, rollback and native resume
 * are declared unsupported; the snapshot probe runs `session/list` against a
 * short-lived process. Authentication stays in the ZCode CLI.
 *
 * @module provider/Drivers/ZcodeDriver
 */
import {
  EventId,
  ProviderDriverKind,
  TextGenerationError,
  TurnId,
  ZcodeSettings,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ServerProvider,
  type ThreadId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { defaultProviderContinuationIdentity, type ProviderDriver } from "../ProviderDriver.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import { makeAcpContentDeltaEvent } from "../acp/AcpCoreRuntimeEvents.ts";
import {
  dispatchZcodeMessage,
  encodeZcodeRequest,
  zcodeSessionsFromListResult,
  zcodeStreamChunkText,
  zcodeTelemetryParams,
  zcodeTurnOutcome,
  type ZcodeServerMessage,
} from "./ZcodeProtocol.ts";

const REQUEST_TIMEOUT = "30 seconds";
const PROBE_TIMEOUT = "20 seconds";
/** Ships inside the desktop app; see docs/zcode-driver.md for the config shim. */
const DEFAULT_SCRIPT = "/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs";

type ZcodeHandle = ChildProcessSpawner.ChildProcessHandle;

const decodeZcodeLine = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

/** Decoded stdout → complete newline-terminated frames, carrying partial tails. */
function zcodeLines<E, R>(stream: Stream.Stream<Uint8Array, E, R>): Stream.Stream<string, E, R> {
  return stream.pipe(
    Stream.decodeText(),
    Stream.mapAccum(
      () => "",
      (buffer: string, chunk: string) => {
        const combined = buffer + chunk;
        const parts = combined.split("\n");
        const rest = parts.pop() ?? "";
        return [rest, parts] as const;
      },
    ),
    Stream.flattenIterable,
  );
}

interface LiveSession {
  readonly scope: Scope.Closeable;
  readonly handle: ZcodeHandle;
  sessionId: string;
  session: ProviderSession;
  nextRequestId: number;
  readonly pending: Map<number | string, Deferred.Deferred<unknown, Error>>;
  readonly answeredServerRequests: Set<number | string>;
  activeTurn: TurnId | null;
  stopped: boolean;
}

export type ZcodeDriverEnv = ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto;

type ZcodeConfig = typeof ZcodeSettings.Type;

export const ZcodeDriver: ProviderDriver<ZcodeConfig, ZcodeDriverEnv> = {
  driverKind: ProviderDriverKind.make("zcode"),
  metadata: { displayName: "ZCode", supportsMultipleInstances: true },
  configSchema: ZcodeSettings,
  defaultConfig: () => Schema.decodeSync(ZcodeSettings)({}),
  create: ({ instanceId, displayName, accentColor, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const crypto = yield* Crypto.Crypto;
      const provider = ProviderDriverKind.make("zcode");
      const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const stamp = Effect.gen(function* () {
        return {
          eventId: EventId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie)),
          createdAt: yield* now,
        };
      });
      const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const emit = (event: ProviderRuntimeEvent) =>
        PubSub.publish(events, event).pipe(Effect.asVoid);
      const sessions = new Map<ThreadId, LiveSession>();
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: provider,
        instanceId,
      });
      const invalid = (operation: string, issue: string) =>
        new ProviderAdapterValidationError({ provider, operation, issue });
      const requestError = (method: string, detail: string) =>
        new ProviderAdapterRequestError({ provider, method, detail });

      const script =
        config.binaryPath.trim().length > 0 ? config.binaryPath.trim() : DEFAULT_SCRIPT;

      const spawnHandle = (cwd: string, scope: Scope.Closeable) =>
        spawner
          .spawn(
            ChildProcess.make(process.execPath, [script, "app-server"], { cwd, extendEnv: true }),
          )
          .pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.mapError((cause) =>
              requestError("spawn", `Failed to start ZCode app-server: ${String(cause)}`),
            ),
          );

      const writeLine = (ctx: LiveSession, line: string) =>
        Stream.fromIterable([line]).pipe(
          Stream.encodeText,
          Stream.run(ctx.handle.stdin),
          Effect.mapError((cause) =>
            requestError("stdin", `Failed to write to ZCode app-server: ${String(cause)}`),
          ),
          Effect.ignore,
        );

      const zcodeRequest = (ctx: LiveSession, method: string, params?: unknown) =>
        Effect.gen(function* () {
          const deferred = yield* Deferred.make<unknown, Error>();
          const id = ctx.nextRequestId++;
          ctx.pending.set(id, deferred);
          yield* writeLine(
            ctx,
            encodeZcodeRequest({ id, method, ...(params === undefined ? {} : { params }) }),
          );
          const result = yield* Deferred.await(deferred).pipe(
            Effect.timeout(REQUEST_TIMEOUT),
            Effect.mapError(
              () => requestError(method, `ZCode Protocol request timed out: ${method}`),
            ),
          );
          ctx.pending.delete(id);
          return result;
        }).pipe(Effect.orDie);

      /** Decodes one stdout frame: replies to server requests, settles pending
       *  requests, and maps telemetry onto provider runtime events. */
      const consumeLine = (ctx: LiveSession, line: string, threadId: ThreadId) =>
        Effect.gen(function* () {
          const decoded = decodeZcodeLine(line.trim());
          if (Option.isNone(decoded)) return;
          const message = decoded.value as ZcodeServerMessage;
          const dispatch = dispatchZcodeMessage(message, ctx.answeredServerRequests);
          if (dispatch.kind === "reply") {
            if ("id" in message && message.id !== undefined) {
              ctx.answeredServerRequests.add(message.id);
            }
            yield* writeLine(ctx, dispatch.line);
            return;
          }
          if (dispatch.kind === "resolve" || dispatch.kind === "reject") {
            const deferred = ctx.pending.get(dispatch.id);
            if (deferred === undefined) return;
            if (dispatch.kind === "resolve") {
              yield* Deferred.succeed(deferred, dispatch.value);
            } else {
              yield* Deferred.fail(deferred, dispatch.error);
            }
            return;
          }
          const event = zcodeTelemetryParams(message);
          if (event === null) return;
          if (event.kind === "turn.started" && ctx.activeTurn !== null) {
            yield* emit({
              type: "turn.started",
              ...(yield* stamp),
              provider,
              threadId,
              turnId: ctx.activeTurn,
              payload: {},
            });
            return;
          }
          const delta = zcodeStreamChunkText(event);
          if (delta !== null && ctx.activeTurn !== null) {
            yield* emit(
              makeAcpContentDeltaEvent({
                stamp: yield* stamp,
                provider,
                threadId,
                turnId: ctx.activeTurn,
                text: delta,
                rawPayload: event,
              }),
            );
            return;
          }
          const outcome = zcodeTurnOutcome(event);
          if (outcome !== null && ctx.activeTurn !== null) {
            yield* emit({
              type: "turn.completed",
              ...(yield* stamp),
              provider,
              threadId,
              turnId: ctx.activeTurn,
              payload:
                outcome.status === "completed"
                  ? { state: "completed" }
                  : { state: "failed", errorMessage: outcome.reason },
            });
            ctx.activeTurn = null;
          }
        });

      const startPump = (ctx: LiveSession, threadId: ThreadId, scope: Scope.Closeable) =>
        zcodeLines(ctx.handle.stdout).pipe(
          Stream.runForEach((line) => consumeLine(ctx, line, threadId)),
          Effect.ignore,
          Effect.forkIn(scope),
        );

      const get = (threadId: ThreadId) =>
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (ctx === undefined || ctx.stopped) {
            return yield* new ProviderAdapterSessionNotFoundError({ provider, threadId });
          }
          return ctx;
        });

      const stopSessionInternal = (threadId: ThreadId) =>
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (ctx === undefined) return;
          ctx.stopped = true;
          sessions.delete(threadId);
          yield* writeLine(
            ctx,
            encodeZcodeRequest({
              id: ctx.nextRequestId++,
              method: "session/close",
              params: { sessionId: ctx.sessionId },
            }),
          );
          yield* Scope.close(ctx.scope, Exit.void);
          yield* emit({
            type: "session.exited",
            ...(yield* stamp),
            provider,
            threadId,
            payload: {},
          });
        });

      const startSessionInternal = (input: ProviderSessionStartInput) =>
        Effect.scoped(
          Effect.gen(function* () {
            if (sessions.has(input.threadId)) {
              return yield* invalid("startSession", "Stop the existing ZCode session first.");
          }
          const cwd = input.cwd ?? "/tmp";
          const createdAt = yield* now;
          const scope = yield* Scope.make();
          let transferred = false;
          yield* Effect.addFinalizer(() =>
            transferred ? Effect.void : Scope.close(scope, Exit.void),
          );
          const handle = yield* spawnHandle(cwd, scope);
          const ctx: LiveSession = {
            scope,
            handle,
            sessionId: "",
            session: {
              provider,
              providerInstanceId: instanceId,
              status: "connecting",
              runtimeMode: "auto",
              cwd,
              threadId: input.threadId,
              createdAt,
              updatedAt: createdAt,
            },
            nextRequestId: 1,
            pending: new Map(),
            answeredServerRequests: new Set(),
            activeTurn: null,
            stopped: false,
          };
          yield* startPump(ctx, input.threadId, scope);

          const created = (yield* zcodeRequest(ctx, "session/create", {
            workspace: { workspacePath: cwd, workspaceKey: cwd },
          })) as { readonly sessionId?: string } | undefined;
          const sessionId = created?.sessionId;
          if (typeof sessionId !== "string" || sessionId.length === 0) {
            return yield* invalid("startSession", "ZCode did not return a session id.");
          }
          ctx.sessionId = sessionId;
          yield* writeLine(
            ctx,
            encodeZcodeRequest({
              id: ctx.nextRequestId++,
              method: "session/subscribe",
              params: { sessionId },
            }),
          );
          ctx.session = {
            ...ctx.session,
            status: "ready",
            updatedAt: yield* now,
            resumeCursor: { sessionId },
          };
          sessions.set(input.threadId, ctx);
          transferred = true;
          yield* emit({
            type: "session.started",
            ...(yield* stamp),
            provider,
            threadId: input.threadId,
            payload: { resume: { sessionId } },
          });
          yield* emit({
            type: "thread.started",
            ...(yield* stamp),
            provider,
            threadId: input.threadId,
            payload: { providerThreadId: sessionId },
          });
          return ctx.session;
          }),
        );

      const adapter = {
        provider,
        capabilities: {
          sessionModelSwitch: "unsupported" as const,
          supportsConversationRollback: false,
        },
        startSession: startSessionInternal,
        sendTurn: (input: ProviderSendTurnInput) =>
          Effect.gen(function* () {
            const ctx = yield* get(input.threadId);
            const text = input.input?.trim();
            if (text === undefined || text.length === 0) {
              return yield* invalid(
                "sendTurn",
                "ZCode preview requires a prompt; promptless continuation is not supported.",
              );
            }
            const turnId = TurnId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
            ctx.activeTurn = turnId;
            yield* zcodeRequest(ctx, "session/send", {
              sessionId: ctx.sessionId,
              content: text,
            });
            return {
              threadId: input.threadId,
              turnId,
              resumeCursor: { sessionId: ctx.sessionId },
            };
          }),
        interruptTurn: (threadId: ThreadId) =>
          Effect.gen(function* () {
            const ctx = yield* get(threadId);
            if (ctx.sessionId.length === 0) return;
            // Best-effort notification; the engine emits turn.terminal itself.
            yield* writeLine(
              ctx,
              encodeZcodeRequest({
                id: ctx.nextRequestId++,
                method: "session/stop",
                params: { sessionId: ctx.sessionId },
              }),
            );
          }),
        respondToRequest: () =>
          Effect.fail(
            invalid("respondToRequest", "ZCode preview does not surface interactive approvals."),
          ),
        respondToUserInput: () =>
          Effect.fail(
            invalid("respondToUserInput", "ZCode preview does not surface interactive inputs."),
          ),
        stopSession: stopSessionInternal,
        stopAll: () =>
          Effect.forEach([...sessions.keys()], (threadId) => stopSessionInternal(threadId), {
            discard: true,
          }),
        listSessions: () =>
          Effect.sync(() => [...sessions.values()].map((ctx) => ctx.session)),
        hasSession: (threadId: ThreadId) => Effect.sync(() => sessions.has(threadId)),
        readThread: (threadId: ThreadId) =>
          Effect.gen(function* () {
            yield* get(threadId);
            return { threadId, turns: [] };
          }),
        rollbackThread: () =>
          Effect.fail(
            invalid("rollbackThread", "ZCode preview does not support conversation rollback."),
          ),
        streamEvents: Stream.fromPubSub(events),
      };

      const snapshotBase: ServerProvider = {
        instanceId,
        driver: provider,
        ...(displayName ? { displayName } : {}),
        ...(accentColor ? { accentColor } : {}),
        badgeLabel: "Preview",
        continuation: { groupKey: continuationIdentity.continuationKey },
        enabled,
        installed: false,
        version: null,
        status: enabled ? "warning" : "disabled",
        auth: { status: "unknown" },
        checkedAt: yield* now,
        message:
          "Refresh to probe the ZCode CLI. Supervised text sessions only; login is managed in the ZCode CLI.",
        models: [
          {
            slug: "cli-default",
            name: "CLI configured model",
            isDefault: true,
            isCustom: false,
            capabilities: {},
          },
        ],
        slashCommands: [],
        skills: [],
      };
      const state = yield* SubscriptionRef.make(snapshotBase);

      const refresh = Effect.gen(function* () {
        if (!enabled) return yield* SubscriptionRef.get(state);
        const checkedAt = yield* now;
        const next = yield* Effect.scoped(
          Effect.gen(function* () {
            const scope = yield* Scope.make();
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
            const handle = yield* spawnHandle("/tmp", scope);
            const probe: LiveSession = {
              scope,
              handle,
              sessionId: "",
              session: {} as ProviderSession,
              nextRequestId: 1,
              pending: new Map(),
              answeredServerRequests: new Set(),
              activeTurn: null,
              stopped: false,
            };
            yield* startPump(probe, "" as ThreadId, scope);
            const listed = yield* zcodeRequest(probe, "session/list", {});
            return zcodeSessionsFromListResult(listed);
          }),
        ).pipe(
          Effect.timeout(PROBE_TIMEOUT),
          Effect.match({
            onFailure: () => ({
              ...snapshotBase,
              checkedAt,
              status: "error" as const,
              message:
                "ZCode app-server unavailable. Check the ZCode desktop install and the provider config shim.",
            }),
            onSuccess: (listed) => ({
              ...snapshotBase,
              checkedAt,
              installed: true,
              status: "ready" as const,
              message: `ZCode Protocol handshake succeeded; ${listed.length} engine session(s) visible.`,
            }),
          }),
        );
        yield* SubscriptionRef.set(state, next);
        return next;
      });
      if (enabled) yield* refresh;

      const unsupportedText = (operation: string) =>
        Effect.fail(
          new TextGenerationError({
            operation,
            detail:
              "ZCode preview does not provide background text generation; select another provider for titles and commit messages.",
          }),
        );

      return {
        instanceId,
        driverKind: provider,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        adapter,
        snapshot: {
          getSnapshot: SubscriptionRef.get(state),
          refresh,
          streamChanges: SubscriptionRef.changes(state),
          applyUsageLimits: () => Effect.void,
          resolveMaintenance: () =>
            Effect.succeed(
              makeManualOnlyProviderMaintenanceCapabilities({ provider, packageName: null }),
            ),
        },
        textGeneration: {
          generateCommitMessage: () => unsupportedText("generateCommitMessage"),
          generatePrContent: () => unsupportedText("generatePrContent"),
          generateBranchName: () => unsupportedText("generateBranchName"),
          generateThreadTitle: () => unsupportedText("generateThreadTitle"),
        },
      };
    }),
};
