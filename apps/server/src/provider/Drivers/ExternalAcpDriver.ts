/** Official stdio ACP only. No terminal scraping, login emulation, or implicit permission grants. */
import {
  DshSettings,
  MCodeSettings,
  EventId,
  ProviderDriverKind,
  RuntimeRequestId,
  TextGenerationError,
  TurnId,
  type ProviderSession,
  type ProviderRuntimeEvent,
  type ServerProvider,
  type ThreadId,
  type ProviderApprovalDecision,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessWorkingDirectory } from "@t3tools/shared/hostProcess";
import {
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  ProviderAdapterSessionNotFoundError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { defaultProviderContinuationIdentity, type ProviderDriver } from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import {
  ExternalAcpResume,
  matchesExternalAcpResume,
  externalPermissionOutcome,
  externalApprovalOptions,
} from "../acp/ExternalAcpPolicy.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpToolCallEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";

const decodeResume = Schema.decodeUnknownEffect(ExternalAcpResume);

type Runtime = AcpSessionRuntime.AcpSessionRuntime["Service"];
interface LiveSession {
  scope: Scope.Closeable;
  runtime: Runtime;
  session: ProviderSession;
  pending: Map<string, Deferred.Deferred<ProviderApprovalDecision>>;
  active?: { id: TurnId; done: Deferred.Deferred<void>; fiber?: Fiber.Fiber<void> };
  stopped: boolean;
}
export type ExternalAcpDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path;

function makeExternalDriver(
  kind: "mcode" | "dsh",
  configSchema: typeof MCodeSettings,
): ProviderDriver<MCodeSettings, ExternalAcpDriverEnv> {
  const provider = ProviderDriverKind.make(kind);
  const name = kind === "mcode" ? "MiniMax Code" : "DeepSeek Harness";
  return {
    driverKind: provider,
    metadata: { displayName: name, supportsMultipleInstances: true },
    configSchema,
    defaultConfig: () => Schema.decodeSync(configSchema)({}),
    create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const crypto = yield* Crypto.Crypto;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const hostCwd = yield* HostProcessWorkingDirectory;
        const env = mergeProviderInstanceEnvironment(environment);
        // Explicit values override dangerous ambient DSH policy. The installed profile must also be trusted.
        const processEnv: NodeJS.ProcessEnv =
          kind === "dsh"
            ? {
                ...env,
                DSH_PERMISSION_MODE: "workspace-write",
                DSH_TELEMETRY_DISABLED: "1",
                ...(config.homePath ? { DSH_HOME: config.homePath } : {}),
              }
            : { ...env, ...(config.homePath ? { MINIMAX_DATA_DIR: config.homePath } : {}) };
        const home =
          kind === "dsh"
            ? (processEnv.DSH_HOME ?? env.HOME ?? env.USERPROFILE ?? "")
            : (processEnv.MINIMAX_DATA_DIR ??
              processEnv.MAVIS_DATA_DIR ??
              env.HOME ??
              env.USERPROFILE ??
              "");
        const continuationIdentity = defaultProviderContinuationIdentity({
          driverKind: provider,
          instanceId,
        });
        const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const sessions = new Map<ThreadId, LiveSession>();
        const lifecycle = yield* Semaphore.make(1);
        const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
        const stamp = Effect.gen(function* () {
          return {
            eventId: EventId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie)),
            createdAt: yield* now,
          };
        });
        const emit = (event: ProviderRuntimeEvent) =>
          PubSub.publish(events, event).pipe(Effect.asVoid);
        const invalid = (operation: string, issue: string) =>
          new ProviderAdapterValidationError({ provider, operation, issue });
        const requestError = (method: string, detail: string) =>
          new ProviderAdapterRequestError({ provider, method, detail });
        const get = (id: ThreadId) =>
          Effect.gen(function* () {
            const ctx = sessions.get(id);
            if (!ctx || ctx.stopped)
              return yield* new ProviderAdapterSessionNotFoundError({ provider, threadId: id });
            return ctx;
          });
        const openRuntime = (cwd: string, scope: Scope.Closeable, resume?: string) =>
          AcpSessionRuntime.make({
            spawn: {
              command: config.binaryPath,
              args: kind === "mcode" ? ["acp"] : ["--profile", "opent3code"],
              cwd,
              env: processEnv,
            },
            cwd,
            clientInfo: { name: "opent3code", version: "0.1.0-alpha.1" },
            ...(resume ? { resumeSessionId: resume, resumeMethod: "resume" as const } : {}),
            cancelBehavior: "wait-for-prompt",
            cancelTimeout: "15 seconds",
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, scope),
          );
        const drainPermissions = (ctx: LiveSession) =>
          Effect.forEach(
            [...ctx.pending.values()],
            (pending) => Deferred.succeed(pending, "cancel"),
            { discard: true },
          );
        const stop = (threadId: ThreadId) =>
          Effect.gen(function* () {
            const ctx = sessions.get(threadId);
            if (!ctx) return;
            ctx.stopped = true;
            sessions.delete(threadId);
            yield* drainPermissions(ctx);
            if (ctx.active) yield* ctx.runtime.cancel.pipe(Effect.ignore);
            yield* Scope.close(ctx.scope, Exit.void);
            ctx.session = { ...ctx.session, status: "closed", updatedAt: yield* now };
            yield* emit({
              type: "session.exited",
              ...(yield* stamp),
              provider,
              threadId,
              payload: { exitKind: "graceful" },
            });
          });
        yield* Effect.addFinalizer(() =>
          Effect.forEach([...sessions.keys()], stop, { discard: true }).pipe(
            Effect.andThen(PubSub.shutdown(events)),
          ),
        );

        const adapter: ProviderAdapterShape<ProviderAdapterError> = {
          provider,
          capabilities: { sessionModelSwitch: "unsupported", supportsConversationRollback: false },
          startSession: (input) =>
            lifecycle.withPermits(1)(
              Effect.gen(function* () {
                if (!enabled)
                  return yield* invalid("startSession", "Enable this provider instance first.");
                if (input.runtimeMode !== "approval-required")
                  return yield* invalid(
                    "startSession",
                    "This preview only supports Supervised permissions. Select Supervised before sending.",
                  );
                if (input.modelSelection && input.modelSelection.model !== "cli-default")
                  return yield* invalid(
                    "startSession",
                    "Select CLI configured model; configure the model in the official CLI.",
                  );
                if (!input.cwd || !path.isAbsolute(input.cwd))
                  return yield* invalid(
                    "startSession",
                    "An absolute existing workspace directory is required.",
                  );
                if (config.homePath && !path.isAbsolute(config.homePath))
                  return yield* invalid(
                    "startSession",
                    "CLI data directory must be absolute; ~ is not expanded.",
                  );
                const cwd = yield* fs
                  .realPath(input.cwd)
                  .pipe(
                    Effect.mapError(() => invalid("startSession", "Workspace is unavailable.")),
                  );
                const info = yield* fs
                  .stat(cwd)
                  .pipe(
                    Effect.mapError(() => invalid("startSession", "Workspace is unavailable.")),
                  );
                if (info.type !== "Directory")
                  return yield* invalid("startSession", "Workspace must be a directory.");
                const expected = { version: 1 as const, driver: kind, instanceId, cwd, home };
                const cursor =
                  input.resumeCursor === undefined
                    ? undefined
                    : yield* decodeResume(input.resumeCursor).pipe(
                        Effect.mapError(() =>
                          invalid(
                            "resume",
                            "Invalid native session cursor; it was not created by this adapter.",
                          ),
                        ),
                      );
                if (cursor && !matchesExternalAcpResume(cursor, expected))
                  return yield* invalid(
                    "resume",
                    "The saved session belongs to another driver, instance, workspace or CLI data directory.",
                  );
                if (sessions.has(input.threadId))
                  return yield* invalid(
                    "startSession",
                    "Stop the existing native session before restarting it.",
                  );
                return yield* Effect.scoped(
                  Effect.gen(function* () {
                    const scope = yield* Scope.make();
                    let transferred = false;
                    yield* Effect.addFinalizer(() =>
                      transferred ? Effect.void : Scope.close(scope, Exit.void),
                    );
                    const runtime = yield* openRuntime(cwd, scope, cursor?.sessionId).pipe(
                      Effect.mapError((error) =>
                        mapAcpToAdapterError(provider, input.threadId, "start", error),
                      ),
                    );
                    const createdAt = yield* now;
                    const ctx: LiveSession = {
                      scope,
                      runtime,
                      pending: new Map(),
                      stopped: false,
                      session: {
                        provider,
                        providerInstanceId: instanceId,
                        threadId: input.threadId,
                        runtimeMode: input.runtimeMode,
                        cwd,
                        model: "cli-default",
                        status: "connecting",
                        createdAt,
                        updatedAt: createdAt,
                      },
                    };
                    yield* runtime.handleRequestPermission((params) =>
                      Effect.gen(function* () {
                        if (ctx.stopped || !ctx.active)
                          return { outcome: { outcome: "cancelled" as const } };
                        const active = ctx.active;
                        const requestId = RuntimeRequestId.make(
                          yield* crypto.randomUUIDv4.pipe(Effect.orDie),
                        );
                        const pending = yield* Deferred.make<ProviderApprovalDecision>();
                        const permissionRequest = parsePermissionRequest(params);
                        ctx.pending.set(requestId, pending);
                        const decision = yield* Effect.gen(function* () {
                          yield* emit(
                            makeAcpRequestOpenedEvent({
                              stamp: yield* stamp,
                              provider,
                              threadId: input.threadId,
                              turnId: active.id,
                              requestId,
                              permissionRequest,
                              approvalOptions: externalApprovalOptions(params.options),
                              detail: permissionRequest.detail ?? "Native CLI permission request",
                              args: params,
                              source: "acp.jsonrpc",
                              method: "session/request_permission",
                              rawPayload: params,
                            }),
                          );
                          return yield* Deferred.await(pending);
                        }).pipe(Effect.ensuring(Effect.sync(() => ctx.pending.delete(requestId))));
                        const outcome = externalPermissionOutcome(params.options, decision);
                        const effective = outcome.outcome === "cancelled" ? "cancel" : decision;
                        yield* emit(
                          makeAcpRequestResolvedEvent({
                            stamp: yield* stamp,
                            provider,
                            threadId: input.threadId,
                            turnId: active.id,
                            requestId,
                            permissionRequest,
                            decision: effective,
                          }),
                        );
                        return { outcome };
                      }),
                    );
                    yield* runtime.getEvents().pipe(
                      Stream.runForEach((event) =>
                        Effect.gen(function* () {
                          if (event._tag === "EventStreamBarrier") {
                            yield* Deferred.succeed(event.acknowledge, undefined);
                            return;
                          }
                          if (ctx.stopped) return;
                          if (event._tag === "ConnectionTerminated") {
                            ctx.session = {
                              ...ctx.session,
                              status: "error",
                              lastError:
                                "ACP process disconnected; restart the session to recover.",
                              updatedAt: yield* now,
                            };
                            yield* drainPermissions(ctx);
                            yield* emit({
                              type: "session.state.changed",
                              ...(yield* stamp),
                              provider,
                              threadId: input.threadId,
                              payload: { state: "error", reason: "ACP process disconnected" },
                            });
                            return;
                          }
                          const common = {
                            stamp: yield* stamp,
                            provider,
                            threadId: input.threadId,
                            turnId: ctx.active?.id,
                          };
                          switch (event._tag) {
                            case "ContentDelta":
                              yield* emit(
                                makeAcpContentDeltaEvent({
                                  ...common,
                                  text: event.text,
                                  ...(event.itemId ? { itemId: event.itemId } : {}),
                                  rawPayload: event.rawPayload,
                                }),
                              );
                              break;
                            case "ThoughtDelta":
                              yield* emit(
                                makeAcpContentDeltaEvent({
                                  ...common,
                                  text: event.text,
                                  streamKind: "reasoning_text",
                                  rawPayload: event.rawPayload,
                                }),
                              );
                              break;
                            case "AssistantItemStarted":
                            case "AssistantItemCompleted":
                              yield* emit(
                                makeAcpAssistantItemEvent({
                                  ...common,
                                  itemId: event.itemId,
                                  lifecycle:
                                    event._tag === "AssistantItemStarted"
                                      ? "item.started"
                                      : "item.completed",
                                }),
                              );
                              break;
                            case "ToolCallUpdated":
                              yield* emit(
                                makeAcpToolCallEvent({
                                  ...common,
                                  toolCall: event.toolCall,
                                  rawPayload: event.rawPayload,
                                }),
                              );
                              break;
                            case "PlanUpdated":
                              yield* emit(
                                makeAcpPlanUpdatedEvent({
                                  ...common,
                                  payload: event.payload,
                                  source: "acp.jsonrpc",
                                  method: "session/update",
                                  rawPayload: event.rawPayload,
                                }),
                              );
                              break;
                          }
                        }),
                      ),
                      Effect.forkIn(scope),
                    );
                    const started = yield* runtime.start().pipe(
                      Effect.timeout("30 seconds"),
                      Effect.mapError((error) =>
                        requestError(
                          "session/start",
                          `Native session failed. Check official CLI login/profile and model configuration. ${error.message}`,
                        ),
                      ),
                    );
                    if (kind === "mcode") {
                      // MCode's normal/plan work mode is NOT its approval policy.
                      // The separate process-scoped permissionMode option must settle to Ask.
                      const configured = yield* runtime
                        .setConfigOption("permissionMode", "default")
                        .pipe(
                          Effect.mapError((error) =>
                            mapAcpToAdapterError(
                              provider,
                              input.threadId,
                              "session/set_config_option",
                              error,
                            ),
                          ),
                        );
                      const permissionOptions = configured.configOptions.filter(
                        (option) => option.id === "permissionMode",
                      );
                      const permission = permissionOptions[0];
                      if (
                        permissionOptions.length !== 1 ||
                        permission?.type !== "select" ||
                        permission.category !== "_permission" ||
                        permission.currentValue !== "default"
                      ) {
                        return yield* invalid(
                          "session/start",
                          "MiniMax Code did not confirm supervised permissions. No prompt was sent.",
                        );
                      }
                      if (started.sessionSetupResult.modes?.currentModeId !== "default") {
                        return yield* invalid(
                          "session/start",
                          "This preview requires MiniMax Code's default work mode; switch out of Plan in the native CLI before resuming.",
                        );
                      }
                    }
                    const resumeCursor = {
                      ...expected,
                      sessionId: started.sessionId,
                    } satisfies ExternalAcpResume;
                    ctx.session = {
                      ...ctx.session,
                      status: "ready",
                      resumeCursor,
                      updatedAt: yield* now,
                    };
                    sessions.set(input.threadId, ctx);
                    transferred = true;
                    yield* emit({
                      type: "session.started",
                      ...(yield* stamp),
                      provider,
                      threadId: input.threadId,
                      payload: { resume: resumeCursor },
                    });
                    yield* emit({
                      type: "thread.started",
                      ...(yield* stamp),
                      provider,
                      threadId: input.threadId,
                      payload: { providerThreadId: started.sessionId },
                    });
                    yield* emit({
                      type: "session.state.changed",
                      ...(yield* stamp),
                      provider,
                      threadId: input.threadId,
                      payload: { state: "ready" },
                    });
                    return ctx.session;
                  }),
                );
              }),
            ),
          sendTurn: (input) =>
            lifecycle.withPermits(1)(
              Effect.gen(function* () {
                const ctx = yield* get(input.threadId);
                if (ctx.session.status === "error")
                  return yield* invalid("sendTurn", "Restart the disconnected session first.");
                if (ctx.active)
                  return yield* invalid(
                    "sendTurn",
                    "A turn is already running. Cancel it or wait; mid-turn steering is not supported in this preview.",
                  );
                if (!input.input?.trim())
                  return yield* invalid("sendTurn", "A text prompt is required.");
                if (input.attachments?.length)
                  return yield* invalid(
                    "sendTurn",
                    "Attachments are not supported by this preview adapter.",
                  );
                if (input.interactionMode && input.interactionMode !== "default")
                  return yield* invalid(
                    "sendTurn",
                    "Plan-mode negotiation is not supported by this preview adapter.",
                  );
                if (input.modelSelection && input.modelSelection.model !== "cli-default")
                  return yield* invalid("sendTurn", "Only the CLI configured model is supported.");
                const turnId = TurnId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
                const done = yield* Deferred.make<void>();
                const dispatched = yield* Deferred.make<void>();
                const active: NonNullable<LiveSession["active"]> = { id: turnId, done };
                ctx.active = active;
                ctx.session = {
                  ...ctx.session,
                  status: "running",
                  activeTurnId: turnId,
                  updatedAt: yield* now,
                };
                const run = Effect.gen(function* () {
                  yield* emit({
                    type: "turn.started",
                    ...(yield* stamp),
                    provider,
                    threadId: input.threadId,
                    turnId,
                    payload: {},
                  });
                  const result = yield* ctx.runtime.prompt(
                    { prompt: [{ type: "text", text: input.input! }] },
                    { dispatched },
                  );
                  yield* ctx.runtime.drainEvents;
                  yield* emit({
                    type: "turn.completed",
                    ...(yield* stamp),
                    provider,
                    threadId: input.threadId,
                    turnId,
                    payload: {
                      state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                      stopReason: result.stopReason,
                    },
                  });
                }).pipe(
                  Effect.catchCause((cause) =>
                    Effect.gen(function* () {
                      yield* ctx.runtime.drainEvents;
                      if (!ctx.stopped)
                        yield* emit({
                          type: "turn.completed",
                          ...(yield* stamp),
                          provider,
                          threadId: input.threadId,
                          turnId,
                          payload: {
                            state: Cause.hasInterrupts(cause) ? "cancelled" : "failed",
                            errorMessage:
                              "ACP turn failed. Check CLI authentication, model configuration and connection.",
                          },
                        });
                    }),
                  ),
                  Effect.ensuring(
                    Effect.gen(function* () {
                      yield* drainPermissions(ctx);
                      if (ctx.active === active) delete ctx.active;
                      if (ctx.session.status !== "error" && !ctx.stopped) {
                        const { activeTurnId: _, ...rest } = ctx.session;
                        ctx.session = { ...rest, status: "ready", updatedAt: yield* now };
                      }
                      yield* Deferred.succeed(done, undefined);
                    }),
                  ),
                );
                active.fiber = yield* run.pipe(Effect.forkIn(ctx.scope));
                yield* Effect.raceFirst(Deferred.await(dispatched), Deferred.await(done));
                return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
              }),
            ),
          interruptTurn: (threadId, turnId) =>
            Effect.gen(function* () {
              const ctx = yield* get(threadId);
              const active = ctx.active;
              if (!active) return;
              if (turnId && active.id !== turnId)
                return yield* invalid("interruptTurn", "The requested turn is no longer active.");
              yield* drainPermissions(ctx);
              yield* ctx.runtime.cancel.pipe(
                Effect.mapError((error) =>
                  mapAcpToAdapterError(provider, threadId, "session/cancel", error),
                ),
              );
              yield* Deferred.await(active.done).pipe(
                Effect.timeout("20 seconds"),
                Effect.mapError(() =>
                  requestError(
                    "session/cancel",
                    "Cancellation did not settle. Stop the native session before retrying.",
                  ),
                ),
              );
            }),
          respondToRequest: (threadId, requestId, decision) =>
            Effect.gen(function* () {
              const ctx = yield* get(threadId);
              const pending = ctx.pending.get(requestId);
              if (!pending)
                return yield* invalid(
                  "respondToRequest",
                  "The approval expired or belongs to another session.",
                );
              yield* Deferred.succeed(pending, decision);
            }),
          respondToUserInput: () =>
            Effect.fail(
              invalid(
                "respondToUserInput",
                "Structured questions are not supported by this preview adapter.",
              ),
            ),
          stopSession: (threadId) => lifecycle.withPermits(1)(stop(threadId)),
          stopAll: () =>
            lifecycle.withPermits(1)(Effect.forEach([...sessions.keys()], stop, { discard: true })),
          listSessions: () => Effect.sync(() => [...sessions.values()].map((ctx) => ctx.session)),
          hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
          readThread: (threadId) =>
            get(threadId).pipe(
              Effect.flatMap(() =>
                Effect.fail(
                  invalid(
                    "readThread",
                    "Native transcript export is unavailable; use the OpenT3Code conversation history.",
                  ),
                ),
              ),
            ),
          rollbackThread: () =>
            Effect.fail(
              invalid(
                "rollbackThread",
                "Native history rollback is unavailable; the OpenT3Code conversation remains authoritative.",
              ),
            ),
          streamEvents: Stream.fromPubSub(events),
        };
        const snapshot: ServerProvider = {
          instanceId,
          driver: provider,
          displayName: displayName ?? name,
          ...(accentColor ? { accentColor } : {}),
          continuation: { groupKey: continuationIdentity.continuationKey },
          badgeLabel: "Preview",
          enabled,
          installed: false,
          version: null,
          status: enabled ? "warning" : "disabled",
          auth: { status: "unknown" },
          checkedAt: yield* now,
          message:
            "Refresh to probe the official CLI. Supervised text sessions only; CLI login is managed outside OpenT3Code.",
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
          supportsTextGeneration: false,
          supportsConversationRollback: false,
          showInteractionModeToggle: false,
          requiresNewThreadForModelChange: true,
        };
        const state = yield* SubscriptionRef.make(snapshot);
        const refresh = Effect.gen(function* () {
          if (!enabled) return yield* SubscriptionRef.get(state);
          const probe = Effect.scoped(
            Effect.gen(function* () {
              const scope = yield* Scope.make();
              yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
              const runtime = yield* openRuntime(hostCwd, scope);
              return yield* runtime.initialize();
            }),
          ).pipe(Effect.timeout("20 seconds"));
          const checkedAt = yield* now;
          const next = yield* probe.pipe(
            Effect.match({
              onFailure: () => ({
                ...snapshot,
                checkedAt,
                status: "error" as const,
                message:
                  "Official ACP CLI unavailable. Check the binary, Node version, and the DSH opent3code profile installation.",
              }),
              onSuccess: (result) => ({
                ...snapshot,
                checkedAt,
                installed: true,
                version: result.agentInfo?.version ?? null,
                status: "ready" as const,
                message:
                  "ACP handshake succeeded; account authentication and model availability are not verified by this probe.",
              }),
            }),
          );
          yield* SubscriptionRef.set(state, next);
          return next;
        });
        // Only opted-in instances are probed, scoped to the provider's lifecycle.
        if (enabled) yield* refresh;
        const unsupportedText = (operation: string) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: `${name} preview does not provide background text generation; select another provider for titles and commit messages.`,
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
}
export const MCodeDriver = makeExternalDriver("mcode", MCodeSettings);
export const DshDriver = makeExternalDriver("dsh", DshSettings);
