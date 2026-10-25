import { AgentScope } from "@t3tools/shared/AgentScope";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  isOrchestrationV2WorkActive,
  ModelSelection,
  OrchestrationV2DomainEvent,
  OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  OrchestrationV2RuntimeRequest,
  ProviderInstanceId,
  ProviderSessionId,
  ThreadId,
  ProviderThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import { normalizeModelMetricLabel } from "../observability/Attributes.ts";
import {
  providerSessionsTotal,
  providerTurnDuration,
  providerTurnsTotal,
  withMetrics,
} from "../observability/Metrics.ts";
import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ServerSettings from "../serverSettings.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import * as EventSink from "./EventSink.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import type { ProviderRuntimeLifetime } from "./ProviderRuntimeLifetime.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const DEFAULT_IDLE_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_MAX_IDLE_PIN_MS = 4 * 60 * 60 * 1000;
const RELEASE_SCOPE_CLOSE_TIMEOUT_MS = 30 * 1000;

const busyTurnPrefix = (providerThreadId: ProviderThreadId) => `${providerThreadId}#`;
const busyTurnKey = (providerThreadId: ProviderThreadId, runOrdinal: number) =>
  `${busyTurnPrefix(providerThreadId)}${runOrdinal}`;
const UNLOAD_THREAD_TIMEOUT_MS = 10 * 1000;
const RELEASE_PERSIST_TIMEOUT_MS = 5 * 1000;
const MAX_PENDING_RELEASES = 128;

export const ProviderSessionReleaseReason = Schema.Literals([
  "idle_timeout",
  "runtime_error",
  "manual_shutdown",
  "server_shutdown",
]);
export type ProviderSessionReleaseReason = typeof ProviderSessionReleaseReason.Type;

export class ProviderSessionOpenError extends Schema.TaggedError<ProviderSessionOpenError>()(
  "ProviderSessionOpenError",
  {
    instanceId: ProviderInstanceId,
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to open provider instance ${this.instanceId} session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionLookupError extends Schema.TaggedError<ProviderSessionLookupError>()(
  "ProviderSessionLookupError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to look up provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionCloseError extends Schema.TaggedError<ProviderSessionCloseError>()(
  "ProviderSessionCloseError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to close provider session ${this.providerSessionId}.`;
  }
}

export class ProviderSessionReleaseError extends Schema.TaggedError<ProviderSessionReleaseError>()(
  "ProviderSessionReleaseError",
  {
    providerSessionId: ProviderSessionId,
    reason: ProviderSessionReleaseReason,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to release provider session ${this.providerSessionId}.`;
  }
}

class ProviderSessionRosterCleanupError extends Schema.TaggedError<ProviderSessionRosterCleanupError>()(
  "ProviderSessionRosterCleanupError",
  { providerSessionId: ProviderSessionId, providerThreadId: ProviderThreadId },
) {
  override get message(): string {
    return `Provider thread ${this.providerThreadId} changed during release cleanup.`;
  }
}

export class ProviderSessionActivityError extends Schema.TaggedError<ProviderSessionActivityError>()(
  "ProviderSessionActivityError",
  {
    providerSessionId: ProviderSessionId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to update provider session activity for ${this.providerSessionId}.`;
  }
}

export const ProviderSessionManagerV2Error = Schema.Union([
  ProviderSessionOpenError,
  ProviderWorkspaceMissingError,
  ProviderSessionLookupError,
  ProviderSessionCloseError,
  ProviderSessionReleaseError,
  ProviderSessionActivityError,
]);
export type ProviderSessionManagerV2Error = typeof ProviderSessionManagerV2Error.Type;

export interface ManagedProviderSessionRuntime
  extends ProviderAdapter.ProviderAdapterV2SessionRuntime {
  readonly runtimeLifetime: ProviderRuntimeLifetime;
}

export interface ProviderSessionManagerV2Shape {
  readonly shutdown: Effect.Effect<void>;
  readonly open: (input: {
    readonly threadId: ThreadId;
    readonly providerSessionId: ProviderSessionId;
    readonly modelSelection: ModelSelection;
    readonly runtimePolicy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
    readonly resumeFromSession?: OrchestrationV2ProviderSession;
    readonly initialNativeThreadId?: string;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<ManagedProviderSessionRuntime, ProviderSessionManagerV2Error>;
  readonly get: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<Option.Option<ManagedProviderSessionRuntime>, ProviderSessionManagerV2Error>;
  /** Whether the session runtime is running. Unlike `get`, not activity. */
  readonly isLive: (providerSessionId: ProviderSessionId) => Effect.Effect<boolean>;
  readonly close: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly closeInstance: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly release: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly reason: ProviderSessionReleaseReason;
    readonly detail?: string;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
  readonly detach: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly threadId: ThreadId;
    readonly detail?: string;
    readonly revokeMcpCredential?: boolean;
  }) => Effect.Effect<void, ProviderSessionManagerV2Error>;
}

export class ProviderSessionManagerV2 extends Context.Service<
  ProviderSessionManagerV2,
  ProviderSessionManagerV2Shape
>()("t3/orchestration-v2/ProviderSessionManager/ProviderSessionManagerV2") {}

interface LiveSessionEntry {
  readonly attachedThreadIds: ReadonlySet<ThreadId>;
  readonly ownedThreadIds: ReadonlySet<ThreadId>;
  readonly loadedProviderThreadKeyByThread: ReadonlyMap<ThreadId, string>;
  /** Scope revocation to recorded credential ids so superseded sessions cannot revoke replacements opened before detach. */
  readonly mcpCredentialIdByThread: ReadonlyMap<ThreadId, string>;
  readonly supportsMultipleProviderThreads: boolean;
  readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
  readonly exposedRuntime: ManagedProviderSessionRuntime;
  readonly runtimeLifetime: ProviderRuntimeLifetime;
  readonly eventSubscribers: Ref.Ref<
    ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
  >;
  readonly requestEventPermit: Semaphore.Semaphore;
  readonly scope: Scope.Closeable;
  readonly idleGeneration: number;
  readonly busyTurns: ReadonlySet<string>;
  readonly lastActivityAtMs: number;
  readonly idleFiber: Fiber.Fiber<void, never> | null;
  readonly pinnedSinceMs: number | null;
  readonly idleThreadUnloads: ReadonlyMap<ThreadId, IdleThreadUnload>;
}

interface IdleThreadUnload {
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly generation: number;
  readonly fiber: Fiber.Fiber<void, never> | null;
}

type ProviderSessionEventSignal =
  | { readonly type: "event"; readonly event: ProviderAdapter.ProviderAdapterV2Event }
  | {
      readonly type: "failure";
      readonly cause: Cause.Cause<ProviderAdapter.ProviderAdapterV2Error>;
    };

export interface ProviderSessionManagerV2LayerOptions {
  readonly idleTimeoutMs?: number;
  readonly maxIdlePinMs?: number;
  readonly configureMcp?: boolean;
}

function releaseStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2ProviderSession["status"] {
  return reason === "runtime_error" ? "error" : "stopped";
}

function releasedRuntimeRequestStatusFor(
  reason: ProviderSessionReleaseReason,
): OrchestrationV2RuntimeRequest["status"] {
  return reason === "manual_shutdown" || reason === "server_shutdown" ? "cancelled" : "expired";
}

function sessionKey(providerSessionId: ProviderSessionId): string {
  return String(providerSessionId);
}

function sessionScopedRuntimeRequestThreadId(
  event: ProviderAdapter.ProviderAdapterV2Event,
): ThreadId | undefined {
  switch (event.type) {
    case "runtime_request.updated":
      return event.runtimeRequest.providerTurnId === null ? event.threadId : undefined;
    case "node.updated":
      return event.node.runId === null && event.node.runtimeRequestId !== null
        ? event.node.threadId
        : undefined;
    case "turn_item.updated":
      return event.turnItem.runId === null &&
        (event.turnItem.type === "approval_request" || event.turnItem.type === "user_input_request")
        ? event.turnItem.threadId
        : undefined;
    default:
      return undefined;
  }
}

function providerThreadRuntimeKey(
  providerThread: Parameters<
    ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"],
): string {
  const nativeThreadRef = providerThread.nativeThreadRef;
  return nativeThreadRef === null
    ? String(providerThread.id)
    : `${nativeThreadRef.driver}:${nativeThreadRef.nativeId}`;
}

function providerThreadLoadKey(input: {
  readonly providerThread: Parameters<
    ProviderAdapter.ProviderAdapterV2SessionRuntime["resumeThread"]
  >[0]["providerThread"];
  readonly modelSelection?: ModelSelection;
  readonly runtimePolicy?: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
}): string {
  return JSON.stringify({
    providerThread: providerThreadRuntimeKey(input.providerThread),
    modelSelection: input.modelSelection ?? null,
    runtimePolicy: input.runtimePolicy ?? null,
  });
}

export const layerWithOptions = (
  options: ProviderSessionManagerV2LayerOptions = {},
): Layer.Layer<
  ProviderSessionManagerV2,
  never,
  | CommandOutputHub.CommandOutputHub
  | EventSink.EventSinkV2
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | McpSessionRegistry.McpSessionRegistry
  | ProjectionStore.ProjectionStoreV2
  | ProviderEventIngestor.ProviderEventIngestorV2
  | ProviderAdapterRegistry.ProviderAdapterRegistryV2
> =>
  Layer.effect(
    ProviderSessionManagerV2,
    Effect.gen(function* () {
      const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
      const fileSystem = yield* FileSystem.FileSystem;
      const mcpSessionRegistry = yield* McpSessionRegistry.McpSessionRegistry;
      const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
      /** Unreadable settings must preserve explicit browser-access opt-outs. */
      const serverSettings = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
      const projectService = yield* Effect.serviceOption(ProjectService.ProjectService);
      const eventSink = yield* EventSink.EventSinkV2;
      const commandOutput = yield* CommandOutputHub.CommandOutputHub;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const providerEventIngestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const agentScope = yield* AgentScope;
      const agentAccessSettings = Effect.fn("ProviderSessionManagerV2.agentAccessSettings")(
        function* (threadId: ThreadId) {
          if (Option.isNone(serverSettings)) return { browser: true, device: false };
          return yield* Effect.gen(function* () {
            const settings = yield* serverSettings.value.getSettings;
            const thread = yield* projectionStore.getThread(threadId);
            const entries = Object.values(settings.projectSettingsOverrides);
            const browserOverridden = entries.some(
              (entry) => entry.enableAgentBrowserAccess !== undefined,
            );
            const deviceOverridden = entries.some(
              (entry) => entry.enableAgentDeviceAccess !== undefined,
            );
            if (browserOverridden || deviceOverridden) {
              const project = Option.isSome(projectService)
                ? yield* projectService.value.getById(thread.projectId)
                : Option.none();
              if (Option.isNone(project))
                return {
                  browser: browserOverridden ? false : settings.enableAgentBrowserAccess,
                  device: deviceOverridden ? false : settings.enableAgentDeviceAccess,
                };
            }
            const effective = resolveProjectSettings(settings, thread.projectId).settings;
            return {
              browser: effective.enableAgentBrowserAccess,
              device: effective.enableAgentDeviceAccess,
            };
          }).pipe(
            Effect.catch((cause) =>
              Effect.logWarning(
                "Could not resolve agent access; withholding browser and device tools.",
                { threadId, cause },
              ).pipe(Effect.as({ browser: false, device: false })),
            ),
          );
        },
      );
      const layerScope = yield* Effect.scope;
      // Release jobs outlive their caller, but remain owned by this service.
      const releaseScope = yield* Scope.make();
      let shuttingDown = false;
      // Ctrl+C, or a stop that signals the whole process group, reaches the
      // provider CLIs with the server. They report their own background work
      // stopped before shutdown captures restart continuations, so provider
      // events after the signal are dropped; restart recovery owns that state.
      const shutdownSignal = { received: false };
      const onShutdownSignal = () => {
        shutdownSignal.received = true;
      };
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          process.on("SIGINT", onShutdownSignal);
          process.on("SIGTERM", onShutdownSignal);
        }),
        () =>
          Effect.sync(() => {
            process.off("SIGINT", onShutdownSignal);
            process.off("SIGTERM", onShutdownSignal);
          }),
      );
      const sessions = yield* Ref.make(new Map<string, LiveSessionEntry>());
      const nextSubscriberId = yield* Ref.make(0);
      const sessionOpen = yield* KeyedLock.make<ProviderSessionId>();
      const threadAttachment = yield* KeyedLock.make<string>();
      const threadAttachmentKey = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) => `${input.providerSessionId}\u0000${input.threadId}`;
      const idleTimeoutMs = Math.max(1, options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS);
      const maxIdlePinMs = Math.max(0, options.maxIdlePinMs ?? DEFAULT_MAX_IDLE_PIN_MS);
      interface PreparedMcpCredential {
        readonly mcpCredentialId: string | undefined;
        readonly issued: boolean;
      }
      interface McpPreparationLease {
        reservedCredentialId: string | undefined;
        prepared: PreparedMcpCredential | undefined;
        installed: boolean;
      }
      const makeMcpPreparationLease = (): McpPreparationLease => ({
        reservedCredentialId: undefined,
        prepared: undefined,
        installed: false,
      });
      /**
       * Adapters like ACP and OpenCode consume the credential eagerly during
       * openSession, so a racing release must not revoke it in that window
       * (rotating afterwards cannot repair an already-configured process).
       */
      const mcpCredentialReservations = new Map<string, number>();
      const mcpReservationKey = (threadId: ThreadId, mcpCredentialId: string) =>
        `${threadId}\0${mcpCredentialId}`;
      const reserveMcpCredential = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        mcpCredentialReservations.set(key, (mcpCredentialReservations.get(key) ?? 0) + 1);
      };
      const dropMcpCredentialReservation = (threadId: ThreadId, mcpCredentialId: string) => {
        const key = mcpReservationKey(threadId, mcpCredentialId);
        const count = mcpCredentialReservations.get(key) ?? 0;
        if (count <= 1) {
          mcpCredentialReservations.delete(key);
        } else {
          mcpCredentialReservations.set(key, count - 1);
        }
      };
      const dropPreparationReservation = (threadId: ThreadId, lease: McpPreparationLease) => {
        if (lease.reservedCredentialId === undefined) return;
        dropMcpCredentialReservation(threadId, lease.reservedCredentialId);
        lease.reservedCredentialId = undefined;
      };
      const isMcpCredentialReserved = (threadId: ThreadId, mcpCredentialId: string) =>
        (mcpCredentialReservations.get(mcpReservationKey(threadId, mcpCredentialId)) ?? 0) > 0;
      const mcpPrepareLock = yield* KeyedLock.make<ThreadId>();
      /** Serialize prepares per thread so concurrent rotations cannot revoke each other's credentials. */
      const prepareMcpSession = (
        threadId: ThreadId,
        providerInstanceId: ProviderInstanceId,
        lease: McpPreparationLease,
      ): Effect.Effect<PreparedMcpCredential> =>
        options.configureMcp === false
          ? mcpSessions
              .clear(threadId)
              .pipe(Effect.as<PreparedMcpCredential>({ mcpCredentialId: undefined, issued: false }))
          : mcpPrepareLock.withLock(
              threadId,
              Effect.gen(function* () {
                // Live providers build MCP clients once; reuse credentials across workspace handoffs.
                const { browser: browserToolsAvailable, device: deviceToolsAvailable } =
                  yield* agentAccessSettings(threadId);
                const capabilities = new Set<
                  import("../mcp/McpInvocationContext.ts").McpCapability
                >(["orchestration", "worktree", "pull-requests"]);
                if (browserToolsAvailable) capabilities.add("preview");
                if (deviceToolsAvailable) capabilities.add("device");
                const existing = yield* mcpSessions.read(threadId);
                if (existing !== undefined) {
                  // Reserve before async validation so release cannot revoke between validation and reservation.
                  reserveMcpCredential(threadId, existing.providerSessionId);
                  lease.reservedCredentialId = existing.providerSessionId;
                  const rawToken = existing.authorizationHeader.replace(/^Bearer\s+/, "");
                  // A stop releases this preparation's lease once, including
                  // when the caller's cleanup subsequently runs too.
                  const resolved = yield* mcpSessionRegistry
                    .resolve(rawToken)
                    .pipe(
                      Effect.onInterrupt(() =>
                        Effect.sync(() => dropPreparationReservation(threadId, lease)),
                      ),
                    );
                  if (
                    resolved !== undefined &&
                    resolved.thread.threadId === threadId &&
                    resolved.thread.providerInstanceId === providerInstanceId &&
                    resolved.capabilities.has("preview") === browserToolsAvailable &&
                    resolved.capabilities.has("device") === deviceToolsAvailable
                  ) {
                    lease.prepared = { mcpCredentialId: existing.providerSessionId, issued: false };
                    return lease.prepared;
                  }
                  dropPreparationReservation(threadId, lease);
                }
                yield* mcpSessionRegistry.revokeThread(threadId);
                return yield* Effect.gen(function* () {
                  const credential = yield* mcpSessionRegistry.issue({
                    threadId,
                    providerInstanceId,
                    browserToolsAvailable,
                    capabilities,
                  });
                  yield* mcpSessions.set(credential.config);
                  reserveMcpCredential(threadId, credential.config.providerSessionId);
                  lease.reservedCredentialId = credential.config.providerSessionId;
                  lease.prepared = {
                    mcpCredentialId: credential.config.providerSessionId,
                    issued: true,
                  };
                  return lease.prepared;
                }).pipe(Effect.uninterruptible);
              }),
            );
      const clearMcpSession = (threadId: ThreadId, mcpCredentialId?: string) =>
        mcpCredentialId === undefined
          ? mcpSessionRegistry
              .revokeThread(threadId)
              .pipe(Effect.tap(() => mcpSessions.clear(threadId)))
          : mcpSessionRegistry.revokeProviderSession(mcpCredentialId).pipe(
              Effect.tap(() =>
                Effect.gen(function* () {
                  if ((yield* mcpSessions.read(threadId))?.providerSessionId === mcpCredentialId) {
                    yield* mcpSessions.clear(threadId);
                  }
                }),
              ),
            );

      const publishToSubscribers = (
        subscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
        signal: ProviderSessionEventSignal,
      ) =>
        Ref.get(subscribers).pipe(
          Effect.flatMap((current) =>
            Effect.forEach(current.values(), (queue) => Queue.offer(queue, signal), {
              discard: true,
            }),
          ),
        );

      const failSubscribers = Effect.fn("ProviderSessionManager.failSubscribers")(function* (
        entry: LiveSessionEntry,
        detail: string,
      ) {
        const error = new ProviderAdapter.ProviderAdapterEventStreamError({
          driver: entry.runtime.driver,
          providerSessionId: entry.runtime.providerSessionId,
          cause: detail,
        });
        const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
        yield* Effect.forEach(
          subscribers.values(),
          (queue) =>
            Queue.offer(queue, {
              type: "failure",
              cause: Cause.fail(error),
            }),
          { discard: true },
        );
      });

      // Server shutdown interrupts subscribers rather than ending them: a
      // cleanly ended stream without a terminal fails its run, while an
      // interrupted one leaves run settlement to restart recovery.
      const interruptSubscribers = Effect.fn("ProviderSessionManager.interruptSubscribers")(
        function* (entry: LiveSessionEntry) {
          const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
          yield* Effect.forEach(
            subscribers.values(),
            (queue) => Queue.clear(queue).pipe(Effect.andThen(Queue.interrupt(queue))),
            { discard: true },
          );
        },
      );

      // Server shutdown intentionally clears them; a provider-announced Stop
      // must let consumers drain them before the stream completes.
      const endSubscribers = Effect.fn("ProviderSessionManager.endSubscribers")(function* (
        entry: LiveSessionEntry,
      ) {
        const subscribers = yield* Ref.getAndSet(entry.eventSubscribers, new Map());
        yield* Effect.forEach(subscribers.values(), (queue) => Queue.end(queue), {
          discard: true,
        });
      });

      const cancelIdleFiber = (fiber: Fiber.Fiber<void, never> | null) =>
        fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.ignore);

      const writeProviderSessionEvents = (input: {
        readonly runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly runtimeLifetime: ProviderRuntimeLifetime;
        readonly released?: boolean;
        readonly threadIds: Iterable<ThreadId>;
        readonly type: "provider-session.attached" | "provider-session.updated";
        readonly payload: OrchestrationV2ProviderSession;
      }) =>
        providerEventIngestor
          .withSessionWrite(
            input.runtimeLifetime,
            Effect.gen(function* () {
              if (
                input.released !== true &&
                (yield* Ref.get(sessions)).get(sessionKey(input.runtime.providerSessionId))
                  ?.runtime !== input.runtime
              )
                return;
              const now = yield* DateTime.now;
              const events = yield* Effect.forEach(input.threadIds, (threadId) =>
                Effect.gen(function* () {
                  return {
                    id: yield* idAllocator.allocate.event({
                      threadId,
                      providerSessionId: input.runtime.providerSessionId,
                    }),
                    type: input.type,
                    threadId,
                    driver: input.runtime.driver,
                    providerInstanceId: input.runtime.instanceId,
                    occurredAt: now,
                    payload: input.payload,
                  } satisfies OrchestrationV2DomainEvent;
                }),
              );
              if (events.length > 0) {
                yield* eventSink.write({ events });
              }
            }),
          )
          .pipe(Effect.asVoid);

      const writeReleasedSessionEvents = Effect.fn(
        "ProviderSessionManager.writeReleasedSessionEvents",
      )(function* (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
      }) {
        const now = yield* DateTime.now;
        const payload: OrchestrationV2ProviderSession = {
          ...input.entry.runtime.providerSession,
          status: releaseStatusFor(input.reason),
          updatedAt: now,
          lastError:
            input.reason === "runtime_error" ? (input.detail ?? "Provider runtime failed.") : null,
        };
        yield* writeProviderSessionEvents({
          runtime: input.entry.runtime,
          runtimeLifetime: input.entry.runtimeLifetime,
          released: true,
          threadIds: input.entry.ownedThreadIds,
          type: "provider-session.updated",
          payload,
        });
      });

      const writeReleasedRuntimeRequestEvents = (input: {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
      }) =>
        providerEventIngestor.withLifetimeWrite(
          input.entry.runtimeLifetime,
          Effect.gen(function* () {
            const providerSessionId = input.entry.runtime.providerSessionId;
            const now = yield* DateTime.now;
            const status = releasedRuntimeRequestStatusFor(input.reason);
            const reason =
              input.reason === "runtime_error"
                ? "Provider session failed before this runtime request was resolved."
                : "Provider session was closed before this runtime request was resolved.";

            for (const group of providerEventIngestor.getOwnedRequestGroups(
              input.entry.runtimeLifetime,
            )) {
              const events: Array<OrchestrationV2DomainEvent> = [];
              const threadId = group.threadId;
              const projection = yield* projectionStore.getThreadRecords(
                threadId,
                ["runtimeRequests", "nodes", "turnItems"],
                { turnItemTypes: ["approval_request", "user_input_request"] },
              );
              const releasedRequests = projection.runtimeRequests.filter(
                (request) =>
                  request.id === group.requestId &&
                  request.nodeId === group.nodeId &&
                  request.providerTurnId === group.providerTurnId &&
                  request.kind === group.kind &&
                  request.status === "pending" &&
                  request.responseCapability.type === "live" &&
                  request.responseCapability.providerSessionId === providerSessionId,
              );

              for (const request of releasedRequests) {
                events.push({
                  id: yield* idAllocator.allocate.event({
                    threadId,
                    providerSessionId,
                  }),
                  type: "runtime-request.updated",
                  threadId,
                  nodeId: request.nodeId,
                  driver: input.entry.runtime.driver,
                  occurredAt: now,
                  payload: {
                    ...request,
                    status,
                    responseCapability: {
                      type: "not_resumable",
                      reason,
                    },
                    resolvedAt: now,
                  },
                });

                const requestNode = projection.nodes.find(
                  (node) => node.id === request.nodeId && node.runtimeRequestId === request.id,
                );
                if (requestNode !== undefined) {
                  events.push({
                    id: yield* idAllocator.allocate.event({
                      threadId,
                      providerSessionId,
                    }),
                    type: "node.updated",
                    threadId,
                    ...(requestNode.runId === null ? {} : { runId: requestNode.runId }),
                    nodeId: requestNode.id,
                    driver: input.entry.runtime.driver,
                    occurredAt: now,
                    payload: {
                      ...requestNode,
                      status: input.reason === "runtime_error" ? "failed" : "cancelled",
                      completedAt: now,
                    },
                  });
                }

                const turnItems = projection.turnItems.filter(
                  (item) =>
                    (item.type === "approval_request" || item.type === "user_input_request") &&
                    item.requestId === request.id &&
                    group.itemIds.includes(item.id),
                );
                for (const turnItem of turnItems) {
                  events.push({
                    id: yield* idAllocator.allocate.event({
                      threadId,
                      providerSessionId,
                    }),
                    type: "turn-item.updated",
                    threadId,
                    ...(turnItem.runId === null ? {} : { runId: turnItem.runId }),
                    ...(turnItem.nodeId === null ? {} : { nodeId: turnItem.nodeId }),
                    driver: input.entry.runtime.driver,
                    occurredAt: now,
                    payload: {
                      ...turnItem,
                      status: input.reason === "runtime_error" ? "failed" : "cancelled",
                      completedAt: now,
                      updatedAt: now,
                    },
                  });
                }
                yield* eventSink.writeIfRuntimeRequestCurrent({
                  threadId,
                  expectedRequest: request,
                  events,
                });
              }
              providerEventIngestor.settleOwnedRequestGroups(input.entry.runtimeLifetime, [
                group.requestId,
              ]);
            }
          }),
        );

      // Read tasks as soon as the session leaves the live map, before a replacement reports its own.
      // Keep OpenCode's roster: its background subagents share a server and can outlive the session.
      const readReleasedBackgroundTasks = Effect.fn(
        "ProviderSessionManager.readReleasedBackgroundTasks",
      )(
        function* (input: {
          readonly entry: LiveSessionEntry;
          readonly reason: ProviderSessionReleaseReason;
          readonly detachedThreadId?: ThreadId;
        }) {
          if (
            input.reason === "server_shutdown" ||
            input.entry.runtime.backgroundTasksEndWithProcess !== true
          ) {
            return [];
          }
          const providerSessionId = input.entry.runtime.providerSessionId;
          // A detach that released the session already removed its thread
          // from the attached set.
          const threadIds = new Set(input.entry.ownedThreadIds);
          if (input.detachedThreadId !== undefined) threadIds.add(input.detachedThreadId);
          const released = [];
          for (const threadId of threadIds) {
            const projection = yield* projectionStore.getThreadRecords(threadId, [
              "providerThreads",
            ]);
            for (const providerThread of projection.providerThreads) {
              const tasks = providerThread.pendingBackgroundTasks ?? [];
              if (providerThread.providerSessionId !== providerSessionId || tasks.length === 0) {
                continue;
              }
              released.push({
                threadId,
                providerSessionId,
                providerThreadId: providerThread.id,
                taskIds: new Set(tasks.map((task) => task.taskId)),
              });
            }
          }
          return released;
        },
        (effect, input) =>
          effect.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.provider-session-release-roster-read-failed", {
                providerSessionId: input.entry.runtime.providerSessionId,
                cause,
              }).pipe(Effect.as([])),
            ),
          ),
      );
      type ReleasedBackgroundTasks = Effect.Success<
        ReturnType<typeof readReleasedBackgroundTasks>
      >[number];

      // Dead processes cannot report task endings; stale rosters keep threads Waiting.
      const writeReleasedBackgroundWorkEvents = (
        released: ReadonlyArray<ReleasedBackgroundTasks>,
      ) =>
        Effect.forEach(
          released,
          ({ threadId, providerSessionId, providerThreadId, taskIds }) =>
            Effect.gen(function* () {
              for (let attempt = 0; attempt < 3; attempt++) {
                const projection = yield* projectionStore.getThreadRecords(threadId, [
                  "providerThreads",
                ]);
                const providerThread = projection.providerThreads.find(
                  (candidate) => candidate.id === providerThreadId,
                );
                const tasks = providerThread?.pendingBackgroundTasks ?? [];
                const remaining = tasks.filter((task) => !taskIds.has(task.taskId));
                if (providerThread === undefined || remaining.length === tasks.length) return;
                const now = yield* DateTime.now;
                const { committed } = yield* eventSink.writeIfProviderThreadUnchanged({
                  expected: providerThread,
                  events: [
                    {
                      id: yield* idAllocator.allocate.event({ threadId, providerSessionId }),
                      type: "provider-thread.updated",
                      threadId,
                      driver: providerThread.driver,
                      providerInstanceId: providerThread.providerInstanceId,
                      occurredAt: now,
                      payload: {
                        ...providerThread,
                        pendingBackgroundTasks: remaining,
                        updatedAt: now,
                      },
                    },
                  ],
                });
                if (committed) return;
              }
              yield* Effect.logWarning(
                "orchestration-v2.provider-session-release-roster-conflict",
                {
                  providerSessionId,
                  providerThreadId,
                },
              );
              // Keep this lifetime's cleanup in the release retry queue until
              // its captured tasks are gone or a compare-and-set commits.
              return yield* new ProviderSessionRosterCleanupError({
                providerSessionId,
                providerThreadId,
              });
            }),
          { discard: true },
        );

      interface ReleaseJob {
        readonly entry: LiveSessionEntry;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        sessionPending: boolean;
        requestsPending: boolean;
        backgroundPending: boolean;
        releasedBackgroundTasks: ReadonlyArray<ReleasedBackgroundTasks>;
      }
      const pendingReleases = new Map<ProviderRuntimeLifetime, ReleaseJob>();

      const clearUnheldCredential = (
        threadId: ThreadId,
        mcpCredentialId: string,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined,
      ) =>
        mcpPrepareLock.withLock(
          threadId,
          Ref.get(sessions).pipe(
            Effect.flatMap((current) => {
              const heldElsewhere =
                isMcpCredentialReserved(threadId, mcpCredentialId) ||
                Array.from(current.values()).some(
                  (other) =>
                    other.runtime !== expectedRuntime &&
                    (other.attachedThreadIds.has(threadId) ||
                      other.mcpCredentialIdByThread.get(threadId) === mcpCredentialId),
                );
              return heldElsewhere ? Effect.void : clearMcpSession(threadId, mcpCredentialId);
            }),
          ),
        );

      const clearReleasedCredentials = (entry: LiveSessionEntry) =>
        Effect.forEach(
          entry.mcpCredentialIdByThread,
          ([threadId, credentialId]) =>
            clearUnheldCredential(threadId, credentialId, entry.runtime),
          { discard: true },
        );

      const persistRelease = Effect.fn("ProviderSessionManager.persistRelease")(function* (
        job: ReleaseJob,
      ) {
        // Both paths are attempted: a failed session record must not keep an
        // independently writable request actionable. Successful groups are
        // removed from the obligation by the ingestor, not a frozen read set.
        const sessionExit = yield* Effect.exit(
          job.sessionPending
            ? writeReleasedSessionEvents(job).pipe(Effect.timeout(RELEASE_PERSIST_TIMEOUT_MS))
            : Effect.void,
        );
        if (Exit.isSuccess(sessionExit)) job.sessionPending = false;
        const requestExit = yield* Effect.exit(
          job.requestsPending
            ? writeReleasedRuntimeRequestEvents(job).pipe(
                Effect.timeout(RELEASE_PERSIST_TIMEOUT_MS),
              )
            : Effect.void,
        );
        if (Exit.isSuccess(requestExit)) job.requestsPending = false;
        const backgroundExit = yield* Effect.exit(
          job.backgroundPending
            ? writeReleasedBackgroundWorkEvents(job.releasedBackgroundTasks).pipe(
                Effect.timeout(RELEASE_PERSIST_TIMEOUT_MS),
              )
            : Effect.void,
        );
        if (Exit.isSuccess(backgroundExit)) job.backgroundPending = false;
        if (!job.sessionPending && !job.requestsPending && !job.backgroundPending)
          pendingReleases.delete(job.entry.runtimeLifetime);
        if (Exit.isFailure(sessionExit)) return yield* Effect.failCause(sessionExit.cause);
        if (Exit.isFailure(requestExit)) return yield* Effect.failCause(requestExit.cause);
        if (Exit.isFailure(backgroundExit)) return yield* Effect.failCause(backgroundExit.cause);
      });

      const retryRelease = Effect.fn("ProviderSessionManager.retryRelease")(function* (
        job: ReleaseJob,
      ) {
        let delay = 1_000;
        while (job.sessionPending || job.requestsPending || job.backgroundPending) {
          if (shuttingDown) return;
          yield* Effect.sleep(Duration.millis(delay));
          if (shuttingDown) return;
          const result = yield* Effect.exit(persistRelease(job));
          if (Exit.isFailure(result)) {
            yield* Effect.logWarning("orchestration-v2.driver-session.release-retry-failed", {
              providerSessionId: job.entry.runtime.providerSessionId,
              reason: job.reason,
              cause: result.cause,
            });
          }
          delay = Math.min(delay * 2, 30_000);
        }
      });

      // Scope close can wedge on a misbehaving adapter finalizer (e.g. a
      // provider process that never yields its message stream).
      const closeScopeWithin = Effect.fn("ProviderSessionManager.closeScopeWithin")(function* (
        scope: Scope.Closeable,
        annotations: { readonly providerSessionId?: ProviderSessionId; readonly reason: string },
      ) {
        const closeFiber = yield* Scope.close(scope, Exit.void).pipe(
          Effect.exit,
          Effect.forkDetach({ startImmediately: true }),
        );
        const closeExit = yield* Fiber.join(closeFiber).pipe(
          Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
        );
        if (Option.isNone(closeExit)) {
          yield* Effect.logWarning("orchestration-v2.provider-session-scope-close-timeout", {
            ...annotations,
            timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
          });
          yield* Fiber.join(closeFiber).pipe(
            Effect.flatMap((exit) =>
              Exit.isFailure(exit)
                ? Effect.logWarning("orchestration-v2.provider-session-scope-close-failed", {
                    ...annotations,
                    cause: exit.cause,
                  })
                : Effect.logInfo(
                    "orchestration-v2.provider-session-scope-close-completed-late",
                    annotations,
                  ),
            ),
            Effect.forkDetach,
          );
        }
        return closeExit;
      });

      const releaseEntry = (input: {
        readonly providerSessionId: ProviderSessionId;
        readonly reason: ProviderSessionReleaseReason;
        readonly detail?: string;
        readonly detachedThreadId?: ThreadId;
        readonly cancelIdleFiber?: boolean;
        readonly onlyIfIdleGeneration?: number;
        readonly expectedRuntime?: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly gracefulSubscribers?: boolean;
      }) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const candidate = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
            if (candidate === undefined) return;
            const removed = yield* providerEventIngestor.withLifetimeWrite(
              candidate.runtimeLifetime,
              Effect.gen(function* () {
                const removed = yield* Ref.modify(sessions, (current) => {
                  const key = sessionKey(input.providerSessionId);
                  const existing = current.get(key);
                  if (
                    existing === undefined ||
                    existing.runtime !== candidate.runtime ||
                    (input.expectedRuntime !== undefined &&
                      existing.runtime !== input.expectedRuntime) ||
                    (input.onlyIfIdleGeneration !== undefined &&
                      (existing.busyTurns.size > 0 ||
                        existing.idleGeneration !== input.onlyIfIdleGeneration))
                  ) {
                    return [Option.none<ReleaseJob>(), current] as const;
                  }
                  providerEventIngestor.retireLifetime(existing.runtimeLifetime);
                  const job: ReleaseJob = {
                    entry: existing,
                    reason: input.reason,
                    ...(input.detail === undefined ? {} : { detail: input.detail }),
                    sessionPending: true,
                    requestsPending: true,
                    backgroundPending: true,
                    releasedBackgroundTasks: [],
                  };
                  pendingReleases.set(existing.runtimeLifetime, job);
                  const updated = new Map(current);
                  updated.delete(key);
                  return [Option.some(job), updated] as const;
                });
                if (Option.isSome(removed)) {
                  removed.value.releasedBackgroundTasks = yield* readReleasedBackgroundTasks({
                    entry: removed.value.entry,
                    reason: input.reason,
                    ...(input.detachedThreadId === undefined
                      ? {}
                      : { detachedThreadId: input.detachedThreadId }),
                  });
                }
                return removed;
              }),
            );
            if (Option.isNone(removed)) return;
            const job = removed.value;
            const entry = job.entry;
            const release = Effect.gen(function* () {
              const nativeExit = yield* Effect.exit(
                Effect.gen(function* () {
                  if (input.cancelIdleFiber !== false)
                    yield* cancelIdleFiber(entry.idleFiber).pipe(Effect.forkIn(releaseScope));
                  yield* Effect.forEach(
                    entry.idleThreadUnloads.values(),
                    (pending) => cancelIdleFiber(pending.fiber).pipe(Effect.forkIn(releaseScope)),
                    { discard: true },
                  );
                  if (input.gracefulSubscribers === true) yield* endSubscribers(entry);
                  else if (input.reason === "server_shutdown") yield* interruptSubscribers(entry);
                  else
                    yield* failSubscribers(
                      entry,
                      input.detail ?? `Provider session released: ${input.reason}.`,
                    );
                  // Never hold the write lane while waiting for native finalizers.
                  const closeFiber = yield* Scope.close(entry.scope, Exit.void).pipe(
                    Effect.exit,
                    Effect.forkDetach({ startImmediately: true }),
                  );
                  const closed = yield* Fiber.join(closeFiber).pipe(
                    Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                  );
                  if (Option.isNone(closed)) {
                    yield* Effect.logWarning(
                      "orchestration-v2.provider-session-scope-close-timeout",
                      {
                        providerSessionId: input.providerSessionId,
                        reason: input.reason,
                        timeoutMs: RELEASE_SCOPE_CLOSE_TIMEOUT_MS,
                      },
                    );
                    yield* Fiber.join(closeFiber).pipe(
                      Effect.flatMap((exit) =>
                        Exit.isFailure(exit)
                          ? Effect.logWarning(
                              "orchestration-v2.provider-session-scope-close-failed",
                              { providerSessionId: input.providerSessionId, cause: exit.cause },
                            )
                          : Effect.logInfo(
                              "orchestration-v2.provider-session-scope-close-completed-late",
                              { providerSessionId: input.providerSessionId },
                            ),
                      ),
                      Effect.forkIn(releaseScope),
                    );
                  } else if (Exit.isFailure(closed.value))
                    return yield* Effect.failCause(closed.value.cause);
                }).pipe(Effect.ensuring(clearReleasedCredentials(entry))),
              );
              const persistenceExit = yield* Effect.exit(persistRelease(job));
              if (job.sessionPending || job.requestsPending || job.backgroundPending)
                yield* retryRelease(job).pipe(Effect.forkIn(releaseScope));
              if (Exit.isFailure(nativeExit)) return yield* Effect.failCause(nativeExit.cause);
              if (Exit.isFailure(persistenceExit))
                return yield* Effect.failCause(persistenceExit.cause);
            }).pipe(
              Effect.interruptible,
              withMetrics({
                counter: providerSessionsTotal,
                attributes: {
                  provider: entry.runtime.driver,
                  operation: "release",
                  reason: input.reason,
                },
              }),
            );
            const fiber = yield* release.pipe(
              Effect.forkIn(releaseScope, { startImmediately: true }),
            );
            // Interrupting the caller only stops waiting; the service retains the job.
            return yield* restore(Fiber.join(fiber));
          }),
        ).pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionReleaseError({
                providerSessionId: input.providerSessionId,
                reason: input.reason,
                cause,
              }),
            ),
          ),
        );

      const releaseIfStillIdle = Effect.fn("ProviderSessionManager.releaseIfStillIdle")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly generation: number;
          readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        }): Effect.fn.Return<void> {
          const current = yield* Ref.get(sessions);
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.runtime !== input.expectedRuntime ||
            entry.busyTurns.size > 0 ||
            entry.idleGeneration !== input.generation
          ) {
            return;
          }
          // Capture runtime identity before yielding; replacements can reuse the session id.
          const probedRuntime = entry.runtime;
          const hasPendingWork =
            probedRuntime.hasPendingBackgroundWork === undefined
              ? false
              : yield* probedRuntime.hasPendingBackgroundWork.pipe(
                  Effect.catchCause(() => Effect.succeed(false)),
                );
          if (hasPendingWork) {
            const now = yield* Clock.currentTimeMillis;
            const pinnedSinceMs = entry.pinnedSinceMs ?? now;
            if (now - pinnedSinceMs < maxIdlePinMs) {
              const shouldContinuePin = yield* Ref.modify(sessions, (latest) => {
                const latestEntry = latest.get(key);
                if (
                  latestEntry === undefined ||
                  latestEntry.busyTurns.size > 0 ||
                  latestEntry.idleGeneration !== input.generation ||
                  latestEntry.runtime !== probedRuntime
                ) {
                  return [false, latest] as const;
                }
                const updated = new Map(latest);
                updated.set(key, { ...latestEntry, pinnedSinceMs });
                return [true, updated] as const;
              });
              if (!shouldContinuePin) {
                return;
              }
              yield* Effect.logInfo("orchestration-v2.driver-session.idle-release-deferred", {
                providerSessionId: input.providerSessionId,
                pinnedForMs: now - pinnedSinceMs,
              });
              // Recheck on this fiber; scheduleIdleReleaseInternal would interrupt it and self-deadlock.
              yield* Effect.sleep(Duration.millis(idleTimeoutMs));
              return yield* releaseIfStillIdle(input);
            }
            yield* Effect.logWarning("orchestration-v2.driver-session.idle-release-pin-expired", {
              providerSessionId: input.providerSessionId,
              pinnedForMs: now - pinnedSinceMs,
            });
          }
          // Adapter probes yield; guard busyTurns and idleGeneration atomically when removing the entry.
          yield* releaseEntry({
            providerSessionId: input.providerSessionId,
            reason: "idle_timeout",
            cancelIdleFiber: false,
            onlyIfIdleGeneration: input.generation,
            expectedRuntime: probedRuntime,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.idle-release-failed", {
                providerSessionId: input.providerSessionId,
                cause,
              }),
            ),
          );
        },
      );

      const withActivityError = <A, E, R>(
        providerSessionId: ProviderSessionId,
        effect: Effect.Effect<A, E, R>,
      ): Effect.Effect<A, ProviderSessionActivityError, R> =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Effect.fail(
              new ProviderSessionActivityError({
                providerSessionId,
                cause,
              }),
            ),
          ),
        );

      const scheduleIdleReleaseInternal = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        options?: { readonly providerActivity?: boolean },
      ) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            const scheduled = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (
                entry === undefined ||
                entry.runtime !== expectedRuntime ||
                entry.busyTurns.size > 0
              )
                return [
                  Option.none<{ generation: number; previous: Fiber.Fiber<void, never> | null }>(),
                  current,
                ] as const;
              const generation = entry.idleGeneration + 1;
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                idleGeneration: generation,
                idleFiber: null,
                lastActivityAtMs: now,
                ...(options?.providerActivity === true ? { pinnedSinceMs: null } : {}),
              });
              return [Option.some({ generation, previous: entry.idleFiber }), updated] as const;
            });
            if (Option.isNone(scheduled)) return;
            const { generation, previous } = scheduled.value;
            const idleFiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
              Effect.andThen(
                releaseIfStillIdle({ providerSessionId, generation, expectedRuntime }),
              ),
              Effect.interruptible,
              Effect.forkIn(layerScope),
            );
            const installed = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (
                entry === undefined ||
                entry.runtime !== expectedRuntime ||
                entry.busyTurns.size > 0 ||
                entry.idleGeneration !== generation
              )
                return [false, current] as const;
              const updated = new Map(current);
              updated.set(key, { ...entry, idleFiber });
              return [true, updated] as const;
            });
            yield* restore(cancelIdleFiber(installed ? previous : idleFiber));
          }),
        );

      const scheduleIdleRelease = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
      ) =>
        withActivityError(
          providerSessionId,
          scheduleIdleReleaseInternal(providerSessionId, expectedRuntime),
        );

      const touchActivity = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        options?: { readonly providerActivity?: boolean },
      ) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const lastActivityAtMs = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(sessionKey(providerSessionId));
              if (entry === undefined || entry.runtime !== expectedRuntime) {
                return current;
              }
              const updated = new Map(current);
              updated.set(sessionKey(providerSessionId), {
                ...entry,
                lastActivityAtMs,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId, expectedRuntime, options);
          }),
        );

      const attachThread = (
        input: {
          readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
          readonly providerSessionId: ProviderSessionId;
          readonly threadId: ThreadId;
        },
        onClaim: () => void,
      ) =>
        withActivityError(
          input.providerSessionId,
          Ref.modify(sessions, (current) => {
            const entry = current.get(sessionKey(input.providerSessionId));
            if (
              entry === undefined ||
              entry.runtime !== input.expectedRuntime ||
              entry.attachedThreadIds.has(input.threadId)
            ) {
              return [false, current] as const;
            }
            const updated = new Map(current);
            updated.set(sessionKey(input.providerSessionId), {
              ...entry,
              attachedThreadIds: new Set([...entry.attachedThreadIds, input.threadId]),
              ownedThreadIds: new Set([...entry.ownedThreadIds, input.threadId]),
            });
            onClaim();
            return [true, updated] as const;
          }),
        );

      const removeThreadAttachment = (input: {
        readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (
            entry === undefined ||
            entry.runtime !== input.expectedRuntime ||
            !entry.attachedThreadIds.has(input.threadId)
          ) {
            return current;
          }
          const attachedThreadIds = new Set(entry.attachedThreadIds);
          attachedThreadIds.delete(input.threadId);
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.delete(input.threadId);
          const updated = new Map(current);
          updated.set(key, {
            ...entry,
            attachedThreadIds,
            loadedProviderThreadKeyByThread,
          });
          return updated;
        });

      const isProviderThreadLoaded = (input: {
        readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.get(sessions).pipe(
          Effect.map((current) => {
            const entry = current.get(sessionKey(input.providerSessionId));
            return (
              entry?.runtime === input.expectedRuntime &&
              entry.loadedProviderThreadKeyByThread.get(input.threadId) === input.providerThreadKey
            );
          }),
        );

      const markProviderThreadLoaded = (input: {
        readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerThreadKey: string;
      }) =>
        Ref.update(sessions, (current) => {
          const key = sessionKey(input.providerSessionId);
          const entry = current.get(key);
          if (entry === undefined || entry.runtime !== input.expectedRuntime) {
            return current;
          }
          const loadedProviderThreadKeyByThread = new Map(entry.loadedProviderThreadKeyByThread);
          loadedProviderThreadKeyByThread.set(input.threadId, input.providerThreadKey);
          const updated = new Map(current);
          updated.set(key, { ...entry, loadedProviderThreadKeyByThread });
          return updated;
        });

      const ensureThreadAttached = (input: {
        readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
        readonly providerSessionId: ProviderSessionId;
        readonly threadId: ThreadId;
        readonly providerInstanceId: ProviderInstanceId;
      }) =>
        threadAttachment.withLock(
          threadAttachmentKey(input),
          Effect.suspend(() => {
            const lease = makeMcpPreparationLease();
            let credentialRecorded = false;
            let claimedAttachment = false;
            return Effect.gen(function* () {
              const attached = yield* attachThread(input, () => {
                claimedAttachment = true;
              });
              if (attached) {
                const prepared = yield* prepareMcpSession(
                  input.threadId,
                  input.providerInstanceId,
                  lease,
                );
                if (prepared.mcpCredentialId !== undefined) {
                  const mcpCredentialId = prepared.mcpCredentialId;
                  yield* Ref.update(sessions, (current) => {
                    const key = sessionKey(input.providerSessionId);
                    const entry = current.get(key);
                    if (entry === undefined || entry.runtime !== input.expectedRuntime)
                      return current;
                    credentialRecorded = true;
                    const mcpCredentialIdByThread = new Map(entry.mcpCredentialIdByThread);
                    mcpCredentialIdByThread.set(input.threadId, mcpCredentialId);
                    const updated = new Map(current);
                    updated.set(key, { ...entry, mcpCredentialIdByThread });
                    return updated;
                  });
                }
                const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
                if (entry === undefined || entry.runtime !== input.expectedRuntime) {
                  return yield* new ProviderSessionActivityError({
                    providerSessionId: input.providerSessionId,
                    cause: "Provider runtime retired during thread attachment.",
                  });
                }
                if (entry.runtime === input.expectedRuntime) {
                  yield* withActivityError(
                    input.providerSessionId,
                    writeProviderSessionEvents({
                      runtime: entry.runtime,
                      runtimeLifetime: entry.runtimeLifetime,
                      threadIds: [input.threadId],
                      type: "provider-session.attached",
                      payload: entry.runtime.providerSession,
                    }),
                  );
                }
              }
            }).pipe(
              Effect.onExit((exit) =>
                Effect.gen(function* () {
                  const credentialId =
                    lease.reservedCredentialId ?? lease.prepared?.mcpCredentialId;
                  dropPreparationReservation(input.threadId, lease);
                  if (Exit.isFailure(exit) && claimedAttachment)
                    yield* removeThreadAttachment(input);
                  if ((Exit.isFailure(exit) || !credentialRecorded) && credentialId !== undefined) {
                    yield* clearUnheldCredential(
                      input.threadId,
                      credentialId,
                      lease.prepared?.issued === true ? input.expectedRuntime : undefined,
                    );
                  }
                }),
              ),
            );
          }),
        );

      const markBusy = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        turnKey: string,
      ) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            const idleFiber = yield* Ref.modify(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || entry.runtime !== expectedRuntime) {
                return [null, current] as const;
              }
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyTurns: new Set(entry.busyTurns).add(turnKey),
                idleFiber: null,
                lastActivityAtMs: now,
                pinnedSinceMs: null,
              });
              return [entry.idleFiber, updated] as const;
            });
            yield* cancelIdleFiber(idleFiber);
          }),
        );

      const markIdle = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        providerThreadId: ProviderThreadId,
        runOrdinal: number,
      ) =>
        withActivityError(
          providerSessionId,
          Effect.gen(function* () {
            const key = sessionKey(providerSessionId);
            const now = yield* Clock.currentTimeMillis;
            yield* Ref.update(sessions, (current) => {
              const entry = current.get(key);
              if (entry === undefined || entry.runtime !== expectedRuntime) {
                return current;
              }
              const busyTurns = new Set(entry.busyTurns);
              busyTurns.delete(busyTurnKey(providerThreadId, runOrdinal));
              const updated = new Map(current);
              updated.set(key, {
                ...entry,
                busyTurns,
                lastActivityAtMs: now,
              });
              return updated;
            });
            yield* scheduleIdleReleaseInternal(providerSessionId, expectedRuntime);
            yield* scheduleThreadUnload(providerSessionId, expectedRuntime, providerThreadId);
          }),
        );

      const hasBusyTurn = (entry: LiveSessionEntry, providerThreadId: ProviderThreadId) => {
        const prefix = busyTurnPrefix(providerThreadId);
        for (const turnKey of entry.busyTurns) {
          if (turnKey.startsWith(prefix)) return true;
        }
        return false;
      };

      const updateIdleThreadUnload = (
        providerSessionId: ProviderSessionId,
        expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        threadId: ThreadId,
        update: (current: IdleThreadUnload | undefined) => IdleThreadUnload | undefined,
      ) =>
        Ref.modify(sessions, (current) => {
          const key = sessionKey(providerSessionId);
          const entry = current.get(key);
          if (entry === undefined || entry.runtime !== expectedRuntime)
            return [undefined, current] as const;
          const previous = entry.idleThreadUnloads.get(threadId);
          const next = update(previous);
          const idleThreadUnloads = new Map(entry.idleThreadUnloads);
          if (next === undefined) idleThreadUnloads.delete(threadId);
          else idleThreadUnloads.set(threadId, next);
          const updated = new Map(current);
          updated.set(key, { ...entry, idleThreadUnloads });
          return [previous, updated] as const;
        });

      /**
       * Called before the thread is resumed or given a turn, so an unload
       * cannot land between a resume that found the thread loaded and the turn
       * that relies on it.
       */
      const holdThreadLoaded = Effect.fn("ProviderSessionManager.holdThreadLoaded")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
          readonly threadId: ThreadId;
          readonly providerThread: OrchestrationV2ProviderThread;
        }) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(input.providerSessionId));
          if (
            entry === undefined ||
            entry.runtime !== input.expectedRuntime ||
            !entry.supportsMultipleProviderThreads ||
            entry.exposedRuntime.unloadThread === undefined ||
            input.providerThread.nativeThreadRef === null
          ) {
            return;
          }
          const previous = yield* updateIdleThreadUnload(
            input.providerSessionId,
            input.expectedRuntime,
            input.threadId,
            (current) => ({
              providerThread: input.providerThread,
              generation: (current?.generation ?? 0) + 1,
              fiber: null,
            }),
          );
          yield* cancelIdleFiber(previous?.fiber ?? null);
        },
      );

      const scheduleThreadUnload = Effect.fn("ProviderSessionManager.scheduleThreadUnload")(
        function* (
          providerSessionId: ProviderSessionId,
          expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
          providerThreadId: ProviderThreadId,
        ) {
          const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
          if (
            entry === undefined ||
            entry.runtime !== expectedRuntime ||
            hasBusyTurn(entry, providerThreadId)
          )
            return;
          const tracked = Array.from(entry.idleThreadUnloads).find(
            ([, pending]) => pending.providerThread.id === providerThreadId,
          );
          if (tracked === undefined) return;
          const [threadId, pending] = tracked;
          const generation = pending.generation + 1;
          const fiber = yield* Effect.sleep(Duration.millis(idleTimeoutMs)).pipe(
            Effect.andThen(
              unloadIdleThread({ providerSessionId, expectedRuntime, threadId, generation }),
            ),
            Effect.forkIn(layerScope),
          );
          // A turn that started meanwhile already moved the generation on.
          const previous = yield* updateIdleThreadUnload(
            providerSessionId,
            expectedRuntime,
            threadId,
            (current) =>
              current?.generation === pending.generation
                ? { ...current, generation, fiber }
                : current,
          );
          yield* cancelIdleFiber(
            previous?.generation === pending.generation ? previous.fiber : fiber,
          );
        },
      );

      const unloadIdleThread = Effect.fn("ProviderSessionManager.unloadIdleThread")(
        function* (input: {
          readonly providerSessionId: ProviderSessionId;
          readonly expectedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime;
          readonly threadId: ThreadId;
          readonly generation: number;
        }): Effect.fn.Return<void> {
          const outcome = yield* threadAttachment.withLock(
            threadAttachmentKey(input),
            Effect.gen(function* () {
              const key = sessionKey(input.providerSessionId);
              const entry = (yield* Ref.get(sessions)).get(key);
              const pending = entry?.idleThreadUnloads.get(input.threadId);
              const unloadThread = entry?.exposedRuntime.unloadThread;
              if (
                entry === undefined ||
                entry.runtime !== input.expectedRuntime ||
                pending === undefined ||
                pending.generation !== input.generation ||
                unloadThread === undefined ||
                !entry.attachedThreadIds.has(input.threadId) ||
                hasBusyTurn(entry, pending.providerThread.id)
              ) {
                return "skipped" as const;
              }
              // Unloading kills background terminals; keep threads with pending work loaded.
              const hasPendingWork =
                entry.runtime.hasPendingBackgroundWorkForThread === undefined
                  ? false
                  : yield* entry.runtime
                      .hasPendingBackgroundWorkForThread(pending.providerThread)
                      .pipe(Effect.catchCause(() => Effect.succeed(false)));
              if (hasPendingWork) return "deferred" as const;
              const unloading = yield* Ref.modify(sessions, (current) => {
                const latest = current.get(key);
                if (
                  latest?.runtime !== entry.runtime ||
                  latest.idleThreadUnloads.get(input.threadId)?.generation !== input.generation
                ) {
                  return [false, current] as const;
                }
                const loadedProviderThreadKeyByThread = new Map(
                  latest.loadedProviderThreadKeyByThread,
                );
                loadedProviderThreadKeyByThread.delete(input.threadId);
                const idleThreadUnloads = new Map(latest.idleThreadUnloads);
                idleThreadUnloads.delete(input.threadId);
                const updated = new Map(current);
                updated.set(key, {
                  ...latest,
                  loadedProviderThreadKeyByThread,
                  idleThreadUnloads,
                });
                return [true, updated] as const;
              });
              if (!unloading) return "skipped" as const;
              yield* unloadThread({ providerThread: pending.providerThread }).pipe(
                Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                Effect.catchCause((cause) =>
                  Effect.logWarning("orchestration-v2.driver-session.idle-unload-failed", {
                    providerSessionId: input.providerSessionId,
                    threadId: input.threadId,
                    providerThreadId: pending.providerThread.id,
                    cause,
                  }),
                ),
              );
              return "unloaded" as const;
            }),
          );
          // Wait outside the lock so the next attach is not blocked.
          if (outcome === "deferred") {
            yield* Effect.sleep(Duration.millis(idleTimeoutMs));
            return yield* unloadIdleThread(input);
          }
        },
      );

      const observeActivity = (
        providerSessionId: ProviderSessionId,
        activity: Effect.Effect<void, ProviderSessionActivityError>,
      ) =>
        activity.pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.driver-session.activity-failed", {
              providerSessionId,
              cause,
            }),
          ),
        );

      const makeEventSubscription = Effect.fn("ProviderSessionManager.makeEventSubscription")(
        function* (
          subscribers: Ref.Ref<
            ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
          >,
        ): Effect.fn.Return<ProviderAdapter.ProviderAdapterV2EventSubscription> {
          const queue = yield* Queue.unbounded<ProviderSessionEventSignal, Cause.Done>();
          const subscriberId = yield* Ref.getAndUpdate(nextSubscriberId, (value) => value + 1);
          yield* Ref.update(subscribers, (current) => {
            const updated = new Map(current);
            updated.set(subscriberId, queue);
            return updated;
          });
          const close = Ref.modify(subscribers, (current) => {
            if (!current.has(subscriberId)) {
              return [false, current] as const;
            }
            const updated = new Map(current);
            updated.delete(subscriberId);
            return [true, updated] as const;
          }).pipe(
            Effect.flatMap((removed) =>
              removed
                ? Queue.clear(queue).pipe(Effect.andThen(Queue.end(queue)), Effect.asVoid)
                : Effect.void,
            ),
          );
          const events = Stream.fromQueue(queue).pipe(
            Stream.mapEffect((signal) =>
              signal.type === "event"
                ? Effect.succeed(signal.event)
                : Effect.failCause(signal.cause),
            ),
            Stream.ensuring(close),
          );
          return { events, close } satisfies ProviderAdapter.ProviderAdapterV2EventSubscription;
        },
      );

      const requireCurrentRuntime = Effect.fn("ProviderSessionManager.requireCurrentRuntime")(
        function* <A, E, R>(
          runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
          effect: Effect.Effect<A, E, R>,
        ): Effect.fn.Return<A, E | ProviderAdapter.ProviderAdapterProtocolError, R> {
          const current = yield* Ref.get(sessions);
          if (current.get(sessionKey(runtime.providerSessionId))?.runtime !== runtime) {
            return yield* new ProviderAdapter.ProviderAdapterProtocolError({
              driver: runtime.driver,
              detail: "Provider runtime lifetime has ended.",
            });
          }
          return yield* effect;
        },
      );

      const decorateRuntime = (
        runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime,
        runtimeLifetime: ProviderRuntimeLifetime,
        eventSubscribers: Ref.Ref<
          ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
        >,
      ): ManagedProviderSessionRuntime => {
        const providerSessionId = runtime.providerSessionId;
        const subscribeEvents = makeEventSubscription(eventSubscribers);
        const turnMetrics = (operation: string, model?: string) =>
          withMetrics({
            counter: providerTurnsTotal,
            ...(operation === "send" ? { timer: providerTurnDuration } : {}),
            attributes: {
              provider: runtime.driver,
              operation,
              modelFamily: normalizeModelMetricLabel(model),
            },
          });
        return {
          ...runtime,
          runtimeLifetime,
          subscribeEvents,
          events: Stream.unwrap(
            subscribeEvents.pipe(Effect.map((subscription) => subscription.events)),
          ),
          ensureThread: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                expectedRuntime: runtime,
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(requireCurrentRuntime(runtime, runtime.ensureThread(input))),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId: input.threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    modelSelection: input.modelSelection,
                    runtimePolicy: input.runtimePolicy,
                  }),
                }),
              ),
            ),
          resumeThread: (input) => {
            const threadId = input.threadId ?? input.providerThread.appThreadId;
            if (threadId === null || threadId === undefined) {
              return requireCurrentRuntime(runtime, runtime.resumeThread(input));
            }
            const providerThreadKey = providerThreadLoadKey({
              providerThread: input.providerThread,
              ...(input.modelSelection === undefined
                ? {}
                : { modelSelection: input.modelSelection }),
              ...(input.runtimePolicy === undefined ? {} : { runtimePolicy: input.runtimePolicy }),
            });
            return observeActivity(
              providerSessionId,
              ensureThreadAttached({
                expectedRuntime: runtime,
                providerSessionId,
                threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                holdThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                isProviderThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId,
                  providerThreadKey,
                }),
              ),
              Effect.flatMap((loaded) =>
                loaded
                  ? Effect.succeed(input.providerThread)
                  : requireCurrentRuntime(runtime, runtime.resumeThread(input)),
              ),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            );
          },
          forkThread: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                expectedRuntime: runtime,
                providerSessionId,
                threadId: input.targetThreadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(requireCurrentRuntime(runtime, runtime.forkThread(input))),
              Effect.tap((providerThread) =>
                markProviderThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId: input.targetThreadId,
                  providerThreadKey: providerThreadLoadKey({
                    providerThread,
                    ...(input.modelSelection === undefined
                      ? {}
                      : { modelSelection: input.modelSelection }),
                    ...(input.runtimePolicy === undefined
                      ? {}
                      : { runtimePolicy: input.runtimePolicy }),
                  }),
                }),
              ),
            ),
          startTurn: (input) =>
            observeActivity(
              providerSessionId,
              ensureThreadAttached({
                expectedRuntime: runtime,
                providerSessionId,
                threadId: input.threadId,
                providerInstanceId: runtime.instanceId,
              }),
            ).pipe(
              Effect.andThen(
                holdThreadLoaded({
                  expectedRuntime: runtime,
                  providerSessionId,
                  threadId: input.threadId,
                  providerThread: input.providerThread,
                }),
              ),
              Effect.andThen(
                Effect.acquireUseRelease(
                  observeActivity(
                    providerSessionId,
                    markBusy(
                      providerSessionId,
                      runtime,
                      busyTurnKey(input.providerThread.id, input.runOrdinal),
                    ),
                  ),
                  () =>
                    requireCurrentRuntime(runtime, runtime.startTurn(input)).pipe(
                      turnMetrics("send", input.modelSelection.model),
                    ),
                  (_, exit) =>
                    Exit.isFailure(exit)
                      ? observeActivity(
                          providerSessionId,
                          markIdle(
                            providerSessionId,
                            runtime,
                            input.providerThread.id,
                            input.runOrdinal,
                          ),
                        )
                      : Effect.void,
                ),
              ),
            ),
          steerTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId, runtime)).pipe(
              Effect.andThen(
                requireCurrentRuntime(runtime, runtime.steerTurn(input)).pipe(turnMetrics("steer")),
              ),
            ),
          interruptTurn: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId, runtime)).pipe(
              Effect.andThen(
                requireCurrentRuntime(runtime, runtime.interruptTurn(input)).pipe(
                  turnMetrics("interrupt"),
                ),
              ),
            ),
          respondToRuntimeRequest: (input) =>
            observeActivity(providerSessionId, touchActivity(providerSessionId, runtime)).pipe(
              Effect.andThen(
                requireCurrentRuntime(runtime, runtime.respondToRuntimeRequest(input)).pipe(
                  turnMetrics("runtime-request-response"),
                ),
              ),
            ),
        };
      };

      const persistProviderSessionUpdate = Effect.fn(
        "ProviderSessionManager.persistProviderSessionUpdate",
      )(
        function* (
          entry: LiveSessionEntry,
          event: Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { readonly type: "provider_session.updated" }
          >,
        ) {
          const current = (yield* Ref.get(sessions)).get(
            sessionKey(entry.runtime.providerSessionId),
          );
          if (current?.runtime !== entry.runtime) {
            return;
          }
          yield* writeProviderSessionEvents({
            runtime: entry.runtime,
            runtimeLifetime: entry.runtimeLifetime,
            threadIds: current.attachedThreadIds,
            type: "provider-session.updated",
            payload: event.providerSession,
          });
        },
        (effect, entry, _event) =>
          effect.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.driver-session.status-persist-failed", {
                providerSessionId: entry.runtime.providerSessionId,
                cause,
              }),
            ),
          ),
      );

      const startEventPump = (entry: LiveSessionEntry) => {
        let stoppedByProvider = false;
        return entry.runtime.events.pipe(
          Stream.runForEach((event) => {
            if (shutdownSignal.received) return Effect.void;
            // Live output bypasses persistence and subscribers so floods cannot delay domain events.
            if (event.type === "command_output.delta") {
              return commandOutput.append(event);
            }
            const releaseLiveOutput =
              event.type === "turn_item.updated" &&
              event.turnItem.type === "command_execution" &&
              !isOrchestrationV2WorkActive(event.turnItem.status)
                ? commandOutput.finish({
                    threadId: event.turnItem.threadId,
                    itemId: event.turnItem.id,
                  })
                : Effect.void;
            if (
              event.type === "provider_session.updated" &&
              event.providerSession.status === "stopped"
            ) {
              stoppedByProvider = true;
            }
            return observeActivity(
              entry.runtime.providerSessionId,
              event.type === "turn.terminal"
                ? markIdle(
                    entry.runtime.providerSessionId,
                    entry.runtime,
                    event.providerThreadId,
                    event.runOrdinal,
                  )
                : touchActivity(entry.runtime.providerSessionId, entry.runtime, {
                    providerActivity: true,
                  }),
            ).pipe(
              Effect.andThen(
                event.type === "provider_session.updated"
                  ? persistProviderSessionUpdate(entry, event)
                  : Effect.void,
              ),
              Effect.andThen(
                Effect.gen(function* () {
                  if (
                    event.type === "provider_thread.updated" &&
                    event.providerThread.status === "error"
                  ) {
                    const threadId = event.providerThread.appThreadId;
                    yield* Ref.update(sessions, (sessions) => {
                      const key = sessionKey(entry.runtime.providerSessionId);
                      const latest = sessions.get(key);
                      if (latest?.runtime !== entry.runtime || threadId === null) return sessions;
                      const loadedProviderThreadKeyByThread = new Map(
                        latest.loadedProviderThreadKeyByThread,
                      );
                      loadedProviderThreadKeyByThread.delete(threadId);
                      return new Map(sessions).set(key, {
                        ...latest,
                        loadedProviderThreadKeyByThread,
                      });
                    });
                  }
                  // Some providers can block before a run subscriber exists
                  // (project trust, login, or session-switch hooks). Persist
                  // their runless request artifacts directly so the normal T3
                  // request UI can answer them and unblock session setup.
                  const threadId = sessionScopedRuntimeRequestThreadId(event);
                  if (threadId !== undefined) {
                    yield* Effect.gen(function* () {
                      yield* providerEventIngestor
                        .ingestNormalized({
                          runtimeLifetime: entry.runtimeLifetime,
                          providerSessionId: entry.runtime.providerSessionId,
                          providerInstanceId: entry.runtime.instanceId,
                          threadId,
                          event,
                        })
                        .pipe(
                          Effect.mapError(
                            (cause) =>
                              new ProviderAdapter.ProviderAdapterEventStreamError({
                                driver: entry.runtime.driver,
                                providerSessionId: entry.runtime.providerSessionId,
                                cause,
                              }),
                          ),
                        );
                    }).pipe(entry.requestEventPermit.withPermits(1));
                    return;
                  }
                  yield* publishToSubscribers(entry.eventSubscribers, { type: "event", event });
                  yield* releaseLiveOutput;
                }),
              ),
            );
          }),
          Effect.ensuring(
            Effect.sync(() => providerEventIngestor.discardBufferedRequests(entry.runtimeLifetime)),
          ),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              if (shutdownSignal.received) return;
              const current = (yield* Ref.get(sessions)).get(
                sessionKey(entry.runtime.providerSessionId),
              );
              if (current?.runtime !== entry.runtime) {
                return;
              }
              if (stoppedByProvider && Exit.isSuccess(exit)) {
                yield* releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  expectedRuntime: entry.runtime,
                  reason: "manual_shutdown",
                  gracefulSubscribers: true,
                }).pipe(Effect.ignore);
                return;
              }
              // Subscribers read an interrupt as server shutdown, so keep it for that.
              const cause =
                Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                  ? exit.cause
                  : Cause.fail(
                      new ProviderAdapter.ProviderAdapterEventStreamError({
                        driver: entry.runtime.driver,
                        providerSessionId: entry.runtime.providerSessionId,
                        cause: "Provider event stream ended unexpectedly.",
                      }),
                    );
              yield* publishToSubscribers(entry.eventSubscribers, {
                type: "failure",
                cause,
              });
              yield* Ref.set(entry.eventSubscribers, new Map());
              yield* releaseEntry({
                providerSessionId: entry.runtime.providerSessionId,
                expectedRuntime: entry.runtime,
                reason: "runtime_error",
                detail: Cause.pretty(cause),
              }).pipe(Effect.ignore);
            }),
          ),
          Effect.forkIn(layerScope),
        );
      };

      // Shutdown must fit the launcher's 5 s deadline with up to 15 sessions.
      const sessionScopes = yield* Scope.make("parallel");
      const shutdown = Effect.gen(function* () {
        shuttingDown = true;
        const activeSessions = [...(yield* Ref.get(sessions)).values()];
        yield* Effect.forEach(
          activeSessions,
          (entry) =>
            releaseEntry({
              providerSessionId: entry.runtime.providerSessionId,
              expectedRuntime: entry.runtime,
              reason: "server_shutdown",
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.shutdown-release-failed", {
                  providerSessionId: entry.runtime.providerSessionId,
                  cause,
                }),
              ),
            ),
          { concurrency: "unbounded", discard: true },
        );
        yield* Effect.forEach(
          [...pendingReleases.values()],
          (job) =>
            persistRelease(job).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("orchestration-v2.driver-session.shutdown-cleanup-failed", {
                  providerSessionId: job.entry.runtime.providerSessionId,
                  cause,
                }),
              ),
            ),
          { concurrency: 8, discard: true },
        ).pipe(Effect.interruptible, Effect.timeoutOption(RELEASE_PERSIST_TIMEOUT_MS * 2));
      });
      yield* Effect.addFinalizer(() =>
        shutdown.pipe(
          Effect.ensuring(closeScopeWithin(sessionScopes, { reason: "server_shutdown" })),
          Effect.ensuring(Scope.close(releaseScope, Exit.void)),
        ),
      );

      return ProviderSessionManagerV2.of({
        shutdown,
        open: (input) =>
          sessionOpen.withLock(
            input.providerSessionId,
            Effect.gen(function* () {
              const cwd = input.runtimePolicy.cwd;
              if (cwd !== null) {
                const workspaceIsDirectory = yield* fileSystem.stat(cwd).pipe(
                  Effect.map((stat) => stat.type === "Directory"),
                  Effect.catch((error) => Effect.succeed(error.reason._tag !== "NotFound")),
                );
                if (!workspaceIsDirectory) {
                  return yield* new ProviderWorkspaceMissingError({
                    threadId: input.threadId,
                    cwd,
                  });
                }
              }
              const key = sessionKey(input.providerSessionId);
              const existing = (yield* Ref.get(sessions)).get(key);
              if (existing !== undefined) {
                const attached = existing.attachedThreadIds.has(input.threadId);
                if (!attached && !existing.supportsMultipleProviderThreads) {
                  return yield* new ProviderSessionOpenError({
                    instanceId: input.modelSelection.instanceId,
                    providerSessionId: input.providerSessionId,
                    cause: `Provider ${existing.runtime.driver} does not support attaching multiple app threads to one session.`,
                  });
                }
                if (!attached) yield* agentScope.clear(input.threadId);
                yield* ensureThreadAttached({
                  expectedRuntime: existing.runtime,
                  providerSessionId: input.providerSessionId,
                  threadId: input.threadId,
                  providerInstanceId: existing.runtime.instanceId,
                });
                yield* touchActivity(input.providerSessionId, existing.runtime);
                return existing.exposedRuntime;
              }

              if (shuttingDown || pendingReleases.size >= MAX_PENDING_RELEASES) {
                return yield* new ProviderSessionOpenError({
                  instanceId: input.modelSelection.instanceId,
                  providerSessionId: input.providerSessionId,
                  cause: shuttingDown
                    ? "Provider session manager is shutting down."
                    : "Provider session cleanup must recover before another runtime can open.",
                });
              }
              const adapter = yield* registry.get(input.modelSelection.instanceId).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderSessionOpenError({
                      instanceId: input.modelSelection.instanceId,
                      providerSessionId: input.providerSessionId,
                      cause,
                    }),
                ),
              );
              return yield* Effect.acquireUseRelease(
                Effect.sync(makeMcpPreparationLease),
                (lease) =>
                  Effect.gen(function* () {
                    const prepared = yield* prepareMcpSession(
                      input.threadId,
                      input.modelSelection.instanceId,
                      lease,
                    );
                    const mcpCredentialId = prepared.mcpCredentialId;
                    // The reservation from prepare protects the credential (which
                    // eager adapters bake into the provider process during
                    // openSession) from racing releases until this session's entry
                    // is recorded below. Dropped exactly once on every path.
                    const dropReservation = Effect.sync(() =>
                      dropPreparationReservation(input.threadId, lease),
                    );
                    const sessionScope = yield* Scope.fork(sessionScopes);
                    yield* agentScope.clear(input.threadId);
                    let registered = false;
                    let openedRuntime: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined;
                    return yield* Effect.acquireUseRelease(
                      Effect.succeed(sessionScope),
                      () =>
                        Effect.gen(function* () {
                          const runtime = yield* adapter
                            .openSession({
                              threadId: input.threadId,
                              providerSessionId: input.providerSessionId,
                              modelSelection: input.modelSelection,
                              runtimePolicy: input.runtimePolicy,
                              ...(input.resumeFromSession === undefined
                                ? {}
                                : { resumeFromSession: input.resumeFromSession }),
                              ...(input.initialNativeThreadId === undefined
                                ? {}
                                : { initialNativeThreadId: input.initialNativeThreadId }),
                              ...(input.initialProviderItemIdentityVersion === undefined
                                ? {}
                                : {
                                    initialProviderItemIdentityVersion:
                                      input.initialProviderItemIdentityVersion,
                                  }),
                            })
                            .pipe(
                              Effect.provideService(Scope.Scope, sessionScope),
                              withMetrics({
                                counter: providerSessionsTotal,
                                attributes: { provider: adapter.driver, operation: "open" },
                              }),
                              Effect.mapError(
                                (cause) =>
                                  new ProviderSessionOpenError({
                                    instanceId: input.modelSelection.instanceId,
                                    providerSessionId: input.providerSessionId,
                                    cause,
                                  }),
                              ),
                            );
                          openedRuntime = runtime;
                          const eventSubscribers = yield* Ref.make<
                            ReadonlyMap<number, Queue.Queue<ProviderSessionEventSignal, Cause.Done>>
                          >(new Map());
                          const runtimeLifetime = yield* providerEventIngestor.createLifetime(
                            input.providerSessionId,
                          );
                          const exposedRuntime = decorateRuntime(
                            runtime,
                            runtimeLifetime,
                            eventSubscribers,
                          );
                          const now = yield* Clock.currentTimeMillis;
                          const entry: LiveSessionEntry = {
                            attachedThreadIds: new Set([input.threadId]),
                            ownedThreadIds: new Set([input.threadId]),
                            loadedProviderThreadKeyByThread: new Map(),
                            mcpCredentialIdByThread:
                              mcpCredentialId === undefined
                                ? new Map()
                                : new Map([[input.threadId, mcpCredentialId]]),
                            supportsMultipleProviderThreads:
                              runtime.providerSession.capabilities.sessions
                                .supportsMultipleProviderThreadsPerSession,
                            runtime,
                            runtimeLifetime,
                            exposedRuntime,
                            eventSubscribers,
                            requestEventPermit: yield* Semaphore.make(1),
                            scope: sessionScope,
                            idleGeneration: 0,
                            busyTurns: new Set(),
                            idleThreadUnloads: new Map(),
                            lastActivityAtMs: now,
                            idleFiber: null,
                            pinnedSinceMs: null,
                          };
                          const installed = yield* providerEventIngestor.withLifetimeWrite(
                            runtimeLifetime,
                            Ref.modify(sessions, (current) => {
                              if (shuttingDown) return [false, current] as const;
                              providerEventIngestor.activateLifetime(runtimeLifetime);
                              registered = true;
                              lease.installed = true;
                              const updated = new Map(current);
                              updated.set(key, entry);
                              return [true, updated] as const;
                            }),
                          );
                          if (!installed)
                            return yield* new ProviderSessionOpenError({
                              instanceId: input.modelSelection.instanceId,
                              providerSessionId: input.providerSessionId,
                              cause:
                                "Provider session manager shut down while the runtime was opening.",
                            });
                          // The entry now guards the credential via its recorded id, so
                          // the pre-open reservation can be dropped.
                          yield* dropReservation;
                          yield* withActivityError(
                            input.providerSessionId,
                            writeProviderSessionEvents({
                              runtime,
                              runtimeLifetime,
                              threadIds: [input.threadId],
                              type: "provider-session.attached",
                              payload: runtime.providerSession,
                            }),
                          ).pipe(
                            Effect.tapError(() =>
                              releaseEntry({
                                providerSessionId: input.providerSessionId,
                                expectedRuntime: runtime,
                                reason: "runtime_error",
                                detail: "Failed to persist the provider-session attachment.",
                              }).pipe(Effect.ignore),
                            ),
                          );
                          if ((yield* Ref.get(sessions)).get(key)?.runtime !== runtime) {
                            return yield* new ProviderSessionOpenError({
                              instanceId: input.modelSelection.instanceId,
                              providerSessionId: input.providerSessionId,
                              cause: "Provider runtime was released while opening.",
                            });
                          }
                          yield* startEventPump(entry);
                          yield* scheduleIdleRelease(input.providerSessionId, runtime);
                          return exposedRuntime;
                        }),
                      (_scope, exit) =>
                        Effect.gen(function* () {
                          yield* dropReservation;
                          if (Exit.isSuccess(exit)) return;
                          if (registered) {
                            if (openedRuntime !== undefined)
                              yield* releaseEntry({
                                providerSessionId: input.providerSessionId,
                                expectedRuntime: openedRuntime,
                                reason: "runtime_error",
                                detail: "Provider runtime opening was interrupted or failed.",
                              }).pipe(Effect.ignore);
                            return;
                          }
                          // Revoke an uninstalled opening's credential before
                          // waiting for native finalizers, which can hang.
                          const credentialId = lease.prepared?.mcpCredentialId;
                          if (credentialId !== undefined) {
                            yield* clearUnheldCredential(input.threadId, credentialId, undefined);
                            lease.prepared = undefined;
                          }
                          const closing = yield* Scope.close(sessionScope, Exit.void).pipe(
                            Effect.exit,
                            Effect.forkDetach({ startImmediately: true }),
                          );
                          const closed = yield* Fiber.join(closing).pipe(
                            Effect.interruptible,
                            Effect.timeoutOption(RELEASE_SCOPE_CLOSE_TIMEOUT_MS),
                          );
                          if (Option.isNone(closed))
                            yield* Effect.logWarning(
                              "orchestration-v2.provider-session-open-cleanup-timeout",
                              { providerSessionId: input.providerSessionId },
                            );
                        }),
                    );
                  }),
                (lease, exit) =>
                  Effect.gen(function* () {
                    const credentialId =
                      lease.reservedCredentialId ?? lease.prepared?.mcpCredentialId;
                    dropPreparationReservation(input.threadId, lease);
                    if (Exit.isFailure(exit) && !lease.installed && credentialId !== undefined) {
                      yield* clearUnheldCredential(input.threadId, credentialId, undefined);
                    }
                  }),
              );
            }),
          ),
        isLive: (providerSessionId) =>
          Ref.get(sessions).pipe(
            Effect.map((current) => current.has(sessionKey(providerSessionId))),
          ),
        get: Effect.fn("ProviderSessionManager.get")(
          function* (providerSessionId) {
            const entry = (yield* Ref.get(sessions)).get(sessionKey(providerSessionId));
            if (entry === undefined) {
              return Option.none<ManagedProviderSessionRuntime>();
            }
            yield* touchActivity(providerSessionId, entry.runtime);
            return Option.some(entry.exposedRuntime);
          },
          (effect, providerSessionId) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderSessionLookupError({
                    providerSessionId,
                    cause,
                  }),
              ),
            ),
        ),
        close: (providerSessionId) =>
          releaseEntry({ providerSessionId, reason: "manual_shutdown" }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderSessionCloseError({
                  providerSessionId,
                  cause,
                }),
            ),
          ),
        closeInstance: Effect.fn("ProviderSessionManager.closeInstance")(
          function* (instanceId) {
            const active = [...(yield* Ref.get(sessions)).values()].filter(
              (entry) => entry.runtime.instanceId === instanceId,
            );
            const outcomes = yield* Effect.forEach(
              active,
              (entry) =>
                releaseEntry({
                  providerSessionId: entry.runtime.providerSessionId,
                  expectedRuntime: entry.runtime,
                  reason: "manual_shutdown",
                  detail: `Provider instance ${instanceId} logged out.`,
                }).pipe(Effect.exit),
              { concurrency: "unbounded" },
            );
            const failure = outcomes.find(Exit.isFailure);
            if (failure !== undefined && Exit.isFailure(failure)) {
              return yield* Effect.failCause(failure.cause);
            }
          },
          (effect, instanceId) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderSessionCloseError({
                    providerSessionId: ProviderSessionId.make(
                      `provider-session:provider-instance:${instanceId}`,
                    ),
                    cause,
                  }),
              ),
            ),
        ),
        release: releaseEntry,
        detach: Effect.fn("ProviderSessionManager.detach")(
          function* (input) {
            const key = sessionKey(input.providerSessionId);
            const currentEntry = (yield* Ref.get(sessions)).get(key);
            let detachedProviderThreads: ReadonlyArray<OrchestrationV2ProviderThread> = [];
            if (currentEntry?.supportsMultipleProviderThreads === true) {
              const projection = yield* Effect.option(
                projectionStore.getThreadRecords(input.threadId, [
                  "providerThreads",
                  "providerTurns",
                ]),
              );
              if (Option.isSome(projection)) {
                const providerThreads = new Map(
                  projection.value.providerThreads
                    .filter((thread) => thread.providerSessionId === input.providerSessionId)
                    .map((thread) => [thread.id, thread] as const),
                );
                detachedProviderThreads = [...providerThreads.values()];
                const activeTurns = projection.value.providerTurns.filter(
                  (turn) => turn.status === "running" && providerThreads.has(turn.providerThreadId),
                );
                yield* Effect.forEach(
                  activeTurns,
                  (turn) =>
                    currentEntry.exposedRuntime
                      .interruptTurn({
                        providerThread: providerThreads.get(turn.providerThreadId)!,
                        providerTurnId: turn.id,
                      })
                      .pipe(
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-interrupt-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerTurnId: turn.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                  { concurrency: 1, discard: true },
                );
              }
            }
            const detachResult = yield* threadAttachment.withLock(
              threadAttachmentKey(input),
              Ref.modify(sessions, (current) => {
                const entry = current.get(key);
                if (
                  entry === undefined ||
                  entry.runtime !== currentEntry?.runtime ||
                  !entry.attachedThreadIds.has(input.threadId)
                ) {
                  return [
                    Option.none<{
                      readonly entry: LiveSessionEntry;
                      readonly idleUnloadFiber: Fiber.Fiber<void, never> | null;
                    }>(),
                    current,
                  ] as const;
                }
                const attachedThreadIds = new Set(entry.attachedThreadIds);
                attachedThreadIds.delete(input.threadId);
                const loadedProviderThreadKeyByThread = new Map(
                  entry.loadedProviderThreadKeyByThread,
                );
                loadedProviderThreadKeyByThread.delete(input.threadId);
                // For a plain (workspace-change) detach, the credential id stays
                // recorded: the thread may re-attach and reuse it, and
                // releaseEntry revokes it when the provider process finally goes
                // away. A terminal detach (archive/delete) prunes the record so
                // nothing vetoes the revocation below.
                const mcpCredentialIdByThread =
                  input.revokeMcpCredential === true
                    ? (() => {
                        const pruned = new Map(entry.mcpCredentialIdByThread);
                        pruned.delete(input.threadId);
                        return pruned;
                      })()
                    : entry.mcpCredentialIdByThread;
                // The detach unloads the thread itself below.
                const idleThreadUnloads = new Map(entry.idleThreadUnloads);
                idleThreadUnloads.delete(input.threadId);
                const updatedEntry = {
                  ...entry,
                  attachedThreadIds,
                  loadedProviderThreadKeyByThread,
                  mcpCredentialIdByThread,
                  idleThreadUnloads,
                };
                const updated = new Map(current);
                updated.set(key, updatedEntry);
                return [
                  Option.some({
                    entry: updatedEntry,
                    idleUnloadFiber: entry.idleThreadUnloads.get(input.threadId)?.fiber ?? null,
                  }),
                  updated,
                ] as const;
              }),
            );
            if (Option.isSome(detachResult)) {
              yield* cancelIdleFiber(detachResult.value.idleUnloadFiber);
            }
            const detached = Option.map(detachResult, (result) => result.entry);
            // Plain detaches preserve credentials held by live shared providers across workspace handoffs.
            if (input.revokeMcpCredential === true) {
              yield* mcpPrepareLock.withLock(
                input.threadId,
                Effect.gen(function* () {
                  const credentialId =
                    currentEntry?.mcpCredentialIdByThread.get(input.threadId) ??
                    (yield* mcpSessions.read(input.threadId))?.providerSessionId;
                  const latest = yield* Ref.get(sessions);
                  const heldByReplacement = Array.from(latest.values()).some(
                    (entry) =>
                      entry.runtime !== currentEntry?.runtime &&
                      (entry.attachedThreadIds.has(input.threadId) ||
                        (credentialId !== undefined &&
                          entry.mcpCredentialIdByThread.get(input.threadId) === credentialId)),
                  );
                  if (
                    !heldByReplacement &&
                    (credentialId === undefined ||
                      !isMcpCredentialReserved(input.threadId, credentialId))
                  ) {
                    yield* clearMcpSession(input.threadId, credentialId);
                  }
                }),
              );
            }
            if (Option.isNone(detached)) {
              return;
            }
            if (
              detached.value.attachedThreadIds.size === 0 &&
              !detached.value.supportsMultipleProviderThreads
            ) {
              yield* releaseEntry({
                providerSessionId: input.providerSessionId,
                expectedRuntime: detached.value.runtime,
                reason: "manual_shutdown",
                ...(input.detail === undefined ? {} : { detail: input.detail }),
                detachedThreadId: input.threadId,
              });
              return;
            }
            const unloadThread = detached.value.exposedRuntime.unloadThread;
            if (detached.value.supportsMultipleProviderThreads && unloadThread !== undefined) {
              // Serialize unload with attachment so attached threads stay loaded and later attaches resume after unload.
              yield* threadAttachment.withLock(
                threadAttachmentKey(input),
                Effect.gen(function* () {
                  const entry = (yield* Ref.get(sessions)).get(key);
                  if (
                    entry?.runtime !== detached.value.runtime ||
                    entry.attachedThreadIds.has(input.threadId)
                  ) {
                    return;
                  }
                  yield* Effect.forEach(
                    detachedProviderThreads.filter((thread) => thread.nativeThreadRef !== null),
                    (providerThread) =>
                      unloadThread({ providerThread }).pipe(
                        Effect.timeout(UNLOAD_THREAD_TIMEOUT_MS),
                        Effect.catchCause((cause) =>
                          Effect.logWarning(
                            "orchestration-v2.driver-session.detach-unload-failed",
                            {
                              providerSessionId: input.providerSessionId,
                              threadId: input.threadId,
                              providerThreadId: providerThread.id,
                              cause,
                            },
                          ),
                        ),
                      ),
                    { concurrency: 1, discard: true },
                  );
                }),
              );
            }
            yield* scheduleIdleRelease(input.providerSessionId, detached.value.runtime);
          },
          (effect, input) =>
            effect.pipe(
              Effect.catchCause((cause) =>
                Effect.fail(
                  new ProviderSessionReleaseError({
                    providerSessionId: input.providerSessionId,
                    reason: "manual_shutdown",
                    cause,
                  }),
                ),
              ),
            ),
        ),
      } satisfies ProviderSessionManagerV2Shape);
    }),
  );

export const layer = layerWithOptions();
