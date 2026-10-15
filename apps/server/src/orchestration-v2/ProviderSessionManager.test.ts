import * as NetAddress from "effect/unstable/net/NetAddress";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type Project,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSessionId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { HttpServer } from "effect/unstable/http";

import { ProviderWorkspaceMissingError } from "../provider/Errors.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as McpProviderSession from "../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import {
  ProviderAdapterEventStreamError,
  type ProviderAdapterV2Event,
  ProviderAdapterProtocolError,
  type ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";

const TestDatabaseLayer = SqlitePersistenceMemory;
const TestStoresLayer = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(TestDatabaseLayer),
);
const TestEventSinkLayer = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(TestStoresLayer, TestDatabaseLayer)),
);
const FailingReleaseEventSinkLayer = Layer.effect(
  EventSink.EventSinkV2,
  Effect.gen(function* () {
    const delegate = yield* EventSink.EventSinkV2;
    return EventSink.EventSinkV2.of({
      ...delegate,
      write: (input) =>
        input.events.some(
          (event) =>
            event.type === "provider-session.updated" &&
            (event.payload.status === "stopped" || event.payload.status === "error"),
        )
          ? Effect.fail(new EventSink.EventSinkWriteError({ eventCount: input.events.length }))
          : delegate.write(input),
    });
  }),
).pipe(Layer.provide(TestEventSinkLayer));

const makePausedReleaseEventSinkLayer = (
  beforeReleaseWrite: Effect.Effect<void, EventSink.EventSinkWriteError>,
) =>
  Layer.effect(
    EventSink.EventSinkV2,
    Effect.gen(function* () {
      const delegate = yield* EventSink.EventSinkV2;
      return EventSink.EventSinkV2.of({
        ...delegate,
        write: (input) =>
          (input.events.some(
            (event) =>
              event.type === "provider-session.updated" && event.payload.status === "stopped",
          )
            ? beforeReleaseWrite
            : Effect.void
          ).pipe(Effect.andThen(delegate.write(input))),
      });
    }),
  ).pipe(Layer.provide(TestEventSinkLayer));

const CodexCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;
const ExclusiveCapabilities: OrchestrationV2ProviderCapabilities = {
  ...CodexCapabilities,
  sessions: {
    ...CodexCapabilities.sessions,
    supportsMultipleProviderThreadsPerSession: false,
  },
};

interface TestProviderRuntimeState {
  readonly openCount: number;
  readonly closeCount: number;
  readonly interruptCount: number;
  readonly resumeCount: number;
  readonly unloadedNativeThreadIds: ReadonlyArray<string>;
  readonly eventQueues: ReadonlyMap<string, Queue.Queue<ProviderAdapterV2Event, Cause.Done>>;
}

const emptyState: TestProviderRuntimeState = {
  openCount: 0,
  closeCount: 0,
  interruptCount: 0,
  resumeCount: 0,
  unloadedNativeThreadIds: [],
  eventQueues: new Map(),
};

const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

const runtimePolicy = {
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: process.cwd(),
} satisfies ProviderAdapterV2RuntimePolicy;

function makeProviderSession(input: {
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
}): OrchestrationV2ProviderSession {
  return {
    id: input.providerSessionId,
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    status: "ready",
    cwd: process.cwd(),
    model: "gpt-5.4",
    capabilities: input.capabilities ?? CodexCapabilities,
    createdAt: input.now,
    updatedAt: input.now,
    lastError: null,
  };
}

function makeThreadCreatedEvent(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly now: DateTime.Utc;
  readonly projectId?: ProjectId;
}) {
  return Effect.gen(function* () {
    const projectId =
      input.projectId ??
      (yield* input.idAllocator.allocate.project({
        fixtureName: "provider-session-manager",
      }));
    const providerThreadId = input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: input.threadId,
      projectId,
      title: "Provider session manager",
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: input.threadId,
      },
      forkedFrom: null,
      createdAt: input.now,
      updatedAt: input.now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    return {
      id: yield* input.idAllocator.allocate.event({ threadId: input.threadId }),
      type: "thread.created" as const,
      threadId: input.threadId,
      occurredAt: input.now,
      payload: thread,
    };
  });
}

function makeProviderThread(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly now: DateTime.Utc;
}): OrchestrationV2ProviderThread {
  return {
    id: input.idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    }),
    driver: CODEX_DRIVER,
    providerInstanceId: modelSelection.instanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.threadId,
    ownerNodeId: null,
    nativeThreadRef: {
      driver: CODEX_DRIVER,
      nativeId: "native-thread",
      strength: "strong",
    },
    nativeConversationHeadRef: null,
    status: "idle",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: input.now,
    updatedAt: input.now,
  };
}

function unimplemented(detail: string) {
  return Effect.fail(
    new ProviderAdapterProtocolError({
      driver: CODEX_DRIVER,
      detail,
    }),
  );
}

function makeProviderAdapter(
  state: Ref.Ref<TestProviderRuntimeState>,
  options: {
    readonly failEventStream?: boolean;
    readonly capabilities?: OrchestrationV2ProviderCapabilities;
    readonly mcpConfigs?: Ref.Ref<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >;
    readonly beforeOpen?: (input: {
      readonly providerSessionId: ProviderSessionId;
      readonly initialProviderItemIdentityVersion?: 2;
    }) => Effect.Effect<void>;
    readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
    readonly hangSessionScopeClose?: boolean;
    readonly beforeUnload?: Effect.Effect<void>;
    readonly beforeClose?: Effect.Effect<void>;
    readonly beforeNativeReturn?: Effect.Effect<void>;
  } = {},
): ProviderAdapterV2Shape {
  return {
    instanceId: ProviderInstanceId.make("codex"),
    driver: CODEX_DRIVER,
    getCapabilities: () => Effect.succeed(options.capabilities ?? CodexCapabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        if (options.beforeOpen !== undefined) {
          yield* options.beforeOpen(input);
        }
        if (options.mcpConfigs !== undefined) {
          yield* Ref.update(options.mcpConfigs, (configs) => [
            ...configs,
            McpProviderSession.readMcpProviderSession(input.threadId),
          ]);
        }
        const now = yield* DateTime.now;
        const events = yield* Queue.unbounded<ProviderAdapterV2Event, Cause.Done>();
        const session = makeProviderSession({
          providerSessionId: input.providerSessionId,
          now,
          ...(options.capabilities === undefined ? {} : { capabilities: options.capabilities }),
        });
        yield* Ref.update(state, (current) => {
          const eventQueues = new Map(current.eventQueues);
          eventQueues.set(String(input.providerSessionId), events);
          return {
            ...current,
            openCount: current.openCount + 1,
            eventQueues,
          };
        });
        yield* Effect.addFinalizer(() =>
          Ref.update(state, (current) => ({
            ...current,
            closeCount: current.closeCount + 1,
          })),
        );
        if (options.beforeClose !== undefined)
          yield* Effect.addFinalizer(() => options.beforeClose!);
        if (options.hangSessionScopeClose === true) {
          // Registered last so it runs first on scope close, wedging the
          // close before the closeCount finalizer, like a provider process
          // that never yields its message stream.
          yield* Effect.addFinalizer(() => Effect.never);
        }

        if (options.beforeNativeReturn !== undefined) yield* options.beforeNativeReturn;

        return {
          instanceId: ProviderInstanceId.make("codex"),
          driver: CODEX_DRIVER,
          providerSessionId: input.providerSessionId,
          providerSession: session,
          events: options.failEventStream
            ? Stream.fail(
                new ProviderAdapterEventStreamError({
                  driver: CODEX_DRIVER,
                  providerSessionId: input.providerSessionId,
                  cause: "process exited",
                }),
              )
            : Stream.fromQueue(events),
          ...(options.hasPendingBackgroundWork === undefined
            ? {}
            : { hasPendingBackgroundWork: options.hasPendingBackgroundWork }),
          ensureThread: () => unimplemented("ensureThread unused in test"),
          resumeThread: (threadInput) =>
            Ref.update(state, (current) => ({
              ...current,
              resumeCount: current.resumeCount + 1,
            })).pipe(Effect.as(threadInput.providerThread)),
          startTurn: () => Effect.void,
          steerTurn: () => Effect.void,
          interruptTurn: () =>
            Ref.update(state, (current) => ({
              ...current,
              interruptCount: current.interruptCount + 1,
            })),
          unloadThread: ({ providerThread }) =>
            (options.beforeUnload ?? Effect.void).pipe(
              Effect.andThen(
                Ref.update(state, (current) => ({
                  ...current,
                  unloadedNativeThreadIds: [
                    ...current.unloadedNativeThreadIds,
                    providerThread.nativeThreadRef?.nativeId ?? "",
                  ],
                })),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => unimplemented("readThreadSnapshot unused in test"),
          rollbackThread: () => unimplemented("rollbackThread unused in test"),
          forkThread: () => unimplemented("forkThread unused in test"),
        } satisfies ProviderAdapterV2SessionRuntime;
      }),
  };
}

function makeTestLayer(input: {
  readonly state: Ref.Ref<TestProviderRuntimeState>;
  readonly idleTimeoutMs: number;
  readonly maxIdlePinMs?: number;
  readonly failEventStream?: boolean;
  readonly capabilities?: OrchestrationV2ProviderCapabilities;
  readonly mcpConfigs?: Ref.Ref<
    ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
  >;
  readonly beforeOpen?: (input: {
    readonly providerSessionId: ProviderSessionId;
    readonly initialProviderItemIdentityVersion?: 2;
  }) => Effect.Effect<void>;
  readonly failReleaseEventWrites?: boolean;
  readonly beforeReleaseWrite?: Effect.Effect<void, EventSink.EventSinkWriteError>;
  readonly beforeRead?: (
    records: ReadonlyArray<string>,
  ) => Effect.Effect<void, ProjectionStore.ProjectionStoreReadError>;
  readonly hasPendingBackgroundWork?: Effect.Effect<boolean>;
  readonly hangSessionScopeClose?: boolean;
  readonly beforeUnload?: Effect.Effect<void>;
  readonly beforeClose?: Effect.Effect<void>;
  readonly beforeNativeReturn?: Effect.Effect<void>;
  readonly beforeAttachmentWrite?: (threadId: ThreadId) => Effect.Effect<void>;
  readonly beforeMcpResolve?: Effect.Effect<void>;
  readonly beforeMcpRevoke?: Effect.Effect<void>;
  readonly serverSettingsLayer?: ReturnType<typeof ServerSettings.layerTest>;
  readonly projectServiceLayer?: Layer.Layer<ProjectService.ProjectService>;
}) {
  const baseEventSinkLayer =
    input.beforeReleaseWrite !== undefined
      ? makePausedReleaseEventSinkLayer(input.beforeReleaseWrite)
      : input.failReleaseEventWrites
        ? FailingReleaseEventSinkLayer
        : TestEventSinkLayer;
  const configuredEventSinkLayer =
    input.beforeAttachmentWrite === undefined
      ? baseEventSinkLayer
      : Layer.effect(
          EventSink.EventSinkV2,
          Effect.gen(function* () {
            const delegate = yield* EventSink.EventSinkV2;
            return EventSink.EventSinkV2.of({
              ...delegate,
              write: (batch) =>
                Effect.forEach(
                  batch.events.filter((event) => event.type === "provider-session.attached"),
                  (event) => input.beforeAttachmentWrite!(event.threadId),
                  { discard: true },
                ).pipe(Effect.andThen(delegate.write(batch))),
            });
          }),
        ).pipe(Layer.provide(baseEventSinkLayer));
  const managerStoresLayer =
    input.beforeRead === undefined
      ? TestStoresLayer
      : Layer.effect(
          ProjectionStore.ProjectionStoreV2,
          Effect.gen(function* () {
            const delegate = yield* ProjectionStore.ProjectionStoreV2;
            return ProjectionStore.ProjectionStoreV2.of({
              ...delegate,
              getThreadRecords: (threadId, records, options) =>
                input.beforeRead!(records).pipe(
                  Effect.andThen(delegate.getThreadRecords(threadId, records, options)),
                ),
            });
          }),
        ).pipe(Layer.provide(TestStoresLayer));
  const managerMcpRegistryLayer =
    input.beforeMcpResolve === undefined && input.beforeMcpRevoke === undefined
      ? TestMcpRegistryLayer
      : Layer.effect(
          McpSessionRegistry.McpSessionRegistry,
          Effect.gen(function* () {
            const delegate = yield* McpSessionRegistry.McpSessionRegistry;
            return McpSessionRegistry.McpSessionRegistry.of({
              ...delegate,
              resolve: (token) =>
                (input.beforeMcpResolve ?? Effect.void).pipe(
                  Effect.andThen(delegate.resolve(token)),
                ),
              revokeProviderSession: (id) =>
                (input.beforeMcpRevoke ?? Effect.void).pipe(
                  Effect.andThen(delegate.revokeProviderSession(id)),
                ),
            });
          }),
        ).pipe(Layer.provide(TestMcpRegistryLayer));
  const registryLayer = ProviderAdapterRegistry.makeSingleLayer(
    makeProviderAdapter(input.state, {
      failEventStream: input.failEventStream ?? false,
      ...(input.capabilities === undefined ? {} : { capabilities: input.capabilities }),
      ...(input.mcpConfigs === undefined ? {} : { mcpConfigs: input.mcpConfigs }),
      ...(input.beforeOpen === undefined ? {} : { beforeOpen: input.beforeOpen }),
      ...(input.hasPendingBackgroundWork === undefined
        ? {}
        : { hasPendingBackgroundWork: input.hasPendingBackgroundWork }),
      ...(input.hangSessionScopeClose === undefined
        ? {}
        : { hangSessionScopeClose: input.hangSessionScopeClose }),
      ...(input.beforeUnload === undefined ? {} : { beforeUnload: input.beforeUnload }),
      ...(input.beforeClose === undefined ? {} : { beforeClose: input.beforeClose }),
      ...(input.beforeNativeReturn === undefined
        ? {}
        : { beforeNativeReturn: input.beforeNativeReturn }),
    }),
  );
  const providerEventIngestorTestLayer = ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        configuredEventSinkLayer,
        IdAllocator.layer,
        TestStoresLayer,
        ThreadCommandExecutor.layer,
      ),
    ),
  );
  return Layer.mergeAll(
    TestStoresLayer,
    providerEventIngestorTestLayer,
    configuredEventSinkLayer,
    IdAllocator.layer,
    TestMcpRegistryLayer,
    ProviderSessionManager.layerWithOptions({
      idleTimeoutMs: input.idleTimeoutMs,
      ...(input.maxIdlePinMs === undefined ? {} : { maxIdlePinMs: input.maxIdlePinMs }),
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          registryLayer,
          configuredEventSinkLayer,
          IdAllocator.layer,
          providerEventIngestorTestLayer,
          managerMcpRegistryLayer,
          managerStoresLayer,
          ...(input.serverSettingsLayer === undefined ? [] : [input.serverSettingsLayer]),
          ...(input.projectServiceLayer === undefined ? [] : [input.projectServiceLayer]),
        ),
      ),
    ),
  ).pipe(Layer.provide(NodeServices.layer));
}

const fakeHttpServer = HttpServer.HttpServer.of({
  address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
  serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
});

const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(EnvironmentId.make("environment-provider-session-manager")),
  getDescriptor: Effect.die("unused"),
});

const TestMcpRegistryLayer = Layer.effect(
  McpSessionRegistry.McpSessionRegistry,
  McpSessionRegistry.__testing.make(),
).pipe(
  Layer.provide(Layer.succeed(HttpServer.HttpServer, fakeHttpServer)),
  Layer.provide(Layer.succeed(ServerEnvironment.ServerEnvironment, fakeEnvironment)),
  Layer.provide(NodeServices.layer),
);

function makeBrowserAccessProject(projectId: ProjectId): Project {
  return {
    id: projectId,
    title: "Browser access project",
    workspaceRoot: process.cwd(),
    repositoryIdentity: null,
    faviconPath: null,
    projectIcon: null,
    defaultModelSelection: null,
    defaultThreadEnvMode: null,
    autoPull: false,
    scripts: [],
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    deletedAt: null,
  };
}

function runBrowserAccessScenario(input: {
  readonly enableAgentBrowserAccess: boolean;
  readonly projectOverride: boolean;
  readonly deviceOverride?: boolean;
  readonly createThread?: boolean;
  readonly projectExists?: boolean;
}) {
  return Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const projectId = ProjectId.make("project-provider-session-manager-browser-access");
    const threadId = ThreadId.make("thread-provider-session-manager-browser-access");
    const projectServiceLayer = Layer.mock(ProjectService.ProjectService)({
      getById: (requestedProjectId) =>
        Effect.succeed(
          input.projectExists === false
            ? Option.none()
            : Option.some(makeBrowserAccessProject(requestedProjectId)),
        ),
    });

    yield* Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      if (input.createThread !== false) {
        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now, projectId })],
        });
      }
      yield* manager
        .open({ threadId, providerSessionId, modelSelection, runtimePolicy })
        .pipe(Effect.ignore);
    }).pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          projectServiceLayer,
          serverSettingsLayer: ServerSettings.layerTest({
            enableAgentBrowserAccess: input.enableAgentBrowserAccess,
            projectSettingsOverrides: {
              [projectId]: {
                enableAgentBrowserAccess: input.projectOverride,
                ...(input.deviceOverride === undefined
                  ? {}
                  : { enableAgentDeviceAccess: input.deviceOverride }),
              },
            },
          }),
        }),
      ),
    );

    return (yield* Ref.get(mcpConfigs))[0];
  });
}

function makePendingRuntimeRequestEvents(input: {
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly threadId: ThreadId;
  readonly providerSessionId: ProviderSessionId;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly now: DateTime.Utc;
}) {
  return Effect.gen(function* () {
    const requestId = yield* input.idAllocator.allocate.runtimeRequest({
      driver: CODEX_DRIVER,
      nativeRequestId: "pending-approval",
    });
    const nodeId = input.idAllocator.derive.approvalNode({ requestId });
    const node = {
      id: nodeId,
      threadId: input.threadId,
      runId: null,
      parentNodeId: null,
      rootNodeId: nodeId,
      kind: "approval_request" as const,
      status: "waiting" as const,
      countsForRun: false,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: requestId,
      checkpointScopeId: null,
      startedAt: input.now,
      completedAt: null,
    };
    const request = {
      id: requestId,
      nodeId,
      providerTurnId: null,
      nativeRequestRef: {
        driver: CODEX_DRIVER,
        nativeId: "pending-approval",
        strength: "strong" as const,
      },
      kind: "command" as const,
      status: "pending" as const,
      responseCapability: {
        type: "live" as const,
        providerSessionId: input.providerSessionId,
      },
      createdAt: input.now,
      resolvedAt: null,
    };
    const turnItem = {
      id: input.idAllocator.derive.approvalTurnItem({ requestId }),
      threadId: input.threadId,
      runId: null,
      nodeId,
      providerThreadId: input.providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: 1,
      status: "waiting" as const,
      title: null,
      startedAt: input.now,
      completedAt: null,
      updatedAt: input.now,
      type: "approval_request" as const,
      requestId,
      requestKind: "command" as const,
    };
    const events = [
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "node.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: node,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "runtime-request.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: request,
      },
      {
        id: yield* input.idAllocator.allocate.event({
          threadId: input.threadId,
          providerSessionId: input.providerSessionId,
        }),
        type: "turn-item.updated" as const,
        threadId: input.threadId,
        nodeId,
        driver: CODEX_DRIVER,
        occurredAt: input.now,
        payload: turnItem,
      },
    ] satisfies ReadonlyArray<OrchestrationV2DomainEvent>;
    const providerEvents = [
      {
        type: "runtime_request.updated" as const,
        driver: CODEX_DRIVER,
        threadId: input.threadId,
        runtimeRequest: request,
      },
      {
        type: "node.updated" as const,
        driver: CODEX_DRIVER,
        node,
      },
      {
        type: "turn_item.updated" as const,
        driver: CODEX_DRIVER,
        turnItem,
      },
    ] satisfies ReadonlyArray<ProviderAdapterV2Event>;
    return { events, providerEvents, requestId, nodeId };
  });
}

it.effect("ProviderSessionManagerV2 opens independent sessions concurrently", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const secondOpenStarted = yield* Deferred.make<void>();
    const releaseOpens = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Effect.gen(function* () {
        const openNumber = yield* Ref.modify(openStartedCount, (count) => [count + 1, count + 1]);
        yield* Deferred.succeed(openNumber === 1 ? firstOpenStarted : secondOpenStarted, undefined);
        yield* Deferred.await(releaseOpens);
      });

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-concurrent-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-concurrent-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      const firstFiber = yield* manager
        .open({
          threadId: firstThreadId,
          providerSessionId: firstProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* manager
        .open({
          threadId: secondThreadId,
          providerSessionId: secondProviderSessionId,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.forkScoped);

      yield* Deferred.await(secondOpenStarted);
      assert.equal(yield* Ref.get(openStartedCount), 2);
      yield* Deferred.succeed(releaseOpens, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.notStrictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 closes every live session for a provider instance", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const firstThreadId = ThreadId.make("thread-provider-session-manager-logout-a");
      const secondThreadId = ThreadId.make("thread-provider-session-manager-logout-b");
      const firstProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: firstThreadId,
      });
      const secondProviderSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: secondThreadId,
      });

      yield* eventSink.write({
        events: [
          yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
          yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
        ],
      });
      yield* manager.open({
        threadId: firstThreadId,
        providerSessionId: firstProviderSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.open({
        threadId: secondThreadId,
        providerSessionId: secondProviderSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* manager.closeInstance(modelSelection.instanceId);

      assert.isTrue(Option.isNone(yield* manager.get(firstProviderSessionId)));
      assert.isTrue(Option.isNone(yield* manager.get(secondProviderSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 opens a duplicate session only once", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const openStartedCount = yield* Ref.make(0);
    const firstOpenStarted = yield* Deferred.make<void>();
    const releaseOpen = yield* Deferred.make<void>();
    const beforeOpen = () =>
      Ref.updateAndGet(openStartedCount, (count) => count + 1).pipe(
        Effect.tap(() => Deferred.succeed(firstOpenStarted, undefined)),
        Effect.andThen(Deferred.await(releaseOpen)),
      );

    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-single-flight");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const open = manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const firstFiber = yield* open.pipe(Effect.forkScoped);
      yield* Deferred.await(firstOpenStarted);
      const secondFiber = yield* open.pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(openStartedCount), 1);

      yield* Deferred.succeed(releaseOpen, undefined);
      const [firstRuntime, secondRuntime] = yield* Effect.all([
        Fiber.join(firstFiber),
        Fiber.join(secondFiber),
      ]);
      assert.strictEqual(firstRuntime, secondRuntime);
      assert.equal((yield* Ref.get(state)).openCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
          beforeOpen,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases live sessions when its layer shuts down", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const liveState = yield* Ref.get(state);
      assert.equal(liveState.openCount, 1);
      assert.equal(liveState.closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 60_000,
        }),
      ),
    );

    assert.equal((yield* Ref.get(state)).closeCount, 1);
  }),
);

it.effect("ProviderSessionManagerV2 closes event subscriptions normally on server shutdown", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-shutdown-subscription");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const bufferedSubscription = yield* runtime.subscribeEvents!;
      const activeSubscription = yield* runtime.subscribeEvents!;
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: runtime.providerSession,
      });
      assert.isTrue(Option.isSome(yield* activeSubscription.events.pipe(Stream.runHead)));

      yield* manager.shutdown;

      assert.isEmpty(yield* bufferedSubscription.events.pipe(Stream.runCollect));
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect("ProviderSessionManagerV2 drains subscribers when the provider stops", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-provider-stop");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const subscription = yield* runtime.subscribeEvents!;
      const collected = yield* subscription.events.pipe(Stream.runCollect, Effect.forkScoped);
      const adapterQueue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterQueue);
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "provider-stop-thread",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "provider-stop-turn",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "turn.terminal",
        driver: CODEX_DRIVER,
        providerThreadId,
        providerTurnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Queue.offer(adapterQueue!, {
        type: "provider_session.updated",
        driver: CODEX_DRIVER,
        providerSession: {
          ...runtime.providerSession,
          status: "stopped",
          updatedAt: now,
        },
      });
      yield* Queue.end(adapterQueue!);

      const events = Array.from(yield* Fiber.join(collected));
      assert.deepEqual(
        events.map((event) => event.type),
        ["turn.terminal", "provider_session.updated"],
      );
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 issues MCP credentials before opening and revokes them on close",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.threadId, threadId);
        assert.equal(captured?.providerInstanceId, modelSelection.instanceId);
        assert.equal(captured?.endpoint, "http://127.0.0.1:43123/mcp");
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(token);
        const resolved = yield* registry.resolve(token!);
        assert.equal(resolved?.threadId, threadId);
        assert.deepEqual(
          resolved?.capabilities,
          new Set(["preview", "orchestration", "worktree", "pull-requests"]),
        );

        yield* manager.close(providerSessionId);
        assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
        assert.isUndefined(yield* registry.resolve(token!));
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 withholds the preview capability when agent browser access is off",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-no-browser");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const captured = (yield* Ref.get(mcpConfigs))[0];
        assert.isDefined(captured);
        assert.equal(captured?.browserToolsAvailable, false);
        const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
        const resolved = yield* registry.resolve(token!);
        assert.deepEqual(
          resolved?.capabilities,
          new Set(["orchestration", "worktree", "pull-requests"]),
        );

        yield* manager.close(providerSessionId);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            // orDie: the test layer's settings-normalization error cannot
            // occur for a literal override and the slot requires error never.
            serverSettingsLayer: ServerSettings.layerTest({
              enableAgentBrowserAccess: false,
            }).pipe(Layer.orDie),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-out", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 honors a project browser-access opt-in", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: false,
      projectOverride: true,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, true);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing project", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      projectExists: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 fails browser access closed for a missing thread", () =>
  Effect.gen(function* () {
    const captured = yield* runBrowserAccessScenario({
      enableAgentBrowserAccess: true,
      projectOverride: true,
      createThread: false,
    });
    assert.isDefined(captured);
    assert.equal(captured?.browserToolsAvailable, false);
  }),
);

it.effect("ProviderSessionManagerV2 revokes MCP credentials when release persistence fails", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-mcp-release-failure");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      const captured = (yield* Ref.get(mcpConfigs))[0];
      const token = captured?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(token);
      assert.isDefined(yield* registry.resolve(token!));

      const closeError = yield* manager.close(providerSessionId).pipe(Effect.flip);
      assert.equal(closeError._tag, "ProviderSessionCloseError");
      assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
      assert.isUndefined(yield* registry.resolve(token!));
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          mcpConfigs,
          failReleaseEventWrites: true,
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 duplicate detach preserves replacement MCP credentials", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-replacement-mcp");
      const oldSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const replacementSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId: oldSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.detach({ providerSessionId: oldSessionId, threadId });
      yield* manager.open({
        threadId,
        providerSessionId: replacementSessionId,
        modelSelection,
        runtimePolicy,
      });

      const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
      assert.isDefined(replacement);
      const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(replacementToken);
      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        replacement?.providerSessionId,
      );

      yield* manager.detach({ providerSessionId: oldSessionId, threadId });

      assert.equal(
        McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
        replacement?.providerSessionId,
      );
      assert.equal((yield* registry.resolve(replacementToken!))?.threadId, threadId);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1_000,
          capabilities: ExclusiveCapabilities,
          mcpConfigs,
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 detach of a superseded live session preserves replacement MCP credentials",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-superseded-mcp");
        const oldSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const replacementSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId: oldSessionId,
          modelSelection,
          runtimePolicy,
        });
        // The replacement opens while the old session is still attached: this is
        // the workspace-handoff sequence, where the queued continuation run can
        // start its session before the outbox executes the old session's detach.
        yield* manager.open({
          threadId,
          providerSessionId: replacementSessionId,
          modelSelection,
          runtimePolicy,
        });

        const replacement = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(replacement);
        const replacementToken = replacement?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(replacementToken);
        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          replacement?.providerSessionId,
        );

        // First (non-duplicate) detach of the superseded session must not revoke
        // the replacement's credential or clear its config slot.
        yield* manager.detach({ providerSessionId: oldSessionId, threadId });

        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          replacement?.providerSessionId,
        );
        assert.equal((yield* registry.resolve(replacementToken!))?.threadId, threadId);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            capabilities: ExclusiveCapabilities,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a thread's MCP credential stable across detach and re-attach on a shared session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stable-mcp");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        assert.isDefined(original);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);

        // Workspace-change handoff on a shared multi-thread session (codex):
        // the thread detaches while the provider process keeps running, and the
        // process's MCP client keeps using the credential it was started with.
        yield* manager.detach({ providerSessionId, threadId, detail: "Workspace changed." });
        assert.equal(
          (yield* registry.resolve(originalToken!))?.threadId,
          threadId,
          "detach must not revoke the credential the live provider process still holds",
        );

        // The continuation run re-attaches the same thread to the same session;
        // the credential must be reused, not rotated, so the provider process's
        // long-lived MCP client stays authorized.
        yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal(
          McpProviderSession.readMcpProviderSession(threadId)?.providerSessionId,
          original?.providerSessionId,
          "re-attach must reuse the existing credential, not rotate it",
        );
        assert.equal((yield* registry.resolve(originalToken!))?.threadId, threadId);

        // Releasing the session (provider process gone) still revokes.
        yield* manager.close(providerSessionId);
        assert.isUndefined(yield* registry.resolve(originalToken!));
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 revokes a rotated credential despite a stale record on another live session",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-stale-record");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        // S1 (shared session) records credential C1 for the thread, then the
        // thread detaches; S1 stays alive with the stale record.
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        yield* manager.detach({ providerSessionId: s1, threadId });

        // The credential dies externally, so S2's attach must rotate to C2.
        yield* registry.revokeThread(threadId);
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });
        const rotated = McpProviderSession.readMcpProviderSession(threadId);
        assert.isDefined(rotated);
        const rotatedToken = rotated?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(yield* registry.resolve(rotatedToken!));

        // Releasing S2 must revoke C2 even though S1 still carries a stale
        // record (of dead C1) for the same thread.
        yield* manager.close(s2);
        assert.isUndefined(
          yield* registry.resolve(rotatedToken!),
          "stale record on S1 must not veto revoking S2's rotated credential",
        );
        yield* manager.close(s1);
      });

      yield* effect.pipe(
        Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000, mcpConfigs })),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 protects a reused credential from a predecessor release during open",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const mcpConfigs = yield* Ref.make<
        ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
      >([]);
      const duringOpen = yield* Ref.make<Effect.Effect<void>>(Effect.void);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const registry = yield* McpSessionRegistry.McpSessionRegistry;
        const now = yield* DateTime.now;
        const threadId = ThreadId.make("thread-provider-session-manager-open-race");
        const s1 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const s2 = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        yield* manager.open({ threadId, providerSessionId: s1, modelSelection, runtimePolicy });
        const original = (yield* Ref.get(mcpConfigs)).at(-1);
        const originalToken = original?.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isDefined(originalToken);
        yield* manager.detach({ providerSessionId: s1, threadId });

        // While S2's provider process is spawning (after prepare reused the
        // credential, before the entry is visible), the predecessor session
        // releases. Eager adapters (ACP, OpenCode) bake the credential into
        // the process during openSession, so the release must not revoke it;
        // rotating afterwards cannot repair those adapters.
        yield* Ref.set(duringOpen, manager.close(s1).pipe(Effect.orDie));
        yield* manager.open({ threadId, providerSessionId: s2, modelSelection, runtimePolicy });

        const slot = McpProviderSession.readMcpProviderSession(threadId);
        assert.equal(
          slot?.providerSessionId,
          original?.providerSessionId,
          "the credential the adapter was configured with must remain current",
        );
        assert.equal(
          (yield* registry.resolve(originalToken!))?.threadId,
          threadId,
          "the predecessor release must not revoke a credential reserved by an in-flight open",
        );
        yield* manager.close(s2);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            mcpConfigs,
            beforeOpen: (input) =>
              input.providerSessionId === undefined
                ? Effect.void
                : Ref.get(duringOpen).pipe(
                    Effect.flatten,
                    Effect.tap(() => Ref.set(duringOpen, Effect.void)),
                  ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 terminal detach revokes the thread's MCP credential", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const mcpConfigs = yield* Ref.make<
      ReadonlyArray<McpProviderSession.McpProviderSessionConfig | undefined>
    >([]);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const now = yield* DateTime.now;
      const threadId = ThreadId.make("thread-provider-session-manager-terminal-detach");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const issued = (yield* Ref.get(mcpConfigs)).at(-1);
      const token = issued?.authorizationHeader.replace(/^Bearer\s+/, "");
      assert.isDefined(yield* registry.resolve(token!));

      // Archive/delete detaches carry revokeMcpCredential: the token must die
      // with the thread even though the shared provider process lives on.
      yield* manager.detach({
        providerSessionId,
        threadId,
        detail: "Thread deleted.",
        revokeMcpCredential: true,
      });
      assert.isUndefined(yield* registry.resolve(token!));
      assert.isUndefined(McpProviderSession.readMcpProviderSession(threadId));
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000, mcpConfigs })));
  }),
);

it.effect("ProviderSessionManagerV2 releases idle sessions without sweeping all sessions", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-idle",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.openCount, 1);
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists release when session scope close hangs", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-hung-close",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-hung-close",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));

      yield* TestClock.adjust("30 seconds");
      yield* Effect.yieldNow;
      const projection = yield* projectionStore.getThreadProjection(threadId);
      assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      assert.equal((yield* Ref.get(state)).closeCount, 0);
    });

    yield* effect.pipe(
      Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000, hangSessionScopeClose: true })),
    );
  }),
);

it.effect("ProviderSessionManagerV2 defers idle release while background work is pending", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const pendingWork = yield* Ref.make(true);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-idle-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-idle-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 0);

      yield* Ref.set(pendingWork, false);
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          hasPendingBackgroundWork: Ref.get(pendingWork),
        }),
      ),
    );
  }),
);

it.effect("ProviderSessionManagerV2 releases pinned idle sessions once the pin cap expires", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-pin-cap",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-pin-cap",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      yield* TestClock.adjust("3 seconds");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));

      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 3000,
          hasPendingBackgroundWork: Effect.succeed(true),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 does not idle-release a session that turns busy during the pending-work check",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const firstCheck = yield* Ref.make(true);
      const checkEntered = yield* Deferred.make<void>();
      const checkGate = yield* Deferred.make<void>();
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-busy-during-check",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-busy-during-check",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-busy-during-check",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;

        yield* TestClock.adjust("1 second");
        yield* Deferred.await(checkEntered);

        // The release fiber is parked inside the pending-work check, so the
        // idle decision it already made is stale once this turn marks the
        // session busy.
        const turnFiber = yield* runtime
          .startTurn({
            appThread,
            threadId,
            runId,
            runOrdinal: 1,
            providerTurnOrdinal: 1,
            attemptId,
            rootNodeId,
            providerThread,
            message: {
              createdBy: "user",
              creationSource: "web",
              messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
              text: "hello",
              attachments: [],
            },
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkDetach);
        for (let i = 0; i < 10; i += 1) {
          yield* Effect.yieldNow;
        }
        yield* Deferred.succeed(checkGate, undefined);
        yield* Fiber.join(turnFiber);
        yield* Effect.yieldNow;

        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1000,
            // Uninterruptible so the markBusy-triggered interrupt cannot land
            // inside the check, mirroring an adapter that masks interruption
            // while inspecting its own state.
            hasPendingBackgroundWork: Effect.uninterruptible(
              Effect.gen(function* () {
                if (yield* Ref.getAndSet(firstCheck, false)) {
                  yield* Deferred.succeed(checkEntered, undefined);
                  yield* Deferred.await(checkGate);
                }
                return false;
              }),
            ),
          }),
        ),
      );
    }),
);

it.effect("ProviderSessionManagerV2 does not apply a stale idle pin to a replacement session", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const firstCheck = yield* Ref.make(true);
    const checkEntered = yield* Deferred.make<void>();
    const checkGate = yield* Deferred.make<void>();
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const now = yield* DateTime.now;
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stale-pin",
        projectId: yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-stale-pin",
        }),
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });

      // Park the first idle fiber inside an uninterruptible pending-work probe.
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(checkEntered);

      // close removes the map entry first, then waits to interrupt the idle
      // fiber (still uninterruptible). That window lets a replacement open
      // under the same providerSessionId before the stale probe finishes.
      const closeFiber = yield* manager.close(providerSessionId).pipe(Effect.forkDetach);
      for (let i = 0; i < 20; i += 1) {
        yield* Effect.yieldNow;
      }
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.equal((yield* Ref.get(state)).openCount, 2);

      // Stale probe reports pending work against the old runtime; the pin
      // stamp must no-op on the replacement (runtime / generation mismatch).
      yield* Deferred.succeed(checkGate, undefined);
      yield* Fiber.join(closeFiber);
      for (let i = 0; i < 10; i += 1) {
        yield* Effect.yieldNow;
      }

      assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 1);

      // Replacement has no pending background work. After one idle window it
      // must release. A stale pin stamp would have deferred release until
      // maxIdlePinMs.
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
      assert.equal((yield* Ref.get(state)).closeCount, 2);
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          maxIdlePinMs: 60_000,
          hasPendingBackgroundWork: Effect.uninterruptible(
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(firstCheck, false)) {
                yield* Deferred.succeed(checkEntered, undefined);
                yield* Deferred.await(checkGate);
                return true;
              }
              return false;
            }),
          ),
        }),
      ),
    );
  }),
);

it.effect(
  "ProviderSessionManagerV2 keeps active sessions alive until the provider turn terminates",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-active",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-active",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        const runId = idAllocator.derive.run({ threadId, ordinal: 1 });
        const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
        const rootNodeId = idAllocator.derive.rootNode({ runId });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn",
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const appThread = (yield* projectionStore.getThreadProjection(threadId)).thread;
        yield* runtime.startTurn({
          appThread,
          threadId,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId,
          rootNodeId,
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({ threadId, ordinal: 1 }),
            text: "hello",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: providerThread.id,
          providerTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;

        const liveSession = yield* manager.get(providerSessionId);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        assert.isTrue(Option.isNone(liveSession));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal(projection.providerSessions.at(-1)?.status, "stopped");
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect("ProviderSessionManagerV2 uses the same release path for runtime failures", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-runtime-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-runtime-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
      assert.equal(projection.providerSessions.at(-1)?.lastError, "process exited");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 releases sessions when provider event streams fail", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-stream-error",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-stream-error",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* runtime.events.pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
      yield* Effect.yieldNow;

      const liveSession = yield* manager.get(providerSessionId);
      const runtimeState = yield* Ref.get(state);
      const projection = yield* projectionStore.getThreadProjection(threadId);

      assert.isTrue(Option.isNone(liveSession));
      assert.equal(runtimeState.closeCount, 1);
      assert.equal(projection.providerSessions.at(-1)?.status, "error");
    });

    yield* effect.pipe(
      Effect.provide(
        makeTestLayer({
          state,
          idleTimeoutMs: 1000,
          failEventStream: true,
        }),
      ),
    );
  }),
);

const ingestRequestFixture = (
  runtime: ProviderSessionManager.ManagedProviderSessionRuntime,
  threadId: ThreadId,
  events: ReadonlyArray<ProviderAdapterV2Event>,
) =>
  Effect.gen(function* () {
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    yield* Effect.forEach(
      events,
      (event) =>
        ingestor.ingestNormalized({
          providerSessionId: runtime.providerSessionId,
          providerInstanceId: runtime.instanceId,
          runtimeLifetime: runtime.runtimeLifetime,
          threadId,
          event,
        }),
      { discard: true },
    );
  });

it.effect("ProviderSessionManagerV2 marks pending runtime requests non-live on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* ingestRequestFixture(runtime, threadId, pendingRequest.providerEvents);
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "approval_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);
it.effect("ProviderSessionManagerV2 terminalizes a pending input transcript item on release", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-request-expire",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-request-expire",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      const runtime = yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      yield* ingestRequestFixture(
        runtime,
        threadId,
        pendingRequest.providerEvents.map((event): ProviderAdapterV2Event =>
          event.type === "runtime_request.updated"
            ? { ...event, runtimeRequest: { ...event.runtimeRequest, kind: "user_input" } }
            : event.type === "node.updated"
              ? { ...event, node: { ...event.node, kind: "user_input_request" } }
              : event.type === "turn_item.updated"
                ? {
                    ...event,
                    turnItem: { ...event.turnItem, type: "user_input_request", questions: [] },
                  }
                : event,
        ),
      );
      yield* manager.release({
        providerSessionId,
        reason: "runtime_error",
        detail: "process exited",
      });

      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.at(-1);
      const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
      const requestTurnItem = projection.turnItems.find(
        (item) => item.type === "user_input_request" && item.requestId === request?.id,
      );

      assert.equal(request?.status, "expired");
      assert.equal(request?.responseCapability.type, "not_resumable");
      assert.equal(requestNode?.status, "failed");
      assert.equal(requestTurnItem?.status, "failed");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect("ProviderSessionManagerV2 persists session-scoped runtime requests without a run", () =>
  Effect.gen(function* () {
    const state = yield* Ref.make(emptyState);
    const effect = Effect.gen(function* () {
      const eventSink = yield* EventSink.EventSinkV2;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const now = yield* DateTime.now;
      const projectId = yield* idAllocator.allocate.project({
        fixtureName: "provider-session-manager-session-request",
      });
      const threadId = yield* idAllocator.allocate.thread({
        fixtureName: "provider-session-manager-session-request",
        projectId,
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId,
      });
      const providerThread = makeProviderThread({
        idAllocator,
        threadId,
        providerSessionId,
        now,
      });

      yield* eventSink.write({
        events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
      });
      yield* manager.open({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const pendingRequest = yield* makePendingRuntimeRequestEvents({
        idAllocator,
        threadId,
        providerSessionId,
        providerThread,
        now,
      });
      const afterSequence = yield* eventSink.latestSequence({ threadId });
      const persistedFiber = yield* eventSink.stream({ threadId, afterSequence }).pipe(
        Stream.filter(
          (stored) =>
            stored.event.type === "runtime-request.updated" ||
            stored.event.type === "node.updated" ||
            stored.event.type === "turn-item.updated",
        ),
        Stream.take(3),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const adapterEvents = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
      assert.isDefined(adapterEvents);
      yield* Queue.offerAll(adapterEvents!, pendingRequest.providerEvents);
      const persisted = Array.from(yield* Fiber.join(persistedFiber));

      assert.sameMembers(
        persisted.map((stored) => stored.event.type),
        ["runtime-request.updated", "node.updated", "turn-item.updated"],
      );
      const projection = yield* projectionStore.getThreadProjection(threadId);
      const request = projection.runtimeRequests.find(
        (candidate) => candidate.id === pendingRequest.requestId,
      );
      const node = projection.nodes.find((candidate) => candidate.id === pendingRequest.nodeId);
      const turnItem = projection.turnItems.find(
        (candidate) =>
          candidate.type === "approval_request" && candidate.requestId === pendingRequest.requestId,
      );
      assert.equal(request?.status, "pending");
      assert.equal(request?.providerTurnId, null);
      assert.equal(node?.runId, null);
      assert.equal(node?.status, "waiting");
      assert.equal(turnItem?.runId, null);
      assert.equal(turnItem?.status, "waiting");
    });

    yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
  }),
);

it.effect(
  "ProviderSessionManagerV2 preserves item identity during eager native session activation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-request-expire",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-request-expire",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });

        yield* eventSink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator, threadId, now })],
        });
        const pendingRequest = yield* makePendingRuntimeRequestEvents({
          idAllocator,
          threadId,
          providerSessionId,
          providerThread,
          now,
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
          initialNativeThreadId: "native-import",
          initialProviderItemIdentityVersion: 2,
        });
        yield* ingestRequestFixture(runtime, threadId, pendingRequest.providerEvents);
        yield* manager.release({
          providerSessionId,
          reason: "runtime_error",
          detail: "process exited",
        });

        const projection = yield* projectionStore.getThreadProjection(threadId);
        const request = projection.runtimeRequests.at(-1);
        const requestNode = projection.nodes.find((node) => node.id === request?.nodeId);
        const requestTurnItem = projection.turnItems.find(
          (item) => item.type === "approval_request" && item.requestId === request?.id,
        );

        assert.equal(request?.status, "expired");
        assert.equal(request?.responseCapability.type, "not_resumable");
        assert.equal(requestNode?.status, "failed");
        assert.equal(requestTurnItem?.status, "failed");
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1000,
            beforeOpen: (input) =>
              Effect.sync(() => assert.equal(input.initialProviderItemIdentityVersion, 2)),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 keeps a multi-thread session alive until all turns finish",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-multi-thread-active",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-multi-thread-active-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
        });
        const secondProviderThread = makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
        });
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        const secondRunId = idAllocator.derive.run({ threadId: secondThreadId, ordinal: 1 });
        const firstProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-a",
        });
        const secondProviderTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "native-turn-b",
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });
        const runtime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.events.pipe(Stream.runDrain, Effect.forkScoped);
        const firstAppThread = (yield* projectionStore.getThreadProjection(firstThreadId)).thread;
        const secondAppThread = (yield* projectionStore.getThreadProjection(secondThreadId)).thread;
        yield* runtime.startTurn({
          appThread: firstAppThread,
          threadId: firstThreadId,
          runId: firstRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: firstRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
          providerThread: firstProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({ threadId: firstThreadId, ordinal: 1 }),
            text: "first",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });
        yield* runtime.startTurn({
          appThread: secondAppThread,
          threadId: secondThreadId,
          runId: secondRunId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: idAllocator.derive.runAttempt({ runId: secondRunId, attemptOrdinal: 1 }),
          rootNodeId: idAllocator.derive.rootNode({ runId: secondRunId }),
          providerThread: secondProviderThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: yield* idAllocator.allocate.message({
              threadId: secondThreadId,
              ordinal: 1,
            }),
            text: "second",
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        });

        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: firstProviderThread.id,
          providerTurnId: firstProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("2 seconds");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 0);

        yield* Queue.offer(queue!, {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId: secondProviderThread.id,
          providerTurnId: secondProviderTurnId,
          runOrdinal: 1,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 opens one shared runtime, broadcasts events, and detaches threads independently",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-shared-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-shared-runtime-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });

        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });
        const firstProviderThread = makeProviderThread({
          idAllocator,
          threadId: firstThreadId,
          providerSessionId,
          now,
        });
        const secondProviderThread = makeProviderThread({
          idAllocator,
          threadId: secondThreadId,
          providerSessionId,
          now,
        });
        const firstRunId = idAllocator.derive.run({ threadId: firstThreadId, ordinal: 1 });
        yield* eventSink.write({
          events: [
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-thread.updated",
              threadId: firstThreadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: firstProviderThread,
            },
            {
              id: yield* idAllocator.allocate.event({ threadId: firstThreadId }),
              type: "provider-turn.updated",
              threadId: firstThreadId,
              runId: firstRunId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: {
                id: idAllocator.derive.providerTurn({
                  driver: CODEX_DRIVER,
                  nativeTurnId: "native-turn-shared-runtime-a",
                }),
                providerThreadId: firstProviderThread.id,
                nodeId: idAllocator.derive.rootNode({ runId: firstRunId }),
                runAttemptId: null,
                nativeTurnRef: null,
                ordinal: 1,
                status: "running",
                startedAt: now,
                completedAt: null,
              },
            },
          ],
        });
        const firstRuntime = yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const secondRuntime = yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });

        assert.strictEqual(firstRuntime, secondRuntime);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        const resumeSecondThread = secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        yield* secondRuntime.resumeThread({
          providerThread: secondProviderThread,
          threadId: secondThreadId,
          modelSelection: { ...modelSelection, model: "gpt-5.4-mini" },
          runtimePolicy,
        });
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 3);
        const subscribe = firstRuntime.subscribeEvents;
        assert.isDefined(subscribe);
        if (subscribe === undefined) return;
        const firstSubscription = yield* subscribe;
        const secondSubscription = yield* subscribe;
        const queue = (yield* Ref.get(state)).eventQueues.get(String(providerSessionId));
        assert.isDefined(queue);
        yield* Queue.offer(queue!, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: firstRuntime.providerSession,
        });
        const received = yield* Effect.all([
          firstSubscription.events.pipe(Stream.runHead),
          secondSubscription.events.pipe(Stream.runHead),
        ]);
        assert.isTrue(received.every(Option.isSome));
        assert.isTrue(
          received.every(
            (event) => Option.isSome(event) && event.value.type === "provider_session.updated",
          ),
        );

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* manager.open({
          threadId: secondThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        yield* resumeSecondThread;
        assert.equal((yield* Ref.get(state)).resumeCount, 4);

        // The second thread has no persisted provider thread, so nothing is unloaded.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, []);

        yield* manager.detach({ providerSessionId, threadId: firstThreadId });
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.equal((yield* Ref.get(state)).interruptCount, 1);
        // The runtime stays up for the second thread; the first thread's
        // native state is unloaded after its turn is interrupted.
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);

        yield* manager.detach({ providerSessionId, threadId: secondThreadId });
        yield* TestClock.adjust("1 second");
        yield* Effect.yieldNow;
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      });

      yield* effect.pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 re-attaching a thread waits for its in-flight unload, then reloads it",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const unloadStarted = yield* Deferred.make<void>();
      const releaseUnload = yield* Deferred.make<void>();
      // Resumes the provider had served when the unload actually reached it.
      let resumesBeforeUnload: number | undefined;
      // The unload parks after detach removed the attachment, leaving the
      // window in which the same thread's next turn re-attaches it.
      const beforeUnload = Effect.gen(function* () {
        yield* Deferred.succeed(unloadStarted, undefined);
        yield* Deferred.await(releaseUnload);
        resumesBeforeUnload = (yield* Ref.get(state)).resumeCount;
      });
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-unload-race",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-a",
          projectId,
        });
        const otherThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-unload-race-b",
          projectId,
        });
        const providerSessionId = idAllocator.derive.providerSession({
          providerInstanceId: modelSelection.instanceId,
        });
        const providerThread = makeProviderThread({
          idAllocator,
          threadId,
          providerSessionId,
          now,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: otherThreadId, now }),
            {
              id: yield* idAllocator.allocate.event({ threadId }),
              type: "provider-thread.updated",
              threadId,
              driver: CODEX_DRIVER,
              occurredAt: now,
              payload: providerThread,
            },
          ],
        });
        const runtime = yield* manager.open({
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // A second thread keeps the shared runtime up after the detach.
        yield* manager.open({
          threadId: otherThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        // Resuming re-attaches the thread to the shared runtime.
        const resume = runtime.resumeThread({
          providerThread,
          threadId,
          modelSelection,
          runtimePolicy,
        });
        yield* resume;

        const detach = yield* manager
          .detach({ providerSessionId, threadId })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(unloadStarted);
        // The same thread's next turn re-attaches while the unload is parked.
        // Give it room to run: unfixed, it reaches the provider's resume
        // here; serialized, it waits for the unload.
        const reattach = yield* resume.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseUnload, undefined);
        yield* Fiber.join(detach);
        yield* Fiber.join(reattach);

        // The unload reached the provider before the re-attached resume, so
        // that resume reloads the thread instead of being torn down after it.
        assert.equal(resumesBeforeUnload, 1);
        assert.equal((yield* Ref.get(state)).resumeCount, 2);
        assert.deepEqual((yield* Ref.get(state)).unloadedNativeThreadIds, ["native-thread"]);
        assert.isTrue(Option.isSome(yield* manager.get(providerSessionId)));
      });

      yield* effect.pipe(
        Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1000, beforeUnload })),
        Effect.scoped,
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 rejects a second thread when the provider runtime is exclusive",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const effect = Effect.gen(function* () {
        const eventSink = yield* EventSink.EventSinkV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const now = yield* DateTime.now;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-session-manager-exclusive-runtime",
        });
        const firstThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-a",
          projectId,
        });
        const secondThreadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-session-manager-exclusive-runtime-b",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: firstThreadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator, threadId: firstThreadId, now }),
            yield* makeThreadCreatedEvent({ idAllocator, threadId: secondThreadId, now }),
          ],
        });

        yield* manager.open({
          threadId: firstThreadId,
          providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        const error = yield* manager
          .open({
            threadId: secondThreadId,
            providerSessionId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.flip);

        assert.equal(error._tag, "ProviderSessionOpenError");
        assert.equal((yield* Ref.get(state)).openCount, 1);
      });

      yield* effect.pipe(
        Effect.provide(
          makeTestLayer({ state, idleTimeoutMs: 1000, capabilities: ExclusiveCapabilities }),
        ),
      );
    }),
);

it.effect.each(["missing", "file"] as const)(
  "rejects a %s workspace before opening a provider session",
  (workspaceState) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      if (workspaceState === "file") yield* fileSystem.writeFileString(cwd, "not a directory");
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make(`thread-${workspaceState}-workspace`);
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const error = yield* manager
          .open({
            threadId,
            providerSessionId,
            modelSelection,
            runtimePolicy: { ...runtimePolicy, cwd },
          })
          .pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.include(error.message, cwd);
        assert.include(error.message, "Restore the folder at this path before retrying.");
        assert.equal((yield* Ref.get(state)).openCount, 0);
        assert.isTrue(Option.isNone(yield* manager.get(providerSessionId)));
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          [],
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "rejects a deleted workspace before reusing a live session without changing its state",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const root = yield* fileSystem.makeTempDirectoryScoped();
      const cwd = `${root}/workspace`;
      yield* fileSystem.makeDirectory(cwd);
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make("thread-deleted-live-workspace");
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        yield* eventSink.write({
          events: [
            yield* makeThreadCreatedEvent({
              idAllocator,
              threadId,
              now: yield* DateTime.now,
            }),
          ],
        });
        const input = {
          threadId,
          providerSessionId,
          modelSelection,
          runtimePolicy: { ...runtimePolicy, cwd },
        };
        const runtime = yield* manager.open(input);
        const before = yield* projectionStore.getThreadProjection(threadId);
        yield* fileSystem.remove(cwd, { recursive: true });
        const error = yield* manager.open(input).pipe(Effect.flip);
        assert.instanceOf(error, ProviderWorkspaceMissingError);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
        assert.strictEqual(Option.getOrThrow(yield* manager.get(providerSessionId)), runtime);
        assert.deepEqual(
          (yield* projectionStore.getThreadProjection(threadId)).providerSessions,
          before.providerSessions,
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderSessionManagerV2 applies project device access independently of browser access",
  () =>
    Effect.gen(function* () {
      const enabled = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
      });
      assert.isTrue(enabled?.capabilities?.has("device"));
      assert.isFalse(enabled?.browserToolsAvailable);
      const denied = yield* runBrowserAccessScenario({
        enableAgentBrowserAccess: false,
        projectOverride: false,
        deviceOverride: true,
        projectExists: false,
      });
      assert.isFalse(denied?.capabilities?.has("device"));
    }),
);

it.effect.each([-500, 0, 500])(
  "ProviderSessionManagerV2 release preserves a replacement after %i ms clock change",
  (clockShift) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make("baseline-concurrent-replacement");
        const providerSessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
        });
        const open = manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
        yield* open;
        const close = yield* manager.close(providerSessionId).pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        yield* TestClock.setTime(DateTime.toEpochMillis(now) + clockShift);
        const replacement = yield* open;
        const request = yield* makePendingRuntimeRequestEvents({
          idAllocator: ids,
          threadId,
          providerSessionId,
          providerThread: makeProviderThread({
            idAllocator: ids,
            threadId,
            providerSessionId,
            now: yield* DateTime.now,
          }),
          now: yield* DateTime.now,
        });
        yield* ingestRequestFixture(replacement, threadId, request.providerEvents);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(close);
        const projection = yield* projections.getThreadProjection(threadId);
        const actual = projection.runtimeRequests.find((r) => r.id === request.requestId);
        assert.deepEqual(
          {
            status: actual?.status,
            capability: actual?.responseCapability.type,
            sessionStatus: projection.providerSessions.at(-1)?.status,
          },
          { status: "pending", capability: "live", sessionStatus: "ready" },
        );
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60000,
            beforeClose: Deferred.succeed(paused, undefined).pipe(
              Effect.andThen(Deferred.await(resume)),
            ),
          }),
        ),
      );
    }),
);

it.effect.each(["live", "not_resumable", "message"] as const)(
  "startup recovery settles runless %s request artifacts consistently",
  (capability) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = ThreadId.make(`runless-recovery-${capability}`);
        const providerSessionId = yield* ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const now = yield* DateTime.now;
        const generated = yield* makePendingRuntimeRequestEvents({
          idAllocator: ids,
          threadId,
          providerSessionId,
          providerThread: makeProviderThread({
            idAllocator: ids,
            threadId,
            providerSessionId,
            now,
          }),
          now,
        });
        const events = generated.events.map((event): OrchestrationV2DomainEvent =>
          event.type !== "runtime-request.updated"
            ? event
            : capability === "message"
              ? { ...event, payload: { ...event.payload, responseCapability: { type: "message" } } }
              : capability === "not_resumable"
                ? {
                    ...event,
                    payload: {
                      ...event.payload,
                      status: "expired",
                      resolvedAt: now,
                      responseCapability: {
                        type: "not_resumable",
                        reason: "Previously expired by startup recovery.",
                      },
                    },
                  }
                : event,
        );
        yield* sink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now }), ...events],
        });
        yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService.pipe(
          Effect.flatMap((recovery) => recovery.recover),
          Effect.provide(
            ProviderRuntimeRecovery.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  ServerSettings.layerTest(),
                  Layer.mock(EffectOutbox.EffectOutboxV2)({
                    reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
                    cancelUnsettled: () => Effect.succeed([]),
                    signalCancellations: () => Effect.void,
                  }),
                ),
              ),
            ),
          ),
        );
        const projection = yield* projections.getThreadProjection(threadId);
        const request = projection.runtimeRequests.find(
          (candidate) => candidate.id === generated.requestId,
        );
        const node = projection.nodes.find((candidate) => candidate.id === generated.nodeId);
        const item = projection.turnItems.find(
          (candidate) =>
            candidate.type === "approval_request" && candidate.requestId === generated.requestId,
        );
        assert.deepEqual(
          {
            request: request?.status,
            capability: request?.responseCapability.type,
            node: node?.status,
            item: item?.status,
          },
          capability !== "message"
            ? {
                request: "expired",
                capability: "not_resumable",
                node: "cancelled",
                item: "cancelled",
              }
            : { request: "pending", capability: "message", node: "waiting", item: "waiting" },
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60000 })));
    }),
);

const makeLifetimeFixture = (name: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
    const registry = yield* McpSessionRegistry.McpSessionRegistry;
    const threadId = ThreadId.make(`extra-${name}`);
    const providerSessionId = yield* ids.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId,
    });
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [yield* makeThreadCreatedEvent({ idAllocator: ids, threadId, now })],
    });
    const open = manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
    const makeRequest = Effect.gen(function* () {
      const now = yield* DateTime.now;
      return yield* makePendingRuntimeRequestEvents({
        idAllocator: ids,
        threadId,
        providerSessionId,
        providerThread: makeProviderThread({ idAllocator: ids, threadId, providerSessionId, now }),
        now,
      });
    });
    return {
      sink,
      ids,
      manager,
      projections,
      ingestor,
      registry,
      threadId,
      providerSessionId,
      open,
      makeRequest,
    };
  });

it.effect(
  "ProviderSessionManagerV2 rejects a queued real subscription request after close and replacement",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const received = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("queued-request");
        const old = yield* f.open;
        const request = yield* f.makeRequest;
        const providerTurnId = f.ids.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "queued-old-turn",
        });
        const runId = f.ids.derive.run({ threadId: f.threadId, ordinal: 1 });
        const events = request.providerEvents.map((event): ProviderAdapterV2Event =>
          event.type === "runtime_request.updated"
            ? { ...event, runtimeRequest: { ...event.runtimeRequest, providerTurnId } }
            : event.type === "node.updated"
              ? { ...event, node: { ...event.node, providerTurnId, runId } }
              : event.type === "turn_item.updated"
                ? { ...event, turnItem: { ...event.turnItem, providerTurnId, runId } }
                : event,
        );
        assert.isDefined(old.subscribeEvents);
        const subscription = yield* old.subscribeEvents!;
        const consumed = yield* Ref.make(0);
        const consumer = yield* subscription.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(received, undefined);
              yield* Deferred.await(resume);
              yield* ingestRequestFixture(old, f.threadId, [event]);
              yield* Ref.update(consumed, (count) => count + 1);
            }),
          ),
          Effect.exit,
          Effect.forkScoped,
        );
        const queue = (yield* Ref.get(state)).eventQueues.get(String(f.providerSessionId))!;
        yield* Queue.offerAll(queue, events);
        yield* Deferred.await(received);
        yield* f.manager.close(f.providerSessionId);
        const replacement = yield* f.open;
        const replacementRequest = yield* f.makeRequest;
        yield* ingestRequestFixture(replacement, f.threadId, replacementRequest.providerEvents);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(consumer);
        assert.equal(yield* Ref.get(consumed), 3);
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        assert.isUndefined(projection.runtimeRequests.find((r) => r.id === request.requestId));
        assert.isUndefined(projection.nodes.find((n) => n.id === request.nodeId));
        assert.isUndefined(
          projection.turnItems.find(
            (i) => i.type === "approval_request" && i.requestId === request.requestId,
          ),
        );
        assert.equal(
          projection.runtimeRequests.find((r) => r.id === replacementRequest.requestId)?.status,
          "pending",
        );
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        assert.equal(projection.providerSessions.at(-1)?.status, "ready");
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 caller interruption does not strand timed-out persistence or its retry",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const attempts = yield* Ref.make(0);
      const beforeReleaseWrite = Effect.gen(function* () {
        const attempt = yield* Ref.getAndUpdate(attempts, (n) => n + 1);
        if (attempt === 0) {
          yield* Deferred.succeed(paused, undefined);
          return yield* Effect.never;
        }
      });
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("interrupted-release");
        const runtime = yield* f.open;
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        const request = yield* f.makeRequest;
        yield* ingestRequestFixture(runtime, f.threadId, request.providerEvents);
        const afterSequence = yield* f.sink.latestSequence({ threadId: f.threadId });
        const stopped = yield* f.sink.stream({ threadId: f.threadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "provider-session.updated" &&
              stored.event.payload.status === "stopped",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        const caller = yield* f.manager.close(f.providerSessionId).pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        yield* Fiber.interrupt(caller);
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.isUndefined(yield* f.registry.resolve(token));
        yield* TestClock.adjust("5 seconds");
        const duringRetry = yield* f.projections.getThreadProjection(f.threadId);
        assert.equal(
          duringRetry.runtimeRequests.find((r) => r.id === request.requestId)?.status,
          "cancelled",
        );
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(stopped);
        assert.equal(yield* Ref.get(attempts), 2);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, beforeReleaseWrite })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 stale terminal detach cannot revoke or mutate its replacement",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const beforeRead = (records: ReadonlyArray<string>) =>
        records.includes("providerThreads")
          ? Deferred.succeed(paused, undefined).pipe(
              Effect.andThen(Deferred.await(resume)),
              Effect.asVoid,
            )
          : Effect.void;
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("stale-detach");
        yield* f.open;
        const detach = yield* f.manager
          .detach({
            providerSessionId: f.providerSessionId,
            threadId: f.threadId,
            revokeMcpCredential: true,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        yield* f.manager.close(f.providerSessionId);
        const replacement = yield* f.open;
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        const request = yield* f.makeRequest;
        yield* ingestRequestFixture(replacement, f.threadId, request.providerEvents);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(detach);
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        assert.equal((yield* f.registry.resolve(token))?.threadId, f.threadId);
        assert.equal(
          McpProviderSession.readMcpProviderSession(f.threadId)?.providerSessionId,
          credential.providerSessionId,
        );
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        assert.equal(projection.providerSessions.at(-1)?.status, "ready");
        assert.equal(
          projection.runtimeRequests.find((r) => r.id === request.requestId)?.status,
          "pending",
        );
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, beforeRead })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 two retired generations retry typed and defect read failures independently",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const attempts = yield* Ref.make(0);
      const beforeRead = (records: ReadonlyArray<string>) =>
        !records.includes("runtimeRequests")
          ? Effect.void
          : Effect.gen(function* () {
              const attempt = yield* Ref.getAndUpdate(attempts, (n) => n + 1);
              if (attempt === 0)
                return yield* new ProjectionStore.ProjectionStoreReadError({
                  threadId: ThreadId.make("extra-two-retired"),
                });
              if (attempt === 1) return yield* Effect.die("simulated release projection defect");
            });
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("two-retired");
        const a = yield* f.open;
        const requestA = yield* f.makeRequest;
        yield* ingestRequestFixture(a, f.threadId, requestA.providerEvents);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(f.manager.close(f.providerSessionId))));
        const b = yield* f.open;
        const requestB = yield* f.makeRequest;
        yield* ingestRequestFixture(b, f.threadId, requestB.providerEvents);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(f.manager.close(f.providerSessionId))));
        const c = yield* f.open;
        const requestC = yield* f.makeRequest;
        yield* ingestRequestFixture(c, f.threadId, requestC.providerEvents);
        const afterSequence = yield* f.sink.latestSequence({ threadId: f.threadId });
        const cancellations = yield* f.sink.stream({ threadId: f.threadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "runtime-request.updated" &&
              stored.event.payload.status === "cancelled",
          ),
          Stream.take(2),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(cancellations);
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        assert.deepEqual(
          [requestA, requestB, requestC].map(
            (request) => projection.runtimeRequests.find((r) => r.id === request.requestId)?.status,
          ),
          ["cancelled", "cancelled", "pending"],
        );
        assert.deepEqual(
          [requestA, requestB].map(
            (request) => projection.nodes.find((n) => n.id === request.nodeId)?.status,
          ),
          ["cancelled", "cancelled"],
        );
        assert.equal(projection.providerSessions.at(-1)?.status, "ready");
        assert.strictEqual(Option.getOrThrow(yield* f.manager.get(f.providerSessionId)), c);
        assert.equal((yield* Ref.get(state)).closeCount, 2);
        assert.equal(yield* Ref.get(attempts), 4);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, beforeRead })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 an existing pending SQL identity never grants replacement ownership",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("pending-sql-identity");
        const request = yield* f.makeRequest;
        yield* f.sink.write({ events: request.events });
        const runtime = yield* f.open;
        yield* ingestRequestFixture(runtime, f.threadId, request.providerEvents);
        assert.isFalse(
          yield* f.ingestor.ownsRequest(runtime.runtimeLifetime, f.threadId, request.requestId),
        );
        yield* f.manager.close(f.providerSessionId);
        const replacement = yield* f.open;
        yield* ingestRequestFixture(replacement, f.threadId, request.providerEvents);
        assert.isFalse(
          yield* f.ingestor.ownsRequest(replacement.runtimeLifetime, f.threadId, request.requestId),
        );
        assert.deepEqual(f.ingestor.getOwnedRequestGroups(replacement.runtimeLifetime), []);
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        assert.equal(
          projection.runtimeRequests.find((r) => r.id === request.requestId)?.status,
          "pending",
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect.each([false, true])(
  "ProviderSessionManagerV2 stale idle probe (%s) leaves replacement credentials and deadline intact",
  (pending) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const firstProbe = yield* Ref.make(true);
      const paused = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const hasPendingBackgroundWork = Effect.uninterruptible(
        Effect.gen(function* () {
          if (!(yield* Ref.getAndSet(firstProbe, false))) return false;
          yield* Deferred.succeed(paused, undefined);
          yield* Deferred.await(resume);
          return pending;
        }),
      );
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture(`stale-idle-${pending}`);
        yield* f.open;
        yield* TestClock.adjust("1 second");
        yield* Deferred.await(paused);
        const close = yield* f.manager
          .close(f.providerSessionId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        const replacement = yield* f.open;
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(close);
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        assert.equal((yield* f.registry.resolve(token))?.threadId, f.threadId);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        const afterSequence = yield* f.sink.latestSequence({ threadId: f.threadId });
        const stopped = yield* f.sink.stream({ threadId: f.threadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "provider-session.updated" &&
              stored.event.payload.status === "stopped",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(stopped);
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 2);
        assert.isUndefined(yield* f.registry.resolve(token));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            maxIdlePinMs: 60_000,
            hasPendingBackgroundWork,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 late native teardown preserves the replacement credential and native handle",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("late-native-close");
        const old = yield* f.open;
        const original = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const close = yield* f.manager.close(f.providerSessionId).pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        const replacement = yield* f.open;
        const current = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = current.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.equal(current.providerSessionId, original.providerSessionId);
        yield* Deferred.succeed(resume, undefined);
        yield* Fiber.join(close);
        assert.equal((yield* f.registry.resolve(token))?.threadId, f.threadId);
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        const providerThread = makeProviderThread({
          idAllocator: f.ids,
          threadId: f.threadId,
          providerSessionId: f.providerSessionId,
          now: yield* DateTime.now,
        });
        const oldResume = yield* Effect.exit(
          old.resumeThread({ providerThread, threadId: f.threadId, modelSelection, runtimePolicy }),
        );
        assert.isTrue(Exit.isFailure(oldResume));
        assert.equal((yield* Ref.get(state)).resumeCount, 0);
        yield* replacement.resumeThread({
          providerThread,
          threadId: f.threadId,
          modelSelection,
          runtimePolicy,
        });
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeClose: Deferred.succeed(paused, undefined).pipe(
              Effect.andThen(Deferred.await(resume)),
            ),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 retired runtime activity cannot extend replacement idle residency",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("stale-runtime-activity");
        const old = yield* f.open;
        yield* f.manager.close(f.providerSessionId);
        const replacement = yield* f.open;
        const request = yield* f.makeRequest;
        yield* ingestRequestFixture(replacement, f.threadId, request.providerEvents);
        const afterSequence = yield* f.sink.latestSequence({ threadId: f.threadId });
        const stopped = yield* f.sink.stream({ threadId: f.threadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "provider-session.updated" &&
              stored.event.payload.status === "stopped",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* TestClock.adjust("500 millis");
        const response = yield* Effect.exit(
          old.respondToRuntimeRequest({ requestId: request.requestId, decision: "accept" }),
        );
        assert.isTrue(Exit.isFailure(response));
        yield* TestClock.adjust("500 millis");
        yield* Fiber.join(stopped);
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 2);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 1_000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 shutdown while native open is parked rejects late installation and closes scope",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("shutdown-open");
        const opening = yield* f.open.pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(paused);
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        yield* f.manager.shutdown;
        yield* Deferred.succeed(resume, undefined);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(opening)));
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.isUndefined(yield* f.registry.resolve(token));
        assert.isUndefined(McpProviderSession.readMcpProviderSession(f.threadId));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeOpen: () =>
              Deferred.succeed(paused, undefined).pipe(Effect.andThen(Deferred.await(resume))),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 interrupted native open before installation closes its scope and credential",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("interrupted-native-open");
        const opening = yield* f.open.pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        yield* Fiber.interrupt(opening);
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.isUndefined(yield* f.registry.resolve(token));
        assert.isUndefined(McpProviderSession.readMcpProviderSession(f.threadId));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeNativeReturn: Deferred.succeed(paused, undefined).pipe(
              Effect.andThen(Effect.never),
            ),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 release cancels every owned transcript item for the same request",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("multiple-items");
        const runtime = yield* f.open;
        const request = yield* f.makeRequest;
        yield* ingestRequestFixture(runtime, f.threadId, request.providerEvents);
        const firstItem = request.providerEvents.find(
          (event) => event.type === "turn_item.updated",
        );
        assert.isDefined(firstItem);
        if (firstItem?.type !== "turn_item.updated") return;
        const secondItemId = f.ids.derive.turnItemFromProviderItem({
          driver: CODEX_DRIVER,
          nativeItemId: "same-request-second-item",
        });
        yield* ingestRequestFixture(runtime, f.threadId, [
          { ...firstItem, turnItem: { ...firstItem.turnItem, id: secondItemId, ordinal: 2 } },
        ]);
        yield* f.manager.close(f.providerSessionId);
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        const items = projection.turnItems.filter(
          (i) => i.type === "approval_request" && i.requestId === request.requestId,
        );
        assert.equal(items.length, 2);
        assert.deepEqual(
          items.map((i) => i.status),
          ["cancelled", "cancelled"],
        );
        assert.isTrue(items.every((i) => i.completedAt !== null));
        assert.equal(
          projection.runtimeRequests.find((r) => r.id === request.requestId)?.status,
          "cancelled",
        );
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000 })));
    }),
);

it.effect(
  "ProviderSessionManagerV2 interrupted existing-credential resolution drops its lease and permits final revocation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const resolving = yield* Deferred.make<void>();
      const closing = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("interrupted-credential-resolution");
        yield* f.open;
        const credential = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const token = credential.authorizationHeader.replace(/^Bearer\s+/, "");
        const replacementId = yield* f.ids.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId: f.threadId,
        });
        const opening = yield* f.manager
          .open({
            threadId: f.threadId,
            providerSessionId: replacementId,
            modelSelection,
            runtimePolicy,
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(resolving);
        const close = yield* f.manager.close(f.providerSessionId).pipe(Effect.forkScoped);
        yield* Deferred.await(closing);
        assert.equal((yield* f.registry.resolve(token))?.threadId, f.threadId);
        yield* Fiber.interrupt(opening);
        yield* Fiber.join(close);
        assert.isUndefined(yield* f.registry.resolve(token));
        assert.isUndefined(McpProviderSession.readMcpProviderSession(f.threadId));
        assert.isTrue(Option.isNone(yield* f.manager.get(replacementId)));
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeMcpResolve: Deferred.succeed(resolving, undefined).pipe(
              Effect.andThen(Effect.never),
            ),
            beforeClose: Deferred.succeed(closing, undefined).pipe(Effect.asVoid),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 credential check and revoke serialize against replacement preparation",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const revoking = yield* Deferred.make<void>();
      const resumeRevoke = yield* Deferred.make<void>();
      const resolutions = yield* Ref.make(0);
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("serialized-credential-revoke");
        yield* f.open;
        const original = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const originalToken = original.authorizationHeader.replace(/^Bearer\s+/, "");
        const close = yield* f.manager.close(f.providerSessionId).pipe(Effect.forkScoped);
        yield* Deferred.await(revoking);
        // Null cwd removes filesystem I/O, so startImmediately reaches the held
        // preparation lock before this fork returns to the test.
        const opening = yield* f.manager
          .open({
            threadId: f.threadId,
            providerSessionId: f.providerSessionId,
            modelSelection,
            runtimePolicy: { ...runtimePolicy, cwd: null },
          })
          .pipe(Effect.forkScoped({ startImmediately: true }));
        assert.equal(yield* Ref.get(resolutions), 0);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        yield* Deferred.succeed(resumeRevoke, undefined);
        yield* Fiber.join(close);
        const replacement = yield* Fiber.join(opening);
        const current = McpProviderSession.readMcpProviderSession(f.threadId)!;
        const currentToken = current.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.notEqual(current.providerSessionId, original.providerSessionId);
        assert.isUndefined(yield* f.registry.resolve(originalToken));
        assert.equal((yield* f.registry.resolve(currentToken))?.threadId, f.threadId);
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeMcpResolve: Ref.update(resolutions, (n) => n + 1),
            beforeMcpRevoke: Deferred.succeed(revoking, undefined).pipe(
              Effect.andThen(Deferred.await(resumeRevoke)),
            ),
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 interrupting touch while its old idle probe is parked preserves the new timer",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const firstProbe = yield* Ref.make(true);
      const probing = yield* Deferred.make<void>();
      const resumeProbe = yield* Deferred.make<void>();
      const hasPendingBackgroundWork = Effect.uninterruptible(
        Effect.gen(function* () {
          if (!(yield* Ref.getAndSet(firstProbe, false))) return false;
          yield* Deferred.succeed(probing, undefined);
          yield* Deferred.await(resumeProbe);
          return true;
        }),
      );
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("interrupted-timer-replacement");
        yield* f.open;
        yield* TestClock.adjust("1 second");
        yield* Deferred.await(probing);
        const touching = yield* f.manager
          .get(f.providerSessionId)
          .pipe(Effect.forkScoped({ startImmediately: true }));
        yield* Fiber.interrupt(touching);
        yield* Deferred.succeed(resumeProbe, undefined);
        const afterSequence = yield* f.sink.latestSequence({ threadId: f.threadId });
        const stopped = yield* f.sink.stream({ threadId: f.threadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "provider-session.updated" &&
              stored.event.payload.status === "stopped",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(stopped);
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 1_000,
            maxIdlePinMs: 60_000,
            hasPendingBackgroundWork,
          }),
        ),
      );
    }),
);

it.effect(
  "ProviderSessionManagerV2 graceful provider stop preserves node-first queued message question on a run subscription",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const siblingsHandled = yield* Deferred.make<void>();
      const requestPaused = yield* Deferred.make<void>();
      const resumeRequest = yield* Deferred.make<void>();
      const nativeClosing = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("queued-message-question");
        const runtime = yield* f.open;
        const request = yield* f.makeRequest;
        const runId = f.ids.derive.run({ threadId: f.threadId, ordinal: 1 });
        const providerTurnId = f.ids.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: "queued-message-question",
        });
        const questions = [
          {
            id: "workspace",
            header: "Workspace",
            question: "Which workspace should I use?",
            options: [{ label: "Existing workspace", description: "Keep this checkout" }],
            allowCustomAnswer: true,
          },
        ];
        const events = request.providerEvents.map((event): ProviderAdapterV2Event =>
          event.type === "runtime_request.updated"
            ? {
                ...event,
                runtimeRequest: {
                  ...event.runtimeRequest,
                  kind: "user_input",
                  providerTurnId,
                  responseCapability: { type: "message" },
                },
              }
            : event.type === "node.updated"
              ? {
                  ...event,
                  node: { ...event.node, kind: "user_input_request", providerTurnId, runId },
                }
              : event.type === "turn_item.updated"
                ? {
                    ...event,
                    turnItem: {
                      ...event.turnItem,
                      type: "user_input_request",
                      providerTurnId,
                      runId,
                      questions,
                    },
                  }
                : event,
        );
        const subscription = yield* runtime.subscribeEvents!;
        const handled = yield* Ref.make(0);
        const consumer = yield* subscription.events.pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              if (event.type === "runtime_request.updated") {
                yield* Deferred.succeed(requestPaused, undefined);
                yield* Deferred.await(resumeRequest);
              }
              yield* f.ingestor.ingestNormalized({
                providerSessionId: f.providerSessionId,
                providerInstanceId: runtime.instanceId,
                runtimeLifetime: runtime.runtimeLifetime,
                threadId: f.threadId,
                runId,
                event,
              });
              if (event.type === "node.updated" || event.type === "turn_item.updated") {
                const count = yield* Ref.updateAndGet(handled, (n) => n + 1);
                if (count === 2) yield* Deferred.succeed(siblingsHandled, undefined);
              }
            }),
          ),
          Effect.forkScoped,
        );
        const queue = (yield* Ref.get(state)).eventQueues.get(String(f.providerSessionId))!;
        yield* Queue.offerAll(
          queue,
          events.filter((event) => event.type !== "runtime_request.updated"),
        );
        yield* Deferred.await(siblingsHandled);
        yield* Queue.offerAll(
          queue,
          events.filter((event) => event.type === "runtime_request.updated"),
        );
        yield* Deferred.await(requestPaused);
        yield* Queue.offer(queue, {
          type: "provider_session.updated",
          driver: CODEX_DRIVER,
          providerSession: { ...runtime.providerSession, status: "stopped" },
        });
        yield* Queue.end(queue);
        yield* Deferred.await(nativeClosing);
        yield* Deferred.succeed(resumeRequest, undefined);
        yield* Fiber.join(consumer);
        const projection = yield* f.projections.getThreadProjection(f.threadId);
        const actualRequest = projection.runtimeRequests.find((r) => r.id === request.requestId);
        const actualNode = projection.nodes.find((n) => n.id === request.nodeId);
        const actualItem = projection.turnItems.find(
          (i) => i.type === "user_input_request" && i.requestId === request.requestId,
        );
        assert.equal(actualRequest?.status, "pending");
        assert.equal(actualRequest?.responseCapability.type, "message");
        assert.equal(actualNode?.status, "waiting");
        assert.equal(actualNode?.runId, runId);
        assert.equal(actualItem?.status, "waiting");
        assert.equal(actualItem?.runId, runId);
        assert.deepEqual(
          actualItem?.type === "user_input_request" ? actualItem.questions : undefined,
          questions,
        );
        assert.isTrue(Option.isNone(yield* f.manager.get(f.providerSessionId)));
      }).pipe(
        Effect.provide(
          makeTestLayer({
            state,
            idleTimeoutMs: 60_000,
            beforeClose: Deferred.succeed(nativeClosing, undefined).pipe(Effect.asVoid),
          }),
        ),
      );
    }),
);

it.effect(
  "extra attach interrupted attachment cannot expose an uncommitted credential to a concurrent same-runtime resume",
  () =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const paused = yield* Deferred.make<void>();
      const attachmentWrites = yield* Ref.make(0);
      const threadId = ThreadId.make("extra-same-runtime-attachment-target");
      const beforeAttachmentWrite = (target: ThreadId) =>
        target !== threadId
          ? Effect.void
          : Effect.gen(function* () {
              const attempt = yield* Ref.getAndUpdate(attachmentWrites, (n) => n + 1);
              if (attempt === 0) {
                yield* Deferred.succeed(paused, undefined);
                return yield* Effect.never;
              }
            });
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture("same-runtime-attachment-owner");
        const runtime = yield* f.open;
        const now = yield* DateTime.now;
        yield* f.sink.write({
          events: [yield* makeThreadCreatedEvent({ idAllocator: f.ids, threadId, now })],
        });
        const providerThread = makeProviderThread({
          idAllocator: f.ids,
          threadId,
          providerSessionId: f.providerSessionId,
          now,
        });
        const resume = runtime.resumeThread({
          threadId,
          providerThread,
          modelSelection,
          runtimePolicy,
        });
        const first = yield* resume.pipe(Effect.forkScoped);
        yield* Deferred.await(paused);
        const stagedCredential = McpProviderSession.readMcpProviderSession(threadId)!;
        const stagedToken = stagedCredential.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.equal((yield* f.registry.resolve(stagedToken))?.threadId, threadId);
        // The second call runs until it reaches the attachment lane held by first.
        const second = yield* resume.pipe(Effect.forkScoped({ startImmediately: true }));
        assert.equal((yield* Ref.get(state)).resumeCount, 0);
        yield* Fiber.interrupt(first);
        yield* Fiber.join(second);
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        const currentCredential = McpProviderSession.readMcpProviderSession(threadId)!;
        const currentToken = currentCredential.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.equal((yield* f.registry.resolve(currentToken))?.threadId, threadId);
        assert.notEqual(currentCredential.providerSessionId, stagedCredential.providerSessionId);
        assert.isUndefined(yield* f.registry.resolve(stagedToken));
        assert.equal(yield* Ref.get(attachmentWrites), 2);
        yield* resume;
        assert.equal((yield* Ref.get(state)).resumeCount, 1);
        assert.equal((yield* Ref.get(state)).openCount, 1);
        assert.equal((yield* Ref.get(state)).closeCount, 0);
      }).pipe(
        Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, beforeAttachmentWrite })),
      );
    }),
);

it.effect.each(["typed", "defect"] as const)(
  "extra shared cleanup retries a second-thread %s read failure without touching a replacement",
  (failure) =>
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyState);
      const reads = yield* Ref.make(0);
      const secondThreadId = ThreadId.make(`extra-shared-cleanup-target-${failure}`);
      const beforeRead = (records: ReadonlyArray<string>) =>
        !records.includes("runtimeRequests")
          ? Effect.void
          : Effect.gen(function* () {
              const count = yield* Ref.updateAndGet(reads, (n) => n + 1);
              if (count !== 2) return;
              if (failure === "typed")
                return yield* new ProjectionStore.ProjectionStoreReadError({
                  threadId: secondThreadId,
                });
              return yield* Effect.die("simulated second attached-thread cleanup read defect");
            });
      yield* Effect.gen(function* () {
        const f = yield* makeLifetimeFixture(`shared-cleanup-owner-${failure}`);
        const runtime = yield* f.open;
        const firstRequest = yield* f.makeRequest;
        yield* ingestRequestFixture(runtime, f.threadId, firstRequest.providerEvents);
        const now = yield* DateTime.now;
        yield* f.sink.write({
          events: [
            yield* makeThreadCreatedEvent({ idAllocator: f.ids, threadId: secondThreadId, now }),
          ],
        });
        const openSecond = f.manager.open({
          threadId: secondThreadId,
          providerSessionId: f.providerSessionId,
          modelSelection,
          runtimePolicy,
        });
        assert.strictEqual(yield* openSecond, runtime);
        const makeSecondRequest = Effect.gen(function* () {
          const now = yield* DateTime.now;
          return yield* makePendingRuntimeRequestEvents({
            idAllocator: f.ids,
            threadId: secondThreadId,
            providerSessionId: f.providerSessionId,
            now,
            providerThread: makeProviderThread({
              idAllocator: f.ids,
              threadId: secondThreadId,
              providerSessionId: f.providerSessionId,
              now,
            }),
          });
        });
        const oldSecondRequest = yield* makeSecondRequest;
        yield* ingestRequestFixture(runtime, secondThreadId, oldSecondRequest.providerEvents);
        const firstToken = McpProviderSession.readMcpProviderSession(
          f.threadId,
        )!.authorizationHeader.replace(/^Bearer\s+/, "");
        const secondToken = McpProviderSession.readMcpProviderSession(
          secondThreadId,
        )!.authorizationHeader.replace(/^Bearer\s+/, "");
        assert.isTrue(Exit.isFailure(yield* Effect.exit(f.manager.close(f.providerSessionId))));
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.isUndefined(yield* f.registry.resolve(firstToken));
        assert.isUndefined(yield* f.registry.resolve(secondToken));
        assert.isUndefined(McpProviderSession.readMcpProviderSession(f.threadId));
        assert.isUndefined(McpProviderSession.readMcpProviderSession(secondThreadId));
        const firstProjection = yield* f.projections.getThreadProjection(f.threadId);
        const secondProjection = yield* f.projections.getThreadProjection(secondThreadId);
        assert.equal(
          firstProjection.runtimeRequests.find((r) => r.id === firstRequest.requestId)?.status,
          "cancelled",
        );
        assert.equal(
          secondProjection.runtimeRequests.find((r) => r.id === oldSecondRequest.requestId)?.status,
          "pending",
        );
        const replacement = yield* openSecond;
        const currentSecondRequest = yield* makeSecondRequest;
        yield* ingestRequestFixture(
          replacement,
          secondThreadId,
          currentSecondRequest.providerEvents,
        );
        const current = McpProviderSession.readMcpProviderSession(secondThreadId)!;
        const currentToken = current.authorizationHeader.replace(/^Bearer\s+/, "");
        const afterSequence = yield* f.sink.latestSequence({ threadId: secondThreadId });
        const settled = yield* f.sink.stream({ threadId: secondThreadId, afterSequence }).pipe(
          Stream.filter(
            (stored) =>
              stored.event.type === "runtime-request.updated" &&
              stored.event.payload.id === oldSecondRequest.requestId &&
              stored.event.payload.status === "cancelled",
          ),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );
        yield* TestClock.adjust("1 second");
        yield* Fiber.join(settled);
        const after = yield* f.projections.getThreadProjection(secondThreadId);
        assert.equal(
          after.runtimeRequests.find((r) => r.id === oldSecondRequest.requestId)?.status,
          "cancelled",
        );
        assert.equal(
          after.nodes.find((n) => n.id === oldSecondRequest.nodeId)?.status,
          "cancelled",
        );
        assert.equal(
          after.turnItems.find(
            (i) => i.type === "approval_request" && i.requestId === oldSecondRequest.requestId,
          )?.status,
          "cancelled",
        );
        assert.equal(
          after.runtimeRequests.find((r) => r.id === currentSecondRequest.requestId)?.status,
          "pending",
        );
        assert.equal(
          after.runtimeRequests.find((r) => r.id === currentSecondRequest.requestId)
            ?.responseCapability.type,
          "live",
        );
        assert.equal(after.providerSessions.at(-1)?.status, "ready");
        assert.equal((yield* f.registry.resolve(currentToken))?.threadId, secondThreadId);
        assert.strictEqual(
          Option.getOrThrow(yield* f.manager.get(f.providerSessionId)),
          replacement,
        );
        assert.equal((yield* Ref.get(state)).closeCount, 1);
        assert.equal(yield* Ref.get(reads), 3);
      }).pipe(Effect.provide(makeTestLayer({ state, idleTimeoutMs: 60_000, beforeRead })));
    }),
);
