import { assert, it } from "@effect/vitest";
import {
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  PlanId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import type { ProviderRuntimeLifetime } from "./ProviderRuntimeLifetime.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import {
  makeProviderEventRoutingState,
  type ProviderEventRouteIdentity,
  routeProviderEvent,
  selectInheritedBackgroundTurnItems,
} from "./RunExecutionService.ts";

const TestDatabaseLayer = SqlitePersistenceMemory;
const TestStoresLayer = Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provide(TestDatabaseLayer),
);

const TestEventSinkLayer = EventSink.layer.pipe(
  Layer.provide(Layer.mergeAll(TestStoresLayer, TestDatabaseLayer)),
);

const TestLayer = Layer.mergeAll(
  TestStoresLayer,
  TestEventSinkLayer,
  IdAllocator.layer,
  ThreadCommandExecutor.layer,
  ProviderEventIngestor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        TestStoresLayer,
        TestEventSinkLayer,
        IdAllocator.layer,
        ThreadCommandExecutor.layer,
      ),
    ),
  ),
);
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const CODEX_DRIVER = ProviderDriverKind.make("codex");

function threadCreatedEvent(
  now: DateTime.Utc,
): Effect.Effect<
  OrchestrationV2DomainEvent,
  IdAllocator.IdAllocatorV2Error,
  IdAllocator.IdAllocatorV2
> {
  return Effect.gen(function* () {
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const projectId = yield* idAllocator.allocate.project({
      fixtureName: "provider-event-ingestor",
    });
    const threadId = yield* idAllocator.allocate.thread({
      fixtureName: "provider-event-ingestor",
      projectId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "native-thread",
    });
    const thread: OrchestrationV2AppThread = {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: "Provider event ingestor",
      providerInstanceId: modelSelection.instanceId,
      modelSelection: modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      branchPullRequest: null,
      activeOrderKey: null,
      activeProviderThreadId: providerThreadId,
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };

    return {
      id: yield* idAllocator.allocate.event({ threadId }),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    };
  });
}

// Each test fixture captures one real lifetime per resident test runtime.
const testIngestor = Effect.gen(function* () {
  const service = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const tokens = new Map<ProviderSessionId, ProviderRuntimeLifetime>();
  type Input = Parameters<typeof service.ingestNormalized>[0];
  const ingestNormalized = (input: Omit<Input, "runtimeLifetime">) =>
    Effect.gen(function* () {
      let token = tokens.get(input.providerSessionId);
      if (token === undefined) {
        token = yield* service.createLifetime(input.providerSessionId);
        service.activateLifetime(token);
        tokens.set(input.providerSessionId, token);
      }
      return yield* service.ingestNormalized({ ...input, runtimeLifetime: token });
    });
  return { ...service, ingestNormalized };
});

const layer = it.layer(TestLayer);

it.effect("records accepted billed turn usage once without billing the context window", () => {
  const recorded: Array<Readonly<Record<string, unknown>>> = [];
  const analytics = Layer.succeed(ProviderEventIngestor.ProviderTurnAnalytics, {
    record: (properties: Readonly<Record<string, unknown>>) =>
      Effect.sync(() => {
        recorded.push(properties);
      }),
  });
  return Effect.gen(function* () {
    const now = yield* DateTime.now;
    const eventSink = yield* EventSink.EventSinkV2;
    const ingestor = yield* testIngestor;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const threadEvent = yield* threadCreatedEvent(now);
    yield* eventSink.write({ events: [threadEvent] });
    const providerSessionId = yield* idAllocator.allocate.providerSession({
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
    });
    const providerThreadId = idAllocator.derive.providerThread({
      driver: CODEX_DRIVER,
      nativeThreadId: "billing-thread",
    });
    const providerTurn = {
      id: idAllocator.derive.providerTurn({ driver: CODEX_DRIVER, nativeTurnId: "billing-turn" }),
      providerThreadId,
      nodeId: NodeId.make("node:billing-turn"),
      runAttemptId: null,
      nativeTurnRef: null,
      ordinal: 1,
      status: "completed" as const,
      startedAt: now,
      completedAt: DateTime.makeUnsafe(DateTime.toEpochMillis(now) + 120),
      tokenUsage: {
        inputTokens: 4000,
        cachedInputTokens: 3000,
        outputTokens: 100,
        usedTokens: 4100,
        updatedAt: DateTime.formatIso(now),
      },
      turnTokenUsage: {
        usageStatus: "complete" as const,
        usageScope: "main_agent" as const,
        hasSubagents: false,
        inputTokens: 40,
        cachedInputTokens: 30,
        outputTokens: 10,
      },
    };
    const input = {
      providerSessionId,
      providerInstanceId: modelSelection.instanceId,
      threadId: threadEvent.threadId,
      analyticsContext: {
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
      },
      event: { type: "provider_turn.updated" as const, driver: CODEX_DRIVER, providerTurn },
    };
    yield* ingestor.ingestNormalized(input);
    yield* ingestor.ingestNormalized(input);
    const ignored = yield* ingestor.ingestNormalized({
      ...input,
      event: {
        ...input.event,
        providerTurn: {
          ...providerTurn,
          id: idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: "stale-billing-turn",
          }),
        },
      },
      writeIfRunCurrent: {
        runId: RunId.make("missing-run"),
        activeAttemptId: RunAttemptId.make("stale-attempt"),
        expectedStatus: "running",
      },
    });
    assert.isEmpty(ignored);
    assert.lengthOf(recorded, 1);
    assert.deepEqual(recorded[0], {
      provider: CODEX_DRIVER,
      terminalStatus: "completed",
      usageStatus: "complete",
      usageScope: "main_agent",
      hasSubagents: false,
      inputTokens: 40,
      cachedInputTokens: 30,
      outputTokens: 10,
      model: modelSelection.model,
      mixedModels: false,
      runtimeMode: "full-access",
      interactionMode: "default",
      durationMs: 120,
    });
  }).pipe(Effect.provide(TestLayer.pipe(Layer.provide(analytics))));
});

layer("ProviderEventIngestorV2", (it) => {
  it.effect("normalizes provider events through the real event log and projection store", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThread: OrchestrationV2ProviderThread = {
        id: idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: "native-thread",
        }),
        driver: CODEX_DRIVER,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId,
        appThreadId: threadEvent.threadId,
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
        createdAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      const storedEvents = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "provider_thread.updated",
          driver: CODEX_DRIVER,
          providerThread,
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const storedDomainEvents = yield* eventStore.read({}).pipe(Stream.runCollect);
      const afterFirstEvent = yield* eventStore
        .read({ afterSequence: 1, threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const latestThreadSequence = yield* eventStore.latestSequence({
        threadId: threadEvent.threadId,
      });

      assert.equal(storedEvents.length, 1);
      assert.equal(storedEvents[0]?.event.type, "provider-thread.updated");
      assert.deepEqual(
        projection.providerThreads.map((thread) => thread.id),
        [providerThread.id],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.event.type),
        ["thread.created", "provider-thread.updated"],
      );
      assert.deepEqual(
        Array.from(storedDomainEvents).map((stored) => stored.sequence),
        [1, 2],
      );
      assert.deepEqual(
        Array.from(afterFirstEvent).map((stored) => stored.event.type),
        ["provider-thread.updated"],
      );
      assert.equal(latestThreadSequence, 2);
    }),
  );

  it.effect("carries plan-step durations through consecutive and restarted ingestion", () =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.parse("2026-09-07T00:00:00.000Z"));
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const planId = PlanId.make("plan:provider-event-duration");
      const nodeId = NodeId.make("node:provider-event-duration");
      type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;
      const plan = (steps: TodoListPlan["steps"]): TodoListPlan => ({
        id: planId,
        threadId: threadEvent.threadId,
        runId: null,
        nodeId,
        kind: "todo_list",
        status: "active",
        steps,
      });
      const ingest = (service: typeof ingestor, steps: TodoListPlan["steps"]) =>
        service.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: threadEvent.threadId,
          event: { type: "plan.updated", driver: CODEX_DRIVER, plan: plan(steps) },
        });

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("3 seconds");
      yield* ingest(ingestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "pending" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);

      const restartedIngestor = yield* testIngestor.pipe(
        Effect.provide(
          Layer.fresh(ProviderEventIngestor.layer).pipe(
            Layer.provide(
              Layer.succeed(ProjectionStore.ProjectionStoreV2, {
                ...projectionStore,
                getThreadProjection: () => Effect.die("Plan timing must not load thread history"),
              }),
            ),
          ),
        ),
      );
      yield* TestClock.adjust("4 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "pending" },
      ]);
      yield* TestClock.adjust("5 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Verify", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Report", status: "completed" },
      ]);

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        persisted?.steps.map(({ id, text, status, durationMs }) => ({
          id,
          text,
          status,
          durationMs,
        })),
        [
          { id: "duplicate-a", text: "Verify", status: "completed", durationMs: 3_000 },
          { id: "duplicate-b", text: "Verify", status: "completed", durationMs: 4_000 },
          { id: "fallback", text: "Report", status: "completed", durationMs: 5_000 },
        ],
      );

      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "running" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      yield* TestClock.adjust("2 seconds");
      yield* ingest(restartedIngestor, [
        { id: "duplicate-a", text: "Inserted task", status: "completed" },
        { id: "duplicate-b", text: "Verify", status: "completed" },
        { id: "fallback", text: "Different completed task", status: "completed" },
      ]);
      const updated = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const changedPlan = updated.plans.find(
        (candidate): candidate is TodoListPlan =>
          candidate.kind === "todo_list" && candidate.id === planId,
      );
      assert.deepEqual(
        changedPlan?.steps.map((step) => step.durationMs),
        [2_000, 4_000, undefined],
      );
    }),
  );

  it.effect(
    "treats successful provider terminal markers as non-persisted orchestration control signals",
    () =>
      Effect.gen(function* () {
        const ingestor = yield* testIngestor;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const projectId = yield* idAllocator.allocate.project({
          fixtureName: "provider-event-terminal",
        });
        const threadId = yield* idAllocator.allocate.thread({
          fixtureName: "provider-event-terminal",
          projectId,
        });
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const normalized = yield* ingestor.normalize({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event: {
            type: "turn.terminal",
            driver: CODEX_DRIVER,
            providerThreadId: idAllocator.derive.providerThread({
              driver: CODEX_DRIVER,
              nativeThreadId: "native-thread",
            }),
            providerTurnId: idAllocator.derive.providerTurn({
              driver: CODEX_DRIVER,
              nativeTurnId: "native-turn",
            }),
            runOrdinal: 1,
            status: "completed",
            failure: null,
            threadDisposition: "reusable",
          },
        });

        assert.deepEqual(normalized, []);
      }),
  );

  it.effect("persists an interrupted run's inherited terminal through the live run router", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-inherited:prior");
      const currentRunId = RunId.make("run:provider-event-inherited:current");
      const itemId = TurnItemId.make("turn-item:provider-event-inherited");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-inherited",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-inherited",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-inherited"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Inherited background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalItem = {
        ...runningItem,
        status: "completed" as const,
        completedAt: now,
        updatedAt: now,
      };

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const identity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-inherited:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "interrupted",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routeState = makeProviderEventRoutingState({
        identity,
        inheritedBackgroundTurnItems,
        providerTurnId: null,
      });
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: terminalItem,
      } as const;
      const [accepted] = routeProviderEvent(terminalEvent, identity, routeState);
      assert.isTrue(accepted);

      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: currentRunId,
        event: terminalEvent,
      });
      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const persisted = projection.turnItems.find((item) => item.id === itemId);

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(persisted?.runId, priorRunId);
      assert.equal(persisted?.threadId, threadEvent.threadId);
      assert.equal(persisted?.status, "completed");
    }),
  );

  it.effect("persists a completed run's late background terminal exactly once", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const eventStore = yield* EventStore.EventStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const priorRunId = RunId.make("run:provider-event-completed:prior");
      const currentRunId = RunId.make("run:provider-event-completed:current");
      const itemId = TurnItemId.make("turn-item:provider-event-completed");
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-completed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-completed",
      });
      const runningItem = {
        id: itemId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        nodeId: NodeId.make("node:provider-event-completed"),
        providerThreadId,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 101,
        status: "running",
        title: "Completed run background command",
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "command_execution",
        input: "sleep 60",
      } satisfies OrchestrationV2TurnItem;
      const terminalEvent = {
        type: "turn_item.updated",
        driver: CODEX_DRIVER,
        turnItem: {
          ...runningItem,
          status: "completed" as const,
          completedAt: now,
          updatedAt: now,
        },
      } as const;

      yield* eventSink.write({ events: [threadEvent] });
      yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        runId: priorRunId,
        event: { type: "turn_item.updated", driver: CODEX_DRIVER, turnItem: runningItem },
      });

      const priorIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: priorRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:prior"),
        providerThreadId,
      };
      const currentIdentity: ProviderEventRouteIdentity = {
        threadId: threadEvent.threadId,
        runId: currentRunId,
        attemptId: RunAttemptId.make("attempt:provider-event-completed:current"),
        providerThreadId,
      };
      const inheritedBackgroundTurnItems = selectInheritedBackgroundTurnItems({
        threadId: threadEvent.threadId,
        currentProviderThreadId: providerThreadId,
        currentRunOrdinal: 2,
        runs: [
          {
            id: priorRunId,
            threadId: threadEvent.threadId,
            ordinal: 1,
            status: "completed",
          } as OrchestrationV2Run,
          {
            id: currentRunId,
            threadId: threadEvent.threadId,
            ordinal: 2,
            status: "running",
          } as OrchestrationV2Run,
        ],
        turnItems: [runningItem],
      });
      const routers = [
        {
          identity: priorIdentity,
          state: makeProviderEventRoutingState({
            identity: priorIdentity,
            providerTurnId: providerTurnId,
          }),
        },
        {
          identity: currentIdentity,
          state: makeProviderEventRoutingState({
            identity: currentIdentity,
            inheritedBackgroundTurnItems,
            providerTurnId: null,
          }),
        },
      ];
      const acceptedRouters = routers.filter(
        ({ identity, state }) => routeProviderEvent(terminalEvent, identity, state)[0],
      );

      yield* Effect.forEach(
        acceptedRouters,
        ({ identity }) =>
          ingestor.ingestNormalized({
            providerSessionId,
            providerInstanceId: modelSelection.instanceId,
            threadId: threadEvent.threadId,
            runId: identity.runId,
            event: terminalEvent,
          }),
        { concurrency: 1 },
      );

      const storedEvents = yield* eventStore
        .read({ threadId: threadEvent.threadId })
        .pipe(Stream.runCollect);
      const storedTerminals = Array.from(storedEvents).filter(
        (stored) =>
          stored.event.type === "turn-item.updated" &&
          stored.event.payload.id === itemId &&
          stored.event.payload.status === "completed",
      );

      assert.equal(storedTerminals.length, 1);
      assert.equal(acceptedRouters.length, 1);
      assert.equal(acceptedRouters[0]?.identity.runId, priorRunId);
    }),
  );

  it.effect.each(["completed", "interrupted", "failed", "cancelled", "control"] as const)(
    "dismisses only native questions when a provider turn ends with %s",
    (terminal) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const eventSink = yield* EventSink.EventSinkV2;
        const eventStore = yield* EventStore.EventStoreV2;
        const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
        const ingestor = yield* testIngestor;
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const threadEvent = yield* threadCreatedEvent(now);
        const threadId = threadEvent.threadId;
        const providerSessionId = yield* idAllocator.allocate.providerSession({
          providerInstanceId: modelSelection.instanceId,
          threadId,
        });
        const providerThreadId = idAllocator.derive.providerThread({
          driver: CODEX_DRIVER,
          nativeThreadId: `${threadId}:questions`,
        });
        const providerTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:questions`,
        });
        const otherTurnId = idAllocator.derive.providerTurn({
          driver: CODEX_DRIVER,
          nativeTurnId: `${threadId}:other-turn`,
        });
        const specs = [
          { key: "native", responseCapability: { type: "live", providerSessionId } },
          {
            key: "unavailable",
            responseCapability: { type: "not_resumable", reason: "Turn ended" },
          },
          { key: "message", responseCapability: { type: "message" } },
          { key: "answered", responseCapability: { type: "live", providerSessionId } },
          { key: "other-turn", responseCapability: { type: "live", providerSessionId } },
          { key: "approval", responseCapability: { type: "live", providerSessionId } },
        ] as const;
        const fixtures = specs.map((spec, ordinal) => {
          const nodeId = NodeId.make(`${threadId}:${spec.key}`);
          const resolved = spec.key === "answered";
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:${spec.key}`),
            nodeId,
            providerTurnId: spec.key === "other-turn" ? otherTurnId : providerTurnId,
            nativeRequestRef: null,
            kind: spec.key === "approval" ? "command" : "user_input",
            status: resolved ? "resolved" : "pending",
            responseCapability: spec.responseCapability,
            createdAt: now,
            resolvedAt: resolved ? now : null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: spec.key === "approval" ? "approval_request" : "user_input_request",
            status: resolved ? "completed" : "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: resolved ? now : null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:${spec.key}`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId: request.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal,
            status: resolved ? "completed" : "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: resolved ? now : null,
            updatedAt: now,
            ...(spec.key === "approval"
              ? { type: "approval_request", requestId: request.id, requestKind: "command" }
              : {
                  type: "user_input_request",
                  requestId: request.id,
                  questions: [],
                  ...(spec.key === "message" ? { responseMode: "message" as const } : {}),
                }),
          };
          return { key: spec.key, request, node, item };
        });
        yield* eventSink.write({ events: [threadEvent] });
        for (const fixture of fixtures) {
          for (const event of [
            {
              type: "runtime_request.updated" as const,
              driver: CODEX_DRIVER,
              runtimeRequest: fixture.request,
            },
            { type: "node.updated" as const, driver: CODEX_DRIVER, node: fixture.node },
            { type: "turn_item.updated" as const, driver: CODEX_DRIVER, turnItem: fixture.item },
          ]) {
            yield* ingestor.ingestNormalized({
              providerSessionId,
              providerInstanceId: modelSelection.instanceId,
              threadId,
              event,
            });
          }
        }
        const input = {
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId,
          event:
            terminal === "control"
              ? {
                  type: "turn.terminal" as const,
                  driver: CODEX_DRIVER,
                  providerThreadId,
                  providerTurnId,
                  runOrdinal: 1,
                  status: "completed" as const,
                  failure: null,
                  threadDisposition: "reusable" as const,
                }
              : {
                  type: "provider_turn.updated" as const,
                  driver: CODEX_DRIVER,
                  providerTurn: {
                    id: providerTurnId,
                    providerThreadId,
                    nodeId: NodeId.make(`${threadId}:root`),
                    runAttemptId: null,
                    nativeTurnRef: null,
                    ordinal: 1,
                    status: terminal,
                    startedAt: now,
                    completedAt: now,
                  },
                },
        };
        const stored = yield* ingestor.ingestNormalized(input);
        const projection = yield* projectionStore.getThreadProjection(threadId);
        for (const fixture of fixtures) {
          const closed = fixture.key === "native" || fixture.key === "unavailable";
          const request = projection.runtimeRequests.find(
            (item) => item.id === fixture.request.id,
          )!;
          const node = projection.nodes.find((item) => item.id === fixture.node.id)!;
          const item = projection.turnItems.find((item) => item.id === fixture.item.id)!;
          if (closed) {
            assert.equal(request.status, "cancelled");
            assert.isNotNull(request.resolvedAt);
            assert.equal(node.status, "cancelled");
            assert.isNotNull(node.completedAt);
            assert.equal(item.status, "cancelled");
            assert.isNotNull(item.completedAt);
          } else {
            assert.equal(request.status, fixture.request.status);
            assert.equal(node.status, fixture.node.status);
            assert.equal(item.status, fixture.item.status);
          }
        }
        assert.equal(
          stored.filter((entry) => entry.event.type === "runtime-request.updated").length,
          1,
        );
        assert.isEmpty(
          (yield* projectionStore.getPendingNativeUserInputs(threadId, providerTurnId))
            .runtimeRequests,
        );
        const replayed = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
        assert.equal(
          replayed.filter(
            (entry) =>
              entry.event.type === "runtime-request.updated" &&
              entry.event.payload.status === "cancelled",
          ).length,
          2,
        );
        const repeated = yield* ingestor.ingestNormalized(input);
        assert.isFalse(repeated.some((entry) => entry.event.type === "runtime-request.updated"));
      }),
  );

  it.effect(
    "preserves an answer committed after terminal normalization reads a pending question",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const eventStore = yield* EventStore.EventStoreV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const idAllocator = yield* IdAllocator.IdAllocatorV2;
          const now = yield* DateTime.now;
          const threadEvent = yield* threadCreatedEvent(now);
          const threadId = threadEvent.threadId;
          const providerSessionId = yield* idAllocator.allocate.providerSession({
            providerInstanceId: modelSelection.instanceId,
            threadId,
          });
          const providerThreadId = idAllocator.derive.providerThread({
            driver: CODEX_DRIVER,
            nativeThreadId: `${threadId}:race`,
          });
          const providerTurnId = idAllocator.derive.providerTurn({
            driver: CODEX_DRIVER,
            nativeTurnId: `${threadId}:race`,
          });
          const nodeId = NodeId.make(`${threadId}:question`);
          const request: OrchestrationV2RuntimeRequest = {
            id: RuntimeRequestId.make(`${threadId}:question`),
            nodeId,
            providerTurnId,
            nativeRequestRef: null,
            kind: "user_input",
            status: "pending",
            responseCapability: { type: "live", providerSessionId },
            createdAt: now,
            resolvedAt: null,
          };
          const node: OrchestrationV2ExecutionNode = {
            id: nodeId,
            threadId,
            runId: null,
            parentNodeId: null,
            rootNodeId: nodeId,
            kind: "user_input_request",
            status: "waiting",
            countsForRun: false,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            runtimeRequestId: request.id,
            checkpointScopeId: null,
            startedAt: now,
            completedAt: null,
          };
          const item: OrchestrationV2TurnItem = {
            id: TurnItemId.make(`${threadId}:question`),
            threadId,
            runId: null,
            nodeId,
            providerThreadId,
            providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "waiting",
            title: "Which option?",
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId: request.id,
            questions: [],
          };
          yield* eventSink.write({ events: [threadEvent] });
          const normalized = yield* Deferred.make<void>();
          const releaseTerminalWrite = yield* Deferred.make<void>();
          const gatedSink = EventSink.EventSinkV2.of({
            ...eventSink,
            writeIfRuntimeRequestCurrent: (input) =>
              !input.events.some(
                (event) =>
                  event.type === "runtime-request.updated" && event.payload.status === "cancelled",
              )
                ? eventSink.writeIfRuntimeRequestCurrent(input)
                : Deferred.succeed(normalized, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseTerminalWrite)),
                    Effect.andThen(eventSink.writeIfRuntimeRequestCurrent(input)),
                  ),
          });
          const ingestor = yield* testIngestor.pipe(
            Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
            Effect.provideService(EventSink.EventSinkV2, gatedSink),
          );
          for (const event of [
            {
              type: "runtime_request.updated" as const,
              driver: CODEX_DRIVER,
              runtimeRequest: request,
            },
            { type: "node.updated" as const, driver: CODEX_DRIVER, node },
            { type: "turn_item.updated" as const, driver: CODEX_DRIVER, turnItem: item },
          ])
            yield* ingestor.ingestNormalized({
              providerSessionId,
              providerInstanceId: modelSelection.instanceId,
              threadId,
              event,
            });
          const terminal = yield* ingestor
            .ingestNormalized({
              providerSessionId,
              providerInstanceId: modelSelection.instanceId,
              threadId,
              event: {
                type: "provider_turn.updated",
                driver: CODEX_DRIVER,
                providerTurn: {
                  id: providerTurnId,
                  providerThreadId,
                  nodeId,
                  runAttemptId: null,
                  nativeTurnRef: null,
                  ordinal: 1,
                  status: "completed",
                  startedAt: now,
                  completedAt: now,
                },
              },
            })
            .pipe(Effect.forkScoped);
          yield* Deferred.await(normalized);
          const answers = { decision: "Use the existing workspace" };
          const responseEvents: Array<OrchestrationV2DomainEvent> = [];
          for (const payload of [
            {
              type: "runtime-request.updated" as const,
              payload: { ...request, status: "resolved" as const, answers, resolvedAt: now },
            },
            {
              type: "node.updated" as const,
              payload: { ...node, status: "completed" as const, completedAt: now },
            },
            {
              type: "turn-item.updated" as const,
              payload: { ...item, status: "completed" as const, completedAt: now },
            },
          ])
            responseEvents.push({
              id: yield* idAllocator.allocate.event({ threadId }),
              threadId,
              occurredAt: now,
              ...payload,
            });
          yield* eventSink.write({ events: responseEvents });
          yield* Deferred.succeed(releaseTerminalWrite, undefined);
          const committedTerminal = yield* Fiber.join(terminal);
          assert.deepEqual(
            committedTerminal.map((stored) => stored.event.type),
            ["provider-turn.updated"],
          );
          const projection = yield* projections.getThreadProjection(threadId);
          const answered = projection.runtimeRequests.find((entry) => entry.id === request.id)!;
          assert.equal(answered.status, "resolved");
          assert.deepEqual(answered.answers, answers);
          assert.equal(projection.nodes.find((entry) => entry.id === nodeId)!.status, "completed");
          assert.equal(
            projection.turnItems.find((entry) => entry.id === item.id)!.status,
            "completed",
          );
          const history = yield* eventStore.read({ threadId }).pipe(Stream.runCollect);
          assert.isFalse(
            history.some(
              (stored) =>
                stored.event.type === "runtime-request.updated" &&
                stored.event.payload.status === "cancelled",
            ),
          );
        }),
      ),
  );

  it.effect("persists a failed provider terminal as one expected error item", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const retryStartedAt = DateTime.makeUnsafe(DateTime.toEpochMillis(now) - 5_000);
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const threadEvent = yield* threadCreatedEvent(now);
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
      });
      const providerThreadId = idAllocator.derive.providerThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-thread-failed",
      });
      const providerTurnId = idAllocator.derive.providerTurn({
        driver: CODEX_DRIVER,
        nativeTurnId: "native-turn-failed",
      });

      yield* eventSink.write({ events: [threadEvent] });
      const stored = yield* ingestor.ingestNormalized({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: threadEvent.threadId,
        event: {
          type: "turn.terminal",
          driver: CODEX_DRIVER,
          providerThreadId,
          providerTurnId,
          runOrdinal: 1,
          failureItemOrdinal: 102,
          status: "failed",
          failure: makeProviderFailure({
            message: "Invalid reasoning effort.",
            code: "invalid_request",
            class: "validation_error",
          }),
          retry: {
            attempt: 3,
            maxAttempts: 3,
            retryDelayMs: 2_000,
          },
          retryStartedAt,
          threadDisposition: "reusable",
        },
      });

      const projection = yield* projectionStore.getThreadProjection(threadEvent.threadId);
      const errorItems = projection.visibleTurnItems.filter(
        (candidate) => candidate.item.type === "error",
      );

      assert.equal(stored.length, 1);
      assert.equal(stored[0]?.event.type, "turn-item.updated");
      assert.equal(errorItems.length, 1);
      const errorItem = errorItems[0]?.item;
      assert.equal(errorItem?.type, "error");
      if (errorItem?.type !== "error") return;
      assert.equal(errorItem.failure.message, "Invalid reasoning effort.");
      assert.equal(errorItem.failure.code, "invalid_request");
      assert.deepEqual(errorItem.retry, {
        attempt: 3,
        maxAttempts: 3,
        retryDelayMs: 2_000,
      });
      const errorStartedAt = errorItem.startedAt;
      assert.ok(errorStartedAt);
      assert.equal(DateTime.toEpochMillis(errorStartedAt), DateTime.toEpochMillis(retryStartedAt));
      assert.equal(errorItem.providerThreadId, providerThreadId);
      assert.equal(errorItem.providerTurnId, providerTurnId);
    }),
  );

  it.effect("routes provider-owned child artifacts to their child app thread", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-subagent-thread",
      });
      const childRootNodeId = NodeId.make("node:subagent-root");
      const childThread: OrchestrationV2AppThread = {
        ...rootEvent.payload,
        id: childThreadId,
        title: "inspect package",
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: rootEvent.threadId,
          relationshipToParent: "subagent",
          rootThreadId: rootEvent.threadId,
        },
        forkedFrom: {
          type: "node",
          nodeId: NodeId.make("node:parent-subagent"),
        },
      };
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });

      const threadEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "app_thread.created",
          driver: CODEX_DRIVER,
          appThread: childThread,
        },
      });
      const messageEvents = yield* ingestor.normalize({
        providerSessionId,
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
        event: {
          type: "message.updated",
          driver: CODEX_DRIVER,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make("message:subagent-response"),
            threadId: childThreadId,
            runId: null,
            nodeId: childRootNodeId,
            role: "assistant",
            text: "Subagent result",
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
          },
        },
      });

      assert.equal(threadEvents[0]?.type, "thread.created");
      assert.equal(threadEvents[0]?.threadId, childThreadId);
      assert.equal(messageEvents[0]?.type, "message.updated");
      assert.equal(messageEvents[0]?.threadId, childThreadId);
    }),
  );

  it.effect("moves a native subagent's thread to the model its provider reports later", () =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const eventSink = yield* EventSink.EventSinkV2;
      const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
      const ingestor = yield* testIngestor;
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      const rootEvent = yield* threadCreatedEvent(now);
      if (rootEvent.type !== "thread.created") {
        throw new Error("Expected a thread.created fixture event");
      }
      const childThreadId = idAllocator.derive.threadFromProviderThread({
        driver: CODEX_DRIVER,
        nativeThreadId: "native-late-model-subagent",
      });
      const providerSessionId = yield* idAllocator.allocate.providerSession({
        providerInstanceId: modelSelection.instanceId,
        threadId: rootEvent.threadId,
      });
      const ingest = (event: ProviderEventIngestor.ProviderEventIngestInput["event"]) =>
        ingestor.ingestNormalized({
          providerSessionId,
          providerInstanceId: modelSelection.instanceId,
          threadId: rootEvent.threadId,
          event,
        });
      yield* eventSink.write({ events: [rootEvent] });
      // The subagent's thread starts on the parent's model and options.
      yield* ingest({
        type: "app_thread.created",
        driver: CODEX_DRIVER,
        appThread: {
          ...rootEvent.payload,
          id: childThreadId,
          title: "review design",
          modelSelection: {
            ...modelSelection,
            options: [{ id: "reasoningEffort", value: "xhigh" }],
          },
          activeProviderThreadId: null,
          lineage: {
            parentThreadId: rootEvent.threadId,
            relationshipToParent: "subagent",
            rootThreadId: rootEvent.threadId,
          },
        },
      });
      const subagentUpdated = {
        type: "subagent.updated",
        driver: CODEX_DRIVER,
        subagent: {
          id: NodeId.make("node:late-model-subagent"),
          threadId: rootEvent.threadId,
          runId: null,
          parentNodeId: NodeId.make("node:root"),
          origin: "provider_native",
          createdBy: "agent",
          driver: CODEX_DRIVER,
          providerInstanceId: modelSelection.instanceId,
          providerThreadId: null,
          childThreadId,
          nativeTaskRef: null,
          prompt: "Review the design",
          title: "review design",
          model: "gpt-6.1-sol",
          status: "running",
          result: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        },
      } satisfies ProviderEventIngestor.ProviderEventIngestInput["event"];

      const first = yield* ingest(subagentUpdated);
      const repeated = yield* ingest(subagentUpdated);
      const childThread = yield* projectionStore.getThread(childThreadId);

      assert.deepEqual(
        first.map((stored) => [stored.event.type, stored.event.threadId]),
        [
          ["subagent.updated", rootEvent.threadId],
          ["thread.model-selection-updated", childThreadId],
        ],
      );
      assert.deepEqual(
        repeated.map((stored) => stored.event.type),
        ["subagent.updated"],
      );
      assert.deepEqual(childThread.modelSelection, {
        instanceId: modelSelection.instanceId,
        model: "gpt-6.1-sol",
      });
    }),
  );
});

const requestFixture = Effect.gen(function* () {
  const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const threadEvent = yield* threadCreatedEvent(now);
  const threadId = threadEvent.threadId;
  yield* sink.write({ events: [threadEvent] });
  const providerSessionId = ProviderSessionId.make(`${threadId}:session`);
  const token = yield* ingestor.createLifetime(providerSessionId);
  ingestor.activateLifetime(token);
  const request: OrchestrationV2RuntimeRequest = {
    id: RuntimeRequestId.make(`${threadId}:request`),
    nodeId: NodeId.make(`${threadId}:question`),
    providerTurnId: null,
    nativeRequestRef: {
      driver: CODEX_DRIVER,
      nativeId: "reusable-native-request",
      strength: "strong",
    },
    kind: "user_input",
    status: "pending",
    responseCapability: { type: "live", providerSessionId },
    createdAt: now,
    resolvedAt: null,
  };
  const node: OrchestrationV2ExecutionNode = {
    id: request.nodeId,
    threadId,
    runId: null,
    parentNodeId: null,
    rootNodeId: request.nodeId,
    kind: "user_input_request",
    status: "waiting",
    countsForRun: false,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: request.id,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  const item: OrchestrationV2TurnItem = {
    id: TurnItemId.make(`${threadId}:question`),
    threadId,
    runId: null,
    nodeId: node.id,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "waiting",
    title: "Choose a workspace",
    startedAt: now,
    completedAt: null,
    updatedAt: now,
    type: "user_input_request",
    requestId: request.id,
    questions: [
      {
        id: "workspace",
        header: "Workspace",
        question: "Which workspace should I use?",
        options: [
          { label: "Existing workspace", description: "Keep the current checkout" },
          { label: "New worktree", description: "Isolate the changes" },
        ],
        allowCustomAnswer: true,
      },
    ],
  };
  const input = {
    providerSessionId,
    runtimeLifetime: token,
    providerInstanceId: modelSelection.instanceId,
    threadId,
  };
  const events = {
    request: {
      type: "runtime_request.updated" as const,
      driver: CODEX_DRIVER,
      runtimeRequest: request,
    },
    node: { type: "node.updated" as const, driver: CODEX_DRIVER, node },
    item: { type: "turn_item.updated" as const, driver: CODEX_DRIVER, turnItem: item },
  };
  const ingest = (event: ProviderEventIngestor.ProviderEventIngestInput["event"]) =>
    ingestor.ingestNormalized({ ...input, event });
  return {
    ingestor,
    sink,
    now,
    threadId,
    token,
    providerSessionId,
    request,
    node,
    item,
    input,
    events,
    ingest,
  };
});

const siblingOrders = [
  ["node", "request", "item"],
  ["item", "request", "node"],
  ["node", "item", "request"],
  ["item", "node", "request"],
  ["request", "node", "item"],
  ["request", "item", "node"],
] as const;

layer("Provider runtime lifetime admission", (it) => {
  it.effect.each(siblingOrders.map((order) => ({ order, name: order.join("/") })))(
    "preserves a real message-mode question across retirement in $name order",
    ({ order }) =>
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        for (let retireAfter = 0; retireAfter <= 3; retireAfter += 1) {
          const f = yield* requestFixture;
          const events = {
            ...f.events,
            request: {
              ...f.events.request,
              runtimeRequest: { ...f.request, responseCapability: { type: "message" as const } },
            },
            item: { ...f.events.item, turnItem: { ...f.item, responseMode: "message" as const } },
          };
          for (const [index, key] of order.entries()) {
            if (index === retireAfter) f.ingestor.retireLifetime(f.token);
            yield* f.ingest(events[key]);
            if (index < order.indexOf("request")) {
              const partial = yield* projections.getThreadProjection(f.threadId);
              assert.isEmpty(partial.nodes);
              assert.isEmpty(partial.turnItems);
            }
          }
          f.ingestor.retireLifetime(f.token);
          const projection = yield* projections.getThreadProjection(f.threadId);
          assert.equal(projection.runtimeRequests[0]?.status, "pending");
          assert.equal(projection.runtimeRequests[0]?.responseCapability.type, "message");
          assert.equal(projection.nodes[0]?.status, "waiting");
          assert.deepEqual(projection.turnItems[0], { ...f.item, responseMode: "message" });
          assert.isEmpty(f.ingestor.getOwnedRequestGroups(f.token));
        }
      }),
  );

  it.effect.each(siblingOrders.map((order) => ({ order, name: order.join("/") })))(
    "drops wholly late live groups in $name order",
    ({ order }) =>
      Effect.gen(function* () {
        const f = yield* requestFixture;
        f.ingestor.retireLifetime(f.token);
        for (const key of order) assert.isEmpty(yield* f.ingest(f.events[key]));
        const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
          f.threadId,
        );
        assert.isEmpty(projection.runtimeRequests);
        assert.isEmpty(projection.nodes);
        assert.isEmpty(projection.turnItems);
      }),
  );

  it.effect(
    "commits buffered siblings with their request and preserves resolved answers on every later sibling",
    () =>
      Effect.gen(function* () {
        const f = yield* requestFixture;
        assert.isEmpty(yield* f.ingest(f.events.node));
        assert.isEmpty(yield* f.ingest(f.events.item));
        assert.equal((yield* f.ingest(f.events.request)).length, 3);
        const answers = { workspace: "Existing workspace" };
        const resolved = yield* f.ingestor.normalize({
          ...f.input,
          event: {
            ...f.events.request,
            runtimeRequest: { ...f.request, status: "resolved", answers, resolvedAt: f.now },
          },
        });
        yield* f.sink.write({ events: resolved });
        for (const key of ["request", "node", "item"] as const) yield* f.ingest(f.events[key]);
        const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
          f.threadId,
        );
        assert.equal(projection.runtimeRequests[0]?.status, "resolved");
        assert.deepEqual(projection.runtimeRequests[0]?.answers, answers);
        assert.equal(projection.nodes[0]?.status, "completed");
        assert.equal(projection.turnItems[0]?.status, "completed");
      }),
  );

  it.effect("settles explicit not-resumable groups and their already committed siblings", () =>
    Effect.gen(function* () {
      for (const initialTerminal of [true, false]) {
        const f = yield* requestFixture;
        if (!initialTerminal)
          for (const key of ["request", "node", "item"] as const) yield* f.ingest(f.events[key]);
        else {
          yield* f.ingest(f.events.node);
          yield* f.ingest(f.events.item);
        }
        yield* f.ingest({
          ...f.events.request,
          runtimeRequest: {
            ...f.request,
            responseCapability: { type: "not_resumable", reason: "Provider callback ended" },
          },
        });
        for (const key of ["node", "item"] as const) yield* f.ingest(f.events[key]);
        const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
          f.threadId,
        );
        assert.equal(projection.runtimeRequests[0]?.status, "cancelled");
        assert.equal(projection.runtimeRequests[0]?.responseCapability.type, "not_resumable");
        assert.equal(projection.nodes[0]?.status, "cancelled");
        assert.equal(projection.turnItems[0]?.status, "cancelled");
      }
    }),
  );

  it.effect(
    "rejects global request IDs, node reservations, linked item collisions, and historical identities",
    () =>
      Effect.gen(function* () {
        const first = yield* requestFixture;
        const second = yield* requestFixture;
        const collision = {
          ...second.events.request,
          runtimeRequest: { ...second.request, id: first.request.id },
        };
        const results = yield* Effect.all(
          [first.ingest(first.events.request), second.ingest(collision)],
          { concurrency: "unbounded" },
        );
        assert.equal(results.filter((events) => events.length > 0).length, 1);
        const winner = results[0]!.length > 0 ? first : second;
        const loser = winner === first ? second : first;
        const occupiedRequest =
          yield* (yield* ProjectionStore.ProjectionStoreV2).getRuntimeRequestById(
            first.threadId,
            first.request.id,
          );
        assert.isDefined(occupiedRequest);
        const nodeCollision = {
          ...loser.events.request,
          runtimeRequest: {
            ...loser.request,
            id: RuntimeRequestId.make(`${loser.threadId}:fresh-node-collision`),
            nodeId: occupiedRequest!.request.nodeId,
          },
        };
        assert.isEmpty(yield* loser.ingest(nodeCollision));

        const source = yield* requestFixture;
        for (const key of ["request", "node", "item"] as const)
          yield* source.ingest(source.events[key]);
        const target = yield* requestFixture;
        yield* target.ingest({
          ...target.events.item,
          turnItem: { ...target.item, id: source.item.id },
        });
        assert.isEmpty(yield* target.ingest(target.events.request));
        assert.equal(
          (yield* (yield* ProjectionStore.ProjectionStoreV2).getTurnItemById(
            source.threadId,
            source.item.id,
          ))?.threadId,
          source.threadId,
        );

        const historical = yield* requestFixture;
        const persisted = yield* historical.ingestor.normalize({
          ...historical.input,
          event: historical.events.request,
        });
        yield* historical.sink.write({ events: persisted });
        assert.isEmpty(yield* historical.ingest(historical.events.request));
        assert.isEmpty(
          yield* historical.ingest({
            ...historical.events.node,
            node: { ...historical.node, kind: "tool_call", runtimeRequestId: null },
          }),
        );
        assert.isFalse(
          yield* historical.ingestor.ownsRequest(
            historical.token,
            historical.threadId,
            historical.request.id,
          ),
        );
      }),
  );

  it.effect(
    "allows native request-ID reuse with a fresh app identity but rejects an old run-scoped event",
    () =>
      Effect.gen(function* () {
        const f = yield* requestFixture;
        yield* f.ingest(f.events.request);
        f.ingestor.retireLifetime(f.token);
        const replacement = yield* f.ingestor.createLifetime(f.providerSessionId);
        f.ingestor.activateLifetime(replacement);
        const fresh = {
          ...f.request,
          id: RuntimeRequestId.make(`${f.request.id}:fresh`),
          nodeId: NodeId.make(`${f.node.id}:fresh`),
        };
        assert.equal(
          (yield* f.ingestor.ingestNormalized({
            ...f.input,
            runtimeLifetime: replacement,
            event: { ...f.events.request, runtimeRequest: fresh },
          })).length,
          1,
        );
        assert.isEmpty(
          yield* f.ingestor.ingestNormalized({
            ...f.input,
            runId: RunId.make(`${f.threadId}:old-run`),
            event: {
              ...f.events.request,
              runtimeRequest: {
                ...f.request,
                id: RuntimeRequestId.make(`${f.request.id}:late`),
                nodeId: NodeId.make(`${f.node.id}:late`),
              },
            },
          }),
        );
        assert.isEmpty(
          yield* f.ingestor.ingestNormalized({
            ...f.input,
            runtimeLifetime: replacement,
            event: f.events.request,
          }),
        );
        assert.isTrue(yield* f.ingestor.ownsRequest(replacement, f.threadId, fresh.id));
        const transcript = yield* f.ingest({
          type: "message.updated",
          driver: CODEX_DRIVER,
          message: {
            id: MessageId.make(`${f.threadId}:late-transcript`),
            threadId: f.threadId,
            runId: null,
            nodeId: null,
            createdBy: "agent",
            creationSource: "provider",
            role: "assistant",
            text: "Finished safely",
            attachments: [],
            streaming: false,
            createdAt: f.now,
            updatedAt: f.now,
          },
        });
        assert.equal(transcript.length, 1);
      }),
  );

  it.effect("rejects old queued responses and pending IDs against a same-ID replacement", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      yield* f.ingest(f.events.request);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      let dispatches = 0;
      let gets = 0;
      let currentToken = f.token;
      const sessions = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
        get: () =>
          Effect.sync(() => {
            gets += 1;
            return Option.some({
              runtimeLifetime: currentToken,
              respondToRuntimeRequest: () =>
                Effect.sync(() => {
                  dispatches += 1;
                }),
            } as unknown as ProviderSessionManager.ManagedProviderSessionRuntime);
          }),
      });
      const respond = RuntimeRequestService.RuntimeRequestServiceV2.pipe(
        Effect.flatMap((service) =>
          service.respond({
            threadId: f.threadId,
            providerSessionId: f.providerSessionId,
            requestId: f.request.id,
            answers: { workspace: "Existing workspace" },
          }),
        ),
        Effect.provide(
          RuntimeRequestService.layer.pipe(
            Layer.provide(
              Layer.mergeAll(
                sessions,
                Layer.succeed(ProjectionStore.ProjectionStoreV2, projections),
                Layer.succeed(ProviderEventIngestor.ProviderEventIngestorV2, f.ingestor),
              ),
            ),
          ),
        ),
      );
      assert.equal((yield* respond.pipe(Effect.flip)).reason, "request-not-ready");
      assert.equal(gets, 0);
      const resolved = yield* f.ingestor.normalize({
        ...f.input,
        event: {
          ...f.events.request,
          runtimeRequest: { ...f.request, status: "resolved", resolvedAt: f.now },
        },
      });
      yield* f.sink.write({ events: resolved });
      f.ingestor.retireLifetime(f.token);
      currentToken = yield* f.ingestor.createLifetime(f.providerSessionId);
      f.ingestor.activateLifetime(currentToken);
      assert.equal((yield* respond.pipe(Effect.flip)).reason, "request-not-resumable");
      assert.equal(dispatches, 0);
    }),
  );

  it.effect("keeps an admitted response bound to the captured runtime during replacement", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* requestFixture;
        yield* f.ingest(f.events.request);
        yield* f.sink.write({
          events: yield* f.ingestor.normalize({
            ...f.input,
            event: {
              ...f.events.request,
              runtimeRequest: { ...f.request, status: "resolved", resolvedAt: f.now },
            },
          }),
        });
        const entered = yield* Deferred.make<void>();
        const finish = yield* Deferred.make<void>();
        let oldDispatches = 0;
        let newDispatches = 0;
        const oldRuntime = {
          runtimeLifetime: f.token,
          respondToRuntimeRequest: () =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(finish)),
              Effect.andThen(
                Effect.sync(() => {
                  oldDispatches += 1;
                }),
              ),
            ),
        } as unknown as ProviderSessionManager.ManagedProviderSessionRuntime;
        let currentRuntime = oldRuntime;
        const sessions = Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          get: () => Effect.sync(() => Option.some(currentRuntime)),
        });
        const running = yield* RuntimeRequestService.RuntimeRequestServiceV2.pipe(
          Effect.flatMap((service) =>
            service.respond({
              threadId: f.threadId,
              providerSessionId: f.providerSessionId,
              requestId: f.request.id,
              answers: { workspace: "Existing workspace" },
            }),
          ),
          Effect.provide(RuntimeRequestService.layer.pipe(Layer.provide(sessions))),
          Effect.forkScoped,
        );
        yield* Deferred.await(entered);
        f.ingestor.retireLifetime(f.token);
        const replacement = yield* f.ingestor.createLifetime(f.providerSessionId);
        f.ingestor.activateLifetime(replacement);
        currentRuntime = {
          runtimeLifetime: replacement,
          respondToRuntimeRequest: () =>
            Effect.sync(() => {
              newDispatches += 1;
            }),
        } as unknown as ProviderSessionManager.ManagedProviderSessionRuntime;
        yield* Deferred.succeed(finish, undefined);
        yield* Fiber.join(running);
        assert.equal(oldDispatches, 1);
        assert.equal(newDispatches, 0);
      }),
    ),
  );
  it.effect("never converts a retired durable message question into a live callback", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      yield* f.ingest({
        ...f.events.request,
        runtimeRequest: { ...f.request, responseCapability: { type: "message" } },
      });
      f.ingestor.retireLifetime(f.token);
      assert.isEmpty(yield* f.ingest(f.events.request));
      const request = yield* (yield* ProjectionStore.ProjectionStoreV2).getRuntimeRequest(
        f.threadId,
        f.request.id,
      );
      assert.equal(request?.responseCapability.type, "message");
      assert.equal(request?.status, "pending");
      assert.isEmpty(f.ingestor.getOwnedRequestGroups(f.token));
    }),
  );

  it.effect("bounds request item identities and terminalizes every accepted item together", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      yield* f.ingest(f.events.request);
      yield* f.ingest(f.events.node);
      for (let index = 0; index < 64; index += 1) {
        yield* f.ingest({
          ...f.events.item,
          turnItem: { ...f.item, id: TurnItemId.make(`${f.item.id}:${index}`), ordinal: index },
        });
      }
      const overflow = yield* f
        .ingest({
          ...f.events.item,
          turnItem: { ...f.item, id: TurnItemId.make(`${f.item.id}:overflow`), ordinal: 64 },
        })
        .pipe(Effect.flip);
      assert.instanceOf(overflow, ProviderEventIngestor.ProviderEventPublishError);
      assert.equal(f.ingestor.getOwnedRequestGroups(f.token)[0]?.itemIds.length, 64);
      yield* f.ingest({
        ...f.events.request,
        runtimeRequest: {
          ...f.request,
          status: "expired",
          resolvedAt: f.now,
          responseCapability: { type: "not_resumable", reason: "Provider ended" },
        },
      });
      const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        f.threadId,
      );
      assert.equal(projection.turnItems.length, 64);
      assert.isTrue(projection.turnItems.every((item) => item.status === "cancelled"));
      assert.equal(projection.nodes[0]?.status, "cancelled");
    }),
  );

  it.effect("bounds unclassified prompt buffers and fails closed after overflow", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      const oversized = { ...f.events.item, turnItem: { ...f.item, title: "x".repeat(600_000) } };
      assert.instanceOf(
        yield* f.ingest(oversized).pipe(Effect.flip),
        ProviderEventIngestor.ProviderEventPublishError,
      );
      assert.isEmpty(yield* f.ingest(f.events.request));
      const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        f.threadId,
      );
      assert.isEmpty(projection.runtimeRequests);
      assert.isEmpty(projection.turnItems);
    }),
  );

  it.effect("discards only the finished consumer's uncommitted siblings", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      const runId = RunId.make(`${f.threadId}:consumer`);
      yield* f.ingestor.ingestNormalized({ ...f.input, runId, event: f.events.node });
      f.ingestor.discardBufferedRequests(f.token);
      assert.equal((yield* f.ingest(f.events.request)).length, 2);
      const another = yield* requestFixture;
      yield* another.ingestor.ingestNormalized({
        ...another.input,
        runId,
        event: another.events.node,
      });
      another.ingestor.discardBufferedRequests(another.token, runId);
      assert.equal((yield* another.ingest(another.events.request)).length, 1);
      assert.isEmpty(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(another.threadId))
          .nodes,
      );
    }),
  );

  it.effect("does not reopen a completed sibling while its request update is still in flight", () =>
    Effect.gen(function* () {
      const f = yield* requestFixture;
      yield* f.ingest(f.events.request);
      yield* f.ingest({
        ...f.events.node,
        node: { ...f.node, status: "completed", completedAt: f.now },
      });
      yield* f.ingest({
        ...f.events.item,
        turnItem: { ...f.item, status: "completed", completedAt: f.now },
      });
      yield* f.ingest(f.events.node);
      yield* f.ingest(f.events.item);
      const projection = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        f.threadId,
      );
      assert.equal(projection.nodes[0]?.status, "completed");
      assert.equal(projection.turnItems[0]?.status, "completed");
    }),
  );
  it.effect("retains failed admission ownership and retries the original complete group", () =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      let fail = true;
      const gatedSink = EventSink.EventSinkV2.of({
        ...sink,
        write: (input) =>
          Effect.suspend(() => {
            if (fail && input.events.some((event) => event.type === "runtime-request.updated")) {
              fail = false;
              return Effect.fail(
                new EventSink.EventSinkWriteError({ eventCount: input.events.length }),
              );
            }
            return sink.write(input);
          }),
      });
      const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
        Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
        Effect.provideService(EventSink.EventSinkV2, gatedSink),
      );
      const f = yield* requestFixture.pipe(
        Effect.provideService(ProviderEventIngestor.ProviderEventIngestorV2, ingestor),
      );
      yield* f.ingest(f.events.node);
      yield* f.ingest(f.events.item);
      yield* f.ingest(f.events.request).pipe(Effect.flip);
      assert.equal(f.ingestor.getOwnedRequestGroups(f.token)[0]?.requestId, f.request.id);
      assert.isEmpty(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(f.threadId))
          .runtimeRequests,
      );
      assert.equal((yield* f.ingest(f.events.request)).length, 3);
      assert.isTrue(yield* f.ingestor.ownsRequest(f.token, f.threadId, f.request.id));
    }),
  );

  it.effect("keeps cleanup ownership when a committed admission's caller is interrupted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const committed = yield* Deferred.make<void>();
        const waitAfterCommit = yield* Deferred.make<void>();
        const gatedSink = EventSink.EventSinkV2.of({
          ...sink,
          write: (input) =>
            sink
              .write(input)
              .pipe(
                Effect.tap(() =>
                  input.events.some((event) => event.type === "runtime-request.updated")
                    ? Deferred.succeed(committed, undefined).pipe(
                        Effect.andThen(Deferred.await(waitAfterCommit)),
                      )
                    : Effect.void,
                ),
              ),
        });
        const ingestor = yield* ProviderEventIngestor.ProviderEventIngestorV2.pipe(
          Effect.provide(Layer.fresh(ProviderEventIngestor.layer)),
          Effect.provideService(EventSink.EventSinkV2, gatedSink),
        );
        const f = yield* requestFixture.pipe(
          Effect.provideService(ProviderEventIngestor.ProviderEventIngestorV2, ingestor),
        );
        const admission = yield* f.ingest(f.events.request).pipe(Effect.forkScoped);
        yield* Deferred.await(committed);
        f.ingestor.retireLifetime(f.token);
        yield* Fiber.interrupt(admission);
        const groups = yield* f.ingestor.withLifetimeWrite(
          f.token,
          Effect.sync(() => f.ingestor.getOwnedRequestGroups(f.token)),
        );
        assert.equal(groups[0]?.requestId, f.request.id);
        assert.equal(
          (yield* (yield* ProjectionStore.ProjectionStoreV2).getRuntimeRequest(
            f.threadId,
            f.request.id,
          ))?.status,
          "pending",
        );
        assert.isFalse(yield* f.ingestor.ownsRequest(f.token, f.threadId, f.request.id));
      }),
    ),
  );
});
