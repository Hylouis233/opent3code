import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CommandPolicy from "./CommandPolicy.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as ThreadForkService from "./ThreadForkService.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

const database = SqlitePersistenceMemory;
const stores = Layer.mergeAll(
  EventStore.layer,
  ProjectionStore.layer,
  ProjectStore.layer,
  CommandReceiptStore.layer,
  EffectOutbox.layer,
  TurnItemPositionStore.layer,
).pipe(Layer.provide(database));
const sinkLayer = EventSink.layerFromStores.pipe(Layer.provide(Layer.merge(stores, database)));
const testLayer = Layer.mergeAll(
  stores,
  sinkLayer,
  IdAllocator.layer,
  NodeServices.layer,
  Layer.mock(CheckpointService.CheckpointServiceV2)({}),
  Layer.mock(CommandPolicy.CommandPolicyV2)({}),
  Layer.mock(ContextHandoffService.ContextHandoffServiceV2)({}),
  Layer.mock(ProviderAdapterRegistry.ProviderAdapterRegistryV2)({}),
  Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
  Layer.mock(ProviderSwitchService.ProviderSwitchServiceV2)({}),
  Layer.mock(RuntimePolicy.RuntimePolicyV2)({}),
  Layer.mock(ThreadForkService.ThreadForkServiceV2)({}),
);

const threadId = ThreadId.make("thread:request-race");
const sessionId = ProviderSessionId.make("session:request-race");
const instanceId = ProviderInstanceId.make("codex");

function requestGroup(kind: "command" | "user_input", now: DateTime.Utc) {
  const id = RuntimeRequestId.make(`request:${kind}`);
  const nodeId = NodeId.make(`node:${kind}`);
  const request: OrchestrationV2RuntimeRequest = {
    id,
    nodeId,
    providerTurnId: null,
    nativeRequestRef: null,
    kind,
    status: "pending",
    responseCapability: { type: "live", providerSessionId: sessionId },
    createdAt: now,
    resolvedAt: null,
  };
  const node: OrchestrationV2ExecutionNode = {
    id: nodeId,
    threadId,
    runId: null,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind: kind === "user_input" ? "user_input_request" : "approval_request",
    status: "waiting",
    countsForRun: false,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: id,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  const item: OrchestrationV2TurnItem = {
    id: TurnItemId.make(`item:${kind}`),
    threadId,
    runId: null,
    nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "waiting",
    title: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
    ...(kind === "user_input"
      ? {
          type: "user_input_request" as const,
          requestId: id,
          questions: [{ id: "q", header: "Question", question: "Continue?", options: [] }],
        }
      : { type: "approval_request" as const, requestId: id, requestKind: kind }),
  };
  return { request, node, item };
}

function groupEvents(
  group: ReturnType<typeof requestGroup>,
  now: DateTime.Utc,
  status: "pending" | "expired" | "cancelled",
): ReadonlyArray<OrchestrationV2DomainEvent> {
  const terminal = status !== "pending";
  return [
    {
      id: EventId.make(`event:${group.request.id}:${status}:request`),
      type: "runtime-request.updated",
      threadId,
      occurredAt: now,
      payload: {
        ...group.request,
        status,
        ...(terminal
          ? {
              responseCapability: { type: "not_resumable", reason: "Session closed" },
              resolvedAt: now,
            }
          : {}),
      },
    },
    {
      id: EventId.make(`event:${group.request.id}:${status}:node`),
      type: "node.updated",
      threadId,
      occurredAt: now,
      payload: {
        ...group.node,
        ...(terminal ? { status: "cancelled", completedAt: now } : {}),
      },
    },
    {
      id: EventId.make(`event:${group.request.id}:${status}:item`),
      type: "turn-item.updated",
      threadId,
      occurredAt: now,
      payload: {
        ...group.item,
        ...(terminal ? { status: "cancelled", completedAt: now, updatedAt: now } : {}),
      },
    },
  ];
}

const seedGroup = Effect.fnUntraced(function* (kind: "command" | "user_input") {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const group = requestGroup(kind, now);
  yield* sink.write({
    events: [
      {
        id: EventId.make("event:request-race:thread"),
        type: "thread.created",
        threadId,
        occurredAt: now,
        payload: {
          id: threadId,
          projectId: ProjectId.make("project:request-race"),
          title: "Request race",
          providerInstanceId: instanceId,
          modelSelection: { instanceId, model: "gpt-5.4" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdBy: "user",
          creationSource: "web",
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
      },
      {
        id: EventId.make("event:request-race:session"),
        type: "provider-session.attached",
        threadId,
        occurredAt: now,
        payload: {
          id: sessionId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          status: "ready",
          cwd: "/unused",
          model: "gpt-5.4",
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
      },
      ...groupEvents(group, now, "pending"),
    ],
  });
  return { group, now };
});

const races = (["command", "user_input"] as const).flatMap((kind) =>
  (["cancelled", "expired"] as const).flatMap((terminalStatus) =>
    (["response", "cleanup"] as const).map((firstCommit) => ({
      kind,
      terminalStatus,
      firstCommit,
    })),
  ),
);

it.effect.each(races)(
  "$kind: $firstCommit wins over concurrent $terminalStatus cleanup/response",
  ({ kind, terminalStatus, firstCommit }) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const { group, now } = yield* seedGroup(kind);
      const readPending = yield* Deferred.make<void>();
      const continueCommit = yield* Deferred.make<void>();
      const commandId = CommandId.make("command:request-race:respond");
      const gatedSink = EventSink.EventSinkV2.of({
        ...sink,
        commitCommand: (input) =>
          firstCommit === "cleanup" && input.commandId === commandId
            ? Deferred.succeed(readPending, undefined).pipe(
                Effect.andThen(Deferred.await(continueCommit)),
                Effect.andThen(sink.commitCommand(input)),
              )
            : sink.commitCommand(input),
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const respond = orchestrator.dispatch({
          type: "runtime-request.respond",
          commandId,
          threadId,
          requestId: group.request.id,
          ...(kind === "user_input" ? { answers: { q: "Yes" } } : { decision: "accept" }),
        });
        const cleanup = Effect.gen(function* () {
          const expectedRequest = yield* projections.getRuntimeRequest(threadId, group.request.id);
          assert.isDefined(expectedRequest);
          assert.equal(expectedRequest.status, "pending");
          if (firstCommit === "response") {
            yield* Deferred.succeed(readPending, undefined);
            yield* Deferred.await(continueCommit);
          }
          return yield* sink.writeIfRuntimeRequestCurrent({
            threadId,
            expectedRequest,
            events: groupEvents(group, now, terminalStatus),
          });
        });
        if (firstCommit === "response") {
          const cleanupFiber = yield* cleanup.pipe(Effect.forkScoped);
          yield* Deferred.await(readPending);
          const response = yield* respond;
          assert.lengthOf(response.storedEvents, 3);
          yield* Deferred.succeed(continueCommit, undefined);
          const skipped = yield* Fiber.join(cleanupFiber);
          assert.isFalse(skipped.committed);
          assert.isEmpty(skipped.storedEvents);
        } else {
          const responseFiber = yield* respond.pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(readPending);
          assert.isTrue((yield* cleanup).committed);
          const cleanupSequence = yield* sink.latestSequence();
          yield* Deferred.succeed(continueCommit, undefined);
          const rejected = yield* Fiber.join(responseFiber);
          assert.equal(rejected._tag, "Failure");
          if (rejected._tag === "Failure") {
            assert.equal(rejected.failure._tag, "OrchestratorCommandRejectedError");
          }
          assert.equal(yield* sink.latestSequence(), cleanupSequence);
          assert.isEmpty(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect));
          const replay = yield* respond.pipe(Effect.result);
          assert.equal(replay._tag, "Failure");
          if (replay._tag === "Failure") {
            assert.equal(replay.failure._tag, "OrchestratorCommandPreviouslyRejectedError");
          }
        }
        const persisted = yield* projections.getRuntimeResponseContext(threadId, group.request.id);
        const receipt = yield* receipts.getByCommandId(commandId);
        assert.isTrue(Option.isSome(receipt));
        if (Option.isSome(receipt)) {
          assert.equal(receipt.value.status, firstCommit === "response" ? "accepted" : "rejected");
        }
        assert.equal(
          persisted.request?.status,
          firstCommit === "response" ? "resolved" : terminalStatus,
        );
        assert.equal(
          persisted.node?.status,
          firstCommit === "response" ? "completed" : "cancelled",
        );
        assert.equal(
          persisted.item?.status,
          firstCommit === "response" ? "completed" : "cancelled",
        );
        const responseEffects = yield* outbox.listByCommandId(commandId);
        assert.lengthOf(responseEffects, firstCommit === "response" ? 1 : 0);
        if (firstCommit === "response") {
          if (kind === "user_input") {
            assert.deepEqual(persisted.request?.answers, { q: "Yes" });
            assert.equal(persisted.item?.type, "user_input_request");
            if (persisted.item?.type === "user_input_request") {
              assert.deepEqual(persisted.item.questionAnswer?.answers, { q: "Yes" });
            }
          } else {
            assert.equal(persisted.request?.decision, "accept");
          }
        } else {
          assert.equal(persisted.request?.responseCapability.type, "not_resumable");
          assert.isUndefined(persisted.request?.decision);
          assert.isUndefined(persisted.request?.answers);
        }
      }).pipe(
        Effect.provide(
          Orchestrator.layer.pipe(Layer.provide(Layer.succeed(EventSink.EventSinkV2, gatedSink))),
        ),
      );
    }).pipe(Effect.provide(testLayer)),
);

it.effect("cleanup compares request identity and capability, never timestamp ownership", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const { group, now } = yield* seedGroup("command");
    const mismatches: ReadonlyArray<Partial<OrchestrationV2RuntimeRequest>> = [
      { id: RuntimeRequestId.make("request:other") },
      { nodeId: NodeId.make("node:other") },
      { providerTurnId: ProviderTurnId.make("turn:other") },
      { kind: "user_input" },
      { status: "resolved" },
      { responseCapability: { type: "message" } },
      {
        responseCapability: {
          type: "live",
          providerSessionId: ProviderSessionId.make("session:other"),
        },
      },
    ];
    const sequence = yield* sink.latestSequence();
    for (const mismatch of mismatches) {
      const result = yield* sink.writeIfRuntimeRequestCurrent({
        threadId,
        expectedRequest: { ...group.request, ...mismatch },
        events: groupEvents(group, now, "expired"),
      });
      assert.isFalse(result.committed);
      assert.isEmpty(result.storedEvents);
      assert.equal(yield* sink.latestSequence(), sequence);
    }
    const result = yield* sink.writeIfRuntimeRequestCurrent({
      threadId,
      expectedRequest: { ...group.request, createdAt: DateTime.add(now, { days: 1 }) },
      events: groupEvents(group, now, "expired"),
    });
    assert.isTrue(result.committed);
    assert.lengthOf(result.storedEvents, 3);
    const persisted = yield* projections.getRuntimeResponseContext(threadId, group.request.id);
    assert.equal(persisted.request?.status, "expired");
    assert.equal(persisted.node?.status, "cancelled");
    assert.equal(persisted.item?.status, "cancelled");
  }).pipe(Effect.provide(testLayer)),
);
