import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Run,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";

const StoresLayer = Layer.mergeAll(
  ProjectionStore.layer,
  EventStore.layer,
  EffectOutbox.layer,
).pipe(Layer.provideMerge(SqlitePersistenceMemory));
const SinkLayer = EventSink.layer.pipe(Layer.provideMerge(StoresLayer));

// Deliberately provide no provider manager, adapters, or worker: recovery must
// finish using persisted state alone and must never open a provider runtime.
const makeTestLayer = (failCommitFor?: ThreadId) => {
  let shouldFail = failCommitFor !== undefined;
  const sink = Layer.effect(
    EventSink.EventSinkV2,
    Effect.gen(function* () {
      const delegate = yield* EventSink.EventSinkV2;
      return EventSink.EventSinkV2.of({
        ...delegate,
        commitCommand: (input) =>
          Effect.suspend(() => {
            if (shouldFail && input.threadId === failCommitFor) {
              shouldFail = false;
              return Effect.fail(
                new EventSink.EventSinkWriteError({ eventCount: input.events.length }),
              );
            }
            return delegate.commitCommand(input);
          }),
      });
    }),
  ).pipe(Layer.provideMerge(SinkLayer));
  return ProviderRuntimeRecovery.layer.pipe(
    Layer.provideMerge(sink),
    Layer.provide(IdAllocator.layer),
    Layer.provide(ServerSettings.layerTest()),
  );
};

const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };

const seedThread = Effect.fn(function* (name: string) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const threadId = ThreadId.make(`thread:request-recovery:${name}`);
  yield* projections.apply({
    id: EventId.make(`event:${threadId}`),
    type: "thread.created",
    threadId,
    occurredAt: now,
    payload: {
      id: threadId,
      createdBy: "user",
      creationSource: "web",
      projectId: ProjectId.make("project:request-recovery"),
      title: name,
      providerInstanceId,
      modelSelection,
      runtimeMode: "approval-required",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  });
  return threadId;
});

const seedRun = Effect.fn(function* (
  threadId: ThreadId,
  ordinal: number,
  status: OrchestrationV2Run["status"],
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const runId = RunId.make(`run:${threadId}:${ordinal}`);
  yield* projections.apply({
    id: EventId.make(`event:${runId}`),
    type: "run.created",
    threadId,
    runId,
    occurredAt: now,
    payload: {
      id: runId,
      threadId,
      ordinal,
      providerInstanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make(`message:${runId}`),
      rootNodeId: null,
      activeAttemptId: null,
      status,
      requestedAt: now,
      startedAt: now,
      completedAt: status === "completed" ? now : null,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  return runId;
});

const seedRequest = Effect.fn(function* (input: {
  readonly threadId: ThreadId;
  readonly name: string;
  readonly runId: RunId | null;
  readonly kind: "command" | "user_input";
  readonly capability: OrchestrationV2RuntimeRequest["responseCapability"]["type"];
  readonly status: OrchestrationV2RuntimeRequest["status"];
  readonly itemWithoutNode?: boolean;
}) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const now = yield* DateTime.now;
  const requestId = RuntimeRequestId.make(`request:${input.threadId}:${input.name}`);
  const nodeId = NodeId.make(`node:${requestId}`);
  const itemId = TurnItemId.make(`item:${requestId}`);
  const request: OrchestrationV2RuntimeRequest = {
    id: requestId,
    nodeId,
    providerTurnId: null,
    nativeRequestRef: { driver, nativeId: `native:${requestId}`, strength: "strong" },
    kind: input.kind,
    status: input.status,
    responseCapability:
      input.capability === "live"
        ? { type: "live", providerSessionId: ProviderSessionId.make("session:dead-process") }
        : input.capability === "not_resumable"
          ? { type: "not_resumable", reason: "The provider process stopped." }
          : { type: "message" },
    createdAt: now,
    resolvedAt: input.status === "pending" ? null : now,
    ...(input.status === "resolved"
      ? input.kind === "command"
        ? { decision: "accept" as const }
        : { answers: { workspace: "Current workspace" } }
      : {}),
  };
  const node = {
    id: nodeId,
    threadId: input.threadId,
    runId: input.runId,
    parentNodeId: null,
    rootNodeId: nodeId,
    kind:
      input.kind === "command" ? ("approval_request" as const) : ("user_input_request" as const),
    status: "waiting" as const,
    countsForRun: true,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: requestId,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
  const item: OrchestrationV2TurnItem = {
    id: itemId,
    threadId: input.threadId,
    runId: input.runId,
    nodeId: input.itemWithoutNode ? null : nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "waiting",
    title: input.kind === "command" ? "Approve a shell command" : "Choose the workspace",
    startedAt: now,
    completedAt: null,
    updatedAt: now,
    ...(input.kind === "command"
      ? { type: "approval_request", requestId, requestKind: "command", prompt: "Run git status?" }
      : {
          type: "user_input_request",
          requestId,
          questions: [
            {
              id: "workspace",
              header: "Workspace",
              question: "Which workspace should I use?",
              options: [
                { label: "Current workspace", description: "Use the project already open." },
              ],
            },
          ],
          ...(input.capability === "message" ? { responseMode: "message" as const } : {}),
        }),
  };
  for (const event of [
    { type: "node.updated" as const, payload: node },
    { type: "runtime-request.updated" as const, payload: request },
    { type: "turn-item.updated" as const, payload: item },
  ]) {
    yield* projections.apply({
      id: EventId.make(`event:${requestId}:${event.type}`),
      threadId: input.threadId,
      occurredAt: now,
      ...event,
    });
  }
  return { request, node, item };
});

const orphanCases = (["pending", "expired", "cancelled"] as const).flatMap((status) =>
  (["live", "not_resumable"] as const).flatMap((capability) =>
    (["command", "user_input"] as const).flatMap((kind) =>
      [true, false].map((runless) => ({
        status,
        capability,
        kind,
        runless,
        runLabel: runless ? "runless work" : "an older settled run",
      })),
    ),
  ),
);

it.effect.each(orphanCases)(
  "repairs $status $capability $kind artifacts on $runLabel",
  ({ status, capability, kind, runless }) =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      const threadId = yield* seedThread("orphan");
      const runId = runless ? null : yield* seedRun(threadId, 1, "completed");
      if (!runless) yield* seedRun(threadId, 2, "completed");
      const group = yield* seedRequest({
        threadId,
        name: "orphan",
        runId,
        status,
        capability,
        kind,
        // A request item can link by requestId without a nodeId.
        itemWithoutNode: kind === "user_input",
      });
      assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), [threadId]);
      const loaded = yield* projections.getRuntimeRecoveryProjection(threadId);
      assert.deepEqual(loaded.runtimeRequests, [group.request]);
      assert.deepEqual(loaded.nodes, [group.node]);
      assert.deepEqual(loaded.turnItems, [group.item]);

      const result = yield* recovery.recover;
      assert.equal(result.closedRequests, status === "pending" ? 1 : 0);
      assert.equal(result.terminalizedRuns, 0);
      const after = yield* projections.getThreadProjection(threadId);
      const request = after.runtimeRequests.find((row) => row.id === group.request.id);
      if (status === "pending") {
        assert.equal(request?.status, "expired");
        assert.equal(request?.responseCapability.type, "not_resumable");
      } else {
        assert.deepEqual(request, group.request);
      }
      assert.equal(after.nodes.find((row) => row.id === group.node.id)?.status, "cancelled");
      assert.equal(after.turnItems.find((row) => row.id === group.item.id)?.status, "cancelled");
      assert.isTrue(after.runs.every((run) => run.status === "completed"));
      assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), []);
      const sequence = yield* sink.latestSequence();
      assert.deepEqual(yield* recovery.recover, {
        terminalizedRuns: 0,
        stoppedSessions: 0,
        closedRequests: 0,
        retiredEffects: 0,
        requeuedEffects: 0,
      });
      assert.equal(yield* sink.latestSequence(), sequence);
      assert.deepEqual(yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, []);
    }).pipe(Effect.provide(makeTestLayer())),
);

it.effect.each(["runless", "running", "completed"] as const)(
  "preserves real message-mode questions and resolved decisions on %s work",
  (runStatus) =>
    Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = yield* seedThread("preserved");
      const runId = runStatus === "runless" ? null : yield* seedRun(threadId, 1, runStatus);
      const groups = [];
      for (const input of [
        { name: "question", kind: "user_input", capability: "message", status: "pending" },
        { name: "approved", kind: "command", capability: "live", status: "resolved" },
        { name: "answered", kind: "user_input", capability: "live", status: "resolved" },
      ] as const) {
        groups.push(yield* seedRequest({ threadId, runId, ...input, itemWithoutNode: true }));
      }
      assert.equal((yield* recovery.recover).terminalizedRuns, runStatus === "running" ? 1 : 0);
      const after = yield* projections.getThreadProjection(threadId);
      for (const group of groups) {
        assert.deepEqual(
          after.runtimeRequests.find((row) => row.id === group.request.id),
          group.request,
        );
        assert.deepEqual(
          after.nodes.find((row) => row.id === group.node.id),
          group.node,
        );
        assert.deepEqual(
          after.turnItems.find((row) => row.id === group.item.id),
          group.item,
        );
      }
      const sequence = yield* sink.latestSequence();
      assert.equal((yield* recovery.recover).closedRequests, 0);
      assert.equal(yield* sink.latestSequence(), sequence);
    }).pipe(Effect.provide(makeTestLayer())),
);

it.effect(
  "fails closed on a partial recovery write and repairs the remaining group on retry",
  () => {
    const failThread = ThreadId.make("thread:request-recovery:b-fails");
    return Effect.gen(function* () {
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
      const first = yield* seedThread("a-succeeds");
      const second = yield* seedThread("b-fails");
      for (const threadId of [first, second]) {
        yield* seedRequest({
          threadId,
          name: "orphan",
          runId: null,
          status: "pending",
          capability: "live",
          kind: "command",
        });
      }
      const error = yield* recovery.recover.pipe(Effect.flip);
      assert.equal(error.operation, "reconcile");
      assert.equal(error.threadId, second);
      assert.equal(
        (yield* projections.getThreadProjection(first)).runtimeRequests[0]?.status,
        "expired",
      );
      assert.equal(
        (yield* projections.getThreadProjection(second)).runtimeRequests[0]?.status,
        "pending",
      );
      assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), [second]);
      assert.equal((yield* recovery.recover).closedRequests, 1);
      assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), []);
    }).pipe(Effect.provide(makeTestLayer(failThread)));
  },
);

it.effect("fails closed when a terminal request has an unreadable linked artifact", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = yield* seedThread("unreadable");
    const group = yield* seedRequest({
      threadId,
      name: "orphan",
      runId: null,
      status: "expired",
      capability: "not_resumable",
      kind: "command",
    });
    yield* sql`
      UPDATE orchestration_v2_projection_nodes SET payload_json = '{broken'
      WHERE node_id = ${group.node.id}
    `;
    assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), [threadId]);
    const error = yield* recovery.recover.pipe(Effect.flip);
    assert.equal(error.operation, "read-projections");
    assert.equal(error.threadId, threadId);
    assert.equal(yield* sink.latestSequence(), 0);
  }).pipe(Effect.provide(makeTestLayer())),
);

it.effect("selects isolated terminal leftovers without reopening message or resolved groups", () =>
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
    const sql = yield* SqlClient.SqlClient;
    const nodeOnly = yield* seedThread("node-only");
    const itemOnly = yield* seedThread("item-only");
    const resolved = yield* seedThread("resolved-only");
    const message = yield* seedThread("terminal-message");
    const completed = yield* seedThread("already-completed");
    for (const threadId of [nodeOnly, itemOnly, resolved, message, completed]) {
      const group = yield* seedRequest({
        threadId,
        name: "orphan",
        runId: null,
        status: threadId === resolved ? "resolved" : "cancelled",
        capability: threadId === message ? "message" : "not_resumable",
        kind: "user_input",
        itemWithoutNode: true,
      });
      if (threadId === nodeOnly) {
        yield* sql`DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${group.item.id}`;
      }
      if (threadId === itemOnly) {
        yield* sql`DELETE FROM orchestration_v2_projection_nodes WHERE node_id = ${group.node.id}`;
      }
      if (threadId === completed) {
        const now = yield* DateTime.now;
        yield* projections.apply({
          id: EventId.make("event:request-recovery:completed-node"),
          type: "node.updated",
          threadId,
          occurredAt: now,
          payload: { ...group.node, status: "completed", completedAt: now },
        });
        yield* projections.apply({
          id: EventId.make("event:request-recovery:completed-item"),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: { ...group.item, status: "completed", completedAt: now },
        });
      }
    }
    const preserved = yield* Effect.all(
      [resolved, message, completed].map((threadId) => projections.getThreadProjection(threadId)),
    );
    assert.deepEqual(
      new Set(yield* projections.getRecoveryThreadIds("runtime")),
      new Set([nodeOnly, itemOnly]),
    );
    assert.equal((yield* recovery.recover).closedRequests, 0);
    assert.equal((yield* projections.getThreadProjection(nodeOnly)).nodes[0]?.status, "cancelled");
    assert.equal(
      (yield* projections.getThreadProjection(itemOnly)).turnItems[0]?.status,
      "cancelled",
    );
    assert.deepEqual(
      yield* Effect.all(
        [resolved, message, completed].map((threadId) => projections.getThreadProjection(threadId)),
      ),
      preserved,
    );
    assert.deepEqual(yield* projections.getRecoveryThreadIds("runtime"), []);
  }).pipe(Effect.provide(makeTestLayer())),
);
