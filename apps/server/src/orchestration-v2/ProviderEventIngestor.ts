import {
  NodeId,
  CommandId,
  OrchestrationV2DomainEvent,
  OrchestrationV2StoredEvent,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Subagent,
  type ModelSelection,
  type RuntimeMode,
  type ProviderInteractionMode,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RawEventId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2RuntimeRequest,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import { ProviderAdapterV2Event } from "./ProviderAdapter.ts";
import { makeProviderFailureTurnItem } from "./ProviderFailure.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProviderRuntimeLifetime from "./ProviderRuntimeLifetime.ts";

export class ProviderEventNormalizeError extends Schema.TaggedError<ProviderEventNormalizeError>()(
  "ProviderEventNormalizeError",
  {
    providerSessionId: ProviderSessionId,
    threadId: ThreadId,
    providerEvent: ProviderAdapterV2Event,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to normalize provider event ${this.providerEvent.type} for thread ${this.threadId}.`;
  }
}

export class ProviderEventPublishError extends Schema.TaggedError<ProviderEventPublishError>()(
  "ProviderEventPublishError",
  {
    providerSessionId: ProviderSessionId,
    eventCount: Schema.Number,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to publish ${this.eventCount} normalized provider event(s).`;
  }
}

export const ProviderEventIngestorV2Error = Schema.Union([
  ProviderEventNormalizeError,
  ProviderEventPublishError,
]);
export type ProviderEventIngestorV2Error = typeof ProviderEventIngestorV2Error.Type;

export interface ProviderTurnAnalyticsContext {
  readonly modelSelection: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
}

export class ProviderTurnAnalytics extends Context.Reference<{
  readonly record: (properties: Readonly<Record<string, unknown>>) => Effect.Effect<void>;
}>("t3/orchestration-v2/ProviderTurnAnalytics", {
  defaultValue: () => ({ record: () => Effect.void }),
}) {}

export const analyticsLive = Layer.effect(
  ProviderTurnAnalytics,
  Effect.gen(function* () {
    const analytics = yield* AnalyticsService.AnalyticsService;
    return {
      record: (properties: Readonly<Record<string, unknown>>) =>
        analytics.record("provider.turn.completed", properties),
    };
  }),
);

function providerTurnAnalyticsProperties(input: {
  readonly driver: ProviderAdapterV2Event["driver"];
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly context?: ProviderTurnAnalyticsContext;
}): Readonly<Record<string, unknown>> {
  const usage = input.providerTurn.turnTokenUsage;
  const modelSelection = input.context?.modelSelection;
  const effort = modelSelection
    ? (getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
      getModelSelectionStringOptionValue(modelSelection, "effort"))
    : undefined;
  return {
    provider: input.driver,
    terminalStatus: input.providerTurn.status,
    usageStatus: usage?.usageStatus ?? "unavailable",
    usageScope: usage?.usageScope ?? "main_agent",
    ...(usage ? { hasSubagents: usage.hasSubagents } : {}),
    ...(usage?.inputTokens === undefined ? {} : { inputTokens: usage.inputTokens }),
    ...(usage?.cachedInputTokens === undefined
      ? {}
      : { cachedInputTokens: usage.cachedInputTokens }),
    ...(usage?.cacheCreationTokens === undefined
      ? {}
      : { cacheCreationTokens: usage.cacheCreationTokens }),
    ...(usage?.outputTokens === undefined ? {} : { outputTokens: usage.outputTokens }),
    ...(usage?.reasoningTokens === undefined ? {} : { reasoningTokens: usage.reasoningTokens }),
    ...(modelSelection ? { model: modelSelection.model, mixedModels: false } : {}),
    ...(effort ? { effort } : {}),
    ...(input.context?.runtimeMode ? { runtimeMode: input.context.runtimeMode } : {}),
    ...(input.context?.interactionMode ? { interactionMode: input.context.interactionMode } : {}),
    ...(input.providerTurn.startedAt && input.providerTurn.completedAt
      ? {
          durationMs: Math.max(
            0,
            DateTime.toEpochMillis(input.providerTurn.completedAt) -
              DateTime.toEpochMillis(input.providerTurn.startedAt),
          ),
        }
      : {}),
  };
}

type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;

function withPlanStepDurations(
  plan: TodoListPlan,
  previous: TodoListPlan | undefined,
  occurredAt: DateTime.Utc,
): TodoListPlan {
  const occurredAtIso = DateTime.formatIso(occurredAt);
  const occurredAtMs = DateTime.toEpochMillis(occurredAt);
  const previousById = new Map(previous?.steps.map((step) => [step.id, step]));
  // Provider step IDs may be positional. Changed text must not inherit another task's timing.
  const previousStep = (step: TodoListPlan["steps"][number]) => {
    const prior = previousById.get(step.id);
    return prior?.text === step.text ? prior : undefined;
  };
  const hasNewCompletion = plan.steps.some((step) => {
    const prior = previousStep(step);
    return step.status === "completed" && prior?.status !== "completed";
  });
  let fallbackCompletionConsumed = false;

  return {
    ...plan,
    steps: plan.steps.map((step) => {
      const prior = previousStep(step);
      const baseStep = { id: step.id, text: step.text, status: step.status };
      if (step.status === "completed") {
        if (prior?.status === "completed") {
          return {
            ...baseStep,
            ...(prior.durationMs === undefined ? {} : { durationMs: prior.durationMs }),
          };
        }
        const durationAnchorAt =
          prior?.status === "running" || !fallbackCompletionConsumed
            ? prior?.durationAnchorAt
            : occurredAtIso;
        fallbackCompletionConsumed = true;
        const anchorMs =
          durationAnchorAt === undefined ? occurredAtMs : Date.parse(durationAnchorAt);
        const durationMs = Number.isFinite(anchorMs) ? Math.max(0, occurredAtMs - anchorMs) : 0;
        return {
          ...baseStep,
          ...(durationMs > 0 ? { durationMs } : {}),
        };
      }
      if (step.status === "running") {
        return {
          ...baseStep,
          durationAnchorAt:
            prior?.status === "running" ? (prior.durationAnchorAt ?? occurredAtIso) : occurredAtIso,
        };
      }
      return {
        ...baseStep,
        durationAnchorAt:
          hasNewCompletion || prior?.status !== "pending"
            ? occurredAtIso
            : (prior.durationAnchorAt ?? occurredAtIso),
      };
    }),
  };
}

export interface ProviderEventIngestInput {
  readonly providerSessionId: ProviderSessionId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly commandId?: CommandId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly nodeId?: NodeId;
  readonly rawEventId?: RawEventId;
  readonly event: ProviderAdapterV2Event;
  readonly analyticsContext?: ProviderTurnAnalyticsContext;
}

export interface ProviderEventIngestorV2Shape
  extends ProviderRuntimeLifetime.ProviderRuntimeLifetimeLifecycle {
  /** Omitted runId discards only the manager's runless consumer buffers. */
  readonly discardBufferedRequests: (
    token: ProviderRuntimeLifetime.ProviderRuntimeLifetime,
    runId?: RunId,
  ) => void;
  readonly normalize: (
    input: ProviderEventIngestInput,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, ProviderEventIngestorV2Error>;
  readonly ingestNormalized: (
    input: ProviderEventIngestInput & {
      readonly runtimeLifetime: ProviderRuntimeLifetime.ProviderRuntimeLifetime;
      /**
       * Atomically reject mutable provider state emitted by an attempt that
       * lost ownership while the adapter event was in flight.
       */
      readonly writeIfRunCurrent?: {
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedStatus: OrchestrationV2Run["status"];
      };
      /**
       * Atomically reject provider-thread snapshots from an attempt that no
       * longer owns the run or from a run that no longer owns the thread.
       */
      readonly writeIfProviderThreadOwner?: {
        readonly providerThreadId: ProviderThreadId;
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedLastRunOrdinal: number;
      };
    },
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, ProviderEventIngestorV2Error>;
}

export class ProviderEventIngestorV2 extends Context.Service<
  ProviderEventIngestorV2,
  ProviderEventIngestorV2Shape
>()("t3/orchestration-v2/ProviderEventIngestor/ProviderEventIngestorV2") {}

function compactUndefined<T extends Record<string, unknown>>(record: T): T {
  return Object.fromEntries(Object.entries(record).filter(([, value]) => value !== undefined)) as T;
}

const decodeDomainEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEvent);

export const layer: Layer.Layer<
  ProviderEventIngestorV2,
  never,
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectionStore.ProjectionStoreV2
  | ThreadCommandExecutor.ThreadCommandExecutor
> = Layer.effect(
  ProviderEventIngestorV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const analytics = yield* ProviderTurnAnalytics;
    const completedTurnAnalytics = new Set<string>();
    const lifetimes = yield* ProviderRuntimeLifetime.make;
    type IngestInput = Parameters<ProviderEventIngestorV2Shape["ingestNormalized"]>[0];
    interface BufferedSiblings {
      readonly groups: Map<RuntimeRequestId, Map<string, OrchestrationV2DomainEvent>>;
      readonly sourceRuns: Map<RuntimeRequestId, RunId | undefined>;
      size: number;
      bytes: number;
      failed: boolean;
    }
    const siblingBuffers = new WeakMap<
      ProviderRuntimeLifetime.ProviderRuntimeLifetime,
      BufferedSiblings
    >();

    const makeDomainEvent = (
      input: ProviderEventIngestInput,
      payloadInput: {
        readonly type: OrchestrationV2DomainEvent["type"];
        readonly payload: OrchestrationV2DomainEvent["payload"];
        readonly threadId?: ThreadId;
        readonly runId?: RunId | null;
        readonly nodeId?: NodeId | null;
        readonly occurredAt?: DateTime.Utc;
      },
    ) =>
      Effect.gen(function* () {
        const threadId = payloadInput.threadId ?? input.threadId;
        const eventId = yield* idAllocator.allocate.event({
          threadId,
          providerSessionId: input.providerSessionId,
        });
        const occurredAt = payloadInput.occurredAt ?? (yield* DateTime.now);
        return yield* decodeDomainEvent(
          compactUndefined({
            id: eventId,
            type: payloadInput.type,
            threadId,
            runId: payloadInput.runId ?? input.runId,
            nodeId: payloadInput.nodeId ?? input.nodeId,
            driver: input.event.driver,
            providerInstanceId: input.providerInstanceId,
            rawEventId: input.rawEventId,
            occurredAt,
            payload: payloadInput.payload,
          }),
        );
      });

    const dismissNativeUserInputs = Effect.fn("ProviderEventIngestor.dismissNativeUserInputs")(
      function* (
        input: ProviderEventIngestInput,
        providerTurnId: ProviderTurnId,
        threadId = input.threadId,
      ) {
        const pending = yield* projections.getPendingNativeUserInputs(threadId, providerTurnId);
        const now = yield* DateTime.now;
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const request of pending.runtimeRequests) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "runtime-request.updated",
              threadId,
              nodeId: request.nodeId,
              payload: { ...request, status: "cancelled", resolvedAt: now },
            }),
          );
        }
        for (const node of pending.nodes) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "node.updated",
              threadId,
              nodeId: node.id,
              runId: node.runId,
              payload: { ...node, status: "cancelled", completedAt: now },
            }),
          );
        }
        for (const item of pending.turnItems) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "turn-item.updated",
              threadId,
              nodeId: item.nodeId,
              runId: item.runId,
              payload: { ...item, status: "cancelled", completedAt: now, updatedAt: now },
            }),
          );
        }
        return events;
      },
    );

    /**
     * A native subagent's thread starts on the parent's model when the
     * provider names the real one later (a Claude agent file's model arrives
     * with the subagent's first reply). Clients read the thread's model, so
     * move the thread to the reported one. Thread commands rewrite the whole
     * thread row under the thread's lock, so this read and write take it too.
     */
    const syncSubagentThreadModel = Effect.fn("ProviderEventIngestor.syncSubagentThreadModel")(
      function* (input: ProviderEventIngestInput, subagent: OrchestrationV2Subagent) {
        const { childThreadId, model } = subagent;
        if (subagent.origin !== "provider_native" || childThreadId === null || model === null) {
          return [];
        }
        const staleThread = projections.getThread(childThreadId).pipe(
          Effect.map((thread) => (thread.modelSelection.model === model ? null : thread)),
          Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
        );
        // Nearly every update already matches; only a mismatch takes the lock.
        if ((yield* staleThread) === null) return [];
        return yield* threadCommands.withLock(
          childThreadId,
          Effect.gen(function* () {
            const thread = yield* staleThread;
            if (thread === null) return [];
            const now = yield* DateTime.now;
            const event = yield* makeDomainEvent(input, {
              type: "thread.model-selection-updated",
              threadId: thread.id,
              // The parent's options belong to the parent's model.
              payload: {
                ...thread,
                modelSelection: { instanceId: thread.modelSelection.instanceId, model },
                updatedAt: now,
              },
              occurredAt: now,
            });
            return yield* eventSink.write({ events: [event] });
          }),
        );
      },
    );

    const normalize: ProviderEventIngestorV2Shape["normalize"] = (input) =>
      Effect.gen(function* () {
        switch (input.event.type) {
          case "app_thread.created":
            return [
              yield* makeDomainEvent(input, {
                type: "thread.created",
                threadId: input.event.appThread.id,
                payload: input.event.appThread,
              }),
            ];
          case "provider_session.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "provider-session.updated",
                payload: input.event.providerSession,
              }),
            ];
          case "provider_thread.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "provider-thread.updated",
                threadId: input.event.providerThread.appThreadId ?? input.threadId,
                payload: input.event.providerThread,
              }),
            ];
          case "provider_turn.updated":
            return [
              ...(["completed", "interrupted", "failed", "cancelled"].includes(
                input.event.providerTurn.status,
              )
                ? yield* dismissNativeUserInputs(
                    input,
                    input.event.providerTurn.id,
                    input.event.threadId,
                  )
                : []),
              yield* makeDomainEvent(input, {
                type: "provider-turn.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.providerTurn,
                nodeId: input.event.providerTurn.nodeId,
              }),
            ];
          case "node.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "node.updated",
                threadId: input.event.node.threadId,
                payload: input.event.node,
                runId: input.event.node.runId,
                nodeId: input.event.node.id,
              }),
            ];
          case "subagent.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "subagent.updated",
                threadId: input.event.subagent.threadId,
                payload: input.event.subagent,
                runId: input.event.subagent.runId,
                nodeId: input.event.subagent.id,
              }),
            ];
          case "message.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "message.updated",
                threadId: input.event.message.threadId,
                payload: input.event.message,
                runId: input.event.message.runId,
                nodeId: input.event.message.nodeId,
              }),
            ];
          case "turn_item.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                threadId: input.event.turnItem.threadId,
                payload: input.event.turnItem,
                runId: input.event.turnItem.runId,
                nodeId: input.event.turnItem.nodeId,
              }),
            ];
          case "runtime_request.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "runtime-request.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.runtimeRequest,
                nodeId: input.event.runtimeRequest.nodeId,
              }),
            ];
          case "plan.updated": {
            const occurredAt = yield* DateTime.now;
            const plan = input.event.plan;
            const previous =
              plan.kind === "todo_list"
                ? yield* projections.getPlan(plan.threadId, plan.id)
                : undefined;
            const payload =
              plan.kind === "todo_list"
                ? withPlanStepDurations(
                    plan,
                    previous?.kind === "todo_list" ? previous : undefined,
                    occurredAt,
                  )
                : plan;
            return [
              yield* makeDomainEvent(input, {
                type: "plan.updated",
                threadId: plan.threadId,
                payload,
                runId: plan.runId,
                nodeId: plan.nodeId,
                occurredAt,
              }),
            ];
          }
          case "turn.terminal":
            const dismissed = yield* dismissNativeUserInputs(input, input.event.providerTurnId);
            if (input.event.status !== "failed") {
              return dismissed;
            }
            const occurredAt = yield* DateTime.now;
            return [
              ...dismissed,
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                payload: makeProviderFailureTurnItem({
                  idAllocator,
                  driver: input.event.driver,
                  threadId: input.threadId,
                  runId: input.runId ?? null,
                  nodeId: input.nodeId ?? null,
                  providerThreadId: input.event.providerThreadId,
                  providerTurnId: input.event.providerTurnId,
                  itemOrdinal: input.event.failureItemOrdinal,
                  failure: input.event.failure,
                  ...(input.event.retry === undefined ? {} : { retry: input.event.retry }),
                  ...(input.event.retryStartedAt === undefined
                    ? {}
                    : { retryStartedAt: input.event.retryStartedAt }),
                  occurredAt,
                }),
              }),
            ];
        }
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderEventNormalizeError({
              providerSessionId: input.providerSessionId,
              threadId: input.threadId,
              providerEvent: input.event,
              cause,
            }),
        ),
      );

    const publish = (
      input: IngestInput,
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
      guardPendingUserInputCancellations = true,
    ) =>
      Effect.gen(function* () {
        if (events.length === 0) return [];
        if (input.writeIfProviderThreadOwner !== undefined) {
          return (yield* eventSink.writeIfProviderThreadOwner({
            guardPendingUserInputCancellations,
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            ...input.writeIfProviderThreadOwner,
            events,
          })).storedEvents;
        }
        if (input.writeIfRunCurrent !== undefined) {
          return (yield* eventSink.writeIfRunCurrent({
            guardPendingUserInputCancellations,
            ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
            threadId: input.threadId,
            ...input.writeIfRunCurrent,
            events,
          })).storedEvents;
        }
        return yield* eventSink.write({
          guardPendingUserInputCancellations,
          ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
          events,
        });
      });

    const linkedRequestId = (event: ProviderAdapterV2Event): RuntimeRequestId | undefined => {
      switch (event.type) {
        case "runtime_request.updated":
          return event.runtimeRequest.id;
        case "node.updated":
          return event.node.runtimeRequestId ?? undefined;
        case "turn_item.updated":
          return event.turnItem.type === "approval_request" ||
            event.turnItem.type === "user_input_request"
            ? event.turnItem.requestId
            : undefined;
        default:
          return undefined;
      }
    };
    // Count retained strings without serializing (and copying) prompt content.
    const bufferedSize = (value: unknown): number => {
      if (typeof value === "string") return value.length * 2;
      if (value === null || typeof value !== "object") return 8;
      if (Array.isArray(value))
        return 64 + value.reduce((total, item) => total + bufferedSize(item), 0);
      return (
        64 +
        Object.entries(value).reduce(
          (total, [key, entry]) => total + key.length * 2 + bufferedSize(entry),
          0,
        )
      );
    };
    const isMutableArtifactStatus = (status: string) =>
      ["idle", "pending", "running", "waiting"].includes(status);
    const siblingKey = (event: OrchestrationV2DomainEvent) =>
      `${event.type}:${"id" in event.payload ? event.payload.id : ""}`;
    const removeBuffer = (
      token: ProviderRuntimeLifetime.ProviderRuntimeLifetime,
      requestId: RuntimeRequestId,
    ) => {
      const buffer = siblingBuffers.get(token);
      const group = buffer?.groups.get(requestId);
      if (buffer === undefined || group === undefined) return;
      buffer.size -= group.size;
      for (const event of group.values()) buffer.bytes -= bufferedSize(event);
      buffer.groups.delete(requestId);
      buffer.sourceRuns.delete(requestId);
    };
    const requestIdentityMatches = (
      group: ProviderRuntimeLifetime.OwnedRuntimeRequestGroup,
      threadId: ThreadId,
      request: OrchestrationV2RuntimeRequest,
    ) =>
      group.threadId === threadId &&
      group.requestId === request.id &&
      group.nodeId === request.nodeId &&
      group.providerTurnId === request.providerTurnId &&
      group.kind === request.kind;

    const ingestRequestGroup = (input: IngestInput, requestId: RuntimeRequestId) =>
      lifetimes.lifecycle.withLifetimeWrite(
        input.runtimeLifetime,
        lifetimes.withAdmission(
          Effect.gen(function* () {
            const token = input.runtimeLifetime;
            const group = lifetimes.group(token, requestId);
            const owner = lifetimes.requestOwner(requestId);
            if (owner !== undefined && owner !== token) return [];
            if (group === undefined && siblingBuffers.get(token)?.failed === true) return [];
            const incomingRequest =
              input.event.type === "runtime_request.updated"
                ? input.event.runtimeRequest
                : undefined;
            const threadId =
              input.event.type === "node.updated"
                ? input.event.node.threadId
                : input.event.type === "turn_item.updated"
                  ? input.event.turnItem.threadId
                  : input.event.type === "runtime_request.updated"
                    ? (input.event.threadId ?? input.threadId)
                    : input.threadId;
            const persisted = yield* projections.getRuntimeRequestById(threadId, requestId);
            // Existing SQL identities are never adopted by a new runtime, even if
            // the application thread and provider's native request ID happen to match.
            if (group === undefined && persisted !== undefined) {
              removeBuffer(token, requestId);
              return [];
            }
            if (
              group !== undefined &&
              (persisted === undefined
                ? incomingRequest === undefined
                : !requestIdentityMatches(group, persisted.threadId, persisted.request))
            )
              return [];
            if (
              incomingRequest !== undefined &&
              ((group !== undefined && !requestIdentityMatches(group, threadId, incomingRequest)) ||
                (incomingRequest.responseCapability.type === "live" &&
                  incomingRequest.responseCapability.providerSessionId !== input.providerSessionId))
            )
              return [];
            if (
              group !== undefined &&
              incomingRequest !== undefined &&
              incomingRequest.responseCapability.type !== group.capability &&
              incomingRequest.responseCapability.type !== "not_resumable"
            )
              return [];
            const capability = group?.capability ?? incomingRequest?.responseCapability.type;
            if (
              capability === "live" &&
              !lifetimes.isActive(token) &&
              (incomingRequest?.status ?? persisted?.request.status ?? "pending") === "pending" &&
              incomingRequest?.responseCapability.type !== "not_resumable"
            ) {
              removeBuffer(token, requestId);
              return [];
            }
            const normalized = yield* normalize(input);
            if (group === undefined && incomingRequest === undefined) {
              let buffer = siblingBuffers.get(token);
              if (buffer === undefined) {
                buffer = {
                  groups: new Map(),
                  sourceRuns: new Map(),
                  size: 0,
                  bytes: 0,
                  failed: false,
                };
                siblingBuffers.set(token, buffer);
              }
              // Waiting for classification must never block the sequential provider
              // stream. Keep only the latest sibling reference, with a hard bound.
              let siblings = buffer.groups.get(requestId);
              if (siblings === undefined) {
                siblings = new Map();
                buffer.groups.set(requestId, siblings);
                buffer.sourceRuns.set(requestId, input.runId);
              }
              for (const event of normalized) {
                const key = siblingKey(event);
                const previous = siblings.get(key);
                if (
                  previous !== undefined &&
                  "status" in previous.payload &&
                  "status" in event.payload &&
                  !isMutableArtifactStatus(String(previous.payload.status)) &&
                  isMutableArtifactStatus(String(event.payload.status))
                )
                  continue;
                if (previous === undefined) buffer.size += 1;
                else buffer.bytes -= bufferedSize(previous);
                buffer.bytes += bufferedSize(event);
                siblings.set(key, event);
              }
              if (buffer.size > 256 || buffer.bytes > 1_048_576) {
                buffer.groups.clear();
                buffer.sourceRuns.clear();
                buffer.size = 0;
                buffer.bytes = 0;
                buffer.failed = true;
                return yield* new ProviderEventPublishError({
                  providerSessionId: input.providerSessionId,
                  eventCount: 0,
                  cause: new Error("Provider request sibling buffer capacity exceeded"),
                });
              }
              return [];
            }
            if (group === undefined && !lifetimes.canRegister(token)) {
              removeBuffer(token, requestId);
              return yield* new ProviderEventPublishError({
                providerSessionId: input.providerSessionId,
                eventCount: 0,
                cause: new Error("Provider runtime request ownership capacity exceeded"),
              });
            }
            const request = persisted?.request ?? incomingRequest!;
            const requestEvent = normalized.find(
              (event) => event.type === "runtime-request.updated",
            );
            const siblings = [
              ...(siblingBuffers.get(token)?.groups.get(requestId)?.values() ?? []),
              ...normalized.filter((event) => event.type !== "runtime-request.updated"),
            ];
            const nextRequest =
              request.status !== "pending" ? request : (incomingRequest ?? request);
            const terminal =
              nextRequest.status !== "pending" ||
              nextRequest.responseCapability.type === "not_resumable";
            const terminalStatus =
              nextRequest.status === "resolved" ? ("completed" as const) : ("cancelled" as const);
            const now = yield* DateTime.now;
            if (terminal && group !== undefined) {
              const node = yield* projections.getNodeById(threadId, group.nodeId);
              if (
                node !== undefined &&
                node.runtimeRequestId === requestId &&
                !siblings.some(
                  (event) => event.type === "node.updated" && event.payload.id === node.id,
                )
              ) {
                siblings.push(
                  yield* makeDomainEvent(input, {
                    type: "node.updated",
                    threadId,
                    runId: node.runId,
                    nodeId: node.id,
                    payload: node,
                  }),
                );
              }
              for (const itemId of group.itemIds) {
                const item = yield* projections.getTurnItemById(threadId, itemId);
                if (
                  item !== undefined &&
                  (item.type === "approval_request" || item.type === "user_input_request") &&
                  item.requestId === requestId &&
                  !siblings.some(
                    (event) => event.type === "turn-item.updated" && event.payload.id === item.id,
                  )
                ) {
                  siblings.push(
                    yield* makeDomainEvent(input, {
                      type: "turn-item.updated",
                      threadId,
                      runId: item.runId,
                      nodeId: item.nodeId,
                      payload: item,
                    }),
                  );
                }
              }
            }
            const events: Array<OrchestrationV2DomainEvent> = [];
            if (
              requestEvent !== undefined &&
              (persisted === undefined || request.status === "pending")
            ) {
              events.push(
                requestEvent.type === "runtime-request.updated" &&
                  nextRequest.status === "pending" &&
                  nextRequest.responseCapability.type === "not_resumable"
                  ? {
                      ...requestEvent,
                      payload: { ...nextRequest, status: "cancelled", resolvedAt: now },
                    }
                  : requestEvent,
              );
            }
            const itemIds = new Set(group?.itemIds ?? []);
            // The request's node key can collide before its node event arrives.
            const nodeReservation = lifetimes.nodeRequestId(request.nodeId);
            if (nodeReservation !== undefined && nodeReservation !== requestId) return [];
            const existingRequestNode = yield* projections.getNodeById(threadId, request.nodeId);
            if (
              existingRequestNode !== undefined &&
              (persisted === undefined ||
                existingRequestNode.threadId !== threadId ||
                existingRequestNode.runtimeRequestId !== requestId)
            )
              return [];
            if (persisted === undefined) {
              const priorNodeRequest = yield* projections.getRuntimeRequestByNodeId(
                threadId,
                request.nodeId,
              );
              if (priorNodeRequest !== undefined) return [];
            }
            for (const event of siblings) {
              if (event.threadId !== threadId) return [];
              if (event.type === "node.updated") {
                if (
                  event.payload.id !== request.nodeId ||
                  event.payload.runtimeRequestId !== requestId
                )
                  return [];
                const existing = yield* projections.getNodeById(threadId, event.payload.id);
                if (
                  existing !== undefined &&
                  (existing.threadId !== threadId || existing.runtimeRequestId !== requestId)
                )
                  return [];
                if (
                  existing !== undefined &&
                  !isMutableArtifactStatus(existing.status) &&
                  (terminal || isMutableArtifactStatus(event.payload.status))
                )
                  continue;
                events.push(
                  terminal
                    ? {
                        ...event,
                        payload: {
                          ...event.payload,
                          status: terminalStatus,
                          completedAt: nextRequest.resolvedAt ?? now,
                        },
                      }
                    : event,
                );
              } else if (
                event.type === "turn-item.updated" &&
                (event.payload.type === "approval_request" ||
                  event.payload.type === "user_input_request")
              ) {
                if (
                  event.payload.requestId !== requestId ||
                  event.payload.nodeId !== request.nodeId
                )
                  return [];
                const existing = yield* projections.getTurnItemById(threadId, event.payload.id);
                if (
                  existing !== undefined &&
                  (existing.threadId !== threadId ||
                    (existing.type !== "approval_request" &&
                      existing.type !== "user_input_request") ||
                    existing.requestId !== requestId ||
                    existing.nodeId !== request.nodeId ||
                    existing.type !== event.payload.type)
                )
                  return [];
                itemIds.add(event.payload.id);
                if (itemIds.size > 64)
                  return yield* new ProviderEventPublishError({
                    providerSessionId: input.providerSessionId,
                    eventCount: 0,
                    cause: new Error("Provider request artifact identity capacity exceeded"),
                  });
                if (
                  existing !== undefined &&
                  !isMutableArtifactStatus(existing.status) &&
                  (terminal || isMutableArtifactStatus(event.payload.status))
                )
                  continue;
                events.push(
                  terminal
                    ? {
                        ...event,
                        payload: {
                          ...event.payload,
                          status: terminalStatus,
                          completedAt: nextRequest.resolvedAt ?? now,
                          updatedAt: now,
                        },
                      }
                    : event,
                );
              }
            }
            // Record the obligation before the commit can yield or be interrupted.
            // A failed write retains its identity, and cleanup re-reads under this
            // same lane rather than assuming a failed attempt committed nothing.
            lifetimes.register(token, {
              threadId,
              requestId,
              nodeId: request.nodeId,
              providerTurnId: request.providerTurnId,
              kind: request.kind,
              capability: capability!,
              itemIds: [...itemIds],
            });
            const storedEvents =
              persisted === undefined
                ? yield* publish(input, events, false)
                : (yield* eventSink.writeIfRuntimeRequestCurrent({
                    threadId,
                    expectedRequest: request,
                    events,
                  })).storedEvents;
            if (storedEvents.length > 0) removeBuffer(token, requestId);
            return storedEvents;
          }),
        ),
      );

    return ProviderEventIngestorV2.of({
      ...lifetimes.lifecycle,
      discardBufferedRequests: (token, runId) => {
        const buffer = siblingBuffers.get(token);
        if (buffer === undefined) return;
        for (const [requestId, sourceRunId] of buffer.sourceRuns) {
          if (sourceRunId === runId) removeBuffer(token, requestId);
        }
      },
      normalize,
      ingestNormalized: (input) =>
        Effect.gen(function* () {
          if (lifetimes.sessionId(input.runtimeLifetime) !== input.providerSessionId) return [];
          const requestId = linkedRequestId(input.event);
          if (requestId !== undefined) return yield* ingestRequestGroup(input, requestId);
          const events = yield* normalize(input);
          // Turn-terminal normalization may cancel user inputs. Only the
          // originating lifetime may contribute those mutable artifact groups.
          const ownedRequestIds = new Set(
            events.flatMap((event) =>
              event.type === "runtime-request.updated" &&
              lifetimes.group(input.runtimeLifetime, event.payload.id) !== undefined
                ? [event.payload.id]
                : [],
            ),
          );
          const cancellationEvents: Array<OrchestrationV2StoredEvent> = [];
          for (const requestEvent of events) {
            if (
              requestEvent.type !== "runtime-request.updated" ||
              !ownedRequestIds.has(requestEvent.payload.id)
            )
              continue;
            const group = lifetimes.group(input.runtimeLifetime, requestEvent.payload.id)!;
            const groupEvents = events.filter(
              (event) =>
                event === requestEvent ||
                (event.type === "node.updated" &&
                  event.payload.runtimeRequestId === group.requestId) ||
                (event.type === "turn-item.updated" &&
                  (event.payload.type === "approval_request" ||
                    event.payload.type === "user_input_request") &&
                  event.payload.requestId === group.requestId),
            );
            const result = yield* lifetimes.lifecycle.withLifetimeWrite(
              input.runtimeLifetime,
              eventSink.writeIfRuntimeRequestCurrent({
                threadId: group.threadId,
                expectedRequest: { ...requestEvent.payload, status: "pending" },
                events: groupEvents,
              }),
            );
            cancellationEvents.push(...result.storedEvents);
          }
          const filtered = events.filter(
            (event) =>
              event.type !== "runtime-request.updated" &&
              !(event.type === "node.updated" && event.payload.runtimeRequestId !== null) &&
              !(
                event.type === "turn-item.updated" &&
                (event.payload.type === "approval_request" ||
                  event.payload.type === "user_input_request")
              ),
          );
          if (input.event.type === "provider_session.updated") {
            return (
              (yield* lifetimes.lifecycle.withSessionWrite(
                input.runtimeLifetime,
                Effect.suspend(() =>
                  lifetimes.isActive(input.runtimeLifetime)
                    ? publish(input, filtered)
                    : Effect.succeed([]),
                ),
              )) ?? []
            );
          }
          // Ordinary transcript updates remain allowed after retirement, but
          // may not steal an identity already assigned to a request artifact.
          if (input.event.type === "node.updated" || input.event.type === "turn_item.updated") {
            return yield* lifetimes.withAdmission(
              Effect.gen(function* () {
                if (input.event.type === "node.updated") {
                  const existing = yield* projections.getNodeById(
                    input.event.node.threadId,
                    input.event.node.id,
                  );
                  if (existing?.runtimeRequestId != null) return [];
                  if (lifetimes.nodeRequestId(input.event.node.id) !== undefined) return [];
                  if (existing === undefined) {
                    const reservation = yield* projections.getRuntimeRequestByNodeId(
                      input.event.node.threadId,
                      input.event.node.id,
                    );
                    if (reservation !== undefined) return [];
                  }
                } else if (input.event.type === "turn_item.updated") {
                  const existing = yield* projections.getTurnItemById(
                    input.event.turnItem.threadId,
                    input.event.turnItem.id,
                  );
                  if (
                    existing?.type === "approval_request" ||
                    existing?.type === "user_input_request"
                  )
                    return [];
                }
                return yield* publish(input, filtered);
              }),
            );
          }
          return [...cancellationEvents, ...(yield* publish(input, filtered))];
        }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderEventPublishError({
                providerSessionId: input.providerSessionId,
                eventCount: 1,
                cause,
              }),
          ),
          Effect.flatMap((storedEvents) =>
            storedEvents.length === 0 || input.event.type !== "subagent.updated"
              ? Effect.succeed(storedEvents)
              : syncSubagentThreadModel(input, input.event.subagent).pipe(
                  Effect.map((synced) => [...storedEvents, ...synced]),
                  Effect.mapError(
                    (cause) =>
                      new ProviderEventPublishError({
                        providerSessionId: input.providerSessionId,
                        eventCount: 1,
                        cause,
                      }),
                  ),
                ),
          ),
          Effect.tap((storedEvents) =>
            Effect.gen(function* () {
              if (storedEvents.length === 0 || input.event.type !== "provider_turn.updated") return;
              const providerTurn = input.event.providerTurn;
              if (
                providerTurn.status !== "completed" &&
                providerTurn.status !== "failed" &&
                providerTurn.status !== "interrupted" &&
                providerTurn.status !== "cancelled"
              )
                return;
              const key = `${input.providerInstanceId}:${providerTurn.id}`;
              if (completedTurnAnalytics.has(key)) return;
              completedTurnAnalytics.add(key);
              if (completedTurnAnalytics.size > 4096) {
                const oldest = completedTurnAnalytics.values().next().value;
                if (oldest !== undefined) completedTurnAnalytics.delete(oldest);
              }
              yield* analytics.record(
                providerTurnAnalyticsProperties({
                  driver: input.event.driver,
                  providerTurn,
                  ...(input.analyticsContext === undefined
                    ? {}
                    : { context: input.analyticsContext }),
                }),
              );
            }),
          ),
        ),
    });
  }),
);
