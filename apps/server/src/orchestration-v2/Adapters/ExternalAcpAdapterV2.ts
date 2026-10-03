/** Fork-only official ACP preview. Every native permission request needs a one-shot response. */
import {
  type ModelSelection,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type RuntimeRequestId,
} from "@t3tools/contracts";
import type * as Cause from "effect/Cause";
import type * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as EffectAcpErrors from "effect-acp/errors";
import { parsePermissionRequest } from "../../provider/acp/AcpRuntimeModel.ts";
import type * as AcpSessionRuntime from "../../provider/acp/AcpSessionRuntime.ts";
import {
  ExternalAcpResume,
  externalApprovalOptions,
  externalPermissionOutcome,
  matchesExternalAcpResume,
} from "../../provider/acp/ExternalAcpPolicy.ts";
import type * as IdAllocator from "../IdAllocator.ts";
import * as Adapter from "../ProviderAdapter.ts";

const decodeResume = Schema.decodeUnknownEffect(ExternalAcpResume);

const capabilities = {
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: false,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: true,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: true,
    emitsProposedPlan: true,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: false,
    supportsFullThreadHandoff: false,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "weak",
    nativeRequestIds: "weak",
  },
  runtimePolicy: { enforcement: "client-boundary" },
} satisfies OrchestrationV2ProviderCapabilities;

type NativeRuntime = AcpSessionRuntime.AcpSessionRuntime["Service"];
type ItemBase = Pick<
  Extract<OrchestrationV2TurnItem, { type: "assistant_message" }>,
  | "id"
  | "threadId"
  | "runId"
  | "nodeId"
  | "providerThreadId"
  | "providerTurnId"
  | "nativeItemRef"
  | "parentItemId"
  | "ordinal"
  | "status"
  | "title"
  | "startedAt"
  | "completedAt"
  | "updatedAt"
>;
interface ActiveTurn {
  readonly input: Adapter.ProviderAdapterV2TurnInput;
  readonly turn: OrchestrationV2ProviderTurn;
  readonly done: Deferred.Deferred<void>;
  readonly items: Map<string, OrchestrationV2TurnItem>;
  nextOrdinal: number;
  cancelling: boolean;
}

export function makeExternalAcpAdapterV2(options: {
  readonly driver: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly enabled: boolean;
  readonly home: string;
  readonly homePath: string;
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly crypto: Crypto.Crypto;
  readonly idAllocator: IdAllocator.IdAllocatorV2Shape;
  readonly openRuntime: (
    cwd: string,
    scope: Scope.Closeable,
    resume?: string,
    canonicalHome?: string,
  ) => Effect.Effect<NativeRuntime, EffectAcpErrors.AcpError>;
}): Adapter.ProviderAdapterV2Shape {
  const { driver, instanceId, idAllocator } = options;
  const invalid = (detail: string) => new Adapter.ProviderAdapterProtocolError({ driver, detail });
  const checkPolicy = (selection: ModelSelection, policy: Adapter.ProviderAdapterV2RuntimePolicy) =>
    Effect.gen(function* () {
      if (!options.enabled) return yield* invalid("Enable this provider instance first");
      if (policy.runtimeMode !== "approval-required")
        return yield* invalid("This preview only supports Supervised permissions");
      if (
        policy.approvalPolicy !== undefined ||
        policy.sandboxPolicy !== undefined ||
        policy.reasoningEffort !== undefined
      )
        return yield* invalid(
          "Runtime policy and reasoning overrides are not supported by this preview adapter",
        );
      if (policy.interactionMode !== "default")
        return yield* invalid("Plan-mode negotiation is not supported by this preview adapter");
      if (
        selection.instanceId !== instanceId ||
        selection.model !== "cli-default" ||
        (selection.options !== undefined && Object.keys(selection.options).length > 0)
      )
        return yield* invalid("Only the CLI configured model without overrides is supported");
      if (!policy.cwd || !options.path.isAbsolute(policy.cwd))
        return yield* invalid("An absolute existing workspace directory is required");
      if (options.homePath && !options.path.isAbsolute(options.homePath))
        return yield* invalid("CLI data directory must be absolute; ~ is not expanded");
      const cwd = yield* options.fileSystem
        .realPath(policy.cwd)
        .pipe(Effect.mapError(() => invalid("Workspace is unavailable")));
      const info = yield* options.fileSystem
        .stat(cwd)
        .pipe(Effect.mapError(() => invalid("Workspace is unavailable")));
      if (info.type !== "Directory") return yield* invalid("Workspace must be a directory");
      return cwd;
    });
  return {
    driver,
    instanceId,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: ({ target }) =>
      Effect.succeed(
        target.instanceId === instanceId &&
          target.model === "cli-default" &&
          (target.options === undefined || Object.keys(target.options).length === 0)
          ? { type: "apply_on_next_turn" as const }
          : { type: "reject" as const, reason: "Only the CLI configured model is supported." },
      ),
    openSession: (input) =>
      Effect.gen(function* () {
        const cwd = yield* checkPolicy(input.modelSelection, input.runtimePolicy);
        if (!options.home || !options.path.isAbsolute(options.home))
          return yield* invalid("The effective CLI home directory must be absolute");
        const home = yield* options.fileSystem
          .realPath(options.home)
          .pipe(
            Effect.mapError(() =>
              invalid(
                "The CLI home directory must already exist; configure it in the official CLI first",
              ),
            ),
          );
        const homeInfo = yield* options.fileSystem
          .stat(home)
          .pipe(Effect.mapError(() => invalid("The CLI home directory is unavailable")));
        if (homeInfo.type !== "Directory")
          return yield* invalid("The CLI home must be a directory");
        const expected = { version: 1 as const, driver, instanceId, cwd, home };
        const scope = yield* Effect.scope;
        const events = yield* Queue.unbounded<Adapter.ProviderAdapterV2Event, Cause.Done>();
        const lifecycle = yield* Semaphore.make(1);
        const pending = new Map<
          RuntimeRequestId,
          {
            readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
            readonly done: Deferred.Deferred<void>;
          }
        >();
        let active: ActiveTurn | undefined;
        let native: NativeRuntime | undefined;
        let nativeScope: Scope.Closeable | undefined;
        let thread: OrchestrationV2ProviderThread | undefined;
        let stopped = false;
        let broken = false;
        const createdAt = yield* DateTime.now;
        let providerSession: OrchestrationV2ProviderSession = {
          id: input.providerSessionId,
          driver,
          providerInstanceId: instanceId,
          cwd,
          model: "cli-default",
          status: "ready",
          capabilities,
          createdAt,
          updatedAt: createdAt,
          lastError: null,
        };
        const emit = (event: Adapter.ProviderAdapterV2Event) =>
          Queue.offer(events, event).pipe(Effect.asVoid);
        const sessionStatus = (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = null,
        ) =>
          Effect.gen(function* () {
            providerSession = {
              ...providerSession,
              status,
              lastError,
              updatedAt: yield* DateTime.now,
            };
            yield* emit({ type: "provider_session.updated", driver, providerSession });
          });
        const drainPermissions = Effect.gen(function* () {
          const requests = [...pending.values()];
          yield* Effect.forEach(
            requests,
            (request) => Deferred.succeed(request.decision, "cancel"),
            { discard: true },
          );
          yield* Effect.forEach(requests, (request) => Deferred.await(request.done), {
            discard: true,
          });
        });
        const itemBase = (ctx: ActiveTurn, key: string, now: DateTime.Utc): ItemBase => {
          const previous = ctx.items.get(key);
          const nativeItemId = `${instanceId}:${ctx.turn.id}:${key}`;
          return {
            id: idAllocator.derive.turnItemFromProviderItem({ driver, nativeItemId }),
            threadId: input.threadId,
            runId: ctx.input.runId,
            nodeId: ctx.input.rootNodeId,
            providerThreadId: ctx.input.providerThread.id,
            providerTurnId: ctx.turn.id,
            nativeItemRef: { driver, nativeId: nativeItemId, strength: "weak" },
            parentItemId: null,
            ordinal: previous?.ordinal ?? ctx.nextOrdinal++,
            status: "running",
            title: null,
            startedAt: previous?.startedAt ?? now,
            completedAt: null,
            updatedAt: now,
          };
        };
        const publishItem = (ctx: ActiveTurn, key: string, turnItem: OrchestrationV2TurnItem) =>
          Effect.gen(function* () {
            ctx.items.set(key, turnItem);
            yield* emit({ type: "turn_item.updated", driver, turnItem });
            if (turnItem.type === "assistant_message") {
              yield* emit({
                type: "message.updated",
                driver,
                message: {
                  id: turnItem.messageId,
                  threadId: input.threadId,
                  runId: ctx.input.runId,
                  nodeId: ctx.input.rootNodeId,
                  role: "assistant",
                  text: turnItem.text,
                  attachments: [],
                  streaming: turnItem.streaming,
                  createdAt: turnItem.startedAt ?? turnItem.updatedAt,
                  updatedAt: turnItem.updatedAt,
                  createdBy: "agent",
                  creationSource: "provider",
                },
              });
            }
          });
        const consume = (event: AcpSessionRuntime.AcpSessionRuntimeEvent) =>
          Effect.gen(function* () {
            if (event._tag === "EventStreamBarrier") {
              yield* Deferred.succeed(event.acknowledge, undefined);
              return;
            }
            if (stopped) return;
            if (event._tag === "ConnectionTerminated") {
              broken = true;
              yield* drainPermissions;
              yield* sessionStatus(
                "error",
                "ACP process disconnected; restart the session to recover.",
              );
              return;
            }
            const ctx = active;
            if (!ctx) return;
            const now = yield* DateTime.now;
            switch (event._tag) {
              case "ContentDelta":
              case "ThoughtDelta": {
                const key =
                  event._tag === "ThoughtDelta"
                    ? "reasoning"
                    : `assistant:${event.itemId ?? "default"}`;
                const previous = ctx.items.get(key);
                const text = (previous && "text" in previous ? previous.text : "") + event.text;
                const base = itemBase(ctx, key, now);
                yield* publishItem(
                  ctx,
                  key,
                  event._tag === "ThoughtDelta"
                    ? { ...base, type: "reasoning", text, streaming: true }
                    : {
                        ...base,
                        type: "assistant_message",
                        text,
                        streaming: true,
                        messageId: idAllocator.derive.messageFromProviderItem({
                          driver,
                          nativeItemId: String(base.id),
                        }),
                      },
                );
                break;
              }
              case "AssistantItemCompleted": {
                const key = `assistant:${event.itemId}`;
                const item = ctx.items.get(key);
                if (item?.type === "assistant_message")
                  yield* publishItem(ctx, key, {
                    ...item,
                    status: "completed",
                    streaming: false,
                    completedAt: now,
                    updatedAt: now,
                  });
                break;
              }
              case "ToolCallUpdated": {
                const tool = event.toolCall;
                const key = `tool:${tool.toolCallId}`;
                const status =
                  tool.status === "completed"
                    ? "completed"
                    : tool.status === "failed"
                      ? "failed"
                      : tool.status === "requiresAction"
                        ? "waiting"
                        : "running";
                yield* publishItem(ctx, key, {
                  ...itemBase(ctx, key, now),
                  type: "dynamic_tool",
                  status,
                  title: tool.title ?? tool.command ?? null,
                  toolName: tool.kind ?? null,
                  input: tool.data.rawInput ?? tool.command ?? tool.detail ?? null,
                  ...((tool.data.rawOutput ?? tool.data.content) === undefined
                    ? {}
                    : { output: tool.data.rawOutput ?? tool.data.content }),
                  completedAt: status === "completed" || status === "failed" ? now : null,
                });
                break;
              }
              case "PlanUpdated": {
                const plan = event.payload;
                if (plan.kind !== "items" && plan.kind !== "markdown") break;
                const key = `plan:${plan.nativePlanId}`;
                const base = itemBase(ctx, key, now);
                const previous = ctx.items.get(key);
                const planId =
                  previous && "planId" in previous
                    ? previous.planId
                    : yield* idAllocator.allocate
                        .plan({ threadId: input.threadId, runId: ctx.input.runId, driver })
                        .pipe(Effect.orDie);
                if (plan.kind === "items") {
                  const steps = plan.plan.map((step, index) => ({
                    id: `${planId}:${index}`,
                    text: step.step,
                    status: step.status === "inProgress" ? ("running" as const) : step.status,
                  }));
                  yield* emit({
                    type: "plan.updated",
                    driver,
                    plan: {
                      id: planId,
                      threadId: input.threadId,
                      runId: ctx.input.runId,
                      nodeId: ctx.input.rootNodeId,
                      kind: "todo_list",
                      status: "active",
                      steps,
                    },
                  });
                  yield* publishItem(ctx, key, { ...base, type: "todo_list", planId, steps });
                } else {
                  yield* emit({
                    type: "plan.updated",
                    driver,
                    plan: {
                      id: planId,
                      threadId: input.threadId,
                      runId: ctx.input.runId,
                      nodeId: ctx.input.rootNodeId,
                      kind: "proposed_plan",
                      status: "active",
                      markdown: plan.markdown,
                    },
                  });
                  yield* publishItem(ctx, key, {
                    ...base,
                    type: "proposed_plan",
                    planId,
                    markdown: plan.markdown,
                    streaming: false,
                  });
                }
                break;
              }
            }
          });
        const checkThread = (candidate: OrchestrationV2ProviderThread) =>
          Effect.gen(function* () {
            const cursor = yield* decodeResume(candidate.nativeMetadata?.externalAcpResume).pipe(
              Effect.mapError(() => invalid("Saved native session identity is missing or invalid")),
            );
            if (
              candidate.driver !== driver ||
              candidate.providerInstanceId !== instanceId ||
              candidate.appThreadId !== input.threadId ||
              candidate.nativeThreadRef?.driver !== driver ||
              candidate.nativeThreadRef.nativeId !== cursor.sessionId ||
              !matchesExternalAcpResume(cursor, expected)
            )
              return yield* invalid(
                "The saved session belongs to another driver, instance, workspace or CLI data directory",
              );
            return cursor;
          });
        const activate = (existing?: OrchestrationV2ProviderThread) =>
          Effect.gen(function* () {
            if (stopped || broken) return yield* invalid("Restart the disconnected session first");
            if (
              existing &&
              (existing.driver !== driver ||
                existing.providerInstanceId !== instanceId ||
                existing.appThreadId !== input.threadId)
            )
              return yield* invalid(
                "This provider thread belongs to another driver, instance or app thread",
              );
            // V2 owns a placeholder row before the first native session exists. A fresh-session
            // fallback deliberately clears nativeThreadRef; stale metadata cannot authorize resume.
            const cursor =
              existing?.nativeThreadRef == null ? undefined : yield* checkThread(existing);
            if (thread) {
              if (existing && thread.id !== existing.id)
                return yield* invalid("This runtime owns another native thread");
              return thread;
            }
            if (!existing && input.initialNativeThreadId !== undefined)
              return yield* invalid("Resume requires the saved native session identity");
            if (
              cursor !== undefined &&
              input.initialNativeThreadId !== undefined &&
              input.initialNativeThreadId !== cursor?.sessionId
            )
              return yield* invalid(
                "The requested native session does not match the saved identity",
              );
            const nextScope = yield* Scope.make();
            const result = yield* Effect.gen(function* () {
              const runtime = yield* options.openRuntime(cwd, nextScope, cursor?.sessionId, home);
              yield* runtime.handleRequestPermission((params) =>
                Effect.gen(function* () {
                  const ctx = active;
                  if (
                    !ctx ||
                    stopped ||
                    ctx.cancelling ||
                    params.sessionId !== thread?.nativeThreadRef?.nativeId
                  )
                    return { outcome: { outcome: "cancelled" as const } };
                  const parsed = parsePermissionRequest(params);
                  const requestId = yield* idAllocator.allocate
                    .runtimeRequest({
                      driver,
                      providerTurnId: ctx.turn.id,
                      nativeRequestId: params.toolCall.toolCallId,
                    })
                    .pipe(Effect.orDie);
                  const decision = yield* Deferred.make<ProviderApprovalDecision>();
                  const responseDone = yield* Deferred.make<void>();
                  const now = yield* DateTime.now;
                  const requestKind =
                    parsed.kind === "edit" || parsed.kind === "delete" || parsed.kind === "move"
                      ? "file-change"
                      : parsed.kind === "read" ||
                          parsed.kind === "search" ||
                          parsed.kind === "fetch"
                        ? "file-read"
                        : "command";
                  const nodeId = idAllocator.derive.approvalNode({ requestId });
                  const request: OrchestrationV2RuntimeRequest = {
                    id: requestId,
                    nodeId,
                    providerTurnId: ctx.turn.id,
                    nativeRequestRef: {
                      driver,
                      nativeId: params.toolCall.toolCallId,
                      strength: "weak",
                    },
                    kind: requestKind,
                    status: "pending",
                    responseCapability: {
                      type: "live",
                      providerSessionId: input.providerSessionId,
                    },
                    createdAt: now,
                    resolvedAt: null,
                  };
                  const key = `approval:${requestId}`;
                  const item: OrchestrationV2TurnItem = {
                    ...itemBase(ctx, key, now),
                    nodeId,
                    type: "approval_request",
                    requestId,
                    requestKind,
                    status: "waiting",
                    options: externalApprovalOptions(params.options),
                    prompt: parsed.detail ?? "Native CLI permission request",
                  };
                  const node = {
                    id: nodeId,
                    threadId: input.threadId,
                    runId: ctx.input.runId,
                    parentNodeId: ctx.input.rootNodeId,
                    rootNodeId: ctx.input.rootNodeId,
                    kind: "approval_request" as const,
                    status: "waiting" as const,
                    countsForRun: false,
                    providerThreadId: ctx.input.providerThread.id,
                    providerTurnId: ctx.turn.id,
                    nativeItemRef: request.nativeRequestRef,
                    runtimeRequestId: requestId,
                    checkpointScopeId: null,
                    startedAt: now,
                    completedAt: null,
                  };
                  pending.set(requestId, { decision, done: responseDone });
                  return yield* Effect.gen(function* () {
                    yield* emit({ type: "node.updated", driver, node });
                    yield* emit({
                      type: "runtime_request.updated",
                      driver,
                      threadId: input.threadId,
                      runtimeRequest: request,
                    });
                    yield* publishItem(ctx, key, item);
                    const answer = yield* Deferred.await(decision);
                    const outcome = externalPermissionOutcome(params.options, answer);
                    const effective = outcome.outcome === "cancelled" ? "cancel" : answer;
                    const resolvedAt = yield* DateTime.now;
                    yield* emit({
                      type: "node.updated",
                      driver,
                      node: {
                        ...node,
                        status:
                          effective === "cancel" || effective === "decline"
                            ? "cancelled"
                            : "completed",
                        completedAt: resolvedAt,
                      },
                    });
                    yield* emit({
                      type: "runtime_request.updated",
                      driver,
                      threadId: input.threadId,
                      runtimeRequest: {
                        ...request,
                        status: effective === "cancel" ? "cancelled" : "resolved",
                        decision: effective,
                        resolvedAt,
                      },
                    });
                    yield* publishItem(ctx, key, {
                      ...item,
                      status:
                        effective === "cancel" || effective === "decline"
                          ? "cancelled"
                          : "completed",
                      completedAt: resolvedAt,
                      updatedAt: resolvedAt,
                    });
                    return { outcome };
                  }).pipe(
                    Effect.ensuring(
                      Effect.gen(function* () {
                        pending.delete(requestId);
                        yield* Deferred.succeed(responseDone, undefined);
                      }),
                    ),
                  );
                }),
              );
              yield* runtime.getEvents().pipe(Stream.runForEach(consume), Effect.forkIn(nextScope));
              const started = yield* runtime.start().pipe(Effect.timeout("30 seconds"));
              if (driver === "mcode") {
                const configured = yield* runtime.setConfigOption("permissionMode", "default");
                const matches = configured.configOptions.filter(
                  (option) => option.id === "permissionMode",
                );
                const permission = matches[0];
                if (
                  matches.length !== 1 ||
                  permission?.type !== "select" ||
                  permission.category !== "_permission" ||
                  permission.currentValue !== "default"
                )
                  return yield* invalid(
                    "MiniMax Code did not confirm supervised permissions. No prompt was sent",
                  );
                if (started.sessionSetupResult.modes?.currentModeId !== "default")
                  return yield* invalid(
                    "This preview requires MiniMax Code's default work mode; switch out of Plan in the native CLI before resuming",
                  );
              }
              const now = yield* DateTime.now;
              const next: OrchestrationV2ProviderThread = {
                ...(existing ?? {
                  id: idAllocator.derive.providerThread({
                    driver,
                    providerInstanceId: instanceId,
                    nativeThreadId: started.sessionId,
                  }),
                  driver,
                  providerInstanceId: instanceId,
                  appThreadId: input.threadId,
                  ownerNodeId: null,
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  contextUsage: null,
                  createdAt: now,
                }),
                providerSessionId: input.providerSessionId,
                status: "idle",
                updatedAt: now,
                nativeThreadRef: { driver, nativeId: started.sessionId, strength: "strong" },
                nativeMetadata: {
                  itemIdentityVersion: 2,
                  externalAcpResume: { ...expected, sessionId: started.sessionId },
                },
              };
              native = runtime;
              nativeScope = nextScope;
              thread = next;
              yield* emit({ type: "provider_thread.updated", driver, providerThread: next });
              return next;
            }).pipe(
              Effect.onError(() => Scope.close(nextScope, Exit.void)),
              Effect.mapError((cause) =>
                cause._tag === "ProviderAdapterProtocolError"
                  ? cause
                  : invalid(
                      "Native session failed. Check official CLI login/profile and model configuration",
                    ),
              ),
            );
            return result;
          });
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            stopped = true;
            if (active) active.cancelling = true;
            yield* drainPermissions;
            if (active && native) yield* native.cancel.pipe(Effect.ignore);
            if (nativeScope) yield* Scope.close(nativeScope, Exit.void);
            yield* Queue.end(events);
          }),
        );
        const validateCurrent = (
          selection: ModelSelection,
          policy: Adapter.ProviderAdapterV2RuntimePolicy,
        ) =>
          checkPolicy(selection, policy).pipe(
            Effect.flatMap((nextCwd) =>
              nextCwd === cwd
                ? Effect.void
                : Effect.fail(invalid("Stop the session before changing workspace")),
            ),
          );
        return {
          driver,
          instanceId,
          providerSessionId: input.providerSessionId,
          providerSession,
          events: Stream.fromQueue(events),
          ensureThread: (request) =>
            lifecycle.withPermits(1)(
              Effect.gen(function* () {
                if (request.threadId !== input.threadId)
                  return yield* invalid("This runtime belongs to another app thread");
                yield* validateCurrent(request.modelSelection, request.runtimePolicy);
                return yield* activate(request.existingProviderThread);
              }),
            ),
          resumeThread: (request) =>
            lifecycle.withPermits(1)(
              Effect.gen(function* () {
                if (request.threadId !== undefined && request.threadId !== input.threadId)
                  return yield* invalid("This runtime belongs to another app thread");
                yield* validateCurrent(
                  request.modelSelection ?? input.modelSelection,
                  request.runtimePolicy ?? input.runtimePolicy,
                );
                return yield* activate(request.providerThread);
              }),
            ),
          startTurn: (request) =>
            lifecycle.withPermits(1)(
              Effect.gen(function* () {
                yield* validateCurrent(request.modelSelection, request.runtimePolicy);
                yield* checkThread(request.providerThread);
                if (
                  request.threadId !== input.threadId ||
                  !native ||
                  !thread ||
                  request.providerThread.id !== thread.id ||
                  stopped ||
                  broken
                )
                  return yield* invalid("Start or resume this native session before sending");
                if (active)
                  return yield* invalid(
                    "A turn is already running. Cancel it or wait; mid-turn steering is not supported",
                  );
                if (!request.message.text.trim())
                  return yield* invalid("A text prompt is required");
                if (request.message.attachments.length)
                  return yield* invalid("Attachments are not supported by this preview adapter");
                const runtime = native;
                const nativeTurnId = `${instanceId}:${yield* options.crypto.randomUUIDv4.pipe(Effect.orDie)}`;
                const done = yield* Deferred.make<void>();
                const dispatched = yield* Deferred.make<void>();
                const startedAt = yield* DateTime.now;
                const ctx: ActiveTurn = {
                  input: request,
                  done,
                  items: new Map(),
                  nextOrdinal: 1,
                  cancelling: false,
                  turn: {
                    id: idAllocator.derive.providerTurn({ driver, nativeTurnId }),
                    providerThreadId: thread.id,
                    nodeId: request.rootNodeId,
                    runAttemptId: request.attemptId,
                    nativeTurnRef: { driver, nativeId: nativeTurnId, strength: "weak" },
                    ordinal: request.providerTurnOrdinal,
                    status: "running",
                    startedAt,
                    completedAt: null,
                  },
                };
                active = ctx;
                const run = Effect.gen(function* () {
                  yield* sessionStatus("running");
                  yield* emit({
                    type: "provider_turn.updated",
                    driver,
                    threadId: input.threadId,
                    providerTurn: ctx.turn,
                  });
                  const result = yield* runtime
                    .prompt(
                      { prompt: [{ type: "text", text: request.message.text }] },
                      { dispatched },
                    )
                    .pipe(Effect.exit);
                  yield* runtime.drainEvents;
                  yield* drainPermissions;
                  const completedAt = yield* DateTime.now;
                  const status = Exit.isFailure(result)
                    ? "failed"
                    : result.value.stopReason === "cancelled" || ctx.cancelling
                      ? "cancelled"
                      : "completed";
                  for (const [key, item] of ctx.items) {
                    if (
                      item.status === "completed" ||
                      item.status === "failed" ||
                      item.status === "cancelled"
                    )
                      continue;
                    yield* publishItem(ctx, key, {
                      ...item,
                      status,
                      completedAt,
                      updatedAt: completedAt,
                      ...("streaming" in item ? { streaming: false } : {}),
                    });
                  }
                  yield* emit({
                    type: "provider_turn.updated",
                    driver,
                    threadId: input.threadId,
                    providerTurn: { ...ctx.turn, status, completedAt },
                  });
                  // Clear ownership before publishing the terminal event so a queued next turn cannot race it.
                  active = undefined;
                  if (!broken && !stopped) yield* sessionStatus("ready");
                  const common = {
                    type: "turn.terminal" as const,
                    driver,
                    providerThreadId: request.providerThread.id,
                    providerTurnId: ctx.turn.id,
                    runOrdinal: request.runOrdinal,
                    threadDisposition: broken ? ("broken" as const) : ("reusable" as const),
                  };
                  yield* emit(
                    status === "failed"
                      ? {
                          ...common,
                          status,
                          failureItemOrdinal: ctx.nextOrdinal++,
                          failure: {
                            class: "transport_error",
                            message:
                              "ACP turn failed. Check CLI authentication, model configuration and connection.",
                            code: null,
                            retryable: null,
                          },
                        }
                      : { ...common, status, failure: null },
                  );
                }).pipe(
                  Effect.ensuring(
                    Effect.gen(function* () {
                      if (active === ctx) active = undefined;
                      yield* drainPermissions;
                      yield* Deferred.succeed(done, undefined);
                    }),
                  ),
                );
                yield* run.pipe(Effect.forkIn(scope));
                yield* Effect.raceFirst(Deferred.await(dispatched), Deferred.await(done));
              }),
            ),
          steerTurn: ({ providerThread }) =>
            Effect.fail(
              new Adapter.ProviderAdapterSteerRunUnsupportedError({
                driver,
                providerThreadId: providerThread.id,
              }),
            ),
          interruptTurn: (request) =>
            Effect.gen(function* () {
              yield* checkThread(request.providerThread);
              const ctx = active;
              if (!ctx) return;
              if (
                ctx.turn.id !== request.providerTurnId ||
                ctx.input.providerThread.id !== request.providerThread.id
              )
                return yield* invalid("The requested turn is no longer active");
              ctx.cancelling = true;
              yield* drainPermissions;
              yield* native!.cancel.pipe(
                Effect.mapError(() =>
                  invalid("Cancellation did not settle; restart the native session"),
                ),
              );
              yield* Deferred.await(ctx.done).pipe(
                Effect.timeout("20 seconds"),
                Effect.mapError(() =>
                  invalid("Cancellation did not settle; restart the native session"),
                ),
              );
            }),
          respondToRuntimeRequest: (request) =>
            Effect.gen(function* () {
              const pendingRequest = pending.get(request.requestId);
              if (
                !pendingRequest ||
                stopped ||
                !(yield* Deferred.succeed(pendingRequest.decision, request.decision ?? "cancel"))
              )
                return yield* invalid(
                  "The approval expired, was already answered or belongs to another session",
                );
            }),
          readThreadSnapshot: () =>
            Effect.fail(
              invalid(
                "Native transcript export is unavailable; use the OpenT3Code conversation history",
              ),
            ),
          rollbackThread: () => Effect.fail(invalid("Native history rollback is unavailable")),
          forkThread: () => Effect.fail(invalid("Native conversation forks are unavailable")),
        } satisfies Adapter.ProviderAdapterV2SessionRuntime;
      }),
  };
}
