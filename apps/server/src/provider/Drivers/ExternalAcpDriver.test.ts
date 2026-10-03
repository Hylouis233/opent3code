// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - isolated subprocess protocol fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as EffectAcpErrors from "effect-acp/errors";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { makeExternalAcpAdapterV2 } from "../../orchestration-v2/Adapters/ExternalAcpAdapterV2.ts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import {
  ProviderAdapterV2Event,
  type ProviderAdapterV2TurnInput,
} from "../../orchestration-v2/ProviderAdapter.ts";
import { MCodeDriver, DshDriver } from "./ExternalAcpDriver.ts";

const decodeEvent = Schema.decodeUnknownEffect(ProviderAdapterV2Event);

const fixture = String.raw`
import { createInterface } from "node:readline";
const send = (message) => process.stdout.write(JSON.stringify(message) + "\n");
const reply = (id, result) => send({jsonrpc:"2.0", id, result});
const update = (value) => send({jsonrpc:"2.0", method:"session/update", params:{sessionId:"session-1", update:value}});
let permissionMode = "bypassPermissions";
let workMode = "default";
const setup = () => ({sessionId:"session-1", modes:{currentModeId:workMode,availableModes:[{id:"default",name:"Default"},{id:"plan",name:"Plan"}]}, configOptions:[{id:"permissionMode", name:"Permissions", category:"_permission", type:"select", currentValue:permissionMode, options:[{value:"default", name:"Supervised"},{value:"bypassPermissions", name:"Full access"}]}]});
let active;
let sessionMethod;
const finish = (text, stopReason="end_turn") => {
  if (!active) return;
  if (text) update({sessionUpdate:"agent_message_chunk", content:{type:"text", text}});
  reply(active, {stopReason}); active=undefined;
};
createInterface({input:process.stdin}).on("line", (line) => {
  const message = JSON.parse(line);
  const {id, method, params} = message;
  if (!method && id === "native-permission") {
    update({sessionUpdate:"tool_call_update", toolCallId:"tool-1", status:"completed",content:[{type:"content",content:{type:"text",text:"tool output only"}}]});
    return finish(message.result?.outcome?.optionId === "opaque-allow-42" ? "approved" : "rejected");
  }
  if (method === "initialize") return reply(id, {protocolVersion:1,agentInfo:{name:"fixture",version:"1.0.0"},agentCapabilities:{sessionCapabilities:{resume:{}}}});
  if (method === "authenticate") return send({jsonrpc:"2.0",id,error:{code:-32601,message:"No authenticate: use saved CLI credentials"}});
  if (method === "session/new" || method === "session/resume") { sessionMethod=method; return reply(id, setup()); }
  if (method === "session/set_mode") { workMode=params.modeId; return reply(id,{}); }
  if (method === "session/set_config_option") { if(params.configId!=="permissionMode") return send({jsonrpc:"2.0",id,error:{code:-32602,message:"Unknown config option"}}); permissionMode=params.value; return reply(id,{configOptions:setup().configOptions}); }
  if (method === "session/cancel") return finish("", "cancelled");
  if (method === "session/prompt") {
    if (process.argv.includes("acp") && permissionMode !== "default") return send({jsonrpc:"2.0",id,error:{code:-32000,message:"Supervised mode was not negotiated"}});
    active=id;
    const text=params.prompt[0].text;
    if (text === "session-method") return finish(sessionMethod);
    if (text === "policy") return finish(JSON.stringify({args:process.argv.slice(2), permission:process.env.DSH_PERMISSION_MODE,telemetry:process.env.DSH_TELEMETRY_DISABLED,home:process.env.DSH_HOME}));
    if (text === "provider-cancel") return finish("", "cancelled");
    if (text === "todo-plan") {
      update({sessionUpdate:"plan",entries:[{content:"First step",status:"in_progress",priority:"medium"},{content:"Second step",status:"pending",priority:"medium"}]});
      update({sessionUpdate:"plan",entries:[{content:"First step",status:"completed",priority:"medium"},{content:"Second step",status:"completed",priority:"medium"}]});
      return finish("plan complete");
    }
    if (text === "crash") return process.exit(17);
    if (text === "hold") { update({sessionUpdate:"agent_message_chunk",content:{type:"text",text:"waiting"}}); return; }
    update({sessionUpdate:"agent_thought_chunk", content:{type:"text", text:"fixture reasoning"}});
    if (text === "permission") {
      update({sessionUpdate:"tool_call", toolCallId:"tool-1",kind:"edit",status:"pending",title:"Write a fixture file"});
      return send({jsonrpc:"2.0",id:"native-permission",method:"session/request_permission",params:{sessionId:"session-1",toolCall:{toolCallId:"tool-1",kind:"edit",title:"Write a fixture file"},options:[{optionId:"opaque-allow-42",kind:"allow_once",name:"Allow once"},{optionId:"opaque-deny-99",kind:"reject_once",name:"Reject"},{optionId:"dangerous-always",kind:"allow_always",name:"Always allow"}]}});
    }
    return finish("fixture answer");
  }
  if (id !== undefined) send({jsonrpc:"2.0",id,error:{code:-32601,message:"Unsupported fixture method"}});
});
`;

const kinds = ["mcode", "dsh"] as const;
interface NativeControls {
  readonly onResponse?: (input: {
    readonly requestId: string;
    readonly acknowledge: Effect.Effect<void>;
    readonly fail: (error: EffectAcpErrors.AcpError) => Effect.Effect<void>;
  }) => Effect.Effect<void>;
  readonly beforeCancel?: Effect.Effect<void>;
  readonly onClosed?: Effect.Effect<void>;
  readonly idAllocator?: IdAllocator.IdAllocatorV2Shape;
}
const harness = (
  kind: "mcode" | "dsh",
  source = fixture,
  enabled = true,
  homePath?: string,
  controls?: NativeControls,
) =>
  Effect.gen(function* () {
    const dir = yield* Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "opent3code-acp-")),
    );
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => NodeFSP.rm(dir, { recursive: true, force: true })),
    );
    const driver = kind === "mcode" ? MCodeDriver : DshDriver;
    const binaryPath = writeFakeCli({ directory: dir, name: kind, source });
    const instanceId = ProviderInstanceId.make(`${kind}-test`);
    const instance = yield* driver.create({
      instanceId,
      displayName: undefined,
      environment: [
        { name: "DSH_PERMISSION_MODE", value: "dangerously-skip-permissions", sensitive: false },
        { name: "DSH_TELEMETRY_DISABLED", value: "0", sensitive: false },
      ],
      enabled,
      config: { ...driver.defaultConfig(), enabled, binaryPath, homePath: homePath ?? dir },
    });
    const threadId = ThreadId.make(`${kind}-thread`);
    const modelSelection = { instanceId, model: "cli-default" };
    const runtimePolicy = {
      cwd: dir,
      runtimeMode: "approval-required" as const,
      interactionMode: "default" as const,
    };
    const start = {
      threadId,
      providerSessionId: ProviderSessionId.make(`${kind}-session`),
      modelSelection,
      runtimePolicy,
    };
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    // Controlled native-boundary callbacks exercise the write receipt rather than relying on
    // a fast subprocess to make handler completion and transport completion look identical.
    const adapter =
      controls === undefined
        ? instance.orchestrationAdapter
        : makeExternalAcpAdapterV2({
            driver: driver.driverKind,
            instanceId,
            enabled,
            home: homePath ?? dir,
            homePath: homePath ?? dir,
            crypto,
            fileSystem,
            path,
            idAllocator: controls.idAllocator ?? idAllocator,
            openRuntime: (cwd, nativeScope, resumeSessionId, canonicalHome, transport) =>
              Effect.gen(function* () {
                const native = yield* AcpSessionRuntime.make({
                  ...transport,
                  cwd,
                  clientInfo: { name: "external-acp-test", version: "1" },
                  spawn: {
                    command: binaryPath,
                    args: kind === "mcode" ? ["acp"] : ["--profile", "opent3code"],
                    cwd,
                    env: {
                      ...process.env,
                      ...(kind === "mcode"
                        ? { MINIMAX_DATA_DIR: canonicalHome }
                        : {
                            DSH_HOME: canonicalHome,
                            DSH_PERMISSION_MODE: "workspace-write",
                            DSH_TELEMETRY_DISABLED: "1",
                          }),
                    },
                  },
                  ...(resumeSessionId === undefined
                    ? {}
                    : { resumeSessionId, resumeMethod: "resume" as const }),
                  authenticateOnAuthRequired: false,
                  cancelBehavior: "wait-for-prompt",
                  cancelTimeout: "15 seconds",
                  onOutgoingResponse: (requestId) => {
                    if (
                      transport?.onOutgoingResponse === undefined ||
                      transport.onOutgoingResponseFailure === undefined
                    )
                      return Effect.die(
                        "External adapter must register transport response callbacks",
                      );
                    return (
                      controls.onResponse?.({
                        requestId,
                        acknowledge: transport.onOutgoingResponse(requestId),
                        fail: (error) => transport.onOutgoingResponseFailure!(requestId, error),
                      }) ?? transport.onOutgoingResponse(requestId)
                    );
                  },
                });
                yield* Effect.addFinalizer(() => controls.onClosed ?? Effect.void);
                return {
                  ...native,
                  cancel: (controls.beforeCancel ?? Effect.void).pipe(
                    Effect.andThen(native.cancel),
                  ),
                };
              }).pipe(
                Effect.provideService(Scope.Scope, nativeScope),
                Effect.provideService(Crypto.Crypto, crypto),
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
              ),
          });
    const open = Effect.gen(function* () {
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const runtime = yield* adapter
        .openSession(start)
        .pipe(Effect.provideService(Scope.Scope, scope));
      const received = yield* Queue.unbounded<ProviderAdapterV2Event>();
      yield* runtime.events.pipe(
        Stream.runForEach((event) => Queue.offer(received, event)),
        Effect.forkIn(scope),
      );
      const next = (
        predicate: (event: ProviderAdapterV2Event) => boolean,
      ): Effect.Effect<ProviderAdapterV2Event> =>
        Effect.gen(function* () {
          while (true) {
            const event = yield* Queue.take(received);
            yield* decodeEvent(event).pipe(Effect.orDie);
            if (predicate(event)) return event;
          }
        });
      const ensure = runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      const turn = (
        providerThread: OrchestrationV2ProviderThread,
        text: string,
        ordinal = 1,
      ): ProviderAdapterV2TurnInput => {
        const now = DateTime.makeUnsafe("2026-10-03T00:00:00.000Z");
        return {
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: threadId,
            projectId: ProjectId.make("test-project"),
            title: "ACP test",
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: runtimePolicy.runtimeMode,
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: providerThread.id,
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
          threadId,
          runId: RunId.make(`run:${ordinal}`),
          runOrdinal: ordinal,
          providerTurnOrdinal: ordinal,
          attemptId: RunAttemptId.make(`attempt:${ordinal}`),
          rootNodeId: NodeId.make(`node:${ordinal}`),
          providerThread,
          message: {
            createdBy: "user",
            creationSource: "web",
            messageId: MessageId.make(`message:${ordinal}`),
            text,
            attachments: [],
          },
          modelSelection,
          runtimePolicy,
        };
      };
      return { runtime, ensure, next, turn, close: Scope.close(scope, Exit.void) };
    });
    return { dir, instance, adapter, start, open, modelSelection, runtimePolicy };
  });
const terminal = (event: ProviderAdapterV2Event) => event.type === "turn.terminal";
const answer = (event: ProviderAdapterV2Event) =>
  event.type === "message.updated" &&
  event.message.role === "assistant" &&
  !event.message.streaming;

it.layer(Layer.merge(NodeServices.layer, IdAllocator.layer))(
  "OpenT3Code official ACP providers",
  (it) => {
    it.effect.each([
      [
        "unsettled approval response",
        fixture.replace("currentValue:permissionMode", 'currentValue:"bypassPermissions"'),
      ],
      ["missing approval identity", fixture.replace('id:"permissionMode"', 'id:"unrelated"')],
      [
        "resumed native Plan mode",
        fixture.replace('let workMode = "default"', 'let workMode = "plan"'),
      ],
    ])("mcode fails closed for %s", ([, source]) =>
      Effect.gen(function* () {
        const h = yield* harness("mcode", source);
        const s = yield* h.open;
        const result = yield* s.ensure.pipe(Effect.result);
        assert.equal(result._tag, "Failure");
      }),
    );

    it.effect.each(kinds)(
      "%s uses saved CLI credentials and streams schema-valid V2 events",
      (kind) =>
        Effect.gen(function* () {
          const h = yield* harness(kind);
          const snapshot = yield* h.instance.snapshot.getSnapshot;
          assert.equal(snapshot.installed, true);
          assert.equal(snapshot.auth.status, "unknown");
          const maintenance = yield* h.instance.snapshot.resolveMaintenance();
          assert.equal(maintenance.packageName, null);
          assert.equal(maintenance.update, null);
          const s = yield* h.open;
          const thread = yield* s.ensure;
          assert.equal(thread.providerInstanceId, `${kind}-test`);
          assert.equal(thread.nativeThreadRef?.nativeId, "session-1");
          yield* s.runtime.startTurn(s.turn(thread, "hello"));
          const thought = yield* s.next(
            (event) => event.type === "turn_item.updated" && event.turnItem.type === "reasoning",
          );
          assert.equal(
            thought.type === "turn_item.updated" &&
              thought.turnItem.type === "reasoning" &&
              thought.turnItem.text,
            "fixture reasoning",
          );
          const response = yield* s.next(answer);
          assert.equal(
            response.type === "message.updated" && response.message.text,
            "fixture answer",
          );
          const completed = yield* s.next(terminal);
          assert.equal(completed.type === "turn.terminal" && completed.status, "completed");
          yield* s.close;
        }),
    );

    it.effect.each(
      kinds.flatMap((kind) =>
        (["accept", "decline", "acceptAlways", "acceptForSession", "cancel"] as const).map(
          (decision) => ({
            kind,
            decision,
          }),
        ),
      ),
    )("$kind maps $decision only to an advertised one-shot permission", ({ kind, decision }) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "permission"));
        const request = yield* s.next(
          (event) =>
            event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
        );
        assert.equal(request.type, "turn_item.updated");
        if (request.type !== "turn_item.updated" || request.turnItem.type !== "approval_request")
          return;
        assert.isFalse(
          request.turnItem.options?.some((option) => option.decision === "acceptAlways") ?? true,
        );
        yield* s.runtime.respondToRuntimeRequest({
          requestId: request.turnItem.requestId,
          decision,
        });
        const response = yield* s.next(answer);
        assert.equal(
          response.type === "message.updated" && response.message.text,
          decision === "accept" ? "approved" : "rejected",
        );
        yield* s.next(terminal);
      }),
    );

    it.effect.each(
      kinds.flatMap((kind) => ["cancel", "close"].map((action) => ({ kind, action }))),
    )("$kind drains a pending approval on $action and rejects late replies", ({ kind, action }) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "permission"));
        const started = yield* s.next(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        const request = yield* s.next(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (request.type !== "runtime_request.updated" || started.type !== "provider_turn.updated")
          return;
        if (action === "close") yield* s.close;
        else {
          yield* s.runtime.interruptTurn({
            providerThread: thread,
            providerTurnId: started.providerTurn.id,
          });
          const resolved = yield* s.next(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status !== "pending",
          );
          assert.equal(
            resolved.type === "runtime_request.updated" && resolved.runtimeRequest.decision,
            "cancel",
          );
          yield* s.next(terminal);
        }
        const late = yield* s.runtime
          .respondToRuntimeRequest({ requestId: request.runtimeRequest.id, decision: "accept" })
          .pipe(Effect.flip);
        assert.equal(late._tag, "ProviderAdapterProtocolError");
      }),
    );

    it.effect.each(kinds)("%s completes a todo artifact only after every step completes", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "todo-plan"));
        const active = yield* s.next(
          (event) => event.type === "plan.updated" && event.plan.kind === "todo_list",
        );
        assert.equal(active.type === "plan.updated" && active.plan.status, "active");
        const completed = yield* s.next(
          (event) => event.type === "plan.updated" && event.plan.kind === "todo_list",
        );
        assert.equal(completed.type === "plan.updated" && completed.plan.status, "completed");
        const item = yield* s.next(
          (event) => event.type === "turn_item.updated" && event.turnItem.type === "todo_list",
        );
        assert.equal(item.type === "turn_item.updated" && item.turnItem.status, "completed");
        assert.isNotNull(item.type === "turn_item.updated" ? item.turnItem.completedAt : null);
        yield* s.next(terminal);
      }),
    );

    it.effect.each(kinds)("%s preserves provider-originated cancelled status", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "provider-cancel"));
        const cancelled = yield* s.next(terminal);
        assert.equal(cancelled.type === "turn.terminal" && cancelled.status, "cancelled");
      }),
    );

    it.effect.each(
      kinds.flatMap((kind) => ["end_turn", "crash"].map((outcome) => ({ kind, outcome }))),
    )("$kind gives explicit Stop priority over $outcome", ({ kind, outcome }) =>
      Effect.gen(function* () {
        const source = fixture.replace(
          'if (method === "session/cancel") return finish("", "cancelled");',
          outcome === "crash"
            ? 'if (method === "session/cancel") return process.exit(17);'
            : 'if (method === "session/cancel") return finish("", "end_turn");',
        );
        const h = yield* harness(kind, source);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "hold"));
        const started = yield* s.next(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        if (started.type !== "provider_turn.updated") return;
        yield* s.runtime
          .interruptTurn({ providerThread: thread, providerTurnId: started.providerTurn.id })
          .pipe(Effect.result);
        const interrupted = yield* s.next(terminal);
        assert.equal(interrupted.type === "turn.terminal" && interrupted.status, "interrupted");
        assert.equal(
          interrupted.type === "turn.terminal" && interrupted.threadDisposition,
          outcome === "crash" ? "broken" : "reusable",
        );
      }),
    );

    it.effect.each(
      kinds.flatMap((kind) =>
        ["cancel", "close"].flatMap((action) =>
          [false, true].map((answered) => ({ kind, action, answered })),
        ),
      ),
    )(
      "$kind waits for a written permission response before $action (answered=$answered)",
      ({ kind, action, answered }) =>
        Effect.gen(function* () {
          const written = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const order: Array<string> = [];
          const h = yield* harness(kind, fixture, true, undefined, {
            onResponse: ({ requestId, acknowledge }) =>
              Effect.gen(function* () {
                assert.equal(requestId, "native-permission");
                order.push("written");
                yield* Deferred.succeed(written, undefined);
                yield* Deferred.await(release);
                order.push("acknowledged");
                yield* acknowledge;
              }),
            beforeCancel: Effect.sync(() => {
              order.push("cancel");
            }),
            onClosed: Effect.sync(() => {
              order.push("closed");
            }),
          });
          const s = yield* h.open;
          const thread = yield* s.ensure;
          yield* s.runtime.startTurn(s.turn(thread, "permission"));
          const request = yield* s.next(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          );
          if (
            request.type !== "runtime_request.updated" ||
            request.runtimeRequest.providerTurnId === null
          )
            return;
          const response = answered
            ? yield* s.runtime
                .respondToRuntimeRequest({
                  requestId: request.runtimeRequest.id,
                  decision: "accept",
                })
                .pipe(Effect.result, Effect.forkChild)
            : undefined;
          if (answered) yield* Deferred.await(written);
          const actionFiber = yield* (
            action === "close"
              ? s.close
              : s.runtime.interruptTurn({
                  providerThread: thread,
                  providerTurnId: request.runtimeRequest.providerTurnId,
                })
          ).pipe(Effect.result, Effect.forkChild);
          yield* Deferred.await(written);
          yield* Effect.yieldNow;
          const blockedUntilAcknowledged =
            actionFiber.pollUnsafe() === undefined &&
            !order.includes("cancel") &&
            !order.includes("closed");
          yield* Deferred.succeed(release, undefined);
          const result = yield* Fiber.join(actionFiber);
          assert.equal(result._tag, "Success");
          if (response) assert.equal((yield* Fiber.join(response))._tag, "Success");
          assert.isTrue(blockedUntilAcknowledged);
          if (action === "close")
            assert.isTrue(order.indexOf("closed") > order.indexOf("acknowledged"));
          if (order.includes("cancel"))
            assert.isTrue(order.indexOf("cancel") > order.indexOf("acknowledged"));
        }),
    );

    it.effect.each(kinds)(
      "%s retires failed permission writes without leaving a running turn",
      (kind) =>
        Effect.gen(function* () {
          const closed = yield* Deferred.make<void>();
          const source = fixture.replace(
            'return finish(message.result?.outcome?.optionId === "opaque-allow-42" ? "approved" : "rejected");',
            "return;",
          );
          const h = yield* harness(kind, source, true, undefined, {
            onResponse: ({ fail }) =>
              fail(
                new EffectAcpErrors.AcpTransportError({
                  detail: "fixture permission write failed",
                  cause: undefined,
                }),
              ),
            onClosed: Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
          });
          const s = yield* h.open;
          const thread = yield* s.ensure;
          yield* s.runtime.startTurn(s.turn(thread, "permission"));
          const request = yield* s.next(
            (event) =>
              event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
          );
          if (request.type !== "runtime_request.updated") return;
          const response = yield* s.runtime
            .respondToRuntimeRequest({ requestId: request.runtimeRequest.id, decision: "accept" })
            .pipe(Effect.flip);
          assert.equal(response._tag, "ProviderAdapterProtocolError");
          const failed = yield* s.next(terminal);
          assert.equal(failed.type === "turn.terminal" && failed.status, "failed");
          assert.equal(failed.type === "turn.terminal" && failed.threadDisposition, "broken");
          yield* Deferred.await(closed);
          const refused = yield* s.runtime.startTurn(s.turn(thread, "next", 2)).pipe(Effect.flip);
          assert.equal(refused._tag, "ProviderAdapterProtocolError");
        }),
    );

    it.effect.each(
      kinds.flatMap((kind) => ["cancel", "close"].map((action) => ({ kind, action }))),
    )("$kind drains permission admission paused before $action", ({ kind, action }) =>
      Effect.gen(function* () {
        const allocating = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const ids = yield* IdAllocator.IdAllocatorV2;
        const order: Array<string> = [];
        const h = yield* harness(kind, fixture, true, undefined, {
          idAllocator: {
            ...ids,
            allocate: {
              ...ids.allocate,
              runtimeRequest: (input) =>
                Deferred.succeed(allocating, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(ids.allocate.runtimeRequest(input)),
                ),
            },
          },
          onResponse: ({ acknowledge }) =>
            Effect.sync(() => {
              order.push("acknowledged");
            }).pipe(Effect.andThen(acknowledge)),
          beforeCancel: Effect.sync(() => {
            order.push("cancel");
          }),
          onClosed: Effect.sync(() => {
            order.push("closed");
          }),
        });
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "permission"));
        const started = yield* s.next(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        if (started.type !== "provider_turn.updated") return;
        yield* Deferred.await(allocating);
        const operation = yield* (
          action === "close"
            ? s.close
            : s.runtime.interruptTurn({
                providerThread: thread,
                providerTurnId: started.providerTurn.id,
              })
        ).pipe(Effect.result, Effect.forkChild);
        yield* Effect.yieldNow;
        const blockedDuringAdmission = operation.pollUnsafe() === undefined && order.length === 0;
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* Fiber.join(operation))._tag, "Success");
        assert.isTrue(blockedDuringAdmission);
        assert.equal(order[0], "acknowledged");
      }),
    );

    it.effect.each(kinds)(
      "%s closes an unresponsive native prompt after the cancellation timeout",
      (kind) =>
        Effect.gen(function* () {
          const cancelling = yield* Deferred.make<void>();
          const closed = yield* Deferred.make<void>();
          const source = fixture.replace(
            'if (method === "session/cancel") return finish("", "cancelled");',
            'if (method === "session/cancel") return;',
          );
          const h = yield* harness(kind, source, true, undefined, {
            beforeCancel: Deferred.succeed(cancelling, undefined).pipe(Effect.asVoid),
            onClosed: Deferred.succeed(closed, undefined).pipe(Effect.asVoid),
          });
          const s = yield* h.open;
          const thread = yield* s.ensure;
          yield* s.runtime.startTurn(s.turn(thread, "hold"));
          yield* s.next((event) => event.type === "message.updated");
          const closing = yield* s.close.pipe(Effect.forkChild);
          yield* Deferred.await(cancelling);
          yield* TestClock.adjust("16 seconds");
          yield* Fiber.join(closing);
          assert.isTrue(yield* Deferred.isDone(closed));
        }),
    );

    it.effect.each(kinds)("%s rejects duplicate approval replies", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "permission"));
        const request = yield* s.next(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (request.type !== "runtime_request.updated") return;
        yield* s.runtime.respondToRuntimeRequest({
          requestId: request.runtimeRequest.id,
          decision: "decline",
        });
        const duplicate = yield* s.runtime
          .respondToRuntimeRequest({ requestId: request.runtimeRequest.id, decision: "accept" })
          .pipe(Effect.flip);
        assert.equal(duplicate._tag, "ProviderAdapterProtocolError");
        const response = yield* s.next(answer);
        assert.equal(response.type === "message.updated" && response.message.text, "rejected");
        yield* s.next(terminal);
      }),
    );

    it.effect.each(kinds)("%s preserves standard content-only tool output", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "permission"));
        const request = yield* s.next(
          (event) =>
            event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending",
        );
        if (request.type !== "runtime_request.updated") return;
        yield* s.runtime.respondToRuntimeRequest({
          requestId: request.runtimeRequest.id,
          decision: "accept",
        });
        const output = yield* s.next(
          (event) =>
            event.type === "turn_item.updated" &&
            event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "completed",
        );
        assert.equal(output.type, "turn_item.updated");
        if (output.type !== "turn_item.updated" || output.turnItem.type !== "dynamic_tool") return;
        assert.deepEqual(output.turnItem.output, [
          { type: "content", content: { type: "text", text: "tool output only" } },
        ]);
        yield* s.next(terminal);
      }),
    );

    it.effect.each(kinds)("%s drains cancellation before admitting the next turn", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "hold"));
        const started = yield* s.next(
          (event) =>
            event.type === "provider_turn.updated" && event.providerTurn.status === "running",
        );
        yield* s.next((event) => event.type === "message.updated");
        const busy = yield* s.runtime.startTurn(s.turn(thread, "second", 2)).pipe(Effect.flip);
        assert.equal(busy._tag, "ProviderAdapterProtocolError");
        if (started.type !== "provider_turn.updated") return;
        yield* s.runtime.interruptTurn({
          providerThread: thread,
          providerTurnId: started.providerTurn.id,
        });
        const cancelled = yield* s.next(terminal);
        assert.equal(cancelled.type === "turn.terminal" && cancelled.status, "interrupted");
        yield* s.runtime.startTurn(s.turn(thread, "next", 2));
        const completed = yield* s.next(terminal);
        assert.equal(completed.type === "turn.terminal" && completed.status, "completed");
      }),
    );

    it.effect.each(kinds)("%s preserves resume identity and rejects mismatched cursors", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const first = yield* h.open;
        const original = yield* first.ensure;
        yield* first.close;
        const second = yield* h.open;
        const cursor = original.nativeMetadata!.externalAcpResume!;
        for (const key of ["driver", "instanceId", "cwd", "home", "sessionId"] as const) {
          const wrong = yield* second.runtime
            .resumeThread({
              providerThread: {
                ...original,
                nativeMetadata: { externalAcpResume: { ...cursor, [key]: "other" } },
              },
            })
            .pipe(Effect.flip);
          assert.equal(wrong._tag, "ProviderAdapterProtocolError");
        }
        const resumed = yield* second.runtime.resumeThread({ providerThread: original });
        assert.deepEqual(resumed.nativeMetadata?.externalAcpResume, cursor);
        yield* second.runtime.startTurn(second.turn(resumed, "hello"));
        yield* second.next(terminal);
      }),
    );

    it.effect.each(kinds)("%s refuses unsupported permissions, content and rollback", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        for (const runtimeMode of ["full-access", "auto-accept-edits"] as const) {
          const result = yield* h.adapter
            .openSession({ ...h.start, runtimePolicy: { ...h.runtimePolicy, runtimeMode } })
            .pipe(Effect.flip);
          assert.equal(result._tag, "ProviderAdapterProtocolError");
        }
        const s = yield* h.open;
        const thread = yield* s.ensure;
        const turn = s.turn(thread, "hello");
        const plan = yield* s.runtime
          .startTurn({ ...turn, runtimePolicy: { ...h.runtimePolicy, interactionMode: "plan" } })
          .pipe(Effect.flip);
        assert.equal(plan._tag, "ProviderAdapterProtocolError");
        const attachment = yield* s.runtime
          .startTurn({
            ...turn,
            message: {
              ...turn.message,
              attachments: [
                {
                  type: "file",
                  id: "fixture-file",
                  name: "fixture.txt",
                  mimeType: "text/plain",
                  sizeBytes: 5,
                },
              ],
            },
          })
          .pipe(Effect.flip);
        assert.equal(attachment._tag, "ProviderAdapterProtocolError");
        const unsupported = yield* h.adapter.getCapabilities();
        assert.isFalse(unsupported.threads.canRollbackThread);
        assert.isFalse(unsupported.turns.supportsActiveSteering);
        assert.isFalse(unsupported.tools.supportsMcpTools);
      }),
    );

    it.effect.each(
      kinds.flatMap((kind) => [false, true].map((staleMetadata) => ({ kind, staleMetadata }))),
    )(
      "$kind adopts a fresh placeholder with stale metadata=$staleMetadata",
      ({ kind, staleMetadata }) =>
        Effect.gen(function* () {
          const h = yield* harness(kind);
          const first = yield* h.open;
          const original = yield* first.ensure;
          yield* first.close;
          const next = yield* h.open;
          const placeholder = {
            ...original,
            id: ProviderThreadId.make("fresh-placeholder"),
            nativeThreadRef: null,
            nativeMetadata: staleMetadata ? original.nativeMetadata : null,
          };
          const adopted = yield* next.runtime.ensureThread({
            threadId: h.start.threadId,
            modelSelection: h.modelSelection,
            runtimePolicy: h.runtimePolicy,
            existingProviderThread: placeholder,
          });
          assert.equal(adopted.id, placeholder.id);
          yield* next.runtime.startTurn(next.turn(adopted, "session-method"));
          const response = yield* next.next(answer);
          assert.equal(response.type === "message.updated" && response.message.text, "session/new");
          yield* next.next(terminal);
        }),
    );

    it.effect.each(kinds)("%s rejects a symlink-retargeted CLI home before native resume", (kind) =>
      Effect.gen(function* () {
        const root = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "opent3code-home-")),
        );
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
        );
        const one = NodePath.join(root, "one");
        const two = NodePath.join(root, "two");
        const link = NodePath.join(root, "home");
        yield* Effect.promise(async () => {
          await NodeFSP.mkdir(one);
          await NodeFSP.mkdir(two);
          await NodeFSP.symlink(one, link);
        });
        const h = yield* harness(kind, fixture, true, link);
        const first = yield* h.open;
        const original = yield* first.ensure;
        assert.equal(original.nativeMetadata?.externalAcpResume?.home, one);
        yield* first.close;
        const same = yield* h.open;
        const resumed = yield* same.runtime.resumeThread({ providerThread: original });
        assert.equal(resumed.nativeMetadata?.externalAcpResume?.home, one);
        yield* same.close;
        yield* Effect.promise(async () => {
          await NodeFSP.unlink(link);
          await NodeFSP.symlink(two, link);
        });
        const different = yield* h.open;
        const refused = yield* different.runtime
          .resumeThread({ providerThread: original })
          .pipe(Effect.flip);
        assert.equal(refused._tag, "ProviderAdapterProtocolError");
      }),
    );

    it.effect.each(kinds)("%s reports process crashes as failed V2 turns", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "crash"));
        const completed = yield* s.next(terminal);
        assert.equal(completed.type === "turn.terminal" && completed.status, "failed");
      }),
    );

    it.effect("dsh pins workspace-write, telemetry off, explicit profile and configured home", () =>
      Effect.gen(function* () {
        const h = yield* harness("dsh");
        const s = yield* h.open;
        const thread = yield* s.ensure;
        yield* s.runtime.startTurn(s.turn(thread, "policy"));
        const response = yield* s.next(answer);
        if (response.type !== "message.updated") return;
        assert.deepEqual(JSON.parse(response.message.text), {
          args: ["--profile", "opent3code"],
          permission: "workspace-write",
          telemetry: "1",
          home: h.dir,
        });
        yield* s.next(terminal);
      }),
    );

    it.effect.each(kinds)("%s does not authenticate when the saved CLI login is missing", (kind) =>
      Effect.gen(function* () {
        const source = fixture
          .replace(
            "import { createInterface }",
            'import { writeFileSync } from "node:fs";\nimport { createInterface }',
          )
          .replace(
            'if (method === "authenticate") return',
            'if (method === "authenticate") writeFileSync("unexpected-auth", "called");\n  if (method === "authenticate") return',
          )
          .replace(
            "return reply(id, setup());",
            'return send({jsonrpc:"2.0",id,error:{code:-32000,message:"Authentication required"}});',
          );
        const h = yield* harness(kind, source);
        const s = yield* h.open;
        const result = yield* s.ensure.pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        const files = yield* Effect.promise(() => NodeFSP.readdir(h.dir));
        assert.isFalse(files.includes("unexpected-auth"));
      }),
    );

    it.effect.each(kinds)("%s stays disabled without probing or opening a session", (kind) =>
      Effect.gen(function* () {
        const h = yield* harness(kind, 'throw new Error("must never launch")', false);
        assert.isFalse((kind === "mcode" ? MCodeDriver : DshDriver).defaultConfig().enabled);
        const snapshot = yield* h.instance.snapshot.getSnapshot;
        assert.equal(snapshot.status, "disabled");
        const result = yield* h.open.pipe(Effect.flip);
        assert.equal(result._tag, "ProviderAdapterProtocolError");
      }),
    );
  },
);
