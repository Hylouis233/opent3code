// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - isolated subprocess protocol fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderInstanceId,
  ThreadId,
  ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { writeFakeCli } from "../../testUtils/fakeCli.ts";
import { MCodeDriver, DshDriver } from "./ExternalAcpDriver.ts";

const decodeEvent = Schema.decodeUnknownEffect(ProviderRuntimeEvent);

const fixture = String.raw`
import { createInterface } from "node:readline";
const send = (message) => process.stdout.write(JSON.stringify(message) + "\n");
const reply = (id, result) => send({jsonrpc:"2.0", id, result});
const update = (value) => send({jsonrpc:"2.0", method:"session/update", params:{sessionId:"session-1", update:value}});
let mode = "bypassPermissions";
const setup = () => ({sessionId:"session-1", configOptions:[{id:"mode", name:"Permissions", category:"mode", type:"select", currentValue:mode, options:[{value:"default", name:"Supervised"},{value:"bypassPermissions", name:"Full access"}]}]});
let active;
const finish = (text, stopReason="end_turn") => {
  if (!active) return;
  if (text) update({sessionUpdate:"agent_message_chunk", content:{type:"text", text}});
  reply(active, {stopReason}); active=undefined;
};
createInterface({input:process.stdin}).on("line", (line) => {
  const message = JSON.parse(line);
  const {id, method, params} = message;
  if (!method && id === "native-permission") {
    update({sessionUpdate:"tool_call_update", toolCallId:"tool-1", status:"completed"});
    return finish(message.result?.outcome?.optionId === "opaque-allow-42" ? "approved" : "rejected");
  }
  if (method === "initialize") return reply(id, {protocolVersion:1,agentInfo:{name:"fixture",version:"1.0.0"},agentCapabilities:{sessionCapabilities:{resume:{}}}});
  if (method === "authenticate") return send({jsonrpc:"2.0",id,error:{code:-32601,message:"No authenticate: use saved CLI credentials"}});
  if (method === "session/new" || method === "session/resume") return reply(id, setup());
  if (method === "session/set_config_option") { mode=params.value; return reply(id,{configOptions:setup().configOptions}); }
  if (method === "session/cancel") return finish("", "cancelled");
  if (method === "session/prompt") {
    if (process.argv.includes("acp") && mode !== "default") return send({jsonrpc:"2.0",id,error:{code:-32000,message:"Supervised mode was not negotiated"}});
    active=id;
    const text=params.prompt[0].text;
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

const harness = (kind: "mcode" | "dsh") =>
  Effect.gen(function* () {
    const dir = yield* Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "opent3code-acp-")),
    );
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => NodeFSP.rm(dir, { recursive: true, force: true })),
    );
    const driver = kind === "mcode" ? MCodeDriver : DshDriver;
    const binaryPath = writeFakeCli({ directory: dir, name: kind, source: fixture });
    const instance = yield* driver.create({
      instanceId: ProviderInstanceId.make(`${kind}-test`),
      displayName: undefined,
      environment: [],
      enabled: true,
      config: { ...driver.defaultConfig(), enabled: true, binaryPath, homePath: dir },
    });
    const received = yield* Queue.unbounded<ProviderRuntimeEvent>();
    yield* instance.adapter.streamEvents.pipe(
      Stream.runForEach((event) => Queue.offer(received, event)),
      Effect.forkChild({ startImmediately: true }),
    );
    const next = (type: ProviderRuntimeEvent["type"]): Effect.Effect<ProviderRuntimeEvent> =>
      Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(received);
          yield* decodeEvent(event).pipe(Effect.orDie);
          if (event.type === type) return event;
        }
      });
    const threadId = ThreadId.make(`${kind}-thread`);
    const start = { threadId, cwd: dir, runtimeMode: "approval-required" as const };
    return { dir, instance, adapter: instance.adapter, threadId, start, next, received };
  });

it.layer(NodeServices.layer)("OpenT3Code official ACP providers", (it) => {
  for (const kind of ["mcode", "dsh"] as const) {
    it.effect(
      `${kind} uses saved CLI credentials, creates a scoped session and streams a complete turn`,
      () =>
        Effect.gen(function* () {
          const h = yield* harness(kind);
          const snapshot = yield* h.instance.snapshot.getSnapshot;
          assert.equal(snapshot.installed, true);
          assert.equal(snapshot.auth.status, "unknown");
          const session = yield* h.adapter.startSession(h.start);
          assert.equal(session.status, "ready");
          assert.equal(session.providerInstanceId, `${kind}-test`);
          yield* h.adapter.sendTurn({ threadId: h.threadId, input: "hello" });
          const thought = yield* h.next("content.delta");
          assert.equal(
            thought.type === "content.delta" && thought.payload.streamKind,
            "reasoning_text",
          );
          const response = yield* h.next("content.delta");
          assert.equal(
            response.type === "content.delta" && response.payload.delta,
            "fixture answer",
          );
          const completed = yield* h.next("turn.completed");
          assert.equal(completed.type === "turn.completed" && completed.payload.state, "completed");
          yield* h.adapter.stopSession(h.threadId);
          assert.isFalse(yield* h.adapter.hasSession(h.threadId));
        }),
    );
    for (const [decision, response] of [
      ["accept", "approved"],
      ["decline", "rejected"],
      ["acceptAlways", "rejected"],
      ["cancel", "rejected"],
    ] as const) {
      it.effect(`${kind} maps ${decision} only to an advertised one-shot permission`, () =>
        Effect.gen(function* () {
          const h = yield* harness(kind);
          yield* h.adapter.startSession(h.start);
          yield* h.adapter.sendTurn({ threadId: h.threadId, input: "permission" });
          const request = yield* h.next("request.opened");
          assert.isDefined(request.requestId);
          if (request.type === "request.opened")
            assert.isFalse(
              request.payload.options?.some((option) => option.decision === "acceptAlways") ?? true,
            );
          yield* h.adapter.respondToRequest(
            h.threadId,
            ApprovalRequestId.make(request.requestId!),
            decision,
          );
          // Skip the thought emitted before the approval and wait for the answer.
          const result = yield* h.next("content.delta");
          assert.equal(result.type === "content.delta" && result.payload.delta, response);
          yield* h.next("turn.completed");
        }),
      );
    }
    it.effect(`${kind} cancels the actual pending prompt and rejects concurrent steering`, () =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        yield* h.adapter.startSession(h.start);
        const turn = yield* h.adapter.sendTurn({ threadId: h.threadId, input: "hold" });
        yield* h.next("content.delta");
        const busy = yield* h.adapter
          .sendTurn({ threadId: h.threadId, input: "second" })
          .pipe(Effect.flip);
        assert.equal(busy._tag, "ProviderAdapterValidationError");
        yield* h.adapter.interruptTurn(h.threadId, turn.turnId);
        const completed = yield* h.next("turn.completed");
        assert.equal(completed.type === "turn.completed" && completed.payload.state, "cancelled");
      }),
    );
    it.effect(`${kind} preserves native resume identity and rejects cross-instance cursors`, () =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        const original = yield* h.adapter.startSession(h.start);
        yield* h.adapter.stopSession(h.threadId);
        const cursor = original.resumeCursor as Record<string, unknown>;
        const wrong = yield* h.adapter
          .startSession({ ...h.start, resumeCursor: { ...cursor, instanceId: "other" } })
          .pipe(Effect.flip);
        assert.equal(wrong._tag, "ProviderAdapterValidationError");
        const resumed = yield* h.adapter.startSession({
          ...h.start,
          resumeCursor: original.resumeCursor,
        });
        assert.deepEqual(resumed.resumeCursor, original.resumeCursor);
      }),
    );
    it.effect(
      `${kind} never upgrades requested permissions or silently drops unsupported content`,
      () =>
        Effect.gen(function* () {
          const h = yield* harness(kind);
          const refused = yield* h.adapter
            .startSession({ ...h.start, runtimeMode: "full-access" })
            .pipe(Effect.flip);
          assert.equal(refused._tag, "ProviderAdapterValidationError");
          yield* h.adapter.startSession(h.start);
          const plan = yield* h.adapter
            .sendTurn({ threadId: h.threadId, input: "plan", interactionMode: "plan" })
            .pipe(Effect.flip);
          assert.equal(plan._tag, "ProviderAdapterValidationError");
          const rollback = yield* h.adapter.rollbackThread(h.threadId, 1).pipe(Effect.flip);
          assert.equal(rollback._tag, "ProviderAdapterValidationError");
        }),
    );
    it.effect(`${kind} reports a process crash instead of a successful answer`, () =>
      Effect.gen(function* () {
        const h = yield* harness(kind);
        yield* h.adapter.startSession(h.start);
        yield* h.adapter.sendTurn({ threadId: h.threadId, input: "crash" });
        const completed = yield* h.next("turn.completed");
        assert.equal(completed.type === "turn.completed" && completed.payload.state, "failed");
      }),
    );
  }
});
