// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off
/** ZcodeDriver live bootstrap: disabled sanity + enabled probe with hard timeout. */
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "@effect/vitest";

import { ProviderDriverKind } from "@t3tools/contracts";
import { ZcodeDriver } from "./ZcodeDriver.ts";

const zcodeInstalled = NodeFS.existsSync("/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs");
const log = (...a: unknown[]) => console.log(new Date().toISOString().slice(11, 23), ...a);

const make = (enabled: boolean) =>
  ZcodeDriver.create({
    instanceId: "zcode" as never,
    displayName: "ZCode",
    accentColor: undefined,
    environment: [],
    enabled,
    config: ZcodeDriver.defaultConfig(),
  }).pipe(Effect.timeout("45 seconds"));

describe.skipIf(!zcodeInstalled)("ZcodeDriver live probe", () => {
  it.effect("disabled instance returns without probing", () =>
    Effect.gen(function* () {
      log("disabled: start");
      const instance = yield* make(false);
      const snapshot = yield* instance.snapshot.getSnapshot;
      log("disabled: snapshot", snapshot.status);
      expect(snapshot.status).toBe("disabled");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("enabled instance probes and reports ready", () =>
    Effect.gen(function* () {
      log("enabled: start");
      const instance = yield* make(true);
      log("enabled: create returned");
      const snapshot = yield* instance.snapshot.getSnapshot;
      log("enabled: snapshot", snapshot.status, "|", snapshot.message);
      expect(snapshot.driver).toBe(ProviderDriverKind.make("zcode"));
      expect(snapshot.status).toBe("ready");
      expect(snapshot.installed).toBe(true);
      expect(snapshot.message).toContain("ZCode Protocol handshake succeeded");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
