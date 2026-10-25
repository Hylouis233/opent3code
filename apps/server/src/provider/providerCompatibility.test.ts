import { assert, describe, it } from "@effect/vitest";
import {
  ORCHESTRATION_PROTOCOL_VERSION,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as ServerConfig from "../config.ts";
import * as ModelManifest from "./ModelManifest.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import * as Schema from "effect/Schema";
import serverPackage from "../../package.json" with { type: "json" };
import {
  applyProviderCompatibility,
  ProviderCompatibilityPolicy,
  resolveProviderCompatibility,
} from "./providerCompatibility.ts";

const driver = ProviderDriverKind.make("codex");
const policy: ProviderCompatibilityPolicy = {
  driver,
  t3CodeRange: ">=0.0.0",
  recommendedVersion: "2.0.0",
  recommendedRange: ">=2.0.0 <3.0.0",
  ranges: [
    { range: "<1.0.0", status: "broken" },
    { range: ">=1.0.0 <1.5.0", status: "unsupported" },
    { range: ">=1.5.0 <2.0.0", status: "graceful" },
    { range: ">=2.0.0 <3.0.0", status: "supported" },
  ],
};
const provider: ServerProvider = {
  driver,
  instanceId: ProviderInstanceId.make("codex-work"),
  enabled: true,
  installed: true,
  version: "0.9.0",
  status: "error",
  message: "Authentication failed",
  checkedAt: "2026-09-22T00:00:00Z",
  auth: { status: "unauthenticated" },
  models: [],
  skills: [],
  slashCommands: [],
};

const V2_RELEASE = "0.0.46";

describe("provider compatibility", () => {
  it.each(["mcode", "dsh"])("keeps %s independent of upstream compatibility claims", (kind) => {
    const previewDriver = ProviderDriverKind.make(kind);
    const previewPolicy = { ...policy, driver: previewDriver, orchestrationProtocolVersion: 2 };
    const preview = { ...provider, driver: previewDriver };
    assert.isUndefined(
      resolveProviderCompatibility([previewPolicy], previewDriver, "2.0.0", V2_RELEASE),
    );
    assert.deepEqual(
      applyProviderCompatibility(
        {
          ...preview,
          compatibilityAdvisory: {
            status: "broken",
            message: "Stale advisory",
            recommendedVersion: "2.0.0",
            recommendedRange: ">=2.0.0 <3.0.0",
          },
        },
        [previewPolicy],
        [previewPolicy],
      ),
      preview,
    );
  });

  it("uses V2 compatibility for this source runtime without relabeling historical releases", () => {
    const policies = ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility;
    const opencode = ProviderDriverKind.make("opencode");
    {
      const advisory = resolveProviderCompatibility(policies, opencode, "2.0.18");
      assert.strictEqual(advisory?.status, "supported");
      assert.strictEqual(advisory?.recommendedRange, ">=2.0.18");
    }
    for (const [kind, version, expected] of [
      ["opencode", "2.0.17", "unsupported"],
      ["opencode", "1.14.19", "graceful"],
      ["opencode", "1.14.18", "broken"],
      ["pi", "0.80.5", "supported"],
      ["pi", "0.80.4", "unsupported"],
    ]) {
      assert.strictEqual(
        resolveProviderCompatibility(policies, ProviderDriverKind.make(kind!), version!)?.status,
        expected,
      );
    }
    // Other drivers stay semver-scoped to the running source package version.
    assert.isUndefined(
      resolveProviderCompatibility(
        [{ ...policy, t3CodeRange: `>${serverPackage.version}` }],
        driver,
        "2.0.0",
      ),
    );
    assert.strictEqual(
      resolveProviderCompatibility(policies, opencode, "2.0.18", "0.0.45")?.status,
      "broken",
    );
  });

  it("keeps fork-only protocol tags on bundled OpenCode and Pi policies", () => {
    // Adapter generations are split by T3 Code range in the manifest: OpenCode V1
    // (< 0.0.46) runs protocol 1, while OpenCode V2 and Pi run the current protocol.
    const tagged = (ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility ?? [])
      .filter((entry) => entry.driver === "opencode" || entry.driver === "pi")
      .map((entry) => ({
        driver: entry.driver,
        t3CodeRange: entry.t3CodeRange,
        orchestrationProtocolVersion: entry.orchestrationProtocolVersion,
      }));
    assert.deepStrictEqual(
      tagged,
      [
        { driver: "opencode", t3CodeRange: ">=0.0.42 <0.0.46", orchestrationProtocolVersion: 1 },
        {
          driver: "opencode",
          t3CodeRange: ">=0.0.46",
          orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        },
        {
          driver: "pi",
          t3CodeRange: ">=0.0.46",
          orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        },
      ],
      "apps/server/src/provider/model-manifest.json must tag OpenCode V1 with " +
        `orchestrationProtocolVersion 1, and OpenCode V2 and Pi with ${ORCHESTRATION_PROTOCOL_VERSION}. ` +
        "The tag is fork-only and upstream's manifest lacks it, so taking upstream's file during a " +
        "sync silently drops it and leaves that generation without any compatibility policy. " +
        "Restore the fork's tags.",
    );
  });

  it("falls back from untagged remote generation policies without discarding tagged warnings", () => {
    const bundled = ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility;
    const opencode = ProviderDriverKind.make("opencode");
    const untagged = bundled?.map(({ orchestrationProtocolVersion: _protocol, ...entry }) => entry);
    assert.isUndefined(resolveProviderCompatibility(untagged, opencode, "2.0.18"));
    const snapshot = {
      ...provider,
      driver: opencode,
      version: "2.0.18",
      status: "ready" as const,
      message: undefined,
      versionAdvisory: {
        status: "behind_latest" as const,
        currentVersion: "2.0.18",
        latestVersion: "2.0.19",
        canUpdate: true,
        updateCommand: "npm install -g @opencode/cli@latest",
        checkedAt: provider.checkedAt,
        message: null,
      },
    };
    const fallback = applyProviderCompatibility(snapshot, untagged, bundled);
    assert.strictEqual(fallback.compatibilityAdvisory?.status, "supported");
    assert.strictEqual(fallback.compatibilityAdvisory?.latestVersionStatus, "supported");
    assert.strictEqual(
      applyProviderCompatibility(
        { ...snapshot, driver: ProviderDriverKind.make("pi"), version: "1.0.0" },
        untagged,
        bundled,
      ).compatibilityAdvisory?.status,
      "supported",
    );
    const remoteWarning: ProviderCompatibilityPolicy = {
      driver: opencode,
      t3CodeRange: ">=0.0.46",
      orchestrationProtocolVersion: 2,
      ranges: [{ range: ">=2.0.18", status: "broken" }],
    };
    assert.strictEqual(
      applyProviderCompatibility(snapshot, [remoteWarning], bundled).compatibilityAdvisory?.status,
      "broken",
    );
    const unknownRemote = applyProviderCompatibility(
      snapshot,
      [{ ...remoteWarning, ranges: [] }],
      bundled,
    );
    assert.strictEqual(unknownRemote.compatibilityAdvisory?.status, "unknown");
    assert.strictEqual(unknownRemote.compatibilityAdvisory?.latestVersionStatus, "unknown");
    assert.isUndefined(
      resolveProviderCompatibility(
        [{ ...remoteWarning, orchestrationProtocolVersion: 1 }],
        opencode,
        "2.0.18",
      ),
    );
    assert.isUndefined(
      resolveProviderCompatibility(
        [{ ...remoteWarning, orchestrationProtocolVersion: 3 }],
        opencode,
        "2.0.18",
      ),
    );
  });

  it("keeps releases before V2 on the OpenCode 1 policy they shipped with", () => {
    // Older builds fetch main's manifest; their V1 orchestrator cannot drive OpenCode 2.
    const opencode = ProviderDriverKind.make("opencode");
    for (const [version, expected] of [
      ["1.14.19", "supported"],
      ["2.0.18", "broken"],
      ["1.14.18", "broken"],
    ] as const) {
      const advisory = resolveProviderCompatibility(
        ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility,
        opencode,
        version,
        "0.0.45",
      );
      assert.strictEqual(advisory?.status, expected, `OpenCode ${version} on 0.0.45`);
      assert.strictEqual(advisory?.recommendedVersion, "1.14.19");
    }
    assert.isUndefined(
      resolveProviderCompatibility(
        ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility,
        ProviderDriverKind.make("pi"),
        "1.0.0",
        "0.0.45",
      ),
    );
  });

  it("compares Cursor build dates without treating semver prereleases as stable", () => {
    const cursor = ProviderDriverKind.make("cursor");
    const cursorPolicy: ProviderCompatibilityPolicy = {
      driver: cursor,
      t3CodeRange: policy.t3CodeRange,
      ranges: [
        { range: "<2026.05.09", status: "unsupported" },
        { range: ">=2026.05.09", status: "supported" },
      ],
    };
    for (const [version, expected] of [
      ["2026.05.08-a1b2c3d", "unsupported"],
      ["2026.05.09-a1b2c3d", "supported"],
      ["2026.09.22-f2b0fcd", "supported"],
      ["2026.05.09", "supported"],
      ["2026.05.09-beta.1", "unknown"],
    ] as const) {
      assert.strictEqual(
        resolveProviderCompatibility([cursorPolicy], cursor, version)?.status,
        expected,
      );
    }
    assert.strictEqual(
      resolveProviderCompatibility([policy], driver, "2.0.0-a1b2c3d")?.status,
      "unknown",
    );
  });

  it("recognizes Antigravity semver release tags while keeping dated candidates unknown", () => {
    const antigravity = ProviderDriverKind.make("antigravity");
    const taggedPolicy = {
      ...policy,
      driver: antigravity,
      ranges: [{ range: "=2.0.0", status: "supported" as const }],
    };
    for (const [version, expected] of [
      ["agy_acp_server_2.0.0", "supported"],
      ["2.0.0", "supported"],
      ["agy_acp_server_2.0.1", "unknown"],
      ["agy_acp_server_2.0.0-beta.1", "unknown"],
      ["agy_acp_server_20260818_01_RC01", "unknown"],
    ] as const) {
      assert.strictEqual(
        resolveProviderCompatibility([taggedPolicy], antigravity, version)?.status,
        expected,
      );
    }
  });

  it("classifies boundaries and treats unlisted versions and release tags as unknown", () => {
    for (const [version, expected] of [
      ["0.9.9", "broken"],
      ["1.0.0", "unsupported"],
      ["1.5.0", "graceful"],
      ["2.0.0", "supported"],
      ["v2.0.0", "supported"],
      ["3.0.0", "unknown"],
      ["2.0.0-beta.1", "unknown"],
      ["agy_acp_server_20260818_01_RC01", "unknown"],
      [null, "unknown"],
    ] as const) {
      assert.strictEqual(resolveProviderCompatibility([policy], driver, version)?.status, expected);
    }
  });

  it("applies policies only within their T3 Code version range", () => {
    const scopedPolicy = { ...policy, t3CodeRange: ">=0.0.42 <0.1.0" };
    for (const version of ["0.0.42", "0.0.45"]) {
      assert.strictEqual(
        resolveProviderCompatibility([scopedPolicy], driver, "0.9.0", version)?.status,
        "broken",
      );
    }
    for (const version of ["0.0.41", "0.1.0"]) {
      assert.isUndefined(resolveProviderCompatibility([scopedPolicy], driver, "0.9.0", version));
    }
  });

  it("relaxes remote policy without losing probe errors, and falls back when a policy is omitted", () => {
    const broken = applyProviderCompatibility(provider, [], [policy]);
    assert.strictEqual(broken.compatibilityAdvisory?.status, "broken");
    const relaxed = { ...policy, ranges: [{ range: ">=0.0.0", status: "supported" as const }] };
    const supported = applyProviderCompatibility(broken, [relaxed], [policy]);
    assert.strictEqual(supported.compatibilityAdvisory?.status, "supported");
    assert.strictEqual(supported.status, "error");
    assert.strictEqual(supported.message, "Authentication failed");
    assert.strictEqual(
      applyProviderCompatibility(supported, [{ ...policy, t3CodeRange: "<0.0.0" }], [policy])
        .compatibilityAdvisory?.status,
      "broken",
    );
    const removed = applyProviderCompatibility(supported, [], []);
    assert.isUndefined(removed.compatibilityAdvisory);
    assert.strictEqual(removed.status, "error");
    assert.strictEqual(removed.message, "Authentication failed");
    assert.isUndefined(
      applyProviderCompatibility({ ...broken, enabled: false }, [], [policy]).compatibilityAdvisory,
    );
    assert.isUndefined(
      applyProviderCompatibility({ ...broken, installed: false }, [], [policy])
        .compatibilityAdvisory,
    );
  });

  it("rejects invalid ranges and recommendations outside the first supported match", () => {
    const decode = Schema.decodeUnknownSync(ProviderCompatibilityPolicy);
    assert.doesNotThrow(() => decode(policy));
    assert.doesNotThrow(() => decode({ ...policy, orchestrationProtocolVersion: 3 }));
    assert.throws(() => decode({ ...policy, orchestrationProtocolVersion: 0 }));
    assert.throws(() => decode({ ...policy, orchestrationProtocolVersion: 1.5 }));
    const prefixed = decode({
      ...policy,
      t3CodeRange: ">=v0.0.42 <v0.1",
      recommendedRange: "^v2",
      ranges: [{ range: ">=v2.0 <v3", status: "supported" }],
    });
    assert.strictEqual(
      resolveProviderCompatibility([prefixed], driver, "2.0.0", "0.0.45")?.status,
      "supported",
    );
    assert.strictEqual(
      resolveProviderCompatibility([prefixed], driver, "3.0.0", "0.0.45")?.status,
      "unknown",
    );
    for (const invalid of [
      { ...policy, t3CodeRange: "*" },
      { ...policy, recommendedVersion: "3.0.0" },
      { ...policy, ranges: [{ range: ">=2.0.0 garbage", status: "supported" }] },
      { ...policy, recommendedVersion: "2.0.0; echo unsafe" },
      { ...policy, ranges: [{ range: ">=0.0.0", status: "broken" }, ...policy.ranges] },
    ])
      assert.throws(() => decode(invalid));
  });
});

it.effect("a remote policy refresh preserves a newer health result on the registry stream", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const health = yield* Ref.make<ServerProvider>({
      ...provider,
      status: "ready" as ServerProvider["status"],
      message: "Healthy",
    });
    const manifest = yield* Ref.make<ModelManifest.ModelManifestData>({
      version: 1,
      currentModels: {},
      compatibility: [policy],
    });
    const instance: ProviderInstance = {
      instanceId: provider.instanceId,
      driverKind: driver,
      enabled: true,
      displayName: undefined,
      continuationIdentity: { driverKind: driver, continuationKey: "test-codex" },
      snapshot: {
        getSnapshot: Ref.get(health),
        refresh: Ref.get(health),
        streamChanges: Stream.empty,
        applyUsageLimits: () => Effect.void,
        resolveMaintenance: () =>
          Effect.succeed(
            makeManualOnlyProviderMaintenanceCapabilities({ provider: driver, packageName: null }),
          ),
      },
      orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
      textGeneration: {} as ProviderInstance["textGeneration"],
    };
    const refresh = Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
      Effect.andThen(
        Ref.updateAndGet(manifest, (current) => ({
          ...current,
          compatibility: [
            { ...policy, ranges: [{ range: ">=0.0.0", status: "supported" as const }] },
          ],
        })),
      ),
    );
    const layerDependencies = Layer.mergeAll(
      ProviderLatestVersions.layer,
      Layer.succeed(ModelManifest.ModelManifest, {
        current: Ref.get(manifest),
        refresh,
        forceRefresh: refresh,
        refreshInBackground: Effect.void,
      }),
      Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
        getInstance: (id) => Effect.succeed(id === instance.instanceId ? instance : undefined),
        listInstances: Effect.succeed([instance]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
      }),
      ServerConfig.layerTest(process.cwd(), { prefix: "compatibility-registry-test" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    yield* Effect.gen(function* () {
      const registry = yield* ProviderRegistry.ProviderRegistry;
      yield* Deferred.await(started);
      assert.strictEqual(
        (yield* registry.getProviders)[0]?.compatibilityAdvisory?.status,
        "broken",
      );
      yield* Ref.set(health, provider);
      const latestHealth = yield* registry.refreshInstance(provider.instanceId);
      assert.strictEqual(latestHealth[0]?.message, "Authentication failed");
      const supported = yield* Stream.toPull(
        registry.streamChanges.pipe(
          Stream.filter((snapshots) => snapshots[0]?.compatibilityAdvisory?.status === "supported"),
        ),
      );
      const subscribed = yield* supported.pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.succeed(release, undefined);
      const updated = (yield* Fiber.join(subscribed))[0]?.[0];
      assert.strictEqual(updated?.compatibilityAdvisory?.status, "supported");
      assert.strictEqual(updated?.status, "error");
      assert.strictEqual(updated?.message, "Authentication failed");
    }).pipe(Effect.provide(ProviderRegistry.layer.pipe(Layer.provide(layerDependencies))));
  }).pipe(Effect.scoped),
);
