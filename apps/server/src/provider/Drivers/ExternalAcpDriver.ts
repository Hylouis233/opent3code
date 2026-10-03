/** Official stdio ACP only. No terminal scraping, login emulation, or implicit permission grants. */
import {
  DshSettings,
  MCodeSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessWorkingDirectory } from "@t3tools/shared/hostProcess";
import { defaultProviderContinuationIdentity, type ProviderDriver } from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import { makeExternalAcpAdapterV2 } from "../../orchestration-v2/Adapters/ExternalAcpAdapterV2.ts";

export type ExternalAcpDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | IdAllocator.IdAllocatorV2;

function makeExternalDriver(
  kind: "mcode" | "dsh",
  configSchema: typeof MCodeSettings,
): ProviderDriver<MCodeSettings, ExternalAcpDriverEnv> {
  const provider = ProviderDriverKind.make(kind);
  const name = kind === "mcode" ? "MiniMax Code" : "DeepSeek Harness";
  return {
    driverKind: provider,
    metadata: { displayName: name, supportsMultipleInstances: true },
    configSchema,
    defaultConfig: () => Schema.decodeSync(configSchema)({}),
    create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const crypto = yield* Crypto.Crypto;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const hostCwd = yield* HostProcessWorkingDirectory;
        const env = mergeProviderInstanceEnvironment(environment);
        // Explicit values override dangerous ambient DSH policy. The installed profile must also be trusted.
        const processEnv: NodeJS.ProcessEnv =
          kind === "dsh"
            ? {
                ...env,
                DSH_PERMISSION_MODE: "workspace-write",
                DSH_TELEMETRY_DISABLED: "1",
                ...(config.homePath ? { DSH_HOME: config.homePath } : {}),
              }
            : { ...env, ...(config.homePath ? { MINIMAX_DATA_DIR: config.homePath } : {}) };
        const homeKey = (
          kind === "dsh"
            ? ["DSH_HOME", "HOME", "USERPROFILE"]
            : ["MINIMAX_DATA_DIR", "MAVIS_DATA_DIR", "HOME", "USERPROFILE"]
        ).find((key) => processEnv[key] !== undefined);
        const home = homeKey === undefined ? "" : processEnv[homeKey]!;
        const continuationIdentity = defaultProviderContinuationIdentity({
          driverKind: provider,
          instanceId,
        });
        const idAllocator = yield* IdAllocator.IdAllocatorV2;
        const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
        const openRuntime = (
          cwd: string,
          scope: Scope.Closeable,
          resume?: string,
          canonicalHome?: string,
          transport?: Pick<
            AcpSessionRuntime.AcpSessionRuntimeOptions,
            "onOutgoingResponse" | "onOutgoingResponseFailure" | "onTermination"
          >,
        ) =>
          AcpSessionRuntime.make({
            ...transport,
            spawn: {
              command: config.binaryPath,
              args: kind === "mcode" ? ["acp"] : ["--profile", "opent3code"],
              cwd,
              env:
                canonicalHome === undefined || homeKey === undefined
                  ? processEnv
                  : { ...processEnv, [homeKey]: canonicalHome },
            },
            cwd,
            clientInfo: { name: "opent3code", version: "0.1.0-alpha.1" },
            ...(resume ? { resumeSessionId: resume, resumeMethod: "resume" as const } : {}),
            authenticateOnAuthRequired: false,
            cancelBehavior: "wait-for-prompt",
            cancelTimeout: "15 seconds",
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, scope),
          );
        const orchestrationAdapter = makeExternalAcpAdapterV2({
          driver: provider,
          instanceId,
          enabled,
          home,
          homePath: config.homePath,
          fileSystem: fs,
          path,
          crypto,
          idAllocator,
          openRuntime,
        });
        const snapshot: ServerProvider = {
          instanceId,
          driver: provider,
          displayName: displayName ?? name,
          ...(accentColor ? { accentColor } : {}),
          continuation: { groupKey: continuationIdentity.continuationKey },
          badgeLabel: "Preview",
          enabled,
          installed: false,
          version: null,
          status: enabled ? "warning" : "disabled",
          auth: { status: "unknown" },
          checkedAt: yield* now,
          message:
            "Refresh to probe the official CLI. Supervised text sessions only; CLI login is managed outside OpenT3Code.",
          models: [
            {
              slug: "cli-default",
              name: "CLI configured model",
              isDefault: true,
              isCustom: false,
              capabilities: {},
            },
          ],
          slashCommands: [],
          skills: [],
          supportsTextGeneration: false,
          supportsConversationRollback: false,
          showInteractionModeToggle: false,
          requiresNewThreadForModelChange: true,
        };
        const state = yield* SubscriptionRef.make(snapshot);
        const refresh = Effect.gen(function* () {
          if (!enabled) return yield* SubscriptionRef.get(state);
          const probe = Effect.scoped(
            Effect.gen(function* () {
              const scope = yield* Scope.make();
              yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
              const runtime = yield* openRuntime(hostCwd, scope);
              return yield* runtime.initialize();
            }),
          ).pipe(Effect.timeout("20 seconds"));
          const checkedAt = yield* now;
          const next = yield* probe.pipe(
            Effect.match({
              onFailure: () => ({
                ...snapshot,
                checkedAt,
                status: "error" as const,
                message:
                  "Official ACP CLI unavailable. Check the binary, Node version, and the DSH opent3code profile installation.",
              }),
              onSuccess: (result) => ({
                ...snapshot,
                checkedAt,
                installed: true,
                version: result.agentInfo?.version ?? null,
                status: "ready" as const,
                message:
                  "ACP handshake succeeded; account authentication and model availability are not verified by this probe.",
              }),
            }),
          );
          yield* SubscriptionRef.set(state, next);
          return next;
        });
        // Only opted-in instances are probed, scoped to the provider's lifecycle.
        if (enabled) yield* refresh;
        const unsupportedText = (operation: string) =>
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: `${name} preview does not provide background text generation; select another provider for titles and commit messages.`,
            }),
          );
        return {
          instanceId,
          driverKind: provider,
          continuationIdentity,
          displayName,
          accentColor,
          enabled,
          orchestrationAdapter,
          snapshot: {
            getSnapshot: SubscriptionRef.get(state),
            refresh,
            streamChanges: SubscriptionRef.changes(state),
            applyUsageLimits: () => Effect.void,
            resolveMaintenance: () =>
              Effect.succeed(
                makeManualOnlyProviderMaintenanceCapabilities({ provider, packageName: null }),
              ),
          },
          textGeneration: {
            generateCommitMessage: () => unsupportedText("generateCommitMessage"),
            generatePrContent: () => unsupportedText("generatePrContent"),
            generateBranchName: () => unsupportedText("generateBranchName"),
            generateThreadTitle: () => unsupportedText("generateThreadTitle"),
          },
        };
      }),
  };
}
export const MCodeDriver = makeExternalDriver("mcode", MCodeSettings);
export const DshDriver = makeExternalDriver("dsh", DshSettings);
