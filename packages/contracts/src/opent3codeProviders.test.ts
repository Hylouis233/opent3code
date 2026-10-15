import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";
import {
  DEFAULT_SERVER_SETTINGS,
  MCodeSettings,
  DshSettings,
  ServerSettings,
  isUnconfiguredDefaultInstanceEnabled,
  resolveProviderInstanceEnabled,
} from "./settings.ts";
import { DEFAULT_MODEL_BY_PROVIDER, PROVIDER_DISPLAY_NAMES } from "./model.ts";

const decodeServerSettings = Schema.decodeSync(ServerSettings);
const encodeServerSettings = Schema.encodeSync(ServerSettings);

const previewProviders = [
  ["mcode", MCodeSettings, "MiniMax Code"],
  ["dsh", DshSettings, "DeepSeek Harness"],
] as const;

describe("OpenT3Code independent provider contracts", () => {
  it.each(previewProviders)(
    "%s stays opt-in and retains official CLI defaults",
    (kind, schema, name) => {
      const settings = Schema.decodeSync(schema)({});
      expect(settings.enabled).toBe(false);
      expect(settings.binaryPath).toBe(kind);
      expect(settings.homePath).toBe("");
      expect(DEFAULT_MODEL_BY_PROVIDER[ProviderDriverKind.make(kind)]).toBe("cli-default");
      expect(PROVIDER_DISPLAY_NAMES[ProviderDriverKind.make(kind)]).toBe(name);
    },
  );

  it.each(previewProviders)(
    "%s round-trips explicit binary and isolated data settings",
    (kind, schema) => {
      const settings = {
        enabled: true,
        binaryPath: `/opt/bin/${kind}`,
        homePath: `/isolated/${kind}`,
      };
      const decoded = Schema.decodeSync(schema)(settings);
      expect(Schema.encodeSync(schema)(decoded)).toMatchObject(settings);
    },
  );

  it.each(previewProviders)("%s requires an explicit instance opt-in", (kind) => {
    const driver = ProviderDriverKind.make(kind);
    expect(DEFAULT_SERVER_SETTINGS.providerInstances).toEqual({});
    expect(isUnconfiguredDefaultInstanceEnabled(ProviderInstanceId.make(kind))).toBe(false);
    expect(resolveProviderInstanceEnabled({ driver, config: {} })).toBe(false);
    expect(resolveProviderInstanceEnabled({ driver, enabled: true, config: {} })).toBe(true);
    expect(resolveProviderInstanceEnabled({ driver, config: { enabled: true } })).toBe(true);
    expect(
      resolveProviderInstanceEnabled({ driver, enabled: true, config: { enabled: false } }),
    ).toBe(false);
    expect(
      resolveProviderInstanceEnabled({ driver, enabled: false, config: { enabled: true } }),
    ).toBe(false);
  });

  it.each(previewProviders)(
    "%s preserves explicit instance identity, opt-in, and isolated configuration",
    (kind) => {
      const instanceId = ProviderInstanceId.make(`${kind}-isolated`);
      const instance = {
        driver: ProviderDriverKind.make(kind),
        enabled: true,
        config: {
          binaryPath: `/opt/bin/${kind}`,
          homePath: `/isolated/${kind}`,
        },
      };
      const settings = decodeServerSettings({
        providerInstances: { [instanceId]: instance },
      });
      const restored = decodeServerSettings(encodeServerSettings(settings));
      expect(restored.providerInstances).toEqual({ [instanceId]: instance });
      expect(resolveProviderInstanceEnabled(restored.providerInstances[instanceId]!)).toBe(true);
      expect(Object.hasOwn(restored.providerInstances, kind)).toBe(false);
    },
  );
});
