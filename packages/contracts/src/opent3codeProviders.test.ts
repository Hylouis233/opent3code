import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { ProviderDriverKind } from "./providerInstance.ts";
import { MCodeSettings, DshSettings } from "./settings.ts";
import { DEFAULT_MODEL_BY_PROVIDER, PROVIDER_DISPLAY_NAMES } from "./model.ts";

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
});
