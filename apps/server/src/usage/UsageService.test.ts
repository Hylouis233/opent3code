import { describe, expect, it } from "@effect/vitest";

import {
  chooseMcodeUsageStore,
  classifyUsageSourceExistence,
  negotiateUsageContractVersion,
  resolveKimiCodeHome,
  resolveMcodeDataDir,
  resolveOpenCodexHome,
  summarizeSourceReadFailures,
} from "./UsageService.ts";

describe("classifyUsageSourceExistence", () => {
  it("keeps I/O failures distinct from missing sources", () => {
    expect(classifyUsageSourceExistence(true)).toBe("present");
    expect(classifyUsageSourceExistence(false)).toBe("missing");
    expect(classifyUsageSourceExistence(null)).toBe("failed");
  });
});

describe("resolveOpenCodexHome", () => {
  it("prefers OPENCODEX_HOME", () => {
    expect(
      resolveOpenCodexHome({ OPENCODEX_HOME: "/custom/opencodex" }, "/home/user/.opencodex"),
    ).toBe("/custom/opencodex");
  });

  it("uses the standard OpenCodex home by default", () => {
    expect(resolveOpenCodexHome({}, "/home/user/.opencodex")).toBe("/home/user/.opencodex");
  });
});

describe("resolveMcodeDataDir", () => {
  it("prefers the current MCode override and accepts the legacy name", () => {
    expect(resolveMcodeDataDir({ MINIMAX_DATA_DIR: "/custom/mcode" }, "/home/user/.minimax")).toBe(
      "/custom/mcode",
    );
    expect(resolveMcodeDataDir({ MAVIS_DATA_DIR: "/legacy/mavis" }, "/home/user/.minimax")).toBe(
      "/legacy/mavis",
    );
  });

  it("uses the shared TUI and desktop directory by default", () => {
    expect(resolveMcodeDataDir({}, "/home/user/.minimax")).toBe("/home/user/.minimax");
  });
});

describe("chooseMcodeUsageStore", () => {
  it("uses the alternate when the primary is only an empty compatibility stub", () => {
    expect(chooseMcodeUsageStore("primary.sqlite", "absent", "alternate.sqlite", "ready")).toBe(
      "alternate.sqlite",
    );
  });

  it("keeps the canonical primary when both stores have accounting", () => {
    expect(chooseMcodeUsageStore("primary.sqlite", "ready", "alternate.sqlite", "ready")).toBe(
      "primary.sqlite",
    );
  });

  it("keeps the primary fingerprint when its probe fails transiently", () => {
    expect(chooseMcodeUsageStore("primary.sqlite", "failed", "alternate.sqlite", "ready")).toBe(
      "primary.sqlite",
    );
  });

  it("keeps the alternate path when it is the only store but its probe fails", () => {
    expect(chooseMcodeUsageStore("primary.sqlite", "absent", "alternate.sqlite", "failed")).toBe(
      "alternate.sqlite",
    );
  });
});

describe("resolveKimiCodeHome", () => {
  it("prefers KIMI_CODE_HOME", () => {
    expect(resolveKimiCodeHome({ KIMI_CODE_HOME: "/custom/kimi" }, "/home/user/.kimi-code")).toBe(
      "/custom/kimi",
    );
  });

  it("uses the standard TUI home by default", () => {
    expect(resolveKimiCodeHome({}, "/home/user/.kimi-code")).toBe("/home/user/.kimi-code");
  });
});

describe("negotiateUsageContractVersion", () => {
  it("keeps the v4 response shape for clients that do not advertise support", () => {
    expect(negotiateUsageContractVersion(undefined)).toBe(4);
    expect(negotiateUsageContractVersion(4)).toBe(4);
  });

  it("serves OpenCodex only to compatible clients", () => {
    expect(negotiateUsageContractVersion(5)).toBe(5);
    expect(negotiateUsageContractVersion(6)).toBe(5);
  });

  it("serves MCode only to compatible clients", () => {
    expect(negotiateUsageContractVersion(5)).toBe(5);
    expect(negotiateUsageContractVersion(6)).toBe(5);
  });

  it("serves Kimi Code only to compatible clients", () => {
    expect(negotiateUsageContractVersion(5)).toBe(5);
    expect(negotiateUsageContractVersion(6)).toBe(5);
  });
});

describe("summarizeSourceReadFailures", () => {
  it("distinguishes healthy, partial, and failed stores", () => {
    expect(summarizeSourceReadFailures(1, 0)).toEqual({ status: "ok", message: null });
  });

  it("reports a healthy source when every file was readable", () => {
    expect(summarizeSourceReadFailures(2, 0)).toEqual({ status: "ok", message: null });
  });

  it("reports partial coverage when only some files failed", () => {
    expect(summarizeSourceReadFailures(2, 1)).toEqual({
      status: "partial",
      message: "1 usage file could not be read.",
    });
    expect(summarizeSourceReadFailures(1, 1)).toEqual({
      status: "failed",
      message: "1 usage file could not be read.",
    });
  });

  it("reports a failed source when every file failed", () => {
    expect(summarizeSourceReadFailures(2, 2)).toEqual({
      status: "failed",
      message: "2 usage files could not be read.",
    });
  });
});
