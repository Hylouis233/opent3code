import { describe, expect, it } from "vite-plus/test";
import {
  externalPermissionOutcome,
  externalApprovalOptions,
  matchesExternalAcpResume,
} from "./ExternalAcpPolicy.ts";

describe("external ACP security policy", () => {
  const allow = { kind: "allow_once", optionId: "opaque-1", name: "Allow once" };
  const deny = { kind: "reject_once", optionId: "opaque-2", name: "Reject once" };
  it("uses negotiated opaque identifiers, not assumed option strings", () => {
    expect(externalPermissionOutcome([allow, deny], "accept")).toEqual({
      outcome: "selected",
      optionId: "opaque-1",
    });
    expect(externalPermissionOutcome([allow, deny], "decline")).toEqual({
      outcome: "selected",
      optionId: "opaque-2",
    });
  });
  it("fails closed for missing, duplicate, always-allow and unknown decisions", () => {
    for (const options of [
      [],
      [allow, allow],
      [{ ...allow, kind: "allow_always" }],
      [{ ...allow, optionId: "" }],
    ]) {
      expect(externalPermissionOutcome(options, "accept")).toEqual({ outcome: "cancelled" });
    }
    for (const decision of ["acceptAlways", "acceptForSession", "cancel"] as const)
      expect(externalPermissionOutcome([allow, deny], decision)).toEqual({ outcome: "cancelled" });
    expect(externalApprovalOptions([allow, allow, deny]).map((option) => option.decision)).toEqual([
      "decline",
      "cancel",
    ]);
  });
  it("pins resume to driver, instance, workspace and data directory", () => {
    const cursor = {
      version: 1 as const,
      driver: "dsh",
      instanceId: "dsh-one",
      cwd: "/workspace",
      home: "/isolated",
      sessionId: "native-1",
    };
    expect(matchesExternalAcpResume(cursor, cursor)).toBe(true);
    for (const key of ["driver", "instanceId", "cwd", "home"] as const)
      expect(matchesExternalAcpResume({ ...cursor, [key]: "other" }, cursor)).toBe(false);
  });
});
