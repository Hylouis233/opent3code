import type { ProviderApprovalDecision } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const ExternalAcpResume = Schema.Struct({
  version: Schema.Literal(1),
  driver: Schema.String,
  instanceId: Schema.String,
  cwd: Schema.String,
  home: Schema.String,
  sessionId: Schema.String.check(Schema.isNonEmpty()),
});
export type ExternalAcpResume = typeof ExternalAcpResume.Type;

/** A cursor is an identity, not permission to open another instance's session. */
export function matchesExternalAcpResume(
  cursor: ExternalAcpResume,
  expected: Omit<ExternalAcpResume, "sessionId">,
): boolean {
  return (
    cursor.version === expected.version &&
    cursor.driver === expected.driver &&
    cursor.instanceId === expected.instanceId &&
    cursor.cwd === expected.cwd &&
    cursor.home === expected.home
  );
}

export interface ExternalPermissionOption {
  readonly optionId: string;
  readonly kind: string;
  readonly name: string;
}

/** Preview approvals are one-shot. Never infer a provider's opaque option IDs. */
export function externalPermissionOutcome(
  options: ReadonlyArray<ExternalPermissionOption>,
  decision: ProviderApprovalDecision,
): { outcome: "selected"; optionId: string } | { outcome: "cancelled" } {
  const kind = decision === "accept" ? "allow_once" : decision === "decline" ? "reject_once" : null;
  const matches =
    kind === null
      ? []
      : options.filter((option) => option.kind === kind && option.optionId.length > 0);
  return matches.length === 1
    ? { outcome: "selected", optionId: matches[0]!.optionId }
    : { outcome: "cancelled" };
}

export function externalApprovalOptions(options: ReadonlyArray<ExternalPermissionOption>) {
  const result: Array<{ decision: ProviderApprovalDecision; label: string }> = [];
  for (const [kind, decision] of [
    ["allow_once", "accept"],
    ["reject_once", "decline"],
  ] as const) {
    const matching = options.filter((option) => option.kind === kind && option.optionId.length > 0);
    if (matching.length === 1) result.push({ decision, label: matching[0]!.name || decision });
  }
  result.push({ decision: "cancel", label: "Cancel request" });
  return result;
}
