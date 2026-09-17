import type { UsageProviderKind } from "@t3tools/contracts";
import { useAppearancePreferences } from "../settings/appearance/AppearancePreferencesProvider";

/**
 * Series and table order. The chart stacks providers from the bottom in this
 * order, so it also fixes which band sits on top of the bars.
 */
export const PROVIDER_ORDER: readonly UsageProviderKind[] = [
  "codex",
  "claude",
  "opencodex",
  "mcode",
  "kimi",
];

export const PROVIDER_LABEL: Record<UsageProviderKind, string> = {
  claude: "Claude Code",
  codex: "Codex",
  opencodex: "OpenCodex",
  mcode: "MCode",
  kimi: "Kimi Code",
};

/**
 * Claude's brand orange holds in both themes; Codex is neutral and must flip
 * with the theme or its bars vanish against the matching background. OpenCodex's
 * purple accent, MCode's official sky blue, and Kimi's violet (adjusted per
 * theme) remain legible in both themes.
 */
export function useProviderColors(): Record<UsageProviderKind, string> {
  const { themeAppearance: scheme } = useAppearancePreferences();
  return {
    claude: "#d97757",
    codex: scheme === "dark" ? "#e6e6e6" : "#3c3c43",
    opencodex: scheme === "dark" ? "#D39CFF" : "#8B2BE2",
    mcode: scheme === "dark" ? "#7DC6FF" : "#2563EB",
    kimi: scheme === "dark" ? "#A89CFF" : "#5B4FD6",
  };
}
