#!/usr/bin/env node
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

// Matches the native packages observed in the pinned preview dependency graph.
// New versions remain unapproved; do not replace this with allow-all or strictDepBuilds:false.
const policy = `packages:\n  - .\nnodeLinker: hoisted\nautoInstallPeers: false\nallowBuilds:\n  '@deepseek-ai/dsh-subprocess-local@0.1.5-rc.2': true\n  'koffi@3.2.1': true\n  'node-pty@1.2.0-beta.15': true\n  '@google/genai': false\n  protobufjs: false\n`;
export function prepareProfile(home) {
  if (!NodePath.isAbsolute(home)) throw new Error("DSH home must be an absolute NodePath.");
  const dir = NodePath.join(home, "profiles", "opent3code");
  const file = NodePath.join(dir, "pnpm-workspace.yaml");
  const marker = NodePath.join(dir, ".opent3code-profile");
  if (NodeFS.existsSync(dir)) {
    if (NodeFS.lstatSync(dir).isSymbolicLink())
      throw new Error("Profile directory must not be a symlink.");
    if (NodeFS.readdirSync(dir).length) {
      if (
        NodeFS.existsSync(marker) &&
        NodeFS.lstatSync(marker).isFile() &&
        NodeFS.readFileSync(marker, "utf8") === "opent3code-source-alpha-1\n" &&
        NodeFS.existsSync(file) &&
        NodeFS.lstatSync(file).isFile() &&
        NodeFS.readFileSync(file, "utf8") === policy
      )
        return dir;
      throw new Error(
        "Existing profile is not an unchanged OpenT3Code setup. Review it manually or choose a new DSH_HOME; no files were overwritten.",
      );
    }
  }
  NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
  NodeFS.writeFileSync(file, policy, { flag: "wx", mode: 0o600 });
  NodeFS.writeFileSync(marker, "opent3code-source-alpha-1\n", { flag: "wx", mode: 0o600 });
  return dir;
}
if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  try {
    if (process.argv.length > 3)
      throw new Error("Usage: node prepare-profile.mjs [ABSOLUTE_DSH_HOME]");
    console.log(
      prepareProfile(
        process.argv[2] ?? process.env.DSH_HOME ?? NodePath.join(NodeOS.homedir(), ".dsh"),
      ),
    );
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
