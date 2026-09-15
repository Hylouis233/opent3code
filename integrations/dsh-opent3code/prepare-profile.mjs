#!/usr/bin/env node
import { existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Matches the native packages observed in the pinned preview dependency graph.
// New versions remain unapproved; do not replace this with allow-all or strictDepBuilds:false.
const policy = `packages:\n  - .\nnodeLinker: hoisted\nautoInstallPeers: false\nallowBuilds:\n  '@deepseek-ai/dsh-subprocess-local@0.1.5-rc.2': true\n  'koffi@3.2.1': true\n  'node-pty@1.2.0-beta.15': true\n  '@google/genai': false\n  protobufjs: false\n`;
export function prepareProfile(home) {
  if (!path.isAbsolute(home)) throw new Error("DSH home must be an absolute path.");
  const dir = path.join(home, "profiles", "opent3code");
  const file = path.join(dir, "pnpm-workspace.yaml");
  const marker = path.join(dir, ".opent3code-profile");
  if (existsSync(dir)) {
    if (lstatSync(dir).isSymbolicLink()) throw new Error("Profile directory must not be a symlink.");
    if (readdirSync(dir).length) {
      if (existsSync(marker) && lstatSync(marker).isFile() && readFileSync(marker, "utf8") === "opent3code-source-alpha-1\n" && existsSync(file) && lstatSync(file).isFile() && readFileSync(file, "utf8") === policy) return dir;
      throw new Error("Existing profile is not an unchanged OpenT3Code setup. Review it manually or choose a new DSH_HOME; no files were overwritten.");
    }
  }
  mkdirSync(dir, { recursive: true, mode: 0o700 });
  writeFileSync(file, policy, { flag: "wx", mode: 0o600 });
  writeFileSync(marker, "opent3code-source-alpha-1\n", { flag: "wx", mode: 0o600 });
  return dir;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length > 3) throw new Error("Usage: node prepare-profile.mjs [ABSOLUTE_DSH_HOME]");
    console.log(prepareProfile(process.argv[2] ?? process.env.DSH_HOME ?? path.join(homedir(), ".dsh")));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
