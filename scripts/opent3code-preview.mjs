#!/usr/bin/env node
/** Isolated, source-only preview. Never edits an existing T3/DSH/MCode profile. */
import { mkdirSync, existsSync, lstatSync, readdirSync, readFileSync, writeFileSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const root = fileURLToPath(new URL("../", import.meta.url));
const within = (child, parent) => {
  const relative = path.relative(parent, child);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
};
function canonical(location) {
  const absolute = path.resolve(location);
  if (existsSync(absolute)) return realpathSync(absolute);
  const parent = path.dirname(absolute);
  return parent === absolute ? absolute : path.join(canonical(parent), path.basename(absolute));
}
function ordinaryFile(location) {
  if (existsSync(location) && !lstatSync(location).isFile()) throw new Error("Preview metadata must be a regular file, not a link or directory.");
}
export function initializePreview(home, userHome = homedir()) {
  if (!path.isAbsolute(home)) throw new Error("--home-dir must be an absolute path.");
  const target = canonical(home);
  for (const reserved of [".t3", ".dsh", ".minimax"]) {
    if (within(target, canonical(path.join(userHome, reserved)))) throw new Error("Refusing to use an existing tool's data namespace.");
  }
  if (existsSync(home) && lstatSync(home).isSymbolicLink()) throw new Error("Preview home must not be a symbolic link.");
  const marker = path.join(home, ".opent3code-preview.json");
  if (existsSync(home) && readdirSync(home).length && !existsSync(marker)) throw new Error("Directory is not an OpenT3Code preview home; choose a new empty directory.");
  mkdirSync(home, { recursive: true, mode: 0o700 });
  ordinaryFile(marker);
  if (!existsSync(marker)) writeFileSync(marker, JSON.stringify({ format: 1, application: "opent3code-preview" }) + "\n", { flag: "wx", mode: 0o600 });
  const identity = JSON.parse(readFileSync(marker, "utf8"));
  if (identity.format !== 1 || identity.application !== "opent3code-preview") throw new Error("Unrecognized preview home marker.");
  const userdata = path.join(home, "userdata");
  if (existsSync(userdata) && lstatSync(userdata).isSymbolicLink()) throw new Error("Preview userdata must not be a symbolic link.");
  mkdirSync(userdata, { recursive: true, mode: 0o700 });
  const settings = path.join(userdata, "settings.json");
  ordinaryFile(settings);
  if (!existsSync(settings)) writeFileSync(settings, JSON.stringify({
    defaultRuntimeMode: "approval-required",
    defaultModelSelection: { instanceId: "mcode", model: "cli-default" },
    providerInstances: {
      mcode: { driver: "mcode", enabled: true, config: { enabled: true, binaryPath: "mcode", homePath: "" } },
      dsh: { driver: "dsh", enabled: false, config: { enabled: true, binaryPath: "dsh", homePath: "" } },
    },
  }, null, 2) + "\n", { flag: "wx", mode: 0o600 });
  return { home, settings };
}
function main(args) {
  let home = path.join(root, ".opent3code-preview");
  let initOnly = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--init-only") initOnly = true;
    else if (args[i] === "--home-dir" && args[i + 1]) home = args[++i];
    else throw new Error("Usage: node scripts/opent3code-preview.mjs [--home-dir ABSOLUTE_PATH] [--init-only]");
  }
  const [major, minor, patch] = process.versions.node.split(".").map(Number);
  if (!initOnly && (major !== 24 || minor < 13 || (minor === 13 && patch < 1))) throw new Error("Use Node.js 24.13.1 or newer 24.x, install Vite+, then run vp i.");
  const initialized = initializePreview(home);
  console.log(`OpenT3Code preview data: ${initialized.home}`);
  if (initOnly) return;
  console.log("Open the pairing URL printed by the dev runner. Do not share its token in chat or screenshots.");
  const child = spawn(process.execPath, ["scripts/dev-runner.ts", "dev", "--home-dir", initialized.home], {
    cwd: root, stdio: "inherit", env: { ...process.env, T3CODE_HOME: initialized.home, T3CODE_DISABLE_AUTO_UPDATE: "1" },
  });
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => child.kill(signal));
  child.on("error", error => { console.error(error.message); process.exitCode = 1; });
  child.on("exit", code => { process.exitCode = code ?? 1; });
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
