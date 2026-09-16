#!/usr/bin/env node
/** Isolated, source-only preview. Never edits an existing T3/DSH/MCode profile. */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeChildProcess from "node:child_process";

const root = NodeURL.fileURLToPath(new URL("../", import.meta.url));
const within = (child, parent) => {
  const relative = NodePath.relative(parent, child);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
};
function canonical(location) {
  const absolute = NodePath.resolve(location);
  if (NodeFS.existsSync(absolute)) return NodeFS.realpathSync(absolute);
  const parent = NodePath.dirname(absolute);
  return parent === absolute
    ? absolute
    : NodePath.join(canonical(parent), NodePath.basename(absolute));
}
function ordinaryFile(location) {
  if (NodeFS.existsSync(location) && !NodeFS.lstatSync(location).isFile())
    throw new Error("Preview metadata must be a regular file, not a link or directory.");
}
export function initializePreview(home, userHome = NodeOS.homedir()) {
  if (!NodePath.isAbsolute(home)) throw new Error("--home-dir must be an absolute NodePath.");
  const target = canonical(home);
  for (const reserved of [".t3", ".dsh", ".minimax"]) {
    if (within(target, canonical(NodePath.join(userHome, reserved))))
      throw new Error("Refusing to use an existing tool's data namespace.");
  }
  if (NodeFS.existsSync(home) && NodeFS.lstatSync(home).isSymbolicLink())
    throw new Error("Preview home must not be a symbolic link.");
  const marker = NodePath.join(home, ".opent3code-preview.json");
  if (NodeFS.existsSync(home) && NodeFS.readdirSync(home).length && !NodeFS.existsSync(marker))
    throw new Error("Directory is not an OpenT3Code preview home; choose a new empty directory.");
  NodeFS.mkdirSync(home, { recursive: true, mode: 0o700 });
  ordinaryFile(marker);
  if (!NodeFS.existsSync(marker))
    NodeFS.writeFileSync(
      marker,
      JSON.stringify({ format: 1, application: "opent3code-preview" }) + "\n",
      {
        flag: "wx",
        mode: 0o600,
      },
    );
  const identity = JSON.parse(NodeFS.readFileSync(marker, "utf8"));
  if (identity.format !== 1 || identity.application !== "opent3code-preview")
    throw new Error("Unrecognized preview home marker.");
  const userdata = NodePath.join(home, "userdata");
  if (NodeFS.existsSync(userdata) && NodeFS.lstatSync(userdata).isSymbolicLink())
    throw new Error("Preview userdata must not be a symbolic link.");
  NodeFS.mkdirSync(userdata, { recursive: true, mode: 0o700 });
  const settings = NodePath.join(userdata, "settings.json");
  ordinaryFile(settings);
  if (!NodeFS.existsSync(settings))
    NodeFS.writeFileSync(
      settings,
      JSON.stringify(
        {
          defaultRuntimeMode: "approval-required",
          defaultModelSelection: { instanceId: "mcode", model: "cli-default" },
          providerInstances: {
            mcode: {
              driver: "mcode",
              enabled: true,
              config: { enabled: true, binaryPath: "mcode", homePath: "" },
            },
            dsh: {
              driver: "dsh",
              enabled: false,
              config: { enabled: true, binaryPath: "dsh", homePath: "" },
            },
          },
        },
        null,
        2,
      ) + "\n",
      { flag: "wx", mode: 0o600 },
    );
  return { home, settings };
}
function main(args) {
  let home = NodePath.join(root, ".opent3code-preview");
  let initOnly = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--init-only") initOnly = true;
    else if (args[i] === "--home-dir" && args[i + 1]) home = args[++i];
    else
      throw new Error(
        "Usage: node scripts/opent3code-preview.mjs [--home-dir ABSOLUTE_PATH] [--init-only]",
      );
  }
  const [major, minor, patch] = process.versions.node.split(".").map(Number);
  if (!initOnly && (major !== 24 || minor < 13 || (minor === 13 && patch < 1)))
    throw new Error("Use Node.js 24.13.1 or newer 24.x, install Vite+, then run vp i.");
  const initialized = initializePreview(home);
  console.log(`OpenT3Code preview data: ${initialized.home}`);
  if (initOnly) return;
  console.log(
    "Open the pairing URL printed by the dev runner. Do not share its token in chat or screenshots.",
  );
  const child = NodeChildProcess.spawn(
    process.execPath,
    ["scripts/dev-runner.ts", "dev", "--home-dir", initialized.home],
    {
      cwd: root,
      stdio: "inherit",
      env: { ...process.env, T3CODE_HOME: initialized.home, T3CODE_DISABLE_AUTO_UPDATE: "1" },
    },
  );
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => child.kill(signal));
  child.on("error", (error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
  child.on("exit", (code) => {
    process.exitCode = code ?? 1;
  });
}
if (
  process.argv[1] &&
  NodePath.resolve(process.argv[1]) === NodeURL.fileURLToPath(import.meta.url)
) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
