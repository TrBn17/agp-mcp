#!/usr/bin/env node
/**
 * agp-mcp CLI & Serverless Stdio MCP Wrapper (zero external npm dependencies).
 *
 * IMPORTANT: In default stdio MCP mode, NEVER write banners or logs to stdout —
 * stdout is strictly reserved for newline-delimited JSON-RPC 2.0 frames.
 */

import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PACKAGE_ROOT = path.resolve(__dirname, "..");

function detectPython() {
  const candidates = [];
  if (process.env.AGP_PYTHON) {
    candidates.push([process.env.AGP_PYTHON, []]);
  }
  if (process.platform === "win32") {
    candidates.push(["python", []], ["py", ["-3"]], ["python3", []]);
  } else {
    candidates.push(["python3", []], ["python", []]);
  }

  for (const [cmd, prefix] of candidates) {
    try {
      const res = spawnSync(
        cmd,
        [...prefix, "-c", "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"],
        { stdio: "ignore", windowsHide: true }
      );
      if (!res.error && res.status === 0) {
        return { cmd, prefix };
      }
    } catch {
      // Try next candidate
    }
  }
  return null;
}

function buildPythonEnv() {
  const existingPyPath = process.env.PYTHONPATH || "";
  const pyPath = existingPyPath
    ? `${PACKAGE_ROOT}${path.delimiter}${existingPyPath}`
    : PACKAGE_ROOT;
  return {
    ...process.env,
    AGP_HOME: PACKAGE_ROOT,
    PYTHONPATH: pyPath,
    PYTHONUTF8: "1",
  };
}

function execPythonModule(moduleName, args) {
  const py = detectPython();
  if (!py) {
    process.stderr.write(
      "[agp-mcp] Error: Python 3.11+ is required on PATH (or set AGP_PYTHON).\n"
    );
    process.exit(1);
  }

  const child = spawn(py.cmd, [...py.prefix, "-m", moduleName, ...args], {
    stdio: "inherit",
    env: buildPythonEnv(),
    windowsHide: false,
  });

  const forwardSignal = (sig) => {
    try {
      child.kill(sig);
    } catch {
      // Ignore already exited child
    }
  };
  process.on("SIGINT", () => forwardSignal("SIGINT"));
  process.on("SIGTERM", () => forwardSignal("SIGTERM"));

  child.on("error", (err) => {
    process.stderr.write(`[agp-mcp] Failed to launch Python: ${err.message}\n`);
    process.exit(1);
  });

  child.on("exit", (code, signal) => {
    if (signal) {
      process.exit(1);
    }
    process.exit(code ?? 0);
  });
}

function copyDirClean(src, dst) {
  fs.mkdirSync(dst, { recursive: true });
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    if (entry.name === "__pycache__" || entry.name.endsWith(".pyc")) {
      continue;
    }
    const srcPath = path.join(src, entry.name);
    const dstPath = path.join(dst, entry.name);
    if (entry.isDirectory()) {
      copyDirClean(srcPath, dstPath);
    } else if (entry.isFile()) {
      fs.copyFileSync(srcPath, dstPath);
    }
  }
}

function runAgyCommand(args) {
  let res = spawnSync("agy", args, {
    stdio: "pipe",
    encoding: "utf-8",
    windowsHide: true,
  });
  if (res.error && process.platform === "win32") {
    res = spawnSync("agy.cmd", args, {
      stdio: "pipe",
      encoding: "utf-8",
      windowsHide: true,
    });
  }
  return {
    ok: !res.error && res.status === 0,
    stdout: (res.stdout || "").trim(),
    stderr: (res.stderr || "").trim(),
  };
}

function installPlugin(args) {
  let codemode = false;
  let skipAgy = false;
  let customTarget = "";

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--codemode") codemode = true;
    else if (a === "--no-agy") skipAgy = true;
    else if (a === "--target-dir" && args[i + 1]) {
      customTarget = args[++i];
    }
  }

  const py = detectPython();
  if (!py) {
    process.stderr.write(
      "[agp-mcp] Error: Python 3.11+ is required before installing hooks.\n"
    );
    process.exit(1);
  }

  const geminiHome = process.env.GEMINI_HOME || path.join(os.homedir(), ".gemini");
  const targetDir =
    customTarget || path.join(geminiHome, "config", "plugins", "agp-mesh");
  const srcPlugin = path.join(PACKAGE_ROOT, "plugins", "agp-mesh");
  const srcAgp = path.join(PACKAGE_ROOT, "agp");

  if (!fs.existsSync(srcPlugin) || !fs.existsSync(srcAgp)) {
    process.stderr.write(`[agp-mcp] Error: Package assets missing in ${PACKAGE_ROOT}\n`);
    process.exit(1);
  }

  // Overwrite in-place cleanly (Windows returns EPERM if removing a plugin directory
  // that an active Antigravity session currently has open).
  if (fs.existsSync(targetDir)) {
    try {
      const st = fs.lstatSync(targetDir);
      if (st.isSymbolicLink()) {
        fs.rmSync(targetDir, { recursive: true, force: true });
      }
    } catch {
      // Proceed with in-place overwrite
    }
  }
  copyDirClean(srcPlugin, targetDir);

  // Bundle pure-stdlib Python runtime inside plugin so hooks & stdio stay fast and offline
  // even if this installer was executed from an ephemeral `npx` cache directory.
  const runtimeAgpDir = path.join(targetDir, "runtime", "agp");
  copyDirClean(srcAgp, runtimeAgpDir);

  // Remove duplicate plugin mcp_config.json when registering globally with agy, or keep portable config
  const stdioScript = path
    .join(targetDir, "scripts", "agp_mcp_stdio.py")
    .replaceAll("\\", "/");
  const pyCmd = py.cmd;
  const mcpArgs = [...py.prefix, stdioScript, ...(codemode ? ["--codemode"] : [])];

  // Update hooks.json to use detected python binary name if non-standard (e.g., python3 on Linux/macOS)
  const hooksPath = path.join(targetDir, "hooks.json");
  if (fs.existsSync(hooksPath) && (pyCmd !== "python" || py.prefix.length > 0)) {
    const rawHooks = fs.readFileSync(hooksPath, "utf-8");
    const pyPrefixStr = [pyCmd, ...py.prefix].join(" ");
    fs.writeFileSync(
      hooksPath,
      rawHooks.replaceAll("python scripts/agp_hook.py", `${pyPrefixStr} scripts/agp_hook.py`),
      "utf-8"
    );
  }

  let agyRegistered = false;
  if (!skipAgy) {
    const probe = runAgyCommand(["--version"]);
    if (probe.ok) {
      // Avoid duplicate MCP registration between global mcp_config and plugin mcp_config
      const pluginMcpCfg = path.join(targetDir, "mcp_config.json");
      if (fs.existsSync(pluginMcpCfg)) {
        fs.rmSync(pluginMcpCfg, { force: true });
      }
      runAgyCommand(["plugin", "enable", "agp-mesh"]);
      runAgyCommand(["mcp", "remove", "agp-broker"]);
      const addRes = runAgyCommand([
        "mcp",
        "add",
        "agp-broker",
        pyCmd,
        ...mcpArgs,
      ]);
      agyRegistered = addRes.ok;
    }
  }

  console.log(`[agp-mcp] Installed Antigravity plugin & serverless MCP runtime:`);
  console.log(`  Plugin  : ${targetDir}`);
  console.log(`  Runtime : ${runtimeAgpDir} (offline, zero-dependency Python stdlib)`);
  console.log(`  Mode    : ${codemode ? "Code Mode (4 tools: whoami, docs, search, execute)" : "Direct Mode (25 tools)"}`);
  console.log(`  State   : ${path.join(geminiHome, "agp")}`);
  if (agyRegistered) {
    console.log(`  agy CLI : enabled plugin 'agp-mesh' & registered MCP 'agp-broker'`);
  } else if (!skipAgy) {
    console.log(`  agy CLI : not found on PATH — add the MCP server to your client config via:`);
    console.log(`            npx -y agp-mcp config${codemode ? " --codemode" : ""}`);
  }
  console.log(`[agp-mcp] Ready! Sessions communicate silently in the background with no HTTP daemon needed.`);
  console.log(`[agp-mcp] Optional UI: npx -y agp-mcp monitor`);
}

function uninstallPlugin(args) {
  let skipAgy = false;
  let customTarget = "";
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--no-agy") skipAgy = true;
    else if (a === "--target-dir" && args[i + 1]) {
      customTarget = args[++i];
    }
  }
  const geminiHome = process.env.GEMINI_HOME || path.join(os.homedir(), ".gemini");
  const targetDir =
    customTarget || path.join(geminiHome, "config", "plugins", "agp-mesh");

  if (!skipAgy) {
    runAgyCommand(["plugin", "disable", "agp-mesh"]);
    runAgyCommand(["mcp", "remove", "agp-broker"]);
  }
  if (fs.existsSync(targetDir)) {
    fs.rmSync(targetDir, { recursive: true, force: true });
    console.log(`[agp-mcp] Removed plugin directory: ${targetDir}`);
  } else {
    console.log(`[agp-mcp] Plugin directory not found: ${targetDir}`);
  }
}

function printHelp() {
  console.log(`agp-mcp — Serverless Inter-Agent MCP Mesh & Agent Teams (v2.1.0)

Usage:
  agp-mcp [flags]                        Run standard stdio MCP server (default, zero HTTP daemon)
  agp-mcp --codemode                     Run 4-tool Code Mode stdio MCP (~900 tokens)
  agp-mcp install [--codemode]           Install Antigravity auto-hooks plugin & offline runtime
  agp-mcp uninstall                      Remove Antigravity plugin & MCP registration
  agp-mcp monitor [--no-browser]         Open optional HTML Live Monitor UI & Streamable HTTP (/mcp)
  agp-mcp listen --agent <uri>           Run zero-token reactive inbox listener
  agp-mcp doctor                         Run environment, SQLite WAL, and lock diagnostics
  agp-mcp config [client] [--codemode]   Print copy-paste MCP JSON config (antigravity|cursor|claude-desktop|claude-code|vscode|windsurf|generic)
  agp-mcp hook <event>                   Execute a lifecycle hook (session_start|pre_invocation|post_invocation|pre_tool_use|post_tool_use|stop)
  agp-mcp --version                      Print version

Flags (stdio / http):
  --codemode                             Expose 4-tool Code Mode (whoami, docs, search, execute)
  --no-truncate                          Disable default ~6,000-token structural JSON truncation
  --transport stdio|http                 MCP transport (default: stdio)
  --data-dir <path>                      Override state directory (default: ~/.gemini/agp)
  --host <host> --port <port>            HTTP bind address (default: 127.0.0.1:47821)
`);
}

function main() {
  const argv = process.argv.slice(2);
  const first = argv[0] || "";
  const rest = argv.slice(1);

  if (first === "help" || first === "--help" || first === "-h") {
    printHelp();
    return;
  }
  if (first === "version" || first === "-v") {
    execPythonModule("agp.mcp_server", ["--version"]);
    return;
  }
  if (first === "doctor") {
    execPythonModule("agp.mcp_server", ["--doctor", ...rest]);
    return;
  }
  if (first === "config") {
    // Support both `agp-mcp config cursor --codemode` and `agp-mcp config --codemode`
    if (rest.length > 0 && !rest[0].startsWith("-")) {
      execPythonModule("agp.mcp_server", ["--print-config", rest[0], ...rest.slice(1)]);
    } else {
      execPythonModule("agp.mcp_server", ["--print-config", "generic", ...rest]);
    }
    return;
  }
  if (first === "monitor") {
    const hasNoBrowser = rest.includes("--no-browser");
    const filtered = rest.filter((a) => a !== "--no-browser");
    const monitorArgs = ["--transport", "http", ...filtered];
    if (!hasNoBrowser && !filtered.includes("--open-browser")) {
      monitorArgs.push("--open-browser");
    }
    execPythonModule("agp.mcp_server", monitorArgs);
    return;
  }
  if (first === "listen") {
    execPythonModule("agp.listen", rest);
    return;
  }
  if (first === "hook") {
    execPythonModule("agp.hook_runner", rest);
    return;
  }
  if (first === "worker") {
    execPythonModule("agp.worker", rest);
    return;
  }
  if (first === "install") {
    installPlugin(rest);
    return;
  }
  if (first === "uninstall") {
    uninstallPlugin(rest);
    return;
  }

  // Default: run standard serverless stdio MCP server
  execPythonModule("agp.mcp_server", argv);
}

main();
