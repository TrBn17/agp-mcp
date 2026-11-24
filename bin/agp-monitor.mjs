#!/usr/bin/env node
/**
 * agp-monitor — Optional Live Monitor HTML Dashboard & Streamable HTTP MCP endpoint.
 */
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const cli = path.join(__dirname, "agp-mcp.mjs");
const child = spawn(process.execPath, [cli, "monitor", ...process.argv.slice(2)], {
  stdio: "inherit",
});
child.on("exit", (code) => process.exit(code ?? 0));
