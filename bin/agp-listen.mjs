#!/usr/bin/env node
/**
 * agp-listen — Zero-token reactive background inbox listener for AGP & Agent Teams.
 */
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const cli = path.join(__dirname, "agp-mcp.mjs");
const child = spawn(process.execPath, [cli, "listen", ...process.argv.slice(2)], {
  stdio: "inherit",
});
child.on("exit", (code) => process.exit(code ?? 0));
