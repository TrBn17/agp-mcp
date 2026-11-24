# AGP MCP Server

> Serverless inter-agent MCP mesh, Agent Teams, and zero-token reactive wakeup — exposing the full multi-agent coordination suite in standard **Direct Mode (25 tools)** or compact **Code Mode (4 tools: `whoami`, `docs`, `search`, `execute`)** with zero external runtime dependencies.

## Token Comparison

| Approach | Tools | Context Footprint |
| --- | --- | --- |
| Raw transcript polling + full diffs inline | — | ~40,000+ tokens/session |
| Direct Mode (full AGP + Teams MCP surface) | 25 | ~6,500 tokens |
| **Code Mode (`--codemode` / `?codemode=true`)** | **4** (`whoami`, `docs`, `search`, `execute`) | **~900 tokens** (~86% reduction) |

## Get Started

### Option 1: `npx` Stdio (Recommended — Zero HTTP Server Required)

Runs completely silently in the background over standard MCP `stdio` against shared local SQLite WAL (`~/.gemini/agp/agp.db`) and cross-process locked Team mailboxes (`~/.gemini/agp/teams/`). No daemon or browser needs to be open.

Add to your MCP client configuration (Antigravity, Claude Desktop, Claude Code, Cursor, Windsurf):

```json
{
  "mcpServers": {
    "agp-broker": {
      "command": "npx",
      "args": ["-y", "github:TrBn17/agp-mcp"]
    }
  }
}
```

For **VS Code** (`.vscode/mcp.json`):

```json
{
  "servers": {
    "agp-broker": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "github:TrBn17/agp-mcp"]
    }
  }
}
```

Or generate a client-specific config directly:

```bash
npx -y github:TrBn17/agp-mcp config cursor
npx -y github:TrBn17/agp-mcp config vscode --codemode
```

### Option 2: Antigravity Auto-Hooks & Offline Runtime (One Command)

To let live Antigravity (`agy`) sessions automatically receive incoming messages mid-turn (`PreInvocation`, `PostInvocation`, `Stop`), register identity on startup (`SessionStart`), and enforce workspace file write leases (`PreToolUse`):

```bash
npx -y github:TrBn17/agp-mcp install
# Or enable 4-tool Code Mode by default:
npx -y github:TrBn17/agp-mcp install --codemode
```

This installs the `agp-mesh` plugin into `~/.gemini/config/plugins/agp-mesh`, bundles the offline zero-dependency Python runtime inside it so hooks execute in <40ms even without network access, and wires `agy` automatically.

### Option 3: Global `npm` or `pip` Install

```bash
# Node / npm (requires Node 18+ and Python 3.11+)
npm install -g github:TrBn17/agp-mcp
agp-mcp doctor

# Or pure Python (no Node required)
pip install git+https://github.com/TrBn17/agp-mcp.git
agp-mcp --doctor
```

### Option 4: Streamable HTTP & Optional Live Monitor UI

Opening the Monitor UI is **strictly optional** — background MCP works identically whether the UI is open or closed. Start it only when you want to inspect sessions, teams, token usage, traces, or expose Streamable HTTP `/mcp` + A2A v1.0:

```bash
npx -y github:TrBn17/agp-mcp monitor
# Dashboard: http://127.0.0.1:47821
# Streamable HTTP MCP: http://127.0.0.1:47821/mcp
# Streamable HTTP Code Mode: http://127.0.0.1:47821/mcp?codemode=true
```

### Code Mode

Enable ultra-compact 4-tool **Code Mode** (`whoami`, `docs`, `search`, `execute`) via `--codemode` (stdio), `AGP_CODEMODE=1` (env), or `?codemode=true` (Streamable HTTP):

```json
{
  "mcpServers": {
    "agp-broker": {
      "command": "npx",
      "args": ["-y", "github:TrBn17/agp-mcp", "--codemode"]
    }
  }
}
```

### Tool Result Truncation

In Code Mode, large tool responses from `execute` are structurally truncated to **~6,000 tokens** (~24,000 chars) while **always producing valid JSON** with `--- TRUNCATED ---` markers and omitted item/key counts (modeled on `@cloudflare/mcp`'s structural truncator).

To inspect full un-truncated output when needed:
- Per call: pass `"truncate": false` in `execute` arguments.
- Per session (stdio): pass `--no-truncate`.
- Per request (HTTP): append `?truncateToolResult=false` to `/mcp`.

## The Problem

Running multiple AI coding sessions in parallel (CLI, IDE, subagents, Claude Code, Cursor) hits four recurring failure modes:

1. **One-way blind messaging & token burn** — Session A sends a message to Session B, B is idle or mid-turn and never sees it, and A burns tokens looping `inbox` polls waiting on a dead reply.
2. **Concurrent file edit collisions** — Two agents edit the same file simultaneously with no advisory/enforced write lock or monotonic fencing tokens against stale writers.
3. **Context window bloat** — Loading 25+ tool schemas, raw transcripts, or 100 KB diffs inline exhausts model context before actual coding starts.
4. **Daemon friction** — Requiring a manually started HTTP server just for agents to coordinate breaks standard plug-and-play MCP ergonomics.

`agp-mcp` solves all four with a **serverless SQLite WAL + cross-process file-locked architecture**, deterministic two-way receive & wakeup channels, **4-tool Code Mode**, and pass-by-reference SHA-256 artifacts.

## How Two-Way Receive & Wakeup Works

| Target Session State | Delivery Mechanism | Token Cost While Waiting |
| --- | --- | --- |
| **Active turn (thinking / calling tools)** | Lifecycle hooks (`PreInvocation`, `PostInvocation`, `Stop`) atomically drain both AGP SQLite and Team mailboxes and inject `<agp_untrusted_message>` / `<team_message>` with exact `[REPLY ROUTING]` commands | 0 extra polls |
| **Idle interactive session (CLI / IDE)** | Zero-token reactive background listener (`agp-listen --agent <uri>`) watches SQLite `PRAGMA data_version` + all Team mailboxes (including teams joined later), exits with messages on arrival to trigger Reactive Wakeup | **0 LLM tokens** |
| **Added to a team (`team_join`)** | Immediately writes a Team Handshake (`team_welcome`) containing the exact `reply_call` and `listen_command`, mirrored to both team mailbox and AGP SQLite | 0 extra polls |
| **Autonomous teammate (`team_spawn` / `auto_cli` / `force_cli`)** | Spawns a self-terminating detached worker (`agp.worker`) that drains unread messages, executes the turn, and **auto-bridges a reply back** if the model responds in plain text | On-demand only |
| **Unreachable idle peer (`agp_ask`)** | Every `agp_send`, `agp_ask`, and `team_send` returns an honest `delivery.will_auto_receive` field; `agp_ask` fails fast in **0 ms** instead of blocking until timeout | **0 wasted tokens** |

## Tools

### Code Mode (4 Tools)

| Tool | Purpose |
| --- | --- |
| `whoami` | Returns current session identity, URI, role, active listener state, unread AGP/Team counts, active leases, and teams. Includes OpenAI profile metadata (`_meta: {"openai/profile": true}`) and `structuredContent`. |
| `docs` | Searches built-in AGP v2.1, Agent Teams, two-way wakeup, security, and tool schema documentation by topic or query. |
| `search` | Discovers matching tools (with full schemas), live peer agents, and teams in a single compact call. |
| `execute` | Executes a single tool or chains a multi-step `steps[]` pipeline in one roundtrip with `$prev.*` and `$steps[i].*` output interpolation and structural JSON truncation. |

```text
Agent ──► search({ query: "team task" })
            │ returns matching tool schemas + live peers
            ▼
Agent ──► execute({ steps: [
            { action: "team_create", args: { team: "checkout", description: "API + UI" } },
            { action: "team_task",   args: { team: "checkout", action: "create", title: "Build API", owner: "backend" } },
            { action: "team_task",   args: { team: "checkout", action: "create", title: "Wire UI", owner: "frontend", blocked_by: ["$prev.task.id"] } }
          ] })
            │ runs in-process against SQLite WAL + CrossProcessFileLock
            ▼
       Valid JSON result (~6,000-token structural truncation, or full via truncate: false)
```

### Direct Mode (25 Tools)

When running without `--codemode`, all 25 tools are exposed directly:

- **Identity & Docs (2)**: `whoami`, `docs`
- **Agent Teams (9)**: `team_create`, `team_join`, `team_spawn`, `team_send`, `team_inbox`, `team_task` (`list|create|claim|unclaim|complete|fail|cancel|update|delete` with DAG cycle detection & auto-unblocking), `team_list`, `team_shutdown`, `team_delete`
- **AGP v2.0 Mesh (14)**: `agp_status`, `agp_register`, `agp_unregister`, `agp_discover`, `agp_ask`, `agp_send`, `agp_inbox`, `agp_ack`, `agp_task`, `agp_lease` (`acquire|renew|release|list|check|force_release` with monotonic fencing tokens), `agp_artifact_put`, `agp_artifact_get`, `agp_artifact_list`, `agp_trace_inspect`
- **Resources & Prompts**: 6 static `agp://` resources, 5 resource templates, 6 guided workflow prompts, `logging/setLevel`, `completion/complete`, and `notifications/cancelled`.

## Skills

`agp-mcp` implements the MCP Skills extension (`io.modelcontextprotocol/skills`), advertising `skills/list`, `skills/get`, and `skill://<name>/<path>` resources with SHA-256 content digests:

| Skill | URI | Contents |
| --- | --- | --- |
| `agp-protocol` | `skill://agp-protocol/SKILL.md` | Full serverless AGP v2.1 & Agent Teams collaboration playbook |
| `two-way-wakeup` | `skill://two-way-wakeup/SKILL.md` | Zero-token reactive wakeup, `team_join` handshake, and anti-polling rules |

## Supported Clients & Protocols

- **MCP Protocol Versions**: `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`, `2026-07-28` (stateless-first)
- **MCP Extensions**: `io.modelcontextprotocol/tasks`, `io.modelcontextprotocol/skills`
- **A2A Gateway (HTTP mode)**: A2A v1.0 signed Agent Card (`/.well-known/agent-card.json`), task endpoints, and webhook delivery
- **Security & Provenance**: Ed25519 JWS message signing, RFC 8693 delegation chains, DLP secret redaction, prompt-injection taint screening, monotonic fencing tokens, circuit breakers, hop-limit loop protection
- **Clients**: Antigravity (`agy`), Claude Code, Claude Desktop, Cursor, VS Code, Windsurf, and any standard MCP stdio or Streamable HTTP client

## Usage

Ask your agent naturally:

- *"Check `whoami` and discover which agents are currently working on `backend/**`."*
- *"Create a team `auth-refactor`, join conversation `<cid>` as `frontend`, create a task DAG where UI waits on the API contract, and wait for their reply at zero token cost."*
- *"Acquire an exclusive write lease on `src/payments/**` before editing, then hand off the git diff via `agp_artifact_put`."*

Example multi-step Code Mode `execute` call:

```json
{
  "steps": [
    {
      "action": "agp_register",
      "args": {
        "display_name": "Backend API (Auth)",
        "role": "backend",
        "owned_paths": ["services/auth/**"]
      }
    },
    {
      "action": "agp_lease",
      "args": {
        "action": "acquire",
        "owner_uri": "$prev.agent.agent_uri",
        "paths": ["services/auth/jwt.py"],
        "ttl_sec": 300
      }
    }
  ]
}
```

## Optional Live Monitor UI

The web dashboard is purely a read/control companion over the exact same `~/.gemini/agp/` SQLite WAL and Team mailboxes. Agents keep working seamlessly when you close it.

```bash
npx -y github:TrBn17/agp-mcp monitor
# Or without auto-opening a browser tab:
npx -y github:TrBn17/agp-mcp monitor --no-browser
```

## Development

```bash
git clone https://github.com/TrBn17/agp-mcp.git
cd agp-mcp

# Run full unit + multi-process MCP test suite
npm test
# or: python -m unittest discover -s tests -v

# Run environment, SQLite WAL & lock diagnostics
node bin/agp-mcp.mjs doctor
```

### Requirements

- **Python 3.11+** (stdlib only — 0 external Python packages)
- **Node.js 18+** (for `npx` / `npm` CLI wrapper — 0 external npm packages)

## License

[MIT](./LICENSE)
