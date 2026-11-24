"""Standard MCP JSON-RPC 2.0 Server (2024-11-05 -> 2026-07-28) for AGP v2.0 & Agent Teams.

Supports both:
  - Standalone embedded Stdio transport (DEFAULT — zero HTTP server required!)
  - Optional Streamable HTTP transport (`POST /mcp`, `GET /mcp`, `DELETE /mcp`) when Monitor UI is open

Implements full MCP specification primitives:
  - `initialize` / `notifications/initialized` / `notifications/cancelled` / `ping`
  - `logging/setLevel`
  - `tools/list` (with MCP Tool Annotations) & `tools/call` (23 scenario-complete AGP & Team tools)
  - `resources/list`, `resources/templates/list`, `resources/read`
  - `prompts/list`, `prompts/get` (6 multi-agent orchestration playbooks)
  - `completion/complete`
  - `io.modelcontextprotocol/tasks` (`tasks/list`, `tasks/get`, `tasks/update`, `tasks/cancel`)
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import threading
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import __protocol_version__, __version__
from .config import DEFAULT_HOST, DEFAULT_PORT, PLUGIN_DIR
from .listen import build_listen_command, collect_unread_team_messages_for_agent, is_conversation_presence_locked
from .storage import AGPStore, epoch_now
from .teams import TeamManager
from .truncate import TRUNCATE_ARGUMENT_DESCRIPTION, format_tool_result

SKILLS_EXTENSION_ID = "io.modelcontextprotocol/skills"
BUNDLED_SKILL_NAMES = ("agp-protocol", "two-way-wakeup")

SUPPORTED_PROTOCOL_VERSIONS = {
    "2026-07-28",
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
}

VALID_LOG_LEVELS = {
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
}

MCP_TASK_STATE_MAP = {
    "TASK_STATE_SUBMITTED": "working",
    "TASK_STATE_WORKING": "working",
    "TASK_STATE_INPUT_REQUIRED": "input_required",
    "TASK_STATE_AUTH_REQUIRED": "input_required",
    "TASK_STATE_COMPLETED": "completed",
    "TASK_STATE_FAILED": "failed",
    "TASK_STATE_CANCELED": "cancelled",
    "TASK_STATE_REJECTED": "failed",
}


def _ann(
    title: str,
    *,
    read_only: bool = False,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
) -> Dict[str, Any]:
    return {
        "title": title,
        "readOnlyHint": read_only,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
        "openWorldHint": open_world,
    }


WHOAMI_TOOL: Dict[str, Any] = {
    "name": "whoami",
    "title": "Current AGP Agent Identity",
    "description": (
        "Return the stable AGP agent identity bound to this MCP connection. "
        "Takes no arguments. The `id` stays consistent across reconnections and metadata updates."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "outputSchema": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Stable AGP URI identity for this connection."},
            "name": {"type": "string", "description": "Human-readable display name."},
            "surface": {"type": "string", "description": "Client surface (cli, ide, app, cursor, claude, mcp)."},
            "role": {"type": "string", "description": "Registered specialist role."},
            "listening": {"type": "boolean", "description": "Whether zero-token agp.listen is currently active."},
        },
        "required": ["id"],
        "additionalProperties": False,
    },
    "annotations": _ann("Current AGP Agent Identity", read_only=True, idempotent=True),
    "_meta": {"openai/profile": True},
}

DOCS_TOOL: Dict[str, Any] = {
    "name": "docs",
    "title": "Search AGP Protocol & Tool Docs",
    "description": (
        "Search built-in AGP v2.1 documentation: protocol contracts, error codes, two-way wakeup "
        "patterns (`agp.listen`, `team_join`, `agp_ask`), workspace leases, and tool schemas."
    ),
    "annotations": _ann("Search AGP Documentation", read_only=True, idempotent=True),
    "inputSchema": {
        "type": "object",
        "required": ["query"],
        "properties": {
            "query": {
                "type": "string",
                "description": "Topic, error code, or tool name to search (e.g. 'wakeup', 'lease', 'team_join', 'ERR_CIRCUIT_OPEN').",
            },
            "limit": {"type": "integer", "default": 6},
        },
    },
}

CODEMODE_TOOLS: List[Dict[str, Any]] = [
    WHOAMI_TOOL,
    DOCS_TOOL,
    {
        "name": "search",
        "title": "Search AGP Tools & Live Mesh State",
        "description": (
            "Discover AGP/Team tool schemas or query live mesh state (agents, teams, tasks, leases, "
            "artifacts, traces) in a single compact call. Output is structurally capped at ~6,000 tokens "
            "unless truncate=false."
        ),
        "annotations": _ann("Search Tools & Mesh State", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["tools", "agents", "teams", "tasks", "leases", "artifacts", "traces", "all"],
                    "default": "tools",
                    "description": "What to search: 'tools' (API catalog & schemas) or live mesh resources.",
                },
                "query": {
                    "type": "string",
                    "description": "Optional substring/keyword filter.",
                },
                "filters": {
                    "type": "object",
                    "description": "Optional structured filters passed to the underlying query (e.g. role, status, team, path_filter).",
                },
                "truncate": {
                    "type": "boolean",
                    "description": TRUNCATE_ARGUMENT_DESCRIPTION,
                },
            },
        },
    },
    {
        "name": "execute",
        "title": "Execute AGP Action or Multi-Step Pipeline",
        "description": (
            "Execute any AGP or Agent Teams action (`action` + `args`), OR chain multiple actions in "
            "one round-trip via `steps: [{action, args}, ...]` with `$prev.field` and `$steps[0].field` "
            "value substitution. Output is structurally capped at ~6,000 tokens unless truncate=false."
        ),
        "annotations": _ann("Execute AGP Action or Pipeline"),
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "Single tool name to run (e.g. 'agp_ask', 'team_join', 'team_send', 'agp_lease').",
                },
                "args": {
                    "type": "object",
                    "description": "Arguments for the single `action`.",
                },
                "steps": {
                    "type": "array",
                    "description": "Optional multi-step pipeline executed sequentially in 1 tool call.",
                    "items": {
                        "type": "object",
                        "required": ["action"],
                        "properties": {
                            "action": {"type": "string"},
                            "args": {"type": "object"},
                        },
                    },
                },
                "stop_on_error": {"type": "boolean", "default": True},
                "truncate": {
                    "type": "boolean",
                    "description": TRUNCATE_ARGUMENT_DESCRIPTION,
                },
            },
        },
    },
]

# Built-in searchable documentation snippets for the `docs` tool
AGP_DOCS_INDEX: List[Dict[str, str]] = [
    {
        "id": "architecture-serverless",
        "title": "Serverless Background MCP & Optional Monitor UI",
        "tags": "serverless stdio background monitor ui http daemon port",
        "content": (
            "agp-mcp runs silently in the background over standard stdio directly against shared "
            "SQLite WAL (~/.gemini/agp/agp.db) and cross-process locked Team mailboxes (~/.gemini/agp/teams/). "
            "No HTTP server needs to run. Start `agp-monitor` (or `npx agp-mcp monitor`) ONLY when you want "
            "to inspect the live HTML dashboard; closing the UI has zero effect on running AI sessions."
        ),
    },
    {
        "id": "two-way-wakeup",
        "title": "Guaranteed Two-Way Delivery & Zero-Token Wakeup",
        "tags": "wakeup listen reactive reply hook preinvocation postinvocation stop agp_ask delivery token",
        "content": (
            "Target sessions receive and reply through 4 coordinated layers: "
            "(1) Active Turn Hooks (PreInvocation, PostInvocation, Stop) drain BOTH AGP SQLite (24h TTL) and Team mailboxes "
            "with exact [REPLY ROUTING] instructions; "
            "(2) Zero-token reactive listener (`agp-listen --agent <uri>`) wakes idle interactive sessions via Reactive Wakeup "
            "at 0 LLM tokens while sleeping; "
            "(3) `team_join` immediately sends a Team Handshake opening the member's receive/reply channel; "
            "(4) `team_spawn` / `wakeup_mode='auto_cli'` / `wake_mode='force_cli'` spawns a self-terminating detached worker "
            "that auto-bridges plain-text replies back to the sender. "
            "Always check `delivery.will_auto_receive` or use `agp_ask` (0ms fail-fast) to never waste tokens polling."
        ),
    },
    {
        "id": "codemode-and-truncation",
        "title": "Code Mode & Structural Valid-JSON Truncation",
        "tags": "codemode search execute truncate token context pipeline batch",
        "content": (
            "Activate Code Mode via `--codemode` (CLI/stdio) or `?codemode=true` (HTTP) to shrink the tool surface from "
            "25 direct tools down to 4 tools (`whoami`, `docs`, `search`, `execute` — ~900 tokens total). "
            "`execute` supports chained `steps` with `$prev.key` and `$steps[i].key` substitution in 1 round-trip. "
            "Code Mode results are structurally capped at ~6,000 tokens while remaining valid JSON (`--- TRUNCATED ---`). "
            "Pass `truncate: false` only when writing full output outside model context."
        ),
    },
    {
        "id": "agent-teams",
        "title": "Claude-Style Agent Teams Workflow",
        "tags": "team team_create team_join team_spawn team_send team_inbox team_task team_list team_shutdown team_delete",
        "content": (
            "1. `team_create(team, description)` initializes roster, task DAG, and mailboxes. "
            "2. `team_task(action='create', blocked_by=[...])` builds a cycle-checked task DAG. "
            "3. `team_join` attaches a live session and sends a welcome handshake with reply & listener commands; "
            "`team_spawn` launches an autonomous CLI teammate. "
            "4. `team_send` routes messages peer-to-peer or to `'all'` and bridges to SQLite hooks. "
            "5. `team_shutdown(team, name='all')` and `team_delete(team)` clean up."
        ),
    },
    {
        "id": "leases-and-fencing",
        "title": "Workspace File Leases & Monotonic Fencing Tokens",
        "tags": "lease lock fencing_token conflict pre_tool_use owned_paths ERR_LEASE_CONFLICT ERR_STALE_FENCING_TOKEN",
        "content": (
            "Before editing shared files, call `agp_lease(action='check'|'acquire', paths=['src/**'], mode='exclusive_write')`. "
            "PreToolUse hooks enforce exclusive locks and monotonic `fencing_token` epochs to prevent split-brain/zombie edits. "
            "Supports relative-vs-absolute Windows/POSIX path overlap detection and automatic lease cleanup on stdio EOF."
        ),
    },
    {
        "id": "security-and-errors",
        "title": "Security Controls, Channels, Artifacts & Error Codes",
        "tags": "security dlp ed25519 jws taint injection artifact channel circuit ERR_CIRCUIT_OPEN ERR_ROUTING_CYCLE_DETECTED ERR_CONTEXT_BUDGET_EXCEEDED ERR_IDEMPOTENCY_CONFLICT",
        "content": (
            "All envelopes are DLP-redacted, signed with Ed25519 JWS (RFC 7515 + RFC 8785 JCS), checked for DAG routing "
            "cycles (`ERR_ROUTING_CYCLE_DETECTED`), hop limits (`ERR_HOP_LIMIT_EXCEEDED`), context budget (`ERR_CONTEXT_BUDGET_EXCEEDED`), "
            "idempotency (`ERR_IDEMPOTENCY_CONFLICT`), taint (`ERR_UNTRUSTED_TAINT`), and circuit breakers (`ERR_CIRCUIT_OPEN`, resettable via `agp_status`). "
            "Store payloads > 4KB with `agp_artifact_put` and paginate with `agp_artifact_get(offset_bytes=...)`. "
            "Broadcasts (`all` and `channel:<topic>`) use per-agent `channel_reads` tracking."
        ),
    },
]

MCP_TOOLS: List[Dict[str, Any]] = [
    WHOAMI_TOOL,
    DOCS_TOOL,
    {
        "name": "agp_status",
        "description": (
            "Inspect your bound AGP identity, unread AGP & Team message counts, held file leases, "
            "open circuit breakers, active async tasks, zero-token listener command, and optional "
            "Monitor UI status. Optionally reset a recovered circuit breaker."
        ),
        "annotations": _ann("AGP Status & Diagnostics", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_uri": {"type": "string", "description": "Optional AGP URI to inspect (defaults to current session)."},
                "reset_circuit_uri": {
                    "type": "string",
                    "description": "Optional target URI whose OPEN circuit breaker should be reset back to CLOSED.",
                },
            },
        },
    },
    {
        "name": "agp_register",
        "description": (
            "Register or update this AI session's Agent Card (display_name, role, owned_paths, "
            "capabilities, channels, wakeup_mode, status) so peers can discover and route messages to it."
        ),
        "annotations": _ann("Register AGP Agent Card", idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_uri": {"type": "string", "description": "Your AGP URI (from SessionStart, e.g. agp://local/cli/<conv_id>)."},
                "display_name": {"type": "string", "description": "Clear task-based human name, e.g. 'Backend API (Auth Fix)'."},
                "role": {"type": "string", "description": "Role label, e.g. backend-specialist, frontend-dev, reviewer, coordinator."},
                "description": {"type": "string", "description": "Short summary of what this agent owns."},
                "cwd": {"type": "string", "description": "Current workspace root directory."},
                "owned_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Glob paths owned by this agent, e.g. ['ai-platform/**'].",
                },
                "capabilities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Declared capabilities, e.g. ['code.read', 'code.edit', 'test.run'].",
                },
                "channels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional pub/sub channels to subscribe to, e.g. ['api-contract', 'release'].",
                },
                "max_context_tokens": {
                    "type": "integer",
                    "description": "Max estimated tokens per incoming AGP message (default 4000).",
                },
                "status": {"type": "string", "enum": ["online", "busy", "waiting", "draining", "offline"]},
                "wakeup_mode": {
                    "type": "string",
                    "enum": ["hook", "auto_cli", "webhook"],
                    "description": "Wakeup mode: 'hook' (active hooks + agp.listen), 'auto_cli' (detached CLI resume), or 'webhook'.",
                },
                "webhook_url": {"type": "string", "description": "Optional HTTPS/HTTP webhook callback URL."},
            },
        },
    },
    {
        "name": "agp_unregister",
        "description": (
            "Gracefully sign off (`peer.bye`) from the AGP mesh, mark this agent offline, "
            "and release all workspace file leases it holds."
        ),
        "annotations": _ann("Unregister Agent & Release Leases", destructive=False, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_uri": {"type": "string", "description": "Your AGP URI (defaults to current session)."},
                "release_leases": {"type": "boolean", "default": True},
                "reason": {"type": "string", "default": "Session complete"},
            },
        },
    },
    {
        "name": "agp_discover",
        "description": (
            "Discover other AI sessions currently active or recently seen on the AGP mesh. "
            "Filter by role, capability, workspace/path ownership, surface, search query, or online status. "
            "Shows whether each peer has an active zero-token listener (`listening`) or `wakeup_mode`."
        ),
        "annotations": _ann("Discover AGP Peers", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "role": {"type": "string", "description": "Optional role substring filter."},
                "capability": {"type": "string", "description": "Optional capability filter."},
                "path_filter": {"type": "string", "description": "Optional file/folder glob filter (e.g. 'ai-platform/**')."},
                "status": {"type": "string", "enum": ["online", "busy", "waiting", "offline"]},
                "surface": {"type": "string", "description": "Optional surface filter (cli, ide, app, mcp, cursor, claude)."},
                "query": {"type": "string", "description": "Optional free-text search across name, task, project, cwd."},
                "include_subagents": {"type": "boolean", "default": False},
                "exclude_self": {"type": "boolean", "default": False, "description": "Exclude current session from results."},
            },
        },
    },
    {
        "name": "agp_send",
        "description": (
            "Send an AGP structured message to another AI session or channel (`@handle`, URI, conversation_id, "
            "'all', or 'channel:<topic>'). Enforces schema validation, DLP secret redaction, Ed25519 JWS, "
            "token budget, DAG cycle guard, idempotency, and circuit breakers. Returns honest `delivery` status "
            "(`will_auto_receive`, `mode`, `remediation`) so you never burn tokens waiting on an unreachable idle peer."
        ),
        "annotations": _ann("Send AGP Message"),
        "inputSchema": {
            "type": "object",
            "required": ["type", "summary", "payload"],
            "properties": {
                "from_uri": {"type": "string", "description": "Sender AGP URI (defaults to current session)."},
                "to_uri": {
                    "type": "string",
                    "description": "Recipient AGP URI, @handle, conversation_id, 'all', or 'channel:<topic>'. Optional if in_reply_to is set.",
                },
                "type": {
                    "type": "string",
                    "enum": [
                        "task.request",
                        "task.ack",
                        "task.progress",
                        "task.clarify",
                        "task.result",
                        "task.cancel",
                        "sync.git_handoff",
                        "review.request",
                        "review.verdict",
                        "conflict.report",
                        "conflict.arbitrate",
                        "hitl.escalate",
                        "hitl.decision",
                        "lease.conflict",
                        "peer.query",
                        "chat.note",
                    ],
                },
                "summary": {"type": "string", "description": "Concise summary (<= 200 tokens) of the message."},
                "payload": {"type": "object", "description": "Structured JSON payload matching the message type contract."},
                "verbatim_constraints": {"type": "array", "items": {"type": "string"}},
                "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"], "default": "normal"},
                "idempotency_key": {"type": "string"},
                "trace_id": {"type": "string"},
                "parent_span_id": {"type": "string"},
                "correlation_id": {"type": "string"},
                "in_reply_to": {"type": "string", "description": "msg_id being replied to (auto-inherits trace_id, correlation_id, to_uri)."},
                "hop_count": {"type": "integer", "default": 1},
                "max_hops": {"type": "integer", "default": 4},
                "visited_uris": {"type": "array", "items": {"type": "string"}},
                "ttl_sec": {"type": "integer", "default": 86400},
                "content_taint": {
                    "type": "string",
                    "enum": ["local_trusted", "peer_generated", "external_untrusted"],
                    "default": "peer_generated",
                },
                "async_task": {"type": "boolean", "default": False},
                "wake": {"type": "boolean", "default": True},
                "wake_mode": {
                    "type": "string",
                    "enum": ["auto", "force_cli", "none"],
                    "default": "auto",
                    "description": "'auto' uses listener/hook/auto_cli; 'force_cli' spawns detached CLI resume if target has a conversation_id; 'none' queues silently.",
                },
                "artifacts": {"type": "array", "items": {"type": "object"}},
            },
        },
    },
    {
        "name": "agp_ask",
        "description": (
            "Send a structured request to a peer AND wait in ONE tool call for the correlated reply (`correlation_id`). "
            "Includes an anti-token-burn guard: if the recipient cannot auto-receive (`will_auto_receive=false`), "
            "returns immediately in 0ms with remediation advice instead of waiting blindly."
        ),
        "annotations": _ann("Ask Peer & Wait for Reply"),
        "inputSchema": {
            "type": "object",
            "required": ["to_uri", "summary"],
            "properties": {
                "from_uri": {"type": "string"},
                "to_uri": {"type": "string", "description": "Recipient AGP URI, @handle, or conversation_id."},
                "type": {
                    "type": "string",
                    "enum": ["task.request", "review.request", "peer.query", "chat.note"],
                    "default": "task.request",
                },
                "summary": {"type": "string"},
                "payload": {"type": "object", "description": "Structured payload (for task.request, goal defaults to summary if omitted)."},
                "verbatim_constraints": {"type": "array", "items": {"type": "string"}},
                "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"], "default": "high"},
                "wake_mode": {"type": "string", "enum": ["auto", "force_cli", "none"], "default": "auto"},
                "wait_timeout_sec": {
                    "type": "number",
                    "default": 35,
                    "description": "Max seconds (1-120) to wait when target can auto-receive.",
                },
                "fail_fast_if_unreachable": {
                    "type": "boolean",
                    "default": True,
                    "description": "Return immediately instead of waiting if target is idle without listener/auto_cli.",
                },
                "artifacts": {"type": "array", "items": {"type": "object"}},
            },
        },
    },
    {
        "name": "agp_inbox",
        "description": (
            "Read or peek incoming AGP messages addressed to this AI session (and subscribed channels). "
            "Supports filtering by correlation_id, trace_id, msg_type, priority, and bounded cross-process "
            "long-polling (`wait_timeout_sec` up to 60s). Set `mark_delivered=false` to peek without consuming."
        ),
        "annotations": _ann("Read AGP Inbox", read_only=False),
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent_uri": {"type": "string", "description": "Your AGP URI (defaults to current session)."},
                "include_delivered": {"type": "boolean", "default": False},
                "mark_delivered": {"type": "boolean", "default": True, "description": "Set false to peek without marking messages read."},
                "include_team_messages": {"type": "boolean", "default": True, "description": "Also surface unread Team mailbox messages linked to this session."},
                "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
                "correlation_id": {"type": "string", "description": "Optional filter to wait for a specific task/reply thread."},
                "trace_id": {"type": "string"},
                "msg_type": {"type": "string"},
                "channels": {"type": "array", "items": {"type": "string"}},
                "since_seq": {"type": "integer", "default": 0},
                "limit": {"type": "integer", "default": 20},
                "wait_timeout_sec": {"type": "number", "default": 0, "description": "Optional seconds (0-60) to wait if inbox is empty."},
            },
        },
    },
    {
        "name": "agp_ack",
        "description": "Acknowledge, accept, reject, complete, or mark failed an incoming AGP message.",
        "annotations": _ann("Acknowledge AGP Message", idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["msg_id", "status"],
            "properties": {
                "msg_id": {"type": "string"},
                "status": {"type": "string", "enum": ["accepted", "rejected", "completed", "failed", "cancelled"]},
                "reason": {"type": "string", "default": ""},
                "actor_uri": {"type": "string"},
            },
        },
    },
    {
        "name": "agp_task",
        "description": (
            "Manage durable async tasks (`io.modelcontextprotocol/tasks` & A2A v1.0 Tasks): "
            "list tasks, get task state/history/result, wait for state transition, supply mid-flight "
            "clarification/HITL input (`update`), or cancel a running task."
        ),
        "annotations": _ann("Manage Durable Async Tasks"),
        "inputSchema": {
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": {"type": "string", "enum": ["list", "get", "wait", "update", "cancel"]},
                "task_id": {"type": "string", "description": "Task ID (required for get, wait, update, cancel)."},
                "context_id": {"type": "string", "description": "Optional contextId filter for list."},
                "state": {"type": "string", "description": "Optional state filter for list (e.g. WORKING, COMPLETED, INPUT_REQUIRED)."},
                "input": {"type": "object", "description": "Mid-flight clarification or HITL input for action='update'."},
                "reason": {"type": "string", "description": "Cancellation reason for action='cancel'."},
                "actor_uri": {"type": "string"},
                "wait_timeout_sec": {"type": "number", "default": 25, "description": "Seconds (0-120) for action='wait'."},
                "limit": {"type": "integer", "default": 30},
            },
        },
    },
    {
        "name": "agp_lease",
        "description": (
            "Acquire, renew, release, check (dry-run), force_release, or list workspace file/path leases "
            "with monotonic `fencing_token` protection to prevent concurrent edit conflicts."
        ),
        "annotations": _ann("Manage Workspace File Leases"),
        "inputSchema": {
            "type": "object",
            "required": ["action"],
            "properties": {
                "action": {"type": "string", "enum": ["acquire", "renew", "release", "list", "check", "force_release"]},
                "owner_uri": {"type": "string", "description": "Your AGP URI (defaults to current session)."},
                "paths": {"type": "array", "items": {"type": "string"}, "description": "File paths, globs, or lease_ids."},
                "mode": {"type": "string", "enum": ["exclusive_write", "shared_read"], "default": "exclusive_write"},
                "ttl_sec": {"type": "integer", "default": 120},
            },
        },
    },
    {
        "name": "agp_artifact_put",
        "description": (
            "Store a large diff, log, OpenAPI spec, or code file in the local SHA-256 Artifact Store "
            "(with DLP secret redaction) and receive a pass-by-reference descriptor to attach to `agp_send`."
        ),
        "annotations": _ann("Store AGP Artifact", idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["kind"],
            "properties": {
                "kind": {"type": "string", "description": "e.g. git_diff, openapi_spec, test_log, design_doc"},
                "owner_uri": {"type": "string"},
                "content": {"type": "string", "description": "UTF-8 text content to store."},
                "source_path": {"type": "string", "description": "Absolute path to an existing file to store."},
                "mime_type": {"type": "string", "default": "text/plain"},
                "redact": {"type": "boolean", "default": True, "description": "Automatically DLP-redact secrets in text content."},
            },
        },
    },
    {
        "name": "agp_artifact_get",
        "description": "Retrieve a SHA-256 verified artifact by artifact_id, supporting `offset_bytes` chunk pagination.",
        "annotations": _ann("Read AGP Artifact", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["artifact_id"],
            "properties": {
                "artifact_id": {"type": "string"},
                "offset_bytes": {"type": "integer", "default": 0, "description": "Byte offset for reading large artifacts in chunks."},
                "max_bytes": {"type": "integer", "default": 65536},
            },
        },
    },
    {
        "name": "agp_artifact_list",
        "description": "List stored artifacts in the local AGP Artifact Store (optionally filtered by kind or owner_uri).",
        "annotations": _ann("List AGP Artifacts", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string"},
                "owner_uri": {"type": "string"},
                "limit": {"type": "integer", "default": 30},
            },
        },
    },
    {
        "name": "agp_trace_inspect",
        "description": (
            "Inspect the full multi-hop distributed trace spans and messages for a `trace_id`, "
            "OR omit `trace_id` to list recent traces."
        ),
        "annotations": _ann("Inspect Distributed Trace", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "trace_id": {"type": "string", "description": "Trace ID to inspect (omit to list recent traces)."},
                "limit": {"type": "integer", "default": 25},
            },
        },
    },
    # ------------------------------------------------------------------
    # Claude-Code-Style Agent Teams Tools
    # ------------------------------------------------------------------
    {
        "name": "team_list",
        "description": (
            "List all AGP Agent Teams or inspect a specific team's roster, live member statuses, "
            "unread mailbox counts, zero-token listener commands, and task board counts."
        ),
        "annotations": _ann("List or Inspect Agent Teams", read_only=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "properties": {
                "team": {"type": "string", "description": "Optional team slug for detailed status (omit to list all teams)."},
            },
        },
    },
    {
        "name": "team_create",
        "description": (
            "Create a Claude-style AGY Agent Team under ~/.gemini/agp/teams/<team>/ with a shared "
            "task list (tasks.json), team roster (config.json), and per-member mailboxes (inboxes/)."
        ),
        "annotations": _ann("Create Agent Team", idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["team"],
            "properties": {
                "team": {"type": "string", "description": "Short team slug, e.g. 'edu-feature'."},
                "description": {"type": "string", "description": "Brief team goal."},
                "lead_name": {"type": "string", "default": "lead"},
                "lead_conversation_id": {"type": "string"},
                "cwd": {"type": "string"},
            },
        },
    },
    {
        "name": "team_join",
        "description": (
            "Attach an existing live AGY session (by conversation_id) into a Team roster AND immediately "
            "open its receive/reply channel (sends structured Team Welcome handshake with exact `team_send` "
            "reply route and zero-token `agp.listen` command; optional `auto_wake=true` for detached CLI wakeup)."
        ),
        "annotations": _ann("Join Live Session to Team", idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["team", "name"],
            "properties": {
                "team": {"type": "string"},
                "name": {"type": "string", "description": "Short peer handle in this team."},
                "role": {"type": "string", "default": "teammate"},
                "conversation_id": {"type": "string", "description": "Existing AGY conversation UUID."},
                "surface": {"type": "string", "default": "cli"},
                "cwd": {"type": "string"},
                "auto_wake": {
                    "type": "boolean",
                    "default": False,
                    "description": "Set true to allow detached `agy --conversation` auto-wakeup when this member is idle.",
                },
                "notify_member": {
                    "type": "boolean",
                    "default": True,
                    "description": "Send a structured Team Handshake welcome message so the joined agent immediately learns its team & reply route.",
                },
                "welcome_note": {"type": "string", "description": "Optional context note included in the welcome handshake."},
            },
        },
    },
    {
        "name": "team_spawn",
        "description": (
            "Spawn a real autonomous AGY teammate (e.g. 'backend', 'frontend', 'reviewer') in a team. "
            "Runs via self-terminating detached worker with persistent conversation_id, automatically wakes "
            "on `team_send`, auto-bridges replies if needed, and sends `idle_notification` when done."
        ),
        "annotations": _ann("Spawn Autonomous Teammate"),
        "inputSchema": {
            "type": "object",
            "required": ["team", "name", "role", "prompt"],
            "properties": {
                "team": {"type": "string"},
                "name": {"type": "string", "description": "Short peer handle, e.g. 'backend', 'frontend', 'reviewer'."},
                "role": {"type": "string", "description": "Specialist role label."},
                "prompt": {"type": "string", "description": "Initial assignment / kickoff instructions for this teammate."},
                "cwd": {"type": "string"},
                "model": {"type": "string"},
                "effort": {"type": "string", "enum": ["low", "medium", "high"], "default": "low"},
            },
        },
    },
    {
        "name": "team_send",
        "description": (
            "Send a direct peer-to-peer message to a teammate by short name (or 'all' to broadcast). "
            "Writes to ~/.gemini/agp/teams/<team>/inboxes/<to>.json, mirrors to live session hooks/listeners, "
            "auto-wakes managed teammates, and returns honest `delivery` status."
        ),
        "annotations": _ann("Send Team Message"),
        "inputSchema": {
            "type": "object",
            "required": ["team", "to", "message"],
            "properties": {
                "team": {"type": "string"},
                "from_name": {"type": "string", "default": "lead"},
                "to": {"type": "string", "description": "Recipient short name (e.g. 'backend', 'lead', or 'all')."},
                "message": {"type": "string"},
                "kind": {
                    "type": "string",
                    "enum": ["message", "broadcast", "plan_approval_request", "plan_approval_response"],
                    "default": "message",
                },
                "wake": {"type": "boolean", "default": True},
                "wake_mode": {
                    "type": "string",
                    "enum": ["auto", "force_cli", "none"],
                    "default": "auto",
                    "description": "'auto' wakes managed/listening/active peers; 'force_cli' spawns detached CLI resume even for unmanaged CLI sessions.",
                },
                "hop": {"type": "integer", "default": 1},
            },
        },
    },
    {
        "name": "team_inbox",
        "description": (
            "Read or peek your team mailbox (~/.gemini/agp/teams/<team>/inboxes/<name>.json). "
            "Supports cross-process fast wait (`wait_sec` 0-30s) and `mark_read=false` peek mode."
        ),
        "annotations": _ann("Read Team Mailbox"),
        "inputSchema": {
            "type": "object",
            "required": ["team", "name"],
            "properties": {
                "team": {"type": "string"},
                "name": {"type": "string", "description": "Your short member name (e.g. 'lead', 'backend')."},
                "unread_only": {"type": "boolean", "default": True},
                "mark_read": {"type": "boolean", "default": True, "description": "Set false to peek without marking read."},
                "wait_sec": {"type": "number", "default": 0, "description": "Optional seconds (0-30) to wait for incoming messages."},
            },
        },
    },
    {
        "name": "team_task",
        "description": (
            "Manage the team's cross-process atomic Shared Task List (`tasks.json`): "
            "list, create (with DAG cycle check), claim (omitting task_id auto-claims next unblocked task), "
            "unclaim, complete (auto-unblocks dependents), fail, cancel, update, or delete."
        ),
        "annotations": _ann("Manage Team Shared Tasks"),
        "inputSchema": {
            "type": "object",
            "required": ["team", "action"],
            "properties": {
                "team": {"type": "string"},
                "action": {
                    "type": "string",
                    "enum": ["list", "create", "claim", "unclaim", "complete", "fail", "cancel", "update", "delete"],
                },
                "task_id": {"type": "string", "description": "Task ID like 'T1' (optional for claim -> auto-claims next unblocked task)."},
                "title": {"type": "string"},
                "description": {"type": "string"},
                "owner": {"type": "string"},
                "status": {"type": "string", "enum": ["pending", "blocked", "in_progress", "completed", "failed", "cancelled"]},
                "blocked_by": {"type": "array", "items": {"type": "string"}},
                "result_summary": {"type": "string"},
            },
        },
    },
    {
        "name": "team_shutdown",
        "description": "Gracefully shut down a spawned teammate (or pass `name='all'` to shut down all teammates).",
        "annotations": _ann("Shut Down Teammate(s)", destructive=False, idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["team", "name"],
            "properties": {
                "team": {"type": "string"},
                "name": {"type": "string", "description": "Member short name, or 'all' to shut down all teammates."},
                "reason": {"type": "string", "default": "Task complete"},
            },
        },
    },
    {
        "name": "team_delete",
        "description": "Shut down all teammates and delete a finished team's state directory cleanly.",
        "annotations": _ann("Delete Completed Team", destructive=True, idempotent=True),
        "inputSchema": {
            "type": "object",
            "required": ["team"],
            "properties": {
                "team": {"type": "string", "description": "Team slug to clean up."},
            },
        },
    },
]

MCP_STATIC_RESOURCES: List[Dict[str, Any]] = [
    {
        "uri": "agp://monitor/snapshot",
        "name": "AGY Live Monitor Snapshot",
        "description": "Current active AI sessions, token counts, costs, elapsed times, teams, leases, and AGP messages.",
        "mimeType": "application/json",
    },
    {
        "uri": "agp://agents/active",
        "name": "Active AGP Agent Cards",
        "description": "Registered Agent Cards, capabilities, owned paths, and zero-token listener status.",
        "mimeType": "application/json",
    },
    {
        "uri": "agp://teams/list",
        "name": "AGP Agent Teams & Shared Tasks",
        "description": "All local Agent Teams with rosters, task boards, and recent team chat.",
        "mimeType": "application/json",
    },
    {
        "uri": "agp://leases/active",
        "name": "Active Workspace File Leases",
        "description": "Currently held exclusive/shared workspace file leases and monotonic fencing tokens.",
        "mimeType": "application/json",
    },
    {
        "uri": "agp://circuits/status",
        "name": "AGP Circuit Breakers",
        "description": "Health and cooldown status of per-target AGP circuit breakers.",
        "mimeType": "application/json",
    },
    {
        "uri": "agp://tasks/active",
        "name": "Durable Async Tasks",
        "description": "MCP io.modelcontextprotocol/tasks and A2A v1.0 durable task state records.",
        "mimeType": "application/json",
    },
]

MCP_RESOURCE_TEMPLATES: List[Dict[str, Any]] = [
    {
        "uriTemplate": "agp://agents/{agent_id}",
        "name": "Specific Agent Card",
        "description": "Inspect a specific Agent Card by conversation_id, @handle, or URL-encoded agent_uri.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "agp://teams/{team}",
        "name": "Specific Agent Team Status",
        "description": "Inspect roster, unread counts, listener commands, and tasks for a specific team slug.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "agp://traces/{trace_id}",
        "name": "Distributed Trace Inspection",
        "description": "Inspect all multi-hop spans and envelopes for a specific trace_id.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "agp://artifacts/{artifact_id}",
        "name": "SHA-256 Verified Artifact",
        "description": "Read a stored pass-by-reference artifact by artifact_id.",
        "mimeType": "application/json",
    },
    {
        "uriTemplate": "agp://tasks/{task_id}",
        "name": "Durable Async Task Detail",
        "description": "Inspect status, history, artifacts, and elicitation schema for a durable task_id.",
        "mimeType": "application/json",
    },
]

MCP_PROMPTS: List[Dict[str, Any]] = [
    {
        "name": "agp_coordinate_team",
        "description": "Set up and coordinate a multi-agent team (shared task DAG, join/spawn teammates, two-way messaging, clean shutdown).",
        "arguments": [
            {"name": "team", "description": "Short team slug (e.g. 'release-v2').", "required": True},
            {"name": "goal", "description": "Overall engineering objective for the team.", "required": True},
            {"name": "roles", "description": "Comma-separated specialist roles (e.g. 'backend,frontend,reviewer').", "required": False},
        ],
    },
    {
        "name": "agp_delegate_task",
        "description": "Delegate a contract-first task.request to a peer agent with lease discipline, artifact handoff, and zero-token-waste waiting.",
        "arguments": [
            {"name": "target", "description": "Target @handle, conversation_id, or agent_uri.", "required": True},
            {"name": "goal", "description": "Concrete task goal and acceptance criteria.", "required": True},
        ],
    },
    {
        "name": "agp_code_review_loop",
        "description": "Run an Evaluator-Optimizer cross-agent code review loop (max 3 rounds) with artifact diff & HITL escalation.",
        "arguments": [
            {"name": "reviewer", "description": "Reviewer @handle or URI.", "required": True},
            {"name": "target", "description": "Branch, PR, or file scope under review.", "required": True},
        ],
    },
    {
        "name": "agp_git_handoff",
        "description": "Prepare a safe Git worktree/branch handoff (`sync.git_handoff`) with SHA-256 diff artifact and test evidence.",
        "arguments": [
            {"name": "branch", "description": "Git branch name to hand off.", "required": True},
            {"name": "recipient", "description": "Coordinator @handle or URI.", "required": True},
        ],
    },
    {
        "name": "agp_conflict_resolution",
        "description": "Resolve a workspace file lease conflict or contradictory results between peer agents via arbitration or HITL.",
        "arguments": [
            {"name": "conflict_summary", "description": "Description of the file lock or result conflict.", "required": True},
            {"name": "peers", "description": "Involved peer handles or URIs.", "required": False},
        ],
    },
    {
        "name": "agp_debug_trace",
        "description": "Perform 5-step distributed trace root-cause analysis across multi-hop AGP interactions.",
        "arguments": [
            {"name": "trace_id", "description": "Optional trace_id (omit to inspect recent traces first).", "required": False},
        ],
    },
]


def _check_optional_monitor_up(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.15):
            return True
    except OSError:
        return False


class MCPHandler:
    """Handles MCP JSON-RPC 2.0 requests over standalone Stdio or optional Streamable HTTP."""

    SUPPORTED_PROTOCOL_VERSIONS = SUPPORTED_PROTOCOL_VERSIONS

    def __init__(
        self,
        store: AGPStore,
        snapshot_fn: Callable[[], Dict[str, Any]],
        notify_fn: Callable[[str, Dict[str, Any]], None],
        teams: Optional[TeamManager] = None,
        resolve_target_fn: Optional[Callable[[str], str]] = None,
        wakeup_agent_fn: Optional[Callable[..., Any]] = None,
        default_agent_uri: Optional[str] = None,
        presence_check_fn: Optional[Callable[[str], bool]] = None,
        codemode: bool = False,
        truncate_by_default: bool = True,
    ) -> None:
        self.store = store
        self.teams = teams or TeamManager(notify_fn=notify_fn)
        self.snapshot_fn = snapshot_fn
        self.notify_fn = notify_fn
        self.resolve_target_fn = resolve_target_fn
        self.wakeup_agent_fn = wakeup_agent_fn
        self.default_agent_uri = (default_agent_uri or os.environ.get("AGP_AGENT_URI") or "").strip() or None
        self.presence_check_fn = presence_check_fn or is_conversation_presence_locked
        self.codemode_default = bool(codemode)
        self.truncate_by_default = bool(truncate_by_default)
        self._lock = threading.RLock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._cancel_events: Dict[str, threading.Event] = {}
        self._skills_cache: Optional[Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Tuple[str, str]]]] = None

    def create_session(self, client_surface: str = "mcp") -> str:
        sid = f"mcp_{uuid.uuid4().hex[:16]}"
        with self._lock:
            self._sessions[sid] = {
                "created_at": time.time(),
                "agent_uri": self.default_agent_uri,
                "client_surface": client_surface,
                "client_name": "",
                "log_level": "info",
                "protocol_version": "2026-07-28",
            }
        return sid

    def ensure_session(self, session_id: str, client_surface: str = "mcp") -> str:
        with self._lock:
            if session_id not in self._sessions:
                self._sessions[session_id] = {
                    "created_at": time.time(),
                    "agent_uri": self.default_agent_uri,
                    "client_surface": client_surface,
                    "client_name": "",
                    "log_level": "info",
                    "protocol_version": "2026-07-28",
                }
        return session_id

    def get_session_agent_uri(self, session_id: Optional[str]) -> Optional[str]:
        if not session_id:
            return self.default_agent_uri
        with self._lock:
            sess = self._sessions.get(session_id)
            if sess and sess.get("agent_uri"):
                return str(sess["agent_uri"])
        return self.default_agent_uri

    def delete_session(self, session_id: Optional[str]) -> None:
        if not session_id:
            return
        with self._lock:
            self._sessions.pop(session_id, None)

    @staticmethod
    def _infer_client_surface(client_name: str) -> str:
        low = (client_name or "").lower()
        if "antigravity-ide" in low or "vscode" in low or "vs code" in low:
            return "ide"
        if "antigravity-cli" in low or "agy" in low:
            return "cli"
        if "antigravity" in low:
            return "app"
        if "cursor" in low:
            return "cursor"
        if "claude" in low:
            return "claude"
        if "windsurf" in low:
            return "windsurf"
        return "mcp"

    def _resolve_actor_uri(self, session_id: Optional[str], explicit_uri: Optional[str]) -> str:
        if explicit_uri and explicit_uri.strip():
            raw = explicit_uri.strip()
            uri = self.resolve_target_fn(raw) if (self.resolve_target_fn and not raw.startswith("agp://")) else raw
            if not uri.startswith("agp://"):
                uri = f"agp://local/mcp/{uri}"
            if session_id:
                with self._lock:
                    if session_id in self._sessions:
                        self._sessions[session_id]["agent_uri"] = uri
            return uri
        if session_id:
            with self._lock:
                sess = self._sessions.get(session_id)
                if sess:
                    if sess.get("agent_uri"):
                        return str(sess["agent_uri"])
                    surf = str(sess.get("client_surface") or "mcp")
                    return f"agp://local/{surf}/{session_id}"
        if self.default_agent_uri:
            return self.default_agent_uri
        return "agp://local/mcp/anonymous"

    def _register_cancel_event(self, req_id: Any) -> Optional[threading.Event]:
        if req_id is None:
            return None
        key = str(req_id)
        ev = threading.Event()
        with self._lock:
            self._cancel_events[key] = ev
        return ev

    def _clear_cancel_event(self, req_id: Any) -> None:
        if req_id is None:
            return
        with self._lock:
            self._cancel_events.pop(str(req_id), None)

    def _signal_cancel(self, req_id: Any) -> None:
        if req_id is None:
            return
        with self._lock:
            ev = self._cancel_events.get(str(req_id))
            if ev is not None:
                ev.set()

    def handle_jsonrpc(
        self,
        payload: Any,
        session_id: Optional[str] = None,
        *,
        codemode: Optional[bool] = None,
        truncate_tool_result: Optional[bool] = None,
    ) -> Tuple[int, Optional[Any], Optional[str]]:
        """Process a JSON-RPC 2.0 request or batch. Returns (http_status, body_or_None, session_id)."""
        eff_codemode = self.codemode_default if codemode is None else bool(codemode)
        eff_truncate = self.truncate_by_default if truncate_tool_result is None else bool(truncate_tool_result)
        if isinstance(payload, list):
            if not payload:
                return (
                    400,
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32600, "message": "Invalid JSON-RPC batch: empty array"},
                    },
                    session_id,
                )
            responses = []
            active_sid = session_id
            for item in payload:
                _, resp, active_sid = self._handle_single(
                    item, active_sid, codemode=eff_codemode, truncate_tool_result=eff_truncate
                )
                if resp is not None:
                    responses.append(resp)
            if not responses:
                return 202, None, active_sid
            return 200, responses, active_sid

        if not isinstance(payload, dict):
            return (
                400,
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "Invalid JSON-RPC request"},
                },
                session_id,
            )

        return self._handle_single(
            payload, session_id, codemode=eff_codemode, truncate_tool_result=eff_truncate
        )

    def _handle_single(
        self,
        req: Any,
        session_id: Optional[str],
        *,
        codemode: bool = False,
        truncate_tool_result: bool = True,
    ) -> Tuple[int, Optional[Dict[str, Any]], Optional[str]]:
        if not isinstance(req, dict):
            return (
                400,
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid JSON-RPC request object"}},
                session_id,
            )
        method = req.get("method")
        req_id = req.get("id")
        params = req.get("params")
        is_notification = "id" not in req

        if not isinstance(method, str) or not method:
            if is_notification:
                return 202, None, session_id
            return (
                400,
                {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32600, "message": "Missing JSON-RPC method"}},
                session_id,
            )
        if params is None:
            params = {}
        elif not isinstance(params, dict):
            if is_notification:
                return 202, None, session_id
            return (
                200,
                {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "params must be a JSON object"}},
                session_id,
            )

        if method == "notifications/cancelled":
            self._signal_cancel(params.get("requestId"))
            return 202, None, session_id

        if method == "initialize":
            client_proto = str(params.get("protocolVersion") or "2026-07-28")
            proto = client_proto if client_proto in SUPPORTED_PROTOCOL_VERSIONS else "2026-07-28"
            client_info = params.get("clientInfo") or {}
            client_name = str(client_info.get("name") or "") if isinstance(client_info, dict) else ""
            client_surface = self._infer_client_surface(client_name)
            # MCP 2026-07-28 is stateless-first over HTTP; keep existing session_id if supplied (e.g. stdio)
            new_sid = session_id if proto == "2026-07-28" else (session_id or self.create_session(client_surface=client_surface))
            if new_sid:
                self.ensure_session(new_sid, client_surface=client_surface)
                with self._lock:
                    self._sessions[new_sid]["client_name"] = client_name
                    self._sessions[new_sid]["client_surface"] = client_surface
                    self._sessions[new_sid]["protocol_version"] = proto
            return (
                200,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": proto,
                        "capabilities": {
                            "tools": {"listChanged": False},
                            "resources": {"subscribe": False, "listChanged": False},
                            "prompts": {"listChanged": False},
                            "logging": {},
                            "completions": {},
                            "extensions": {
                                "io.modelcontextprotocol/tasks": {"version": "1.0"},
                                SKILLS_EXTENSION_ID: {},
                            },
                        },
                        "serverInfo": {
                            "name": "agp-broker",
                            "version": __version__,
                        },
                        "instructions": (
                            "AGP v2.0 Inter-Agent Mesh (Standard Serverless Stdio MCP + Optional Monitor UI). "
                            "Runs silently in background without needing any HTTP server open. "
                            "Use team_* tools for Agent Teams (team_create, team_join, team_spawn, team_send, team_inbox, "
                            "team_task, team_list, team_shutdown) and agp_* tools for cross-session collaboration "
                            "(agp_status, agp_register, agp_discover, agp_send, agp_ask, agp_inbox, agp_ack, agp_task, "
                            "agp_lease, agp_artifact_put/get/list, agp_trace_inspect). "
                            "Always check `delivery.will_auto_receive` on send and use zero-token `agp.listen` to stay reachable while idle."
                        ),
                    },
                },
                new_sid,
            )

        if is_notification or method.startswith("notifications/"):
            return 202, None, session_id

        if method == "ping":
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {}}, session_id

        if method == "logging/setLevel":
            level = str(params.get("level") or "").lower()
            if level not in VALID_LOG_LEVELS:
                return (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32602, "message": f"Invalid log level '{level}'. Must be one of {sorted(VALID_LOG_LEVELS)}"},
                    },
                    session_id,
                )
            if session_id:
                self.ensure_session(session_id)
                with self._lock:
                    self._sessions[session_id]["log_level"] = level
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {}}, session_id

        # Official MCP 2026-07-28 `io.modelcontextprotocol/tasks` extension methods
        if method == "tasks/list":
            tasks = self.store.list_tasks(
                context_id=params.get("contextId"),
                state=params.get("status") or params.get("state"),
                limit=int(params.get("limit") or 50),
            )
            return (
                200,
                {"jsonrpc": "2.0", "id": req_id, "result": {"tasks": [self._format_mcp_task(t) for t in tasks]}},
                session_id,
            )

        if method == "tasks/get":
            tid = str(params.get("taskId") or params.get("id") or "")
            task = self.store.get_task(tid)
            if not task:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": f"Task not found: {tid}"}},
                    session_id,
                )
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": self._format_mcp_task(task)}, session_id

        if method == "tasks/update":
            tid = str(params.get("taskId") or params.get("id") or "")
            meta = params.get("_meta") or {}
            actor = self._resolve_actor_uri(session_id, params.get("actor_uri") or meta.get("agent_uri"))
            input_data = params.get("input") or params.get("inputResponses") or {}
            ok_u, res_u = self.store.update_task_input(tid, input_data=input_data, actor_uri=actor)
            if not ok_u:
                return (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32602, "message": res_u.get("error", "Cannot update task")},
                    },
                    session_id,
                )
            if self.wakeup_agent_fn and res_u["task"].get("_to_uri"):
                self._call_wakeup(res_u["task"]["_to_uri"], wake_mode="auto")
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": self._format_mcp_task(res_u["task"])}, session_id

        if method == "tasks/cancel":
            tid = str(params.get("taskId") or params.get("id") or "")
            reason = str(params.get("reason") or "Cancelled via MCP tasks/cancel")
            ok_c, res_c = self.store.cancel_task(tid, reason=reason)
            if not ok_c:
                return (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32602, "message": res_c.get("error", "Cannot cancel task")},
                    },
                    session_id,
                )
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": self._format_mcp_task(res_c["task"])}, session_id

        if method == "skills/list":
            if params.get("cursor") is not None:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "Unknown cursor"}},
                    session_id,
                )
            skills, _, _ = self._load_skills_catalog()
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {"skills": skills}}, session_id

        if method == "skills/get":
            s_uri = str(params.get("uri") or "")
            _, by_uri, _ = self._load_skills_catalog()
            skill = by_uri.get(s_uri)
            if not skill:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": f"No skill is served at {s_uri}"}},
                    session_id,
                )
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {"skill": skill}}, session_id

        if method == "tools/list":
            tools = CODEMODE_TOOLS if codemode else MCP_TOOLS
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}, session_id

        if method == "tools/call":
            tool_name = params.get("name", "")
            raw_args = params.get("arguments")
            if raw_args is None:
                args: Dict[str, Any] = {}
            elif isinstance(raw_args, dict):
                args = dict(raw_args)
            else:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "tools/call arguments must be an object"}},
                    session_id,
                )
            meta = params.get("_meta") or {}
            if isinstance(meta, dict):
                if meta.get("agent_uri") and not args.get("from_uri") and not args.get("agent_uri"):
                    args.setdefault("_meta_agent_uri", meta["agent_uri"])
                if meta.get("task") and tool_name == "agp_send":
                    args["async_task"] = True

            explicit_trunc = args.get("truncate") if tool_name in {"search", "execute"} else None
            should_truncate = (
                (bool(explicit_trunc) if explicit_trunc is not None else truncate_tool_result)
                if (codemode or tool_name in {"search", "execute"})
                else False
            )

            cancel_ev = self._register_cancel_event(req_id)
            try:
                ok, result_obj = self._execute_tool(tool_name, args, session_id, cancel_event=cancel_ev)
            except Exception as exc:
                ok = False
                result_obj = {
                    "ok": False,
                    "error_code": "ERR_INTERNAL",
                    "error": f"Tool '{tool_name}' failed: {exc}",
                }
            finally:
                self._clear_cancel_event(req_id)

            rendered_text = format_tool_result(result_obj, truncate=should_truncate)
            rpc_result: Dict[str, Any] = {
                "content": [
                    {
                        "type": "text",
                        "text": rendered_text,
                    }
                ],
                "isError": not ok,
            }
            if ok and tool_name == "whoami":
                rpc_result["structuredContent"] = result_obj
            if ok and result_obj.get("resultType") == "task":
                rpc_result["resultType"] = "task"
                rpc_result["task"] = result_obj["task"]
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": rpc_result}, session_id

        if method == "resources/list":
            skills, _, _ = self._load_skills_catalog()
            skill_resources = [
                {
                    "uri": sk["uri"],
                    "name": sk["frontmatter"]["name"],
                    "description": sk["frontmatter"]["description"],
                    "mimeType": "text/markdown",
                }
                for sk in skills
            ]
            return (
                200,
                {"jsonrpc": "2.0", "id": req_id, "result": {"resources": [*MCP_STATIC_RESOURCES, *skill_resources]}},
                session_id,
            )

        if method == "resources/templates/list":
            return (
                200,
                {"jsonrpc": "2.0", "id": req_id, "result": {"resourceTemplates": MCP_RESOURCE_TEMPLATES}},
                session_id,
            )

        if method == "resources/read":
            uri = str(params.get("uri") or "")
            if uri.startswith("skill://"):
                _, _, files_by_uri = self._load_skills_catalog()
                entry = files_by_uri.get(uri)
                if not entry:
                    return (
                        200,
                        {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": f"No skill resource at {uri}"}},
                        session_id,
                    )
                mime_t, text_body = entry
                return (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "result": {"contents": [{"uri": uri, "mimeType": mime_t, "text": text_body}]},
                    },
                    session_id,
                )
            ok_r, data_r, err_r = self._read_resource(uri)
            if not ok_r:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": err_r}},
                    session_id,
                )
            return (
                200,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "application/json",
                                "text": json.dumps(data_r, ensure_ascii=False, indent=2),
                            }
                        ]
                    },
                },
                session_id,
            )

        if method == "prompts/list":
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": {"prompts": MCP_PROMPTS}}, session_id

        if method == "prompts/get":
            pname = str(params.get("name") or "")
            pargs = params.get("arguments") or {}
            if not isinstance(pargs, dict):
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": "prompt arguments must be an object"}},
                    session_id,
                )
            ok_p, res_p, err_p = self._get_prompt(pname, pargs)
            if not ok_p:
                return (
                    200,
                    {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32602, "message": err_p}},
                    session_id,
                )
            return 200, {"jsonrpc": "2.0", "id": req_id, "result": res_p}, session_id

        if method == "completion/complete":
            ref = params.get("ref") or {}
            arg = params.get("argument") or {}
            values = self._complete_argument(
                ref_type=str(ref.get("type") or ""),
                ref_name=str(ref.get("name") or ref.get("uri") or ""),
                arg_name=str(arg.get("name") or ""),
                prefix=str(arg.get("value") or ""),
            )
            return (
                200,
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "completion": {
                            "values": values[:20],
                            "total": len(values),
                            "hasMore": len(values) > 20,
                        }
                    },
                },
                session_id,
            )

        return (
            200,
            {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}},
            session_id,
        )

    # ------------------------------------------------------------------
    # Resources, Prompts & Completions Helpers
    # ------------------------------------------------------------------

    def _read_resource(self, uri: str) -> Tuple[bool, Any, str]:
        if uri == "agp://monitor/snapshot":
            return True, self.snapshot_fn(), ""
        if uri == "agp://agents/active":
            return True, {"agents": self.store.list_agents()}, ""
        if uri == "agp://teams/list":
            return True, {"teams": self.teams.list_teams_snapshot()}, ""
        if uri == "agp://leases/active":
            _, res = self.store.manage_lease("list", owner_uri="agp://local/system/mcp")
            return True, res, ""
        if uri == "agp://circuits/status":
            return True, {"circuits": self.store.list_circuit_breakers()}, ""
        if uri == "agp://tasks/active":
            return True, {"tasks": [self._format_mcp_task(t) for t in self.store.list_tasks(limit=50)]}, ""

        if uri.startswith("agp://agents/"):
            raw_id = urllib.parse.unquote(uri[len("agp://agents/") :]).strip("/")
            resolved = self.resolve_target_fn(raw_id) if self.resolve_target_fn else raw_id
            ag = self.store.get_agent(resolved) or self.store.get_agent_by_conversation(raw_id)
            if not ag:
                return False, None, f"Agent not found: {raw_id}"
            return True, {"agent": ag}, ""

        if uri.startswith("agp://teams/"):
            team_slug = urllib.parse.unquote(uri[len("agp://teams/") :]).strip("/")
            ok, data = self.teams.get_team_status(team_slug)
            if not ok:
                return False, None, data.get("error", f"Team not found: {team_slug}")
            return True, data, ""

        if uri.startswith("agp://traces/"):
            trace_id = urllib.parse.unquote(uri[len("agp://traces/") :]).strip("/")
            return True, self.store.inspect_trace(trace_id), ""

        if uri.startswith("agp://artifacts/"):
            art_id = urllib.parse.unquote(uri[len("agp://artifacts/") :]).strip("/")
            ok, data = self.store.get_artifact(art_id)
            if not ok:
                return False, None, data.get("error", f"Artifact not found: {art_id}")
            return True, data, ""

        if uri.startswith("agp://tasks/"):
            task_id = urllib.parse.unquote(uri[len("agp://tasks/") :]).strip("/")
            task = self.store.get_task(task_id)
            if not task:
                return False, None, f"Task not found: {task_id}"
            return True, {"task": self._format_mcp_task(task), "raw": task}, ""

        return False, None, f"Unknown resource URI: {uri}"

    def _get_prompt(self, name: str, args: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str]:
        spec = next((p for p in MCP_PROMPTS if p["name"] == name), None)
        if not spec:
            return False, {}, f"Unknown prompt: {name}"
        for arg_def in spec.get("arguments") or []:
            if arg_def.get("required") and not str(args.get(arg_def["name"]) or "").strip():
                return False, {}, f"Prompt '{name}' requires argument '{arg_def['name']}'"

        if name == "agp_coordinate_team":
            team = str(args["team"]).strip()
            goal = str(args["goal"]).strip()
            roles = str(args.get("roles") or "backend,frontend,reviewer").strip()
            text = (
                f"Coordinate AGP Agent Team `{team}` for goal: {goal}\n"
                f"Target specialist roles: {roles}\n\n"
                "Follow this exact protocol:\n"
                f"1. Call `team_create(team='{team}', description='{goal}')`.\n"
                "2. Break work into tasks with `team_task(action='create', blocked_by=[...])` (DAG dependencies).\n"
                "3. Either `team_join` existing live sessions (which opens their receive/reply channel immediately) "
                "or `team_spawn` autonomous teammates.\n"
                "4. Start your zero-token background listener (`agp_status` gives `listen_command`) before waiting "
                "so teammate replies wake you instantly with 0 wasted tokens.\n"
                "5. Verify results, then run `team_shutdown(team='{team}', name='all')`."
            )
        elif name == "agp_delegate_task":
            target = str(args["target"]).strip()
            goal = str(args["goal"]).strip()
            text = (
                f"Delegate a contract-first task to `{target}`:\n"
                f"Goal: {goal}\n\n"
                "1. Call `agp_discover` to confirm target URI, owned_paths, and whether `listening` or `auto_cli` is active.\n"
                "2. Put any diff/spec > 4KB into `agp_artifact_put` first.\n"
                "3. Call `agp_ask(to_uri=..., type='task.request', summary=..., payload={'goal': ...}, verbatim_constraints=[...])`.\n"
                "4. Check `delivery.will_auto_receive` — NEVER loop polling `agp_inbox` if the peer is idle without listener/auto_cli."
            )
        elif name == "agp_code_review_loop":
            reviewer = str(args["reviewer"]).strip()
            target = str(args["target"]).strip()
            text = (
                f"Run an Evaluator-Optimizer cross-agent review loop with `{reviewer}` on `{target}` (max 3 rounds):\n"
                "1. Store full diff in `agp_artifact_put(kind='git_diff', ...)`.\n"
                "2. Send `review.request` with `payload={'target': ..., 'iteration': 1, 'max_iterations': 3}` + artifact ref.\n"
                "3. Apply requested fixes, re-test, and loop up to 3 iterations. Escalate via `hitl.escalate` if unresolved."
            )
        elif name == "agp_git_handoff":
            branch = str(args["branch"]).strip()
            recipient = str(args["recipient"]).strip()
            text = (
                f"Prepare Git handoff of branch `{branch}` to `{recipient}`:\n"
                "1. Run tests and capture evidence.\n"
                "2. Store patch/diff via `agp_artifact_put(kind='git_diff', ...)`.\n"
                "3. Send `agp_send(to_uri=..., type='sync.git_handoff', payload={'branch': ..., 'commit_sha': ..., 'tests_passed': true, 'evidence': [...]})`.\n"
                "4. Release any held file leases with `agp_lease(action='release')`."
            )
        elif name == "agp_conflict_resolution":
            conflict_summary = str(args["conflict_summary"]).strip()
            peers = str(args.get("peers") or "involved peers").strip()
            text = (
                f"Resolve multi-agent conflict ({peers}): {conflict_summary}\n\n"
                "1. Inspect active locks via `agp_lease(action='list')` and recent messages via `agp_trace_inspect`.\n"
                "2. Send `conflict.report` or `conflict.arbitrate` with concrete options.\n"
                "3. If irreversible or unresolved, escalate to human via `agp_send(to_uri='agp://local/human/monitor', type='hitl.escalate', payload={'reason': ...})`."
            )
        else:  # agp_debug_trace
            tid = str(args.get("trace_id") or "").strip()
            text = (
                f"Perform 5-step Root Cause Analysis for AGP trace `{tid or '(inspect recent traces first)'}`:\n"
                "1. Call `agp_trace_inspect(" + (f"trace_id='{tid}'" if tid else "") + ")`.\n"
                "2. Reconstruct hop chain via `parent_span_id` and `visited_uris`.\n"
                "3. Separate agent reasoning errors vs contract/schema/lease/timeout failures.\n"
                "4. Check circuit breakers & leases via `agp_status`.\n"
                "5. Report root cause + concrete prevention fix."
            )

        return (
            True,
            {
                "description": spec["description"],
                "messages": [{"role": "user", "content": {"type": "text", "text": text}}],
            },
            "",
        )

    def _complete_argument(self, ref_type: str, ref_name: str, arg_name: str, prefix: str) -> List[str]:
        low = (prefix or "").lower()
        candidates: List[str] = []
        if arg_name in {"team"}:
            candidates = [t["team"] for t in self.teams.list_teams_snapshot()]
        elif arg_name in {"agent_id", "target", "reviewer", "recipient"}:
            for a in self.store.list_agents():
                if a.get("conversation_id"):
                    candidates.append(a["conversation_id"])
                if a.get("agent_uri"):
                    candidates.append(a["agent_uri"])
        elif arg_name == "trace_id":
            candidates = [t["trace_id"] for t in self.store.list_traces(limit=30).get("traces", [])]
        elif arg_name == "artifact_id":
            candidates = [a["artifact_id"] for a in self.store.list_artifacts(limit=30)]
        elif arg_name == "task_id":
            candidates = [t["id"] for t in self.store.list_tasks(limit=30)]
        unique = list(dict.fromkeys(c for c in candidates if c and (not low or low in c.lower())))
        return unique

    @staticmethod
    def _format_mcp_task(task: Dict[str, Any]) -> Dict[str, Any]:
        raw_state = (task.get("status") or {}).get("state", "TASK_STATE_WORKING")
        mcp_status = MCP_TASK_STATE_MAP.get(raw_state, "working")
        out: Dict[str, Any] = {
            "taskId": task["id"],
            "contextId": task.get("contextId", ""),
            "status": mcp_status,
            "a2aState": raw_state,
            "updatedAt": (task.get("status") or {}).get("timestamp", ""),
        }
        if "result" in task:
            out["result"] = task["result"]
        if "elicitation" in task:
            out["elicitation"] = task["elicitation"]
        return out

    # ------------------------------------------------------------------
    # Delivery & Wakeup Analysis (Prevents 1-Way Blind Messaging & Token Burn)
    # ------------------------------------------------------------------

    def _call_wakeup(self, target_uri: str, wake_mode: str = "auto") -> bool:
        if not self.wakeup_agent_fn or not target_uri or "/channel/" in target_uri:
            return False
        try:
            return bool(self.wakeup_agent_fn(target_uri, wake_mode=wake_mode))
        except TypeError:
            return bool(self.wakeup_agent_fn(target_uri))

    def _assess_delivery(
        self,
        resolved_to: str,
        wake: bool = True,
        wake_mode: str = "auto",
    ) -> Dict[str, Any]:
        """Determine the exact delivery path for a target URI and trigger wakeup when appropriate."""
        eff_mode = "none" if not wake else (wake_mode or "auto")
        if "/channel/" in resolved_to:
            online = self.store.list_agents(status="online")
            listeners = sum(1 for a in online if a.get("listening"))
            return {
                "mode": "channel_broadcast",
                "will_auto_receive": listeners > 0 or len(online) > 0,
                "online_peers": len(online),
                "listening_peers": listeners,
                "auto_woken": False,
                "remediation": "",
            }

        target = self.store.get_agent(resolved_to)
        cid = (target.get("conversation_id") if target else "") or (
            resolved_to.rsplit("/", 1)[-1] if resolved_to.startswith("agp://local/") else ""
        )
        now = epoch_now()
        target_listening = bool(target and self.store.is_listener_active(resolved_to))
        last_seen_age = (now - float(target.get("last_seen_at") or 0)) if target else 999999.0
        target_act = (target.get("activity_state") if target else "offline") or "offline"
        target_status = (target.get("status") if target else "offline") or "offline"
        active_mid_turn = (
            target is not None
            and target_act in {"running_tool", "thinking", "communicating"}
            and last_seen_age < 35.0
        )
        presence_locked = bool(cid and self.presence_check_fn(cid))
        listen_cmd = build_listen_command(resolved_to, data_dir=self.teams.data_dir)

        # 1. Best case: Target has an active zero-token `agp.listen` background listener -> immediate native wakeup!
        if target_listening:
            return {
                "mode": "reactive_listener",
                "will_auto_receive": True,
                "target_status": "online",
                "target_activity": "waiting",
                "target_listening": True,
                "target_presence_locked": presence_locked,
                "auto_woken": False,
                "expected_wakeup": "immediate",
                "remediation": "",
            }

        # 2. Target is actively mid-turn -> PreInvocation/PostInvocation/Stop hooks will inject on next step!
        if active_mid_turn:
            return {
                "mode": "active_hook_injection",
                "will_auto_receive": True,
                "target_status": "online",
                "target_activity": target_act,
                "target_listening": False,
                "target_presence_locked": presence_locked,
                "auto_woken": False,
                "expected_wakeup": "next_step",
                "remediation": "",
            }

        # 3. Target is `auto_cli` or caller explicitly requested `force_cli` -> trigger detached CLI wakeup worker!
        should_cli_wake = (
            eff_mode != "none"
            and (
                (target is not None and target.get("wakeup_mode") == "auto_cli")
                or (eff_mode == "force_cli" and bool(cid))
            )
        )
        if should_cli_wake:
            woken = self._call_wakeup(resolved_to, wake_mode=eff_mode)
            if woken:
                return {
                    "mode": "auto_cli_worker",
                    "will_auto_receive": True,
                    "target_status": target_status,
                    "target_activity": target_act,
                    "target_listening": False,
                    "target_presence_locked": presence_locked,
                    "auto_woken": True,
                    "expected_wakeup": "background_cli_turn",
                    "remediation": "",
                }

        # 4. Target has webhook wakeup mode
        if eff_mode != "none" and target and target.get("wakeup_mode") == "webhook" and target.get("webhook_url"):
            return {
                "mode": "webhook",
                "will_auto_receive": True,
                "target_status": target_status,
                "target_activity": target_act,
                "target_listening": False,
                "auto_woken": False,
                "expected_wakeup": "webhook_callback",
                "remediation": "",
            }

        # 5. Target is idle / offline with NO listener and NO auto_cli -> HONEST diagnostic so sender won't burn tokens!
        mode = "queued_target_idle" if (presence_locked or target_status == "online") else "queued_target_offline"
        remediation = (
            f"Target `{resolved_to}` is currently {'idle at an open interactive prompt' if presence_locked else 'idle/offline'} "
            "without an active `agp.listen` background channel and is not registered with `wakeup_mode='auto_cli'`. "
            "Message is safely persisted (24h TTL) and will be injected automatically on its next turn, "
            "but do NOT loop calling `agp_inbox` right now (wastes tokens). Options: "
            "(1) pass `wake_mode='force_cli'` to run a detached CLI turn on that conversation, "
            "(2) use `team_spawn` for an autonomous teammate, or "
            f"(3) have the target open its zero-token receive channel: `{listen_cmd}`."
        )
        return {
            "mode": mode,
            "will_auto_receive": False,
            "target_status": target_status,
            "target_activity": target_act,
            "target_listening": False,
            "target_presence_locked": presence_locked,
            "auto_woken": False,
            "recipient_listen_command": listen_cmd,
            "remediation": remediation,
        }

    def _mirror_agp_reply_to_team_if_applicable(
        self,
        actor: str,
        resolved_to: str,
        args: Dict[str, Any],
        envelope: Dict[str, Any],
    ) -> None:
        """Ensure an `agp_send` reply to a team-originated message ALSO lands in the Team mailbox."""
        payload = envelope.get("payload") or {}
        team_slug = str(payload.get("team") or "").strip() if isinstance(payload, dict) else ""
        to_handle = str(payload.get("to_handle") or "").strip() if isinstance(payload, dict) else ""
        from_handle = str(payload.get("from_handle") or "").strip() if isinstance(payload, dict) else ""

        if (not team_slug or not to_handle) and args.get("in_reply_to"):
            parent = self.store.get_message(str(args["in_reply_to"]))
            pp = (parent.get("payload") or {}) if parent else {}
            if isinstance(pp, dict) and pp.get("team"):
                team_slug = team_slug or str(pp.get("team") or "")
                # Flip handles on reply!
                to_handle = to_handle or str(pp.get("from_handle") or "")
                from_handle = from_handle or str(pp.get("to_handle") or "")

        if not team_slug or not to_handle:
            return
        if isinstance(payload, dict) and payload.get("_team_msg_id"):
            # Already originated from team_send bridge; avoid echo loop
            return
        try:
            text_body = envelope.get("summary") or ""
            if isinstance(payload, dict):
                extra = payload.get("reply") or payload.get("message") or payload.get("response")
                if extra and str(extra) not in text_body:
                    text_body = f"{text_body}\n{extra}".strip()
            self.teams.send_message(
                team=team_slug,
                from_name=from_handle or actor.rsplit("/", 1)[-1][:16],
                to=to_handle,
                message=text_body[:2500] or "AGP reply received",
                wake=False,
                hop=int(envelope.get("hop_count") or 1),
                agp_msg_id=str(envelope.get("msg_id") or ""),
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Tool Execution Router (23 Complete Tools)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # MCP Skills Extension (`io.modelcontextprotocol/skills`), Code Mode & Docs
    # ------------------------------------------------------------------

    def _load_skills_catalog(
        self,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Tuple[str, str]]]:
        """Load bundled AGP skills for `skills/list`, `skills/get`, and `skill://<name>/<path>`."""
        if self._skills_cache is not None:
            return self._skills_cache

        skill_md_path = PLUGIN_DIR / "skills" / "agp-protocol" / "SKILL.md"
        rules_md_path = PLUGIN_DIR / "rules" / "AGENTS.md"
        agp_skill_text = (
            skill_md_path.read_text(encoding="utf-8", errors="replace")
            if skill_md_path.exists()
            else "# AGP Protocol Playbook\nUse team_* and agp_* tools for multi-agent coordination.\n"
        )
        rules_text = (
            rules_md_path.read_text(encoding="utf-8", errors="replace")
            if rules_md_path.exists()
            else "# AGP Collaboration Rules\nAlways reply to peer messages and avoid blind polling.\n"
        )
        wakeup_skill_text = (
            "---\n"
            "name: two-way-wakeup\n"
            "description: Zero-token reactive wakeup (`agp-listen`), `team_join` channel handshake, `agp_ask` fail-fast, and active lifecycle hooks.\n"
            "---\n\n"
            "# Two-Way Delivery & Zero-Token Wakeup\n\n"
            "1. Active turns receive both AGP SQLite (24h TTL) and Team mailbox messages via PreInvocation, PostInvocation, and Stop hooks.\n"
            "2. Idle interactive sessions stay reachable at 0 tokens by running `agp-listen --agent <your_agent_uri>` in the background.\n"
            "3. `team_join` immediately sends a Team Handshake opening the joined peer's reply route.\n"
            "4. Use `agp_ask` (0ms fail-fast) or inspect `delivery.will_auto_receive` — never loop polling `agp_inbox`.\n"
        )

        raw_skills: List[Tuple[str, str, Dict[str, Tuple[str, str]]]] = [
            (
                "agp-protocol",
                "Serverless AGP v2.1 inter-agent mesh, Claude-style Agent Teams, workspace file leases, SHA-256 artifacts, and distributed tracing.",
                {
                    "SKILL.md": ("text/markdown", agp_skill_text),
                    "RULES.md": ("text/markdown", rules_text),
                },
            ),
            (
                "two-way-wakeup",
                "Zero-token reactive wakeup (`agp-listen`), `team_join` handshake, and `agp_ask` anti-token-burn patterns.",
                {
                    "SKILL.md": ("text/markdown", wakeup_skill_text),
                },
            ),
        ]

        skills_out: List[Dict[str, Any]] = []
        by_uri: Dict[str, Dict[str, Any]] = {}
        files_by_uri: Dict[str, Tuple[str, str]] = {}

        for sname, sdesc, file_map in raw_skills:
            entry_uri = f"skill://{sname}/SKILL.md"
            file_entries: List[Dict[str, Any]] = []
            for rel_path, (mime_t, content_str) in file_map.items():
                f_uri = f"skill://{sname}/{rel_path}"
                raw_b = content_str.encode("utf-8")
                digest = "sha256:" + hashlib.sha256(raw_b).hexdigest()
                file_entries.append(
                    {
                        "uri": f_uri,
                        "path": rel_path,
                        "mimeType": mime_t,
                        "size": len(raw_b),
                        "digest": digest,
                    }
                )
                files_by_uri[f_uri] = (mime_t, content_str)
            skill_obj = {
                "uri": entry_uri,
                "frontmatter": {"name": sname, "description": sdesc},
                "files": file_entries,
            }
            skills_out.append(skill_obj)
            by_uri[entry_uri] = skill_obj

        self._skills_cache = (skills_out, by_uri, files_by_uri)
        return self._skills_cache

    @staticmethod
    def _resolve_step_templates(obj: Any, step_results: List[Dict[str, Any]]) -> Any:
        """Substitute `$prev.a.b` or `$steps[0].a.b` references inside chained Code Mode `execute` pipelines."""
        if isinstance(obj, str):
            m = re.fullmatch(r"\$(prev|steps\[(\d+)\])(?:\.([A-Za-z0-9_.\[\]]+))?", obj.strip())
            if not m:
                return obj
            idx = (len(step_results) - 1) if m.group(1) == "prev" else int(m.group(2))
            if idx < 0 or idx >= len(step_results):
                return obj
            cur: Any = step_results[idx]
            path_expr = m.group(3) or ""
            if path_expr:
                for seg in path_expr.split("."):
                    if isinstance(cur, dict) and seg in cur:
                        cur = cur[seg]
                    elif isinstance(cur, list) and seg.isdigit() and int(seg) < len(cur):
                        cur = cur[int(seg)]
                    else:
                        return obj
            return cur
        if isinstance(obj, list):
            return [MCPHandler._resolve_step_templates(x, step_results) for x in obj]
        if isinstance(obj, dict):
            return {k: MCPHandler._resolve_step_templates(v, step_results) for k, v in obj.items()}
        return obj

    def _execute_tool(
        self,
        name: str,
        args: Dict[str, Any],
        session_id: Optional[str],
        cancel_event: Optional[threading.Event] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        meta_actor = args.pop("_meta_agent_uri", None)

        if name == "whoami":
            actor = self._resolve_actor_uri(session_id, meta_actor)
            ag = self.store.get_agent(actor)
            with self._lock:
                sess = self._sessions.get(session_id or "") or {}
                client_surf = str(sess.get("client_surface") or "mcp")
            out_profile: Dict[str, Any] = {
                "id": actor,
                "name": (ag.get("display_name") if ag else None) or actor.rsplit("/", 1)[-1],
                "surface": (ag.get("surface") if ag else None) or client_surf,
                "role": (ag.get("role") if ag else None) or "general-engineer",
                "listening": self.store.is_listener_active(actor),
            }
            return True, out_profile

        if name == "docs":
            q = str(args.get("query") or "").strip().lower()
            limit = max(1, min(int(args.get("limit") or 6), 20))
            terms = [w for w in re.split(r"[\s,./_-]+", q) if w]
            scored: List[Tuple[int, Dict[str, Any]]] = []
            for doc in AGP_DOCS_INDEX:
                hay = f"{doc['title']} {doc['tags']} {doc['content']}".lower()
                score = (10 if q and q in hay else 0) + sum(3 for t in terms if t in hay)
                if not q or score > 0:
                    scored.append((score, {"kind": "guide", **doc}))
            for tool_def in MCP_TOOLS:
                if tool_def["name"] in {"docs", "whoami"}:
                    continue
                hay = f"{tool_def['name']} {tool_def['description']} {json.dumps(tool_def.get('inputSchema', {}))}".lower()
                score = (12 if q and q in tool_def["name"].lower() else 0) + sum(2 for t in terms if t in hay)
                if q and score > 0:
                    scored.append(
                        (
                            score,
                            {
                                "kind": "tool",
                                "name": tool_def["name"],
                                "description": tool_def["description"],
                                "required": (tool_def.get("inputSchema") or {}).get("required", []),
                                "properties": list(((tool_def.get("inputSchema") or {}).get("properties") or {}).keys()),
                            },
                        )
                    )
            scored.sort(key=lambda x: x[0], reverse=True)
            matches = [item for _, item in scored[:limit]]
            return True, {"ok": True, "query": args.get("query", ""), "count": len(matches), "results": matches}

        if name == "search":
            scope = str(args.get("scope") or "tools").lower()
            q = str(args.get("query") or "").strip().lower()
            filters = args.get("filters") if isinstance(args.get("filters"), dict) else {}
            if scope == "tools":
                matched_tools = []
                for td in MCP_TOOLS:
                    if td["name"] in {"search", "execute"}:
                        continue
                    hay = f"{td['name']} {td['description']}".lower()
                    if not q or q in hay:
                        matched_tools.append(
                            {
                                "name": td["name"],
                                "description": td["description"],
                                "required": (td.get("inputSchema") or {}).get("required", []),
                                "properties": (td.get("inputSchema") or {}).get("properties", {}),
                            }
                        )
                return True, {"ok": True, "scope": "tools", "count": len(matched_tools), "tools": matched_tools}
            if scope == "agents":
                d_args = dict(filters)
                if q:
                    d_args.setdefault("query", q)
                return self._execute_tool("agp_discover", d_args, session_id, cancel_event=cancel_event)
            if scope == "teams":
                return self._execute_tool("team_list", dict(filters), session_id, cancel_event=cancel_event)
            if scope == "tasks":
                t_args = {"action": "list", **filters}
                return self._execute_tool("agp_task", t_args, session_id, cancel_event=cancel_event)
            if scope == "leases":
                return self._execute_tool("agp_lease", {"action": "list", **filters}, session_id, cancel_event=cancel_event)
            if scope == "artifacts":
                return self._execute_tool("agp_artifact_list", dict(filters), session_id, cancel_event=cancel_event)
            if scope == "traces":
                tr_args = dict(filters)
                if q and "trace_id" not in tr_args:
                    tr_args["trace_id"] = q
                return self._execute_tool("agp_trace_inspect", tr_args, session_id, cancel_event=cancel_event)
            if scope == "all":
                _, ag_r = self._execute_tool("agp_discover", {"query": q} if q else {}, session_id, cancel_event=cancel_event)
                _, tm_r = self._execute_tool("team_list", {}, session_id, cancel_event=cancel_event)
                _, ls_r = self._execute_tool("agp_lease", {"action": "list"}, session_id, cancel_event=cancel_event)
                _, tk_r = self._execute_tool("agp_task", {"action": "list"}, session_id, cancel_event=cancel_event)
                return True, {
                    "ok": True,
                    "scope": "all",
                    "agents": ag_r.get("agents", []),
                    "teams": tm_r.get("teams", []),
                    "leases": ls_r.get("leases", []),
                    "tasks": tk_r.get("tasks", []),
                }
            return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": f"Unknown search scope: {scope}"}

        if name == "execute":
            steps = args.get("steps")
            stop_on_error = bool(args.get("stop_on_error", True))
            if isinstance(steps, list) and steps:
                results_list: List[Dict[str, Any]] = []
                all_ok = True
                for idx, st in enumerate(steps):
                    if not isinstance(st, dict) or not st.get("action"):
                        all_ok = False
                        results_list.append({"step": idx, "ok": False, "error": "Each step requires 'action'"})
                        if stop_on_error:
                            break
                        continue
                    act = str(st["action"])
                    if act in {"execute", "search"}:
                        all_ok = False
                        results_list.append({"step": idx, "action": act, "ok": False, "error": "Nested execute/search not allowed"})
                        if stop_on_error:
                            break
                        continue
                    raw_step_args = st.get("args") if isinstance(st.get("args"), dict) else {}
                    resolved_args = self._resolve_step_templates(raw_step_args, results_list)
                    if meta_actor:
                        resolved_args.setdefault("_meta_agent_uri", meta_actor)
                    ok_s, res_s = self._execute_tool(act, resolved_args, session_id, cancel_event=cancel_event)
                    entry = dict(res_s) if isinstance(res_s, dict) else {"result": res_s}
                    entry["_step"] = idx
                    entry["_action"] = act
                    results_list.append(entry)
                    if not ok_s:
                        all_ok = False
                        if stop_on_error:
                            break
                return all_ok, {"ok": all_ok, "steps_executed": len(results_list), "results": results_list}

            action = str(args.get("action") or "").strip()
            if not action:
                return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": "Provide `action` or `steps[]`"}
            if action in {"execute", "search"}:
                return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": "Nested execute/search is not allowed"}
            sub_args = dict(args.get("args") or {}) if isinstance(args.get("args"), dict) else {}
            if meta_actor:
                sub_args.setdefault("_meta_agent_uri", meta_actor)
            return self._execute_tool(action, sub_args, session_id, cancel_event=cancel_event)

        if name == "agp_status":
            actor = self._resolve_actor_uri(session_id, args.get("agent_uri") or meta_actor)
            reset_res = None
            if args.get("reset_circuit_uri"):
                r_target = str(args["reset_circuit_uri"]).strip()
                r_uri = self.resolve_target_fn(r_target) if self.resolve_target_fn else r_target
                reset_res = self.store.reset_circuit(r_uri)
            agent_card = self.store.get_agent(actor)
            cid = (agent_card.get("conversation_id") if agent_card else "") or (
                actor.rsplit("/", 1)[-1] if actor.startswith("agp://local/") and "/mcp/" not in actor else ""
            )
            unread_agp = self.store.count_unread(actor)
            unread_team = len(
                collect_unread_team_messages_for_agent(
                    self.teams.teams_dir, conversation_id=cid, mark_read=False
                )
            )
            _, lease_res = self.store.manage_lease("list", owner_uri=actor)
            all_leases = lease_res.get("leases") or []
            my_leases = [l for l in all_leases if l.get("owner_uri") == actor]
            circuits = self.store.list_circuit_breakers()
            open_circuits = [c for c in circuits if c.get("state") != "CLOSED"]
            active_tasks = [
                self._format_mcp_task(t)
                for t in self.store.list_tasks(limit=20)
                if (t.get("status") or {}).get("state") in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING", "TASK_STATE_INPUT_REQUIRED", "TASK_STATE_AUTH_REQUIRED"}
            ]
            monitor_up = _check_optional_monitor_up()
            out: Dict[str, Any] = {
                "ok": True,
                "version": __version__,
                "protocol_version": __protocol_version__,
                "mode": "embedded_serverless_mcp",
                "agent_uri": actor,
                "registered": agent_card is not None,
                "agent": agent_card,
                "listening": self.store.is_listener_active(actor),
                "listen_command": build_listen_command(actor, data_dir=self.teams.data_dir),
                "unread_agp_messages": unread_agp,
                "unread_team_messages": unread_team,
                "my_leases": my_leases,
                "total_active_leases": len(all_leases),
                "open_circuits": open_circuits,
                "active_tasks": active_tasks,
                "storage_db": str(self.store.db_path),
                "optional_monitor_ui": {
                    "running": monitor_up,
                    "url": f"http://{DEFAULT_HOST}:{DEFAULT_PORT}" if monitor_up else "",
                    "note": (
                        "Monitor UI is optional (for viewing dashboard only). "
                        "All MCP tools, Agent Teams, leases, and hooks operate silently without it."
                    ),
                },
            }
            if reset_res:
                out["reset_circuit"] = reset_res
            return True, out

        if name == "agp_register":
            actor = self._resolve_actor_uri(session_id, args.get("agent_uri") or meta_actor)
            payload = dict(args)
            payload["agent_uri"] = actor
            if not payload.get("conversation_id") and actor.startswith("agp://local/") and "/mcp/" not in actor:
                payload["conversation_id"] = actor.rsplit("/", 1)[-1]
            card = self.store.upsert_agent(payload)
            self.notify_fn("agent_updated", card)
            return True, {
                "ok": True,
                "agent": card,
                "listen_command": build_listen_command(actor, data_dir=self.teams.data_dir),
            }

        if name == "agp_unregister":
            actor = self._resolve_actor_uri(session_id, args.get("agent_uri") or meta_actor)
            res = self.store.unregister_agent(
                agent_uri=actor,
                release_leases=bool(args.get("release_leases", True)),
                reason=str(args.get("reason") or "Session complete"),
            )
            self.notify_fn("agent_updated", {"agent_uri": actor, "status": "offline"})
            return True, res

        if name == "agp_discover":
            include_subagents = bool(args.get("include_subagents", False))
            exclude_self = bool(args.get("exclude_self", False))
            self_uri = self._resolve_actor_uri(session_id, meta_actor) if exclude_self else None
            snap = self.snapshot_fn()
            agents = self.store.list_agents(
                role=args.get("role"),
                capability=args.get("capability"),
                path_filter=args.get("path_filter"),
                status=args.get("status"),
                surface=args.get("surface"),
                query=args.get("query"),
                exclude_uri=self_uri,
            )
            sess_by_uri = {s["agent_uri"]: s for s in snap.get("sessions", [])}
            by_uri: Dict[str, Dict[str, Any]] = {}
            for a in agents:
                uri = a["agent_uri"]
                s = sess_by_uri.get(uri)
                if s and s.get("is_subagent") and not include_subagents:
                    continue
                ident = (s or {}).get("identity") or {}
                enriched = dict(a)
                if ident:
                    enriched["full_label"] = ident.get("full_label", a.get("display_name", ""))
                    enriched["name"] = ident.get("name", a.get("display_name", ""))
                    enriched["task_label"] = ident.get("task_label", "")
                    enriched["handle"] = ident.get("handle", "")
                    enriched["at_handle"] = ident.get("at_handle", "")
                    enriched["project"] = ident.get("project", "")
                    enriched["team"] = ident.get("team", "")
                if s:
                    enriched["is_subagent"] = bool(s.get("is_subagent"))
                    enriched["title"] = s.get("title", "")
                    enriched["total_tokens"] = s.get("total_tokens", 0)
                by_uri[uri] = enriched

            q_low = str(args.get("query") or "").strip().lower()
            surf_low = str(args.get("surface") or "").strip().lower()
            for s in snap.get("sessions", []):
                if s.get("is_subagent") and not include_subagents:
                    continue
                uri = s["agent_uri"]
                if self_uri and uri == self_uri:
                    continue
                if uri not in by_uri:
                    if args.get("status") and s.get("status") != args["status"]:
                        continue
                    if surf_low and (s.get("surface") or "").lower() != surf_low:
                        continue
                    if args.get("role") and args["role"].lower() not in (s.get("role") or "").lower():
                        continue
                    if args.get("capability") and args["capability"] not in (s.get("capabilities") or []):
                        continue
                    if args.get("path_filter"):
                        cwd = s.get("cwd") or s.get("inferred_workspace", "")
                        if not cwd or args["path_filter"].lower().rstrip("/*") not in cwd.lower():
                            continue
                    ident = s.get("identity") or {}
                    if q_low:
                        hay = " ".join(
                            [
                                uri,
                                str(ident.get("full_label") or s.get("display_name") or ""),
                                str(s.get("title") or ""),
                                str(s.get("cwd") or s.get("inferred_workspace") or ""),
                            ]
                        ).lower()
                        if q_low not in hay:
                            continue
                    by_uri[uri] = {
                        "agent_uri": uri,
                        "conversation_id": s["conversation_id"],
                        "display_name": ident.get("full_label") or s["display_name"],
                        "full_label": ident.get("full_label") or s["display_name"],
                        "name": ident.get("name") or s["display_name"],
                        "task_label": ident.get("task_label", ""),
                        "handle": ident.get("handle", ""),
                        "at_handle": ident.get("at_handle", ""),
                        "project": ident.get("project", ""),
                        "team": ident.get("team", ""),
                        "surface": s["surface"],
                        "role": s.get("role", "general-engineer"),
                        "model": s.get("model"),
                        "cwd": s.get("cwd") or s.get("inferred_workspace", ""),
                        "owned_paths": s.get("owned_paths", []),
                        "capabilities": s.get("capabilities", []),
                        "wakeup_mode": s.get("wakeup_mode", "hook"),
                        "listening": False,
                        "status": s["status"],
                        "activity_state": s["activity_state"],
                        "current_action": s.get("current_action", ""),
                        "title": s.get("title", ""),
                        "is_subagent": bool(s.get("is_subagent")),
                        "total_tokens": s.get("total_tokens", 0),
                    }
            return True, {"ok": True, "count": len(by_uri), "agents": list(by_uri.values())}

        if name == "agp_send":
            actor = self._resolve_actor_uri(session_id, args.get("from_uri") or meta_actor)
            raw_to = str(args.get("to_uri") or "").strip()
            if not raw_to and args.get("in_reply_to"):
                parent = self.store.get_message(str(args["in_reply_to"]))
                if parent:
                    raw_to = str((parent.get("from") or {}).get("uri") or "")
            resolved_to = self.resolve_target_fn(raw_to) if (self.resolve_target_fn and raw_to) else raw_to
            resolved_to = self.store._normalize_target_uri(resolved_to)

            raw = {
                "from": {"uri": actor},
                "to": {"uri": resolved_to},
                "type": args.get("type", "chat.note"),
                "summary": args.get("summary", ""),
                "payload": args.get("payload") if isinstance(args.get("payload"), dict) else {},
                "verbatim_constraints": args.get("verbatim_constraints") or [],
                "priority": args.get("priority", "normal"),
                "idempotency_key": args.get("idempotency_key"),
                "trace_id": args.get("trace_id"),
                "parent_span_id": args.get("parent_span_id"),
                "correlation_id": args.get("correlation_id"),
                "in_reply_to": args.get("in_reply_to"),
                "hop_count": args.get("hop_count", 1),
                "max_hops": args.get("max_hops", 4),
                "visited_uris": args.get("visited_uris") or [],
                "ttl_sec": args.get("ttl_sec") or 86400,
                "security": {"content_taint": args.get("content_taint", "peer_generated")},
                "artifacts": args.get("artifacts") or [],
            }
            ok, result = self.store.send_message(raw)
            if ok:
                env = result.get("envelope", {})
                self.notify_fn("agp_message", env)
                self._mirror_agp_reply_to_team_if_applicable(actor, resolved_to, args, env)

                if args.get("async_task"):
                    task_obj = self.store.upsert_task(
                        task_id=result["correlation_id"],
                        context_id=result["trace_id"],
                        protocol="mcp",
                        from_uri=actor,
                        to_uri=resolved_to,
                        state="TASK_STATE_WORKING",
                        title=env.get("summary", "")[:120],
                        append_message={
                            "messageId": result["msg_id"],
                            "contextId": result["trace_id"],
                            "taskId": result["correlation_id"],
                            "role": "ROLE_USER",
                            "parts": [{"text": env.get("summary", "")}, {"data": env.get("payload", {})}],
                        },
                    )
                    result["resultType"] = "task"
                    result["task"] = self._format_mcp_task(task_obj)

                delivery = self._assess_delivery(
                    resolved_to,
                    wake=bool(args.get("wake", True)),
                    wake_mode=str(args.get("wake_mode") or "auto"),
                )
                result["delivery"] = delivery
                if delivery.get("auto_woken"):
                    result["auto_woken"] = True
            return ok, result

        if name == "agp_ask":
            actor = self._resolve_actor_uri(session_id, args.get("from_uri") or meta_actor)
            msg_type = str(args.get("type") or "task.request")
            summary = str(args.get("summary") or "").strip()
            payload = dict(args.get("payload") or {}) if isinstance(args.get("payload"), dict) else {}
            if msg_type == "task.request" and not payload.get("goal"):
                payload["goal"] = summary
            if msg_type == "review.request" and not payload.get("target"):
                payload["target"] = summary
            send_args = {
                "from_uri": actor,
                "to_uri": args.get("to_uri", ""),
                "type": msg_type,
                "summary": summary,
                "payload": payload,
                "verbatim_constraints": args.get("verbatim_constraints") or [],
                "priority": args.get("priority", "high"),
                "wake": True,
                "wake_mode": args.get("wake_mode", "auto"),
                "artifacts": args.get("artifacts") or [],
            }
            ok_s, sent = self._execute_tool("agp_send", send_args, session_id, cancel_event=cancel_event)
            if not ok_s:
                return False, sent

            delivery = sent.get("delivery") or {}
            corr_id = sent.get("correlation_id")
            fail_fast = bool(args.get("fail_fast_if_unreachable", True))
            if fail_fast and not delivery.get("will_auto_receive"):
                return True, {
                    "ok": True,
                    "replied": False,
                    "status": "target_unreachable_idle",
                    "msg_id": sent.get("msg_id"),
                    "correlation_id": corr_id,
                    "trace_id": sent.get("trace_id"),
                    "delivery": delivery,
                    "replies": [],
                    "note": "Returned immediately (0ms wait) to prevent token burn because target cannot auto-receive.",
                }

            wait_sec = min(120.0, max(0.0, float(args.get("wait_timeout_sec") or 35.0)))
            replies = self.store.wait_for_inbox(
                agent_uri=actor,
                wait_timeout_sec=wait_sec,
                correlation_id=corr_id,
                include_delivered=False,
                mark_delivered=True,
                limit=10,
                cancel_event=cancel_event,
            )
            return True, {
                "ok": True,
                "replied": len(replies) > 0,
                "status": "replied" if replies else "timeout_waiting_for_reply",
                "msg_id": sent.get("msg_id"),
                "correlation_id": corr_id,
                "trace_id": sent.get("trace_id"),
                "delivery": delivery,
                "replies": replies,
                "listen_command": build_listen_command(actor, data_dir=self.teams.data_dir),
            }

        if name == "agp_inbox":
            actor = self._resolve_actor_uri(session_id, args.get("agent_uri") or meta_actor)
            mark_deliv = bool(args.get("mark_delivered", True))
            msgs = self.store.wait_for_inbox(
                agent_uri=actor,
                wait_timeout_sec=min(60.0, max(0.0, float(args.get("wait_timeout_sec") or 0.0))),
                include_delivered=bool(args.get("include_delivered", False)),
                priority=args.get("priority"),
                since_seq=int(args.get("since_seq") or 0),
                limit=int(args.get("limit") or 20),
                mark_delivered=mark_deliv,
                correlation_id=args.get("correlation_id"),
                trace_id=args.get("trace_id"),
                msg_type=args.get("msg_type"),
                channels=args.get("channels"),
                cancel_event=cancel_event,
            )
            out_res: Dict[str, Any] = {
                "ok": True,
                "agent_uri": actor,
                "count": len(msgs),
                "messages": msgs,
            }
            if bool(args.get("include_team_messages", True)) and not args.get("correlation_id"):
                cid = actor.rsplit("/", 1)[-1] if actor.startswith("agp://local/") and "/mcp/" not in actor else ""
                bridged_ids = {
                    str((m.get("payload") or {}).get("_team_msg_id"))
                    for m in msgs
                    if isinstance(m.get("payload"), dict) and (m.get("payload") or {}).get("_team_msg_id")
                }
                team_msgs = collect_unread_team_messages_for_agent(
                    self.teams.teams_dir,
                    conversation_id=cid,
                    mark_read=mark_deliv,
                    exclude_ids=bridged_ids,
                )
                if team_msgs:
                    out_res["team_messages"] = team_msgs
                    out_res["team_message_count"] = len(team_msgs)
            return True, out_res

        if name == "agp_ack":
            actor = self._resolve_actor_uri(session_id, args.get("actor_uri") or meta_actor)
            ok, res = self.store.ack_message(
                msg_id=args.get("msg_id", ""),
                status=args.get("status", "accepted"),
                reason=args.get("reason", ""),
                actor_uri=actor,
            )
            if ok:
                self.notify_fn("agp_ack", res)
            return ok, res

        if name == "agp_task":
            action = str(args.get("action") or "list")
            tid = str(args.get("task_id") or "")
            if action == "list":
                tasks = self.store.list_tasks(
                    context_id=args.get("context_id"),
                    state=args.get("state"),
                    limit=int(args.get("limit") or 30),
                )
                return True, {"ok": True, "count": len(tasks), "tasks": [self._format_mcp_task(t) for t in tasks]}
            if not tid:
                return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": f"task_id is required for action='{action}'"}
            if action == "get":
                task = self.store.get_task(tid)
                if not task:
                    return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"Task not found: {tid}"}
                return True, {"ok": True, "task": self._format_mcp_task(task), "history": task.get("history", []), "artifacts": task.get("artifacts", [])}
            if action == "wait":
                task = self.store.wait_for_task(
                    tid,
                    wait_timeout_sec=float(args.get("wait_timeout_sec") or 25.0),
                    cancel_event=cancel_event,
                )
                if not task:
                    return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"Task not found: {tid}"}
                return True, {"ok": True, "task": self._format_mcp_task(task), "history": task.get("history", [])}
            if action == "update":
                actor = self._resolve_actor_uri(session_id, args.get("actor_uri") or meta_actor)
                input_data = args.get("input") if isinstance(args.get("input"), dict) else {}
                ok_u, res_u = self.store.update_task_input(tid, input_data=input_data, actor_uri=actor)
                if not ok_u:
                    return False, res_u
                if res_u["task"].get("_to_uri"):
                    self._call_wakeup(res_u["task"]["_to_uri"], wake_mode="auto")
                return True, {"ok": True, "task": self._format_mcp_task(res_u["task"])}
            if action == "cancel":
                ok_c, res_c = self.store.cancel_task(tid, reason=str(args.get("reason") or "Cancelled via agp_task"))
                if not ok_c:
                    return False, res_c
                return True, {"ok": True, "task": self._format_mcp_task(res_c["task"])}
            return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": f"Unknown agp_task action: {action}"}

        if name == "agp_lease":
            actor = self._resolve_actor_uri(session_id, args.get("owner_uri") or meta_actor)
            ok, res = self.store.manage_lease(
                action=args.get("action", "list"),
                owner_uri=actor,
                paths=args.get("paths"),
                mode=args.get("mode", "exclusive_write"),
                ttl_sec=int(args.get("ttl_sec") or 120),
            )
            if ok and args.get("action") in {"acquire", "renew", "release", "force_release"}:
                self.notify_fn("lease_updated", res)
            return ok, res

        if name == "agp_artifact_put":
            actor = self._resolve_actor_uri(session_id, args.get("owner_uri") or meta_actor)
            ok, res = self.store.put_artifact(
                kind=args.get("kind", "text"),
                owner_uri=actor,
                content=args.get("content"),
                source_path=args.get("source_path"),
                mime_type=args.get("mime_type", "text/plain"),
                redact=bool(args.get("redact", True)),
            )
            if ok:
                self.notify_fn("artifact_created", res.get("artifact", {}))
            return ok, res

        if name == "agp_artifact_get":
            return self.store.get_artifact(
                artifact_id=args.get("artifact_id", ""),
                max_bytes=int(args.get("max_bytes") or 65536),
                offset_bytes=int(args.get("offset_bytes") or 0),
            )

        if name == "agp_artifact_list":
            items = self.store.list_artifacts(
                limit=int(args.get("limit") or 30),
                kind=args.get("kind"),
                owner_uri=args.get("owner_uri"),
            )
            return True, {"ok": True, "count": len(items), "artifacts": items}

        if name == "agp_trace_inspect":
            tid = str(args.get("trace_id") or "").strip()
            if not tid or tid in {"*", "recent"}:
                return True, self.store.list_traces(limit=int(args.get("limit") or 25))
            return True, self.store.inspect_trace(tid)

        # Claude-style Team tools
        if name == "team_list":
            t_slug = str(args.get("team") or "").strip()
            if t_slug:
                return self.teams.get_team_status(t_slug)
            teams_snap = self.teams.list_teams_snapshot()
            summary = [
                {
                    "team": t["team"],
                    "description": t["description"],
                    "lead": t["lead"],
                    "member_count": len(t.get("members") or []),
                    "members": [
                        {
                            "name": m.get("name"),
                            "role": m.get("role"),
                            "status": m.get("status"),
                            "managed": bool(m.get("managed")),
                            "conversation_id": m.get("conversation_id", ""),
                        }
                        for m in (t.get("members") or [])
                    ],
                    "task_count": len(t.get("tasks") or []),
                    "message_count": len(t.get("messages") or []),
                }
                for t in teams_snap
            ]
            return True, {"ok": True, "count": len(summary), "teams": summary}

        if name == "team_create":
            lead_cid = str(args.get("lead_conversation_id") or "").strip()
            if not lead_cid:
                actor = self._resolve_actor_uri(session_id, meta_actor)
                if actor.startswith("agp://local/") and "/mcp/" not in actor:
                    lead_cid = actor.rsplit("/", 1)[-1]
            return self.teams.create_team(
                team=args.get("team", ""),
                description=args.get("description", ""),
                lead_name=args.get("lead_name", "lead"),
                lead_conversation_id=lead_cid,
                cwd=args.get("cwd", ""),
            )

        if name == "team_join":
            cid = str(args.get("conversation_id") or "").strip()
            if not cid:
                actor = self._resolve_actor_uri(session_id, meta_actor)
                if actor.startswith("agp://local/") and "/mcp/" not in actor:
                    cid = actor.rsplit("/", 1)[-1]
            return self.teams.join_team(
                team=args.get("team", ""),
                name=args.get("name", ""),
                role=args.get("role", "teammate"),
                conversation_id=cid,
                surface=args.get("surface", "cli"),
                cwd=args.get("cwd", ""),
                managed=bool(args.get("auto_wake", False)),
                auto_wake=bool(args.get("auto_wake", False)),
                notify_member=bool(args.get("notify_member", True)),
                welcome_note=str(args.get("welcome_note") or ""),
            )

        if name == "team_spawn":
            return self.teams.spawn_teammate(
                team=args.get("team", ""),
                name=args.get("name", ""),
                role=args.get("role", "teammate"),
                prompt=args.get("prompt", ""),
                cwd=args.get("cwd", ""),
                model=args.get("model", ""),
                effort=args.get("effort", "low"),
            )

        if name == "team_send":
            return self.teams.send_message(
                team=args.get("team", ""),
                to=args.get("to", ""),
                message=args.get("message", ""),
                from_name=args.get("from_name", "lead"),
                kind=args.get("kind", "message"),
                wake=bool(args.get("wake", True)),
                wake_mode=str(args.get("wake_mode") or "auto"),
                hop=int(args.get("hop", 1)),
            )

        if name == "team_inbox":
            return self.teams.read_inbox(
                team=args.get("team", ""),
                name=args.get("name", ""),
                unread_only=bool(args.get("unread_only", True)),
                mark_read=bool(args.get("mark_read", True)),
                wait_sec=float(args.get("wait_sec") or 0.0),
                cancel_event=cancel_event,
            )

        if name == "team_task":
            return self.teams.manage_task(
                team=args.get("team", ""),
                action=args.get("action", "list"),
                task_id=args.get("task_id"),
                title=args.get("title", ""),
                description=args.get("description", ""),
                owner=args.get("owner", ""),
                status=args.get("status", ""),
                blocked_by=args.get("blocked_by"),
                result_summary=args.get("result_summary", ""),
            )

        if name == "team_shutdown":
            return self.teams.shutdown_teammate(
                team=args.get("team", ""),
                name=args.get("name", ""),
                reason=args.get("reason", "Task complete"),
            )

        if name == "team_delete":
            return self.teams.delete_team(team=args.get("team", ""))

        return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"Unknown tool: {name}"}
