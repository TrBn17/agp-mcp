"""Thread-safe SQLite WAL persistence for AGP Broker state, queues, leases, artifacts & traces."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import (
    ARTIFACT_DIR,
    CIRCUIT_COOLDOWN_SEC,
    CIRCUIT_FAILURE_THRESHOLD,
    CIRCUIT_FAILURE_WINDOW,
    DB_PATH,
    DEFAULT_HEARTBEAT_TTL_SEC,
    DEFAULT_LEASE_TTL_SEC,
    DEFAULT_MAX_CONTEXT_TOKENS_PER_MSG,
    DEFAULT_MESSAGE_TTL_SEC,
    KEYS_DIR,
    LISTENER_STALE_SEC,
    LOCKS_DIR,
    TRACE_DIR,
    ensure_data_dirs,
    get_or_create_broker_key,
)
from .crypto import build_act_chain, get_or_create_agent_keypair
from .security import (
    detect_injection_signals,
    merge_taint,
    redact_secrets,
    sign_envelope,
    sign_envelope_jws,
    validate_envelope,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def epoch_now() -> float:
    return time.time()


def _normalize_path(path_str: str) -> str:
    return path_str.replace("\\", "/").strip()


def _is_abs_like(norm_path: str) -> bool:
    return norm_path.startswith("/") or (len(norm_path) >= 2 and norm_path[1] == ":")


def paths_overlap(pattern_a: str, pattern_b: str) -> bool:
    """Check whether two file paths or glob patterns overlap (including relative vs absolute)."""
    a = _normalize_path(pattern_a).lower()
    b = _normalize_path(pattern_b).lower()
    if not a or not b:
        return False
    if a == b:
        return True
    if fnmatch.fnmatch(a, b) or fnmatch.fnmatch(b, a):
        return True
    # Directory prefix match (e.g., "ai-platform/**" vs "ai-platform/app/main.py")
    a_prefix = a.rstrip("*").rstrip("/")
    b_prefix = b.rstrip("*").rstrip("/")
    if a.endswith("**") and a_prefix and (b == a_prefix or b.startswith(a_prefix + "/")):
        return True
    if b.endswith("**") and b_prefix and (a == b_prefix or a.startswith(b_prefix + "/")):
        return True

    # Relative vs absolute suffix/segment match (e.g. "src/api.py" vs "d:/proj/src/api.py" or "src/**" vs "d:/proj/src/a.py")
    a_abs = _is_abs_like(a)
    b_abs = _is_abs_like(b)
    if a_abs != b_abs:
        rel, full = (a, b) if not a_abs else (b, a)
        rel_clean = rel.lstrip("./")
        rel_prefix = rel_clean.rstrip("*").rstrip("/")
        if rel_clean.endswith("**") and rel_prefix:
            if full.endswith("/" + rel_prefix) or ("/" + rel_prefix + "/") in full:
                return True
        elif "*" in rel_clean or "?" in rel_clean:
            if fnmatch.fnmatch(full, "*/" + rel_clean):
                return True
        elif rel_clean and (full.endswith("/" + rel_clean) or full == rel_clean):
            return True
    return False


class AGPStore:
    """Centralized SQLite WAL state store for the AGP v1.0/v2.0 & A2A v1.0 broker."""

    def __init__(self, db_path: Path = DB_PATH, keys_dir: Optional[Path] = None) -> None:
        ensure_data_dirs()
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        is_default_db = self.db_path.resolve() == DB_PATH.resolve()
        self.keys_dir = Path(keys_dir) if keys_dir else (KEYS_DIR if is_default_db else (self.db_path.parent / "keys"))
        self.artifacts_dir = ARTIFACT_DIR if is_default_db else (self.db_path.parent / "artifacts")
        self.traces_dir = TRACE_DIR if is_default_db else (self.db_path.parent / "traces")
        self.locks_dir = LOCKS_DIR if is_default_db else (self.db_path.parent / "locks")
        for d in (self.keys_dir, self.artifacts_dir, self.traces_dir, self.locks_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.secret_key = get_or_create_broker_key()
        self._lock = threading.RLock()
        self._inbox_cond = threading.Condition(self._lock)
        self._task_listeners: List[Any] = []
        self._closed = False
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=15.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def get_data_version(self) -> int:
        """Return SQLite PRAGMA data_version to detect writes made by OTHER processes in O(1)."""
        with self._lock:
            if self._closed:
                return 0
            row = self._conn.execute("PRAGMA data_version;").fetchone()
            return int(row[0]) if row else 0

    def register_task_listener(self, callback: Any) -> None:
        with self._lock:
            if callback not in self._task_listeners:
                self._task_listeners.append(callback)

    def _emit_task_updated(self, task_obj: Dict[str, Any]) -> None:
        with self._lock:
            listeners = list(self._task_listeners)
        for fn in listeners:
            try:
                fn(task_obj)
            except Exception:
                pass

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=NORMAL;")
            cur.execute("PRAGMA busy_timeout=10000;")
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_uri TEXT PRIMARY KEY,
                    conversation_id TEXT,
                    display_name TEXT,
                    custom_name TEXT DEFAULT '',
                    surface TEXT,
                    role TEXT,
                    description TEXT,
                    model TEXT,
                    cwd TEXT,
                    workspace_paths_json TEXT,
                    owned_paths_json TEXT,
                    capabilities_json TEXT,
                    transcript_path TEXT,
                    trust_tier TEXT DEFAULT 'local-same-user',
                    max_context_tokens INTEGER DEFAULT 4000,
                    status TEXT DEFAULT 'online',
                    activity_state TEXT DEFAULT 'idle',
                    current_action TEXT DEFAULT '',
                    current_tool TEXT DEFAULT '',
                    current_tool_started_at REAL,
                    last_user_input TEXT DEFAULT '',
                    turn_started_at REAL,
                    started_at REAL,
                    last_seen_at REAL,
                    ttl_sec INTEGER DEFAULT 90,
                    stop_continuations INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_agents_conv ON agents(conversation_id);

                CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT UNIQUE NOT NULL,
                    idempotency_key TEXT,
                    trace_id TEXT NOT NULL,
                    span_id TEXT NOT NULL,
                    parent_span_id TEXT,
                    correlation_id TEXT,
                    in_reply_to TEXT,
                    from_uri TEXT NOT NULL,
                    to_uri TEXT NOT NULL,
                    msg_type TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    summary TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    delivered_at REAL,
                    acked_at REAL,
                    ack_reason TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_messages_to_status ON messages(to_uri, status, seq);
                CREATE INDEX IF NOT EXISTS idx_messages_trace ON messages(trace_id, seq);
                CREATE INDEX IF NOT EXISTS idx_messages_idem ON messages(from_uri, idempotency_key);

                CREATE TABLE IF NOT EXISTS leases (
                    lease_id TEXT PRIMARY KEY,
                    owner_uri TEXT NOT NULL,
                    path_pattern TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'exclusive_write',
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_leases_owner ON leases(owner_uri);

                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    owner_uri TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    excerpt TEXT,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_id TEXT,
                    agent_uri TEXT,
                    conversation_id TEXT,
                    event_type TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);

                CREATE TABLE IF NOT EXISTS circuit_breakers (
                    target_uri TEXT PRIMARY KEY,
                    state TEXT NOT NULL DEFAULT 'CLOSED',
                    recent_results_json TEXT NOT NULL DEFAULT '[]',
                    opened_at REAL
                );

                CREATE TABLE IF NOT EXISTS sequences (
                    name TEXT PRIMARY KEY,
                    val INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    context_id TEXT NOT NULL,
                    protocol TEXT NOT NULL DEFAULT 'a2a',
                    from_uri TEXT NOT NULL,
                    to_uri TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'TASK_STATE_SUBMITTED',
                    title TEXT DEFAULT '',
                    history_json TEXT NOT NULL DEFAULT '[]',
                    artifacts_json TEXT NOT NULL DEFAULT '[]',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    result_json TEXT,
                    elicitation_json TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_ctx ON tasks(context_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state, updated_at DESC);

                CREATE TABLE IF NOT EXISTS push_configs (
                    config_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    token TEXT DEFAULT '',
                    auth_json TEXT NOT NULL DEFAULT '{}',
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_push_configs_task ON push_configs(task_id);

                CREATE TABLE IF NOT EXISTS channel_reads (
                    msg_id TEXT NOT NULL,
                    agent_uri TEXT NOT NULL,
                    read_at REAL NOT NULL,
                    PRIMARY KEY (msg_id, agent_uri)
                );

                CREATE INDEX IF NOT EXISTS idx_channel_reads_agent ON channel_reads(agent_uri, msg_id);
                """
            )
            # Safe forward migrations for existing agp.db files
            cols = {r[1] for r in cur.execute("PRAGMA table_info(agents)").fetchall()}
            for col_name, col_def in (
                ("custom_name", "TEXT DEFAULT ''"),
                ("public_key", "TEXT DEFAULT ''"),
                ("key_id", "TEXT DEFAULT ''"),
                ("wakeup_mode", "TEXT DEFAULT 'hook'"),
                ("webhook_url", "TEXT DEFAULT ''"),
                ("last_taint", "TEXT DEFAULT 'local_trusted'"),
                ("channels_json", "TEXT DEFAULT '[]'"),
                ("listener_pid", "INTEGER"),
                ("listener_heartbeat_at", "REAL"),
            ):
                if col_name not in cols:
                    cur.execute(f"ALTER TABLE agents ADD COLUMN {col_name} {col_def}")

            lease_cols = {r[1] for r in cur.execute("PRAGMA table_info(leases)").fetchall()}
            if "fencing_token" not in lease_cols:
                cur.execute("ALTER TABLE leases ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 1")
            self._conn.commit()

    def _next_sequence(self, name: str) -> int:
        """Atomically increment and return a strictly monotonic integer sequence (e.g. fencing_token)."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sequences (name, val) VALUES (?, 1)
                ON CONFLICT(name) DO UPDATE SET val = val + 1
                """,
                (name,),
            )
            row = self._conn.execute("SELECT val FROM sequences WHERE name = ?", (name,)).fetchone()
            return int(row["val"]) if row else 1

    @property
    def is_closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        with self._inbox_cond:
            if self._closed:
                return
            self._closed = True
            self._inbox_cond.notify_all()
            self._conn.close()

    # ------------------------------------------------------------------
    # Agents / Agent Card Registry
    # ------------------------------------------------------------------

    def upsert_agent(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Register or update an AGY Agent Card and live runtime telemetry."""
        now = epoch_now()
        conv_id = data.get("conversation_id") or data.get("conversationId") or ""
        surface = data.get("surface") or self._infer_surface(data.get("transcript_path", ""))
        agent_uri = data.get("agent_uri") or (
            f"agp://local/{surface}/{conv_id}" if conv_id else f"agp://local/{surface}/{uuid.uuid4().hex[:12]}"
        )

        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM agents WHERE agent_uri = ?", (agent_uri,)
            ).fetchone()
            existing_d = dict(existing) if existing else {}

            workspace_paths = data.get("workspace_paths")
            if workspace_paths is None:
                workspace_paths = json.loads(existing_d.get("workspace_paths_json") or "[]")

            owned_paths = data.get("owned_paths")
            if owned_paths is None:
                owned_paths = json.loads(existing_d.get("owned_paths_json") or "[]")

            capabilities = data.get("capabilities")
            if capabilities is None:
                capabilities = json.loads(
                    existing_d.get("capabilities_json")
                    or '["code.read","code.edit","terminal.run","agp.v1"]'
                )

            raw_channels = data.get("channels")
            if raw_channels is None:
                channels = json.loads(existing_d.get("channels_json") or "[]")
            else:
                channels = []
                for ch in raw_channels:
                    c = str(ch or "").strip()
                    if not c:
                        continue
                    if not c.startswith("agp://"):
                        c = f"agp://local/channel/{c.lstrip('#').removeprefix('channel:')}"
                    if c not in channels:
                        channels.append(c)

            cwd = data.get("cwd") or existing_d.get("cwd") or (workspace_paths[0] if workspace_paths else "")
            folder_name = Path(cwd).name if cwd else surface.upper()
            default_name = f"AGY-{surface.upper()} ({folder_name})" if folder_name else f"AGY-{surface.upper()}"

            pub_key = data.get("public_key") or existing_d.get("public_key") or ""
            key_id = data.get("key_id") or existing_d.get("key_id") or ""
            if not pub_key or not key_id:
                kp = get_or_create_agent_keypair(agent_uri, keys_dir=self.keys_dir)
                pub_key = kp["public_key"]
                key_id = kp["kid"]

            row = {
                "agent_uri": agent_uri,
                "conversation_id": conv_id or existing_d.get("conversation_id") or "",
                "display_name": data.get("display_name") or existing_d.get("display_name") or default_name,
                "custom_name": data["custom_name"]
                if "custom_name" in data and data["custom_name"] is not None
                else existing_d.get("custom_name", ""),
                "surface": surface,
                "role": data.get("role") or existing_d.get("role") or "general-engineer",
                "description": data.get("description")
                if data.get("description") is not None
                else existing_d.get("description", ""),
                "model": data.get("model") or existing_d.get("model") or "auto",
                "cwd": cwd,
                "workspace_paths_json": json.dumps(workspace_paths, ensure_ascii=False),
                "owned_paths_json": json.dumps(owned_paths, ensure_ascii=False),
                "capabilities_json": json.dumps(capabilities, ensure_ascii=False),
                "channels_json": json.dumps(channels, ensure_ascii=False),
                "transcript_path": data.get("transcript_path") or existing_d.get("transcript_path") or "",
                "trust_tier": data.get("trust_tier") or existing_d.get("trust_tier") or "local-same-user",
                "public_key": pub_key,
                "key_id": key_id,
                "wakeup_mode": data.get("wakeup_mode") or existing_d.get("wakeup_mode") or "hook",
                "webhook_url": data.get("webhook_url")
                if data.get("webhook_url") is not None
                else existing_d.get("webhook_url", ""),
                "last_taint": data.get("last_taint") or existing_d.get("last_taint") or "local_trusted",
                "listener_pid": data["listener_pid"] if "listener_pid" in data else existing_d.get("listener_pid"),
                "listener_heartbeat_at": data["listener_heartbeat_at"]
                if "listener_heartbeat_at" in data
                else existing_d.get("listener_heartbeat_at"),
                "max_context_tokens": int(
                    data.get("max_context_tokens")
                    or existing_d.get("max_context_tokens")
                    or DEFAULT_MAX_CONTEXT_TOKENS_PER_MSG
                ),
                "status": data.get("status") or "online",
                "activity_state": data.get("activity_state") or existing_d.get("activity_state") or "idle",
                "current_action": data.get("current_action")
                if data.get("current_action") is not None
                else existing_d.get("current_action", ""),
                "current_tool": data.get("current_tool")
                if data.get("current_tool") is not None
                else existing_d.get("current_tool", ""),
                "current_tool_started_at": data["current_tool_started_at"]
                if "current_tool_started_at" in data
                else existing_d.get("current_tool_started_at"),
                "last_user_input": data.get("last_user_input")
                if data.get("last_user_input") is not None
                else existing_d.get("last_user_input", ""),
                "turn_started_at": data["turn_started_at"]
                if "turn_started_at" in data
                else existing_d.get("turn_started_at") or now,
                "started_at": existing_d.get("started_at") or data.get("started_at") or now,
                "last_seen_at": now,
                "ttl_sec": int(data.get("ttl_sec") or existing_d.get("ttl_sec") or DEFAULT_HEARTBEAT_TTL_SEC),
                "stop_continuations": int(
                    data["stop_continuations"]
                    if "stop_continuations" in data
                    else existing_d.get("stop_continuations", 0)
                ),
            }

            cols = list(row.keys())
            placeholders = ", ".join("?" for _ in cols)
            updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "agent_uri")
            self._conn.execute(
                f"INSERT INTO agents ({', '.join(cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT(agent_uri) DO UPDATE SET {updates}",
                [row[c] for c in cols],
            )
            self._conn.commit()
            return self._deserialize_agent(row)

    def get_agent(self, agent_uri: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM agents WHERE agent_uri = ?", (agent_uri,)
            ).fetchone()
            return self._deserialize_agent(dict(row)) if row else None

    def get_agent_by_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM agents WHERE conversation_id = ? ORDER BY last_seen_at DESC LIMIT 1",
                (conversation_id,),
            ).fetchone()
            return self._deserialize_agent(dict(row)) if row else None

    def list_agents(
        self,
        role: Optional[str] = None,
        capability: Optional[str] = None,
        path_filter: Optional[str] = None,
        status: Optional[str] = None,
        surface: Optional[str] = None,
        query: Optional[str] = None,
        exclude_uri: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        now = epoch_now()
        q_low = (query or "").strip().lower()
        surf_low = (surface or "").strip().lower()
        with self._lock:
            rows = self._conn.execute("SELECT * FROM agents ORDER BY last_seen_at DESC").fetchall()
            result: List[Dict[str, Any]] = []
            for r in rows:
                item = self._deserialize_agent(dict(r))
                if exclude_uri and item["agent_uri"] == exclude_uri:
                    continue
                is_listening = bool(
                    item.get("listener_pid")
                    and item.get("listener_heartbeat_at")
                    and (now - float(item["listener_heartbeat_at"])) <= LISTENER_STALE_SEC
                )
                item["listening"] = is_listening
                if item["status"] != "offline" and not is_listening and (now - item["last_seen_at"]) > item["ttl_sec"]:
                    item["status"] = "offline"
                    item["activity_state"] = "offline"
                if is_listening and item["status"] == "offline":
                    item["status"] = "online"
                    item["activity_state"] = "waiting"
                if role and role.lower() not in item["role"].lower():
                    continue
                if capability and capability not in item["capabilities"]:
                    continue
                if surf_low and (item.get("surface") or "").lower() != surf_low:
                    continue
                if path_filter:
                    owned = item["owned_paths"] or item["workspace_paths"]
                    if not any(paths_overlap(p, path_filter) for p in owned):
                        continue
                if q_low:
                    hay = " ".join(
                        [
                            item.get("agent_uri", ""),
                            item.get("display_name", ""),
                            item.get("custom_name", ""),
                            item.get("role", ""),
                            item.get("description", ""),
                            item.get("cwd", ""),
                            item.get("last_user_input", ""),
                        ]
                    ).lower()
                    if q_low not in hay:
                        continue
                if status and item["status"] != status:
                    continue
                result.append(item)
            return result

    def set_listener_state(
        self,
        agent_uri: str,
        pid: Optional[int],
        heartbeat_at: Optional[float] = None,
    ) -> None:
        """Record or clear an active zero-token `agp.listen` background listener for an agent."""
        now = epoch_now() if heartbeat_at is None else float(heartbeat_at)
        with self._lock:
            existing = self._conn.execute(
                "SELECT agent_uri FROM agents WHERE agent_uri = ?", (agent_uri,)
            ).fetchone()
            if not existing:
                if pid is not None:
                    self.upsert_agent(
                        {
                            "agent_uri": agent_uri,
                            "status": "online",
                            "activity_state": "waiting",
                            "current_action": "Listening for AGP peer messages (0 tokens)",
                            "listener_pid": int(pid),
                            "listener_heartbeat_at": now,
                        }
                    )
                return
            if pid is None:
                self._conn.execute(
                    "UPDATE agents SET listener_pid = NULL, listener_heartbeat_at = NULL WHERE agent_uri = ?",
                    (agent_uri,),
                )
            else:
                self._conn.execute(
                    "UPDATE agents SET listener_pid = ?, listener_heartbeat_at = ?, last_seen_at = ? WHERE agent_uri = ?",
                    (int(pid), now, now, agent_uri),
                )
            self._conn.commit()

    def is_listener_active(self, agent_uri: str, stale_sec: float = LISTENER_STALE_SEC) -> bool:
        now = epoch_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT listener_pid, listener_heartbeat_at FROM agents WHERE agent_uri = ?",
                (agent_uri,),
            ).fetchone()
            if not row or not row["listener_pid"] or not row["listener_heartbeat_at"]:
                return False
            return (now - float(row["listener_heartbeat_at"])) <= stale_sec

    def heartbeat_agent(
        self,
        agent_uri: str,
        status: Optional[str] = None,
        activity_state: Optional[str] = None,
        current_action: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = epoch_now()
        with self._lock:
            row = self._conn.execute("SELECT * FROM agents WHERE agent_uri = ?", (agent_uri,)).fetchone()
            if not row:
                return None
            new_status = status or row["status"] or "online"
            new_act = activity_state or row["activity_state"] or "idle"
            new_action = current_action if current_action is not None else (row["current_action"] or "")
            self._conn.execute(
                "UPDATE agents SET status = ?, activity_state = ?, current_action = ?, last_seen_at = ? WHERE agent_uri = ?",
                (new_status, new_act, new_action, now, agent_uri),
            )
            self._conn.commit()
            return self.get_agent(agent_uri)

    def unregister_agent(
        self,
        agent_uri: str,
        release_leases: bool = True,
        reason: str = "Graceful disconnect",
    ) -> Dict[str, Any]:
        """Mark agent offline (`peer.bye`), clear listener state, and optionally release held leases."""
        now = epoch_now()
        released_count = 0
        with self._lock:
            self._conn.execute(
                """
                UPDATE agents
                SET status = 'offline', activity_state = 'offline',
                    current_action = ?, current_tool = '', current_tool_started_at = NULL,
                    listener_pid = NULL, listener_heartbeat_at = NULL, last_seen_at = ?
                WHERE agent_uri = ?
                """,
                (f"Offline ({reason})"[:120], now, agent_uri),
            )
            if release_leases:
                cur = self._conn.execute("DELETE FROM leases WHERE owner_uri = ?", (agent_uri,))
                released_count = int(cur.rowcount or 0)
            self._conn.commit()
        self.record_event(
            "peer.bye",
            {"reason": reason, "released_leases": released_count},
            agent_uri=agent_uri,
        )
        return {
            "ok": True,
            "agent_uri": agent_uri,
            "status": "offline",
            "released_leases": released_count,
            "reason": reason,
        }

    def rename_agent(
        self,
        agent_uri: str = "",
        conversation_id: str = "",
        custom_name: str = "",
        role: Optional[str] = None,
        surface: str = "cli",
    ) -> Tuple[bool, Dict[str, Any]]:
        """Persist a human-chosen nickname (and optional role) so hooks never overwrite it."""
        cid = (conversation_id or "").strip()
        uri = (agent_uri or "").strip()
        if not uri and cid:
            existing_by_cid = self.get_agent_by_conversation(cid)
            uri = existing_by_cid["agent_uri"] if existing_by_cid else f"agp://local/{surface}/{cid}"
        if not uri:
            return False, {"ok": False, "error": "agent_uri or conversation_id is required"}

        clean_name = " ".join((custom_name or "").strip().split())[:64]
        clean_role = " ".join((role or "").strip().split())[:48] if role is not None else None
        now = epoch_now()

        with self._lock:
            existing = self._conn.execute("SELECT * FROM agents WHERE agent_uri = ?", (uri,)).fetchone()
            if existing:
                if clean_role is not None:
                    self._conn.execute(
                        "UPDATE agents SET custom_name = ?, display_name = CASE WHEN ? != '' THEN ? ELSE display_name END, role = CASE WHEN ? != '' THEN ? ELSE role END WHERE agent_uri = ?",
                        (clean_name, clean_name, clean_name, clean_role, clean_role, uri),
                    )
                else:
                    self._conn.execute(
                        "UPDATE agents SET custom_name = ?, display_name = CASE WHEN ? != '' THEN ? ELSE display_name END WHERE agent_uri = ?",
                        (clean_name, clean_name, clean_name, uri),
                    )
                self._conn.commit()
                updated = self.get_agent(uri)
                return True, {"ok": True, "agent": updated}

        # Create minimal offline card if this transcript-only session hasn't fired hooks yet
        card = self.upsert_agent(
            {
                "agent_uri": uri,
                "conversation_id": cid or uri.split("/")[-1],
                "surface": surface,
                "display_name": clean_name or f"AGY-{surface.upper()}",
                "custom_name": clean_name,
                "role": clean_role or "general-engineer",
                "status": "offline",
                "activity_state": "offline",
                "started_at": now,
            }
        )
        return True, {"ok": True, "agent": card}

    @staticmethod
    def _infer_surface(transcript_path: str) -> str:
        low = (transcript_path or "").lower()
        if "antigravity-cli" in low:
            return "cli"
        if "antigravity-ide" in low:
            return "ide"
        if "antigravity" in low:
            return "app"
        return "cli"

    @staticmethod
    def _deserialize_agent(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "agent_uri": row["agent_uri"],
            "conversation_id": row["conversation_id"],
            "display_name": row["display_name"],
            "custom_name": row.get("custom_name") or "",
            "surface": row["surface"],
            "role": row["role"],
            "description": row["description"],
            "model": row["model"],
            "cwd": row["cwd"],
            "workspace_paths": json.loads(row.get("workspace_paths_json") or "[]"),
            "owned_paths": json.loads(row.get("owned_paths_json") or "[]"),
            "capabilities": json.loads(row.get("capabilities_json") or "[]"),
            "channels": json.loads(row.get("channels_json") or "[]"),
            "transcript_path": row.get("transcript_path") or "",
            "trust_tier": row.get("trust_tier") or "local-same-user",
            "public_key": row.get("public_key") or "",
            "key_id": row.get("key_id") or "",
            "wakeup_mode": row.get("wakeup_mode") or "hook",
            "webhook_url": row.get("webhook_url") or "",
            "last_taint": row.get("last_taint") or "local_trusted",
            "listener_pid": row.get("listener_pid"),
            "listener_heartbeat_at": row.get("listener_heartbeat_at"),
            "listening": bool(
                row.get("listener_pid")
                and row.get("listener_heartbeat_at")
                and (epoch_now() - float(row["listener_heartbeat_at"])) <= LISTENER_STALE_SEC
            ),
            "max_context_tokens": int(row.get("max_context_tokens") or DEFAULT_MAX_CONTEXT_TOKENS_PER_MSG),
            "status": row.get("status") or "online",
            "activity_state": row.get("activity_state") or "idle",
            "current_action": row.get("current_action") or "",
            "current_tool": row.get("current_tool") or "",
            "current_tool_started_at": row.get("current_tool_started_at"),
            "last_user_input": row.get("last_user_input") or "",
            "turn_started_at": row.get("turn_started_at"),
            "started_at": row.get("started_at"),
            "last_seen_at": row.get("last_seen_at"),
            "ttl_sec": int(row.get("ttl_sec") or DEFAULT_HEARTBEAT_TTL_SEC),
            "stop_continuations": int(row.get("stop_continuations") or 0),
        }

    # ------------------------------------------------------------------
    # Circuit Breaker
    # ------------------------------------------------------------------

    def check_circuit(self, target_uri: str) -> Tuple[bool, str]:
        """Return (allowed, state). Transitions OPEN -> HALF-OPEN after cooldown."""
        now = epoch_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM circuit_breakers WHERE target_uri = ?", (target_uri,)
            ).fetchone()
            if not row:
                return True, "CLOSED"
            state = row["state"]
            opened_at = row["opened_at"] or 0.0
            if state == "OPEN":
                if (now - opened_at) >= CIRCUIT_COOLDOWN_SEC:
                    self._conn.execute(
                        "UPDATE circuit_breakers SET state = 'HALF-OPEN' WHERE target_uri = ?",
                        (target_uri,),
                    )
                    self._conn.commit()
                    return True, "HALF-OPEN"
                return False, "OPEN"
            return True, state

    def record_circuit_outcome(self, target_uri: str, success: bool) -> str:
        """Record success/failure for target_uri and update circuit state."""
        now = epoch_now()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM circuit_breakers WHERE target_uri = ?", (target_uri,)
            ).fetchone()
            results: List[bool] = json.loads(row["recent_results_json"]) if row else []
            state = row["state"] if row else "CLOSED"
            opened_at = row["opened_at"] if row else None

            results.append(bool(success))
            results = results[-CIRCUIT_FAILURE_WINDOW:]
            failures = sum(1 for r in results if not r)

            if state == "HALF-OPEN":
                if success:
                    state = "CLOSED"
                    results = [True]
                    opened_at = None
                else:
                    state = "OPEN"
                    opened_at = now
            elif failures >= CIRCUIT_FAILURE_THRESHOLD:
                state = "OPEN"
                opened_at = now
            else:
                state = "CLOSED"
                opened_at = None

            self._conn.execute(
                """
                INSERT INTO circuit_breakers (target_uri, state, recent_results_json, opened_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(target_uri) DO UPDATE SET
                    state=excluded.state,
                    recent_results_json=excluded.recent_results_json,
                    opened_at=excluded.opened_at
                """,
                (target_uri, state, json.dumps(results), opened_at),
            )
            self._conn.commit()
            return state

    def reset_circuit(self, target_uri: str) -> Dict[str, Any]:
        """Manually reset a tripped circuit breaker back to CLOSED after recovery."""
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO circuit_breakers (target_uri, state, recent_results_json, opened_at)
                VALUES (?, 'CLOSED', '[]', NULL)
                ON CONFLICT(target_uri) DO UPDATE SET
                    state='CLOSED',
                    recent_results_json='[]',
                    opened_at=NULL
                """,
                (target_uri,),
            )
            self._conn.commit()
        return {"ok": True, "target_uri": target_uri, "state": "CLOSED"}

    def list_circuit_breakers(self) -> List[Dict[str, Any]]:
        now = epoch_now()
        with self._lock:
            rows = self._conn.execute("SELECT * FROM circuit_breakers").fetchall()
            out = []
            for r in rows:
                results = json.loads(r["recent_results_json"])
                opened_at = r["opened_at"]
                cooldown_remaining = (
                    max(0, int(CIRCUIT_COOLDOWN_SEC - (now - opened_at)))
                    if r["state"] == "OPEN" and opened_at
                    else 0
                )
                out.append(
                    {
                        "target_uri": r["target_uri"],
                        "state": r["state"],
                        "recent_failures": sum(1 for x in results if not x),
                        "window_size": len(results),
                        "cooldown_remaining_sec": cooldown_remaining,
                    }
                )
            return out

    # ------------------------------------------------------------------
    # Messages (AGP v1.0 Envelope Bus)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_target_uri(uri: str) -> str:
        u = (uri or "").strip()
        if not u:
            return ""
        if u in {"all", "@all"}:
            return "agp://local/channel/all"
        if u.startswith("channel:"):
            return f"agp://local/channel/{u.split(':', 1)[1].strip().lstrip('#')}"
        if u.startswith("#") and len(u) > 1:
            return f"agp://local/channel/{u[1:].strip()}"
        return u

    def get_message(self, msg_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single persisted AGP message envelope by msg_id."""
        if not msg_id:
            return None
        with self._lock:
            row = self._conn.execute("SELECT * FROM messages WHERE msg_id = ?", (msg_id,)).fetchone()
            if not row:
                return None
            env = json.loads(row["envelope_json"])
            env["_meta"] = {
                "seq": row["seq"],
                "status": row["status"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "delivered_at": row["delivered_at"],
                "acked_at": row["acked_at"],
                "ack_reason": row["ack_reason"],
            }
            return env

    def has_agent_sent_since(
        self,
        from_uri: str,
        since_epoch: float,
        to_uri: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> bool:
        """Check whether an agent already sent an outbound AGP message since a timestamp."""
        with self._lock:
            q = "SELECT 1 FROM messages WHERE from_uri = ? AND created_at >= ?"
            params: List[Any] = [from_uri, float(since_epoch)]
            if to_uri:
                q += " AND to_uri = ?"
                params.append(to_uri)
            if correlation_id:
                q += " AND correlation_id = ?"
                params.append(correlation_id)
            q += " LIMIT 1"
            return self._conn.execute(q, params).fetchone() is not None

    def send_message(self, raw: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Validate, DLP-redact, sign, persist, and trace an AGP v1.0 message."""
        now = epoch_now()
        raw_in_reply_to = raw.get("in_reply_to")
        parent_msg = self.get_message(str(raw_in_reply_to)) if raw_in_reply_to else None

        from_uri = (raw.get("from") or {}).get("uri") or raw.get("from_uri") or "agp://local/unknown/anonymous"
        raw_to = (raw.get("to") or {}).get("uri") or raw.get("to_uri") or ""
        if not raw_to and parent_msg:
            raw_to = (parent_msg.get("from") or {}).get("uri") or ""
        to_uri = self._normalize_target_uri(raw_to)

        # Check circuit breaker for direct targets (unless control/hitl message)
        msg_type = raw.get("type", "chat.note")
        if to_uri and not msg_type.startswith("sys.") and not msg_type.startswith("hitl."):
            allowed, cb_state = self.check_circuit(to_uri)
            if not allowed:
                return False, {
                    "ok": False,
                    "error_code": "ERR_CIRCUIT_OPEN",
                    "error": f"Circuit breaker for {to_uri} is OPEN ({cb_state}). Use fallback agent or retry after cooldown.",
                }

        # Resolve target context budget
        target_agent = self.get_agent(to_uri) if to_uri else None
        target_budget = (
            target_agent["max_context_tokens"]
            if target_agent
            else DEFAULT_MAX_CONTEXT_TOKENS_PER_MSG
        )

        # DLP redact summary, constraints, and payload before persistence
        clean_summary, red_s = redact_secrets(raw.get("summary", ""), "summary")
        clean_constraints, red_c = redact_secrets(
            raw.get("verbatim_constraints", []), "verbatim_constraints"
        )
        clean_payload, red_p = redact_secrets(raw.get("payload", {}), "payload")
        redacted_fields = sorted(set(red_s + red_c + red_p))

        # Prompt-injection heuristic scan
        scan_text = f"{clean_summary}\n{json.dumps(clean_payload, ensure_ascii=False)}"
        injection_hits = detect_injection_signals(scan_text)

        from_agent = self.get_agent(from_uri)
        sender_kp = get_or_create_agent_keypair(from_uri, keys_dir=self.keys_dir)
        msg_id = raw.get("msg_id") or f"msg_{uuid.uuid4().hex}"
        trace_id = raw.get("trace_id") or (parent_msg.get("trace_id") if parent_msg else None) or f"trc_{uuid.uuid4().hex[:12]}"
        span_id = raw.get("span_id") or f"spn_{uuid.uuid4().hex[:10]}"
        parent_span_id = raw.get("parent_span_id") or (parent_msg.get("span_id") if parent_msg else None)
        correlation_id = (
            raw.get("correlation_id")
            or (parent_msg.get("correlation_id") if parent_msg else None)
            or f"corr_{uuid.uuid4().hex[:10]}"
        )
        ttl_sec = int(raw.get("ttl_sec") or DEFAULT_MESSAGE_TTL_SEC)
        idem_key = raw.get("idempotency_key")

        prior_visited = [str(u) for u in (raw.get("visited_uris") or []) if str(u).strip()]
        if not prior_visited and parent_msg and msg_type not in {"task.result", "task.ack", "task.progress", "review.verdict", "hitl.decision", "chat.note"}:
            prior_visited = [str(u) for u in (parent_msg.get("visited_uris") or []) if str(u).strip()]
        visited_uris = list(prior_visited)
        if from_uri and (not visited_uris or visited_uris[-1] != from_uri):
            visited_uris.append(from_uri)

        raw_sec = raw.get("security") or {}
        effective_taint = merge_taint(
            raw_sec.get("content_taint"),
            from_agent.get("last_taint") if (from_agent and from_agent.get("last_taint") != "local_trusted") else None,
            "external_untrusted" if injection_hits else None,
        )
        act_chain = build_act_chain(from_uri, prior_act=raw_sec.get("act"))

        envelope: Dict[str, Any] = {
            "agp_version": "1.0",
            "msg_id": msg_id,
            "idempotency_key": idem_key,
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "correlation_id": correlation_id,
            "in_reply_to": raw_in_reply_to,
            "timestamp": utc_now_iso(),
            "ttl_sec": ttl_sec,
            "hop_count": int(raw.get("hop_count", len(visited_uris))),
            "max_hops": int(raw.get("max_hops", 4)),
            "visited_uris": visited_uris,
            "from": {
                "uri": from_uri,
                "role": (raw.get("from") or {}).get("role")
                or (from_agent["role"] if from_agent else "unknown"),
            },
            "to": {
                "uri": to_uri,
                "routing": (raw.get("to") or {}).get("routing")
                or ("channel" if "/channel/" in to_uri else "direct"),
            },
            "type": msg_type,
            "priority": raw.get("priority", "normal"),
            "requires_ack": bool(raw.get("requires_ack", msg_type in {"task.request", "review.request", "hitl.escalate"})),
            "security": {
                "trust_tier": raw_sec.get("trust_tier")
                or (from_agent["trust_tier"] if from_agent else "local-same-user"),
                "content_taint": effective_taint,
                "act": act_chain,
                "sender_kid": sender_kp["kid"],
                "sender_pubkey": sender_kp["public_key"],
                "redacted_fields": redacted_fields,
                "injection_flagged": bool(injection_hits),
            },
            "context_budget": {
                "summary_max_tokens": 200,
                "reply_max_tokens": int((raw.get("context_budget") or {}).get("reply_max_tokens", 1500)),
            },
            "summary": clean_summary,
            "verbatim_constraints": clean_constraints if isinstance(clean_constraints, list) else [],
            "payload": clean_payload if isinstance(clean_payload, dict) else {},
            "artifacts": raw.get("artifacts") or [],
        }

        ok, err_code, err_msg = validate_envelope(envelope, target_max_tokens=target_budget)
        if not ok:
            self.append_trace(
                trace_id,
                {
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "agent_uri": from_uri,
                    "event": "message.rejected",
                    "error_code": err_code,
                    "error": err_msg,
                    "timestamp": envelope["timestamp"],
                },
            )
            return False, {"ok": False, "error_code": err_code, "error": err_msg}

        envelope["security"]["signature"] = sign_envelope(envelope, self.secret_key)
        envelope["security"]["jws"] = sign_envelope_jws(
            envelope,
            private_seed_b64=sender_kp["private_seed"],
            kid=sender_kp["kid"],
        )

        with self._inbox_cond:
            # Idempotency check
            if idem_key:
                existing = self._conn.execute(
                    "SELECT * FROM messages WHERE from_uri = ? AND idempotency_key = ?",
                    (from_uri, idem_key),
                ).fetchone()
                if existing:
                    prev_env = json.loads(existing["envelope_json"])
                    if (
                        prev_env.get("type") == envelope["type"]
                        and prev_env.get("payload") == envelope["payload"]
                        and prev_env.get("to", {}).get("uri") == to_uri
                    ):
                        return True, {
                            "ok": True,
                            "deduplicated": True,
                            "msg_id": existing["msg_id"],
                            "seq": existing["seq"],
                            "trace_id": existing["trace_id"],
                            "correlation_id": existing["correlation_id"],
                            "envelope": prev_env,
                        }
                    return False, {
                        "ok": False,
                        "error_code": "ERR_IDEMPOTENCY_CONFLICT",
                        "error": f"idempotency_key '{idem_key}' already used with a different payload",
                    }

            cur = self._conn.execute(
                """
                INSERT INTO messages (
                    msg_id, idempotency_key, trace_id, span_id, parent_span_id,
                    correlation_id, in_reply_to, from_uri, to_uri, msg_type,
                    priority, status, summary, envelope_json, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
                """,
                (
                    msg_id,
                    idem_key,
                    trace_id,
                    span_id,
                    envelope["parent_span_id"],
                    envelope["correlation_id"],
                    envelope["in_reply_to"],
                    from_uri,
                    to_uri,
                    msg_type,
                    envelope["priority"],
                    clean_summary,
                    json.dumps(envelope, ensure_ascii=False),
                    now,
                    now + ttl_sec,
                ),
            )
            seq = cur.lastrowid
            self._conn.commit()

        # Sync any linked A2A / MCP async task matched by correlation_id
        self._sync_task_from_envelope(envelope)

        # Update circuit breaker on task.result outcomes
        if msg_type == "task.result":
            res_status = envelope["payload"].get("status")
            if res_status in {"success", "partial"}:
                self.record_circuit_outcome(from_uri, True)
            elif res_status == "failed":
                self.record_circuit_outcome(from_uri, False)

        self.append_trace(
            trace_id,
            {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": envelope["parent_span_id"],
                "msg_id": msg_id,
                "from_uri": from_uri,
                "to_uri": to_uri,
                "msg_type": msg_type,
                "priority": envelope["priority"],
                "summary": clean_summary,
                "redacted_fields": redacted_fields,
                "injection_flagged": bool(injection_hits),
                "event": "message.sent",
                "timestamp": envelope["timestamp"],
            },
        )
        self.record_event(
            event_type="agp.message",
            trace_id=trace_id,
            agent_uri=from_uri,
            detail={
                "seq": seq,
                "msg_id": msg_id,
                "from_uri": from_uri,
                "to_uri": to_uri,
                "type": msg_type,
                "priority": envelope["priority"],
                "summary": clean_summary,
            },
        )
        with self._inbox_cond:
            self._inbox_cond.notify_all()
        return True, {
            "ok": True,
            "deduplicated": False,
            "seq": seq,
            "msg_id": msg_id,
            "trace_id": trace_id,
            "correlation_id": envelope["correlation_id"],
            "redacted_fields": redacted_fields,
            "envelope": envelope,
        }

    def _resolve_agent_channels(
        self, agent_uri: str, extra_channels: Optional[List[str]] = None
    ) -> List[str]:
        chans: List[str] = ["agp://local/channel/all"]
        ag = self.get_agent(agent_uri)
        for ch in (ag.get("channels") if ag else []) or []:
            norm = self._normalize_target_uri(ch)
            if norm and norm not in chans:
                chans.append(norm)
        for ch in extra_channels or []:
            norm = self._normalize_target_uri(ch)
            if norm and norm not in chans:
                chans.append(norm)
        return chans

    def poll_inbox(
        self,
        agent_uri: str,
        include_delivered: bool = False,
        priority: Optional[str] = None,
        since_seq: int = 0,
        limit: int = 20,
        mark_delivered: bool = True,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        msg_type: Optional[str] = None,
        channels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch messages addressed to agent_uri or its subscribed channels (per-agent channel read tracking)."""
        now = epoch_now()
        with self._lock:
            # Expire stale queued messages
            self._conn.execute(
                "UPDATE messages SET status = 'expired' WHERE status = 'queued' AND expires_at < ?",
                (now,),
            )

            sub_channels = self._resolve_agent_channels(agent_uri, channels)
            chan_ph = ", ".join("?" for _ in sub_channels)

            if include_delivered:
                direct_clause = "(to_uri = ? AND status IN ('queued', 'delivered', 'accepted', 'completed'))"
                channel_clause = f"(to_uri IN ({chan_ph}) AND status IN ('queued', 'delivered', 'accepted', 'completed'))"
                params: List[Any] = [agent_uri, *sub_channels, agent_uri, since_seq]
            else:
                direct_clause = "(to_uri = ? AND status = 'queued')"
                channel_clause = (
                    f"(to_uri IN ({chan_ph}) AND status IN ('queued', 'delivered') AND expires_at >= ? "
                    "AND NOT EXISTS (SELECT 1 FROM channel_reads cr WHERE cr.msg_id = messages.msg_id AND cr.agent_uri = ?))"
                )
                params = [agent_uri, *sub_channels, now, agent_uri, agent_uri, since_seq]

            query = f"SELECT * FROM messages WHERE ({direct_clause} OR {channel_clause}) AND from_uri != ? AND seq > ?"
            if priority:
                query += " AND priority = ?"
                params.append(priority)
            if correlation_id:
                query += " AND correlation_id = ?"
                params.append(correlation_id)
            if trace_id:
                query += " AND trace_id = ?"
                params.append(trace_id)
            if msg_type:
                query += " AND msg_type = ?"
                params.append(msg_type)
            query += " ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, seq ASC LIMIT ?"
            params.append(limit)

            rows = self._conn.execute(query, params).fetchall()
            out: List[Dict[str, Any]] = []
            direct_delivered_ids: List[str] = []
            channel_read_ids: List[str] = []
            for r in rows:
                env = json.loads(r["envelope_json"])
                env["_meta"] = {
                    "seq": r["seq"],
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "expires_at": r["expires_at"],
                    "delivered_at": r["delivered_at"],
                    "acked_at": r["acked_at"],
                    "ack_reason": r["ack_reason"],
                }
                out.append(env)
                if mark_delivered:
                    if "/channel/" in (r["to_uri"] or ""):
                        channel_read_ids.append(r["msg_id"])
                    elif r["status"] == "queued":
                        direct_delivered_ids.append(r["msg_id"])

            if direct_delivered_ids:
                ph = ", ".join("?" for _ in direct_delivered_ids)
                self._conn.execute(
                    f"UPDATE messages SET status = 'delivered', delivered_at = ? WHERE msg_id IN ({ph})",
                    [now, *direct_delivered_ids],
                )
            if channel_read_ids:
                self._conn.executemany(
                    "INSERT OR IGNORE INTO channel_reads (msg_id, agent_uri, read_at) VALUES (?, ?, ?)",
                    [(mid, agent_uri, now) for mid in channel_read_ids],
                )
            # Update recipient's highest observed taint level when untrusted peer data enters its context
            if out and mark_delivered:
                rec_agent = self.get_agent(agent_uri)
                if rec_agent:
                    merged_taint = merge_taint(
                        rec_agent.get("last_taint"),
                        *((m.get("security") or {}).get("content_taint") for m in out),
                    )
                    if merged_taint != rec_agent.get("last_taint"):
                        self._conn.execute(
                            "UPDATE agents SET last_taint = ? WHERE agent_uri = ?",
                            (merged_taint, agent_uri),
                        )
            self._conn.commit()
            return out

    def wait_for_inbox(
        self,
        agent_uri: str,
        wait_timeout_sec: float = 0.0,
        include_delivered: bool = False,
        priority: Optional[str] = None,
        since_seq: int = 0,
        limit: int = 20,
        mark_delivered: bool = True,
        correlation_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        msg_type: Optional[str] = None,
        channels: Optional[List[str]] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> List[Dict[str, Any]]:
        """Event-driven + fast cross-process WAL wait for incoming AGP messages."""
        cap = 120.0
        deadline = epoch_now() + min(cap, max(0.0, float(wait_timeout_sec or 0.0)))
        step_wait = 0.05
        with self._inbox_cond:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    return []
                msgs = self.poll_inbox(
                    agent_uri=agent_uri,
                    include_delivered=include_delivered,
                    priority=priority,
                    since_seq=since_seq,
                    limit=limit,
                    mark_delivered=mark_delivered,
                    correlation_id=correlation_id,
                    trace_id=trace_id,
                    msg_type=msg_type,
                    channels=channels,
                )
                remaining = deadline - epoch_now()
                if msgs or remaining <= 0:
                    return msgs
                self._inbox_cond.wait(timeout=min(remaining, step_wait))
                step_wait = min(0.18, step_wait * 1.35)

    def count_unread(
        self,
        agent_uri: str,
        priorities: Optional[Tuple[str, ...]] = None,
        channels: Optional[List[str]] = None,
    ) -> int:
        now = epoch_now()
        with self._lock:
            sub_channels = self._resolve_agent_channels(agent_uri, channels)
            chan_ph = ", ".join("?" for _ in sub_channels)
            q = (
                "SELECT COUNT(*) AS cnt FROM messages WHERE "
                f"((to_uri = ? AND status = 'queued') OR "
                f"(to_uri IN ({chan_ph}) AND status IN ('queued', 'delivered') "
                "AND NOT EXISTS (SELECT 1 FROM channel_reads cr WHERE cr.msg_id = messages.msg_id AND cr.agent_uri = ?))) "
                "AND from_uri != ? AND expires_at >= ?"
            )
            params: List[Any] = [agent_uri, *sub_channels, agent_uri, agent_uri, now]
            if priorities:
                p_ph = ", ".join("?" for _ in priorities)
                q += f" AND priority IN ({p_ph})"
                params.extend(priorities)
            row = self._conn.execute(q, params).fetchone()
            return int(row["cnt"]) if row else 0

    def count_unread_urgent(self, agent_uri: str) -> int:
        return self.count_unread(agent_uri, priorities=("urgent", "high"))

    def ack_message(
        self,
        msg_id: str,
        status: str = "accepted",
        reason: str = "",
        actor_uri: str = "",
    ) -> Tuple[bool, Dict[str, Any]]:
        now = epoch_now()
        valid_statuses = {"accepted", "rejected", "completed", "failed", "cancelled"}
        if status not in valid_statuses:
            return False, {
                "ok": False,
                "error_code": "ERR_SCHEMA_INVALID",
                "error": f"status must be one of {sorted(valid_statuses)}",
            }
        with self._inbox_cond:
            row = self._conn.execute(
                "SELECT * FROM messages WHERE msg_id = ?", (msg_id,)
            ).fetchone()
            if not row:
                return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"Message {msg_id} not found"}
            self._conn.execute(
                "UPDATE messages SET status = ?, acked_at = ?, ack_reason = ? WHERE msg_id = ?",
                (status, now, reason, msg_id),
            )
            self._conn.commit()
            trace_id = row["trace_id"]
            to_uri = row["to_uri"]
            corr_id = row["correlation_id"]
            self._inbox_cond.notify_all()

        if corr_id:
            state_map = {
                "accepted": "TASK_STATE_WORKING",
                "rejected": "TASK_STATE_REJECTED",
                "completed": "TASK_STATE_COMPLETED",
                "failed": "TASK_STATE_FAILED",
                "cancelled": "TASK_STATE_CANCELED",
            }
            if status in state_map and self.get_task(corr_id):
                self.upsert_task(
                    task_id=corr_id,
                    state=state_map[status],
                    status_reason=reason,
                )

        if status in {"accepted", "completed"}:
            self.record_circuit_outcome(to_uri, True)
        elif status == "failed":
            self.record_circuit_outcome(to_uri, False)

        self.append_trace(
            trace_id,
            {
                "trace_id": trace_id,
                "msg_id": msg_id,
                "agent_uri": actor_uri or to_uri,
                "event": f"message.{status}",
                "reason": reason,
                "timestamp": utc_now_iso(),
            },
        )
        return True, {"ok": True, "msg_id": msg_id, "status": status, "reason": reason}

    def list_recent_messages(
        self,
        limit: int = 80,
        trace_id: Optional[str] = None,
        agent_uri: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        now = epoch_now()
        with self._lock:
            self._conn.execute(
                "UPDATE messages SET status = 'expired' WHERE status = 'queued' AND expires_at < ?",
                (now,),
            )
            self._conn.commit()
            query = "SELECT * FROM messages WHERE 1=1"
            params: List[Any] = []
            if trace_id:
                query += " AND trace_id = ?"
                params.append(trace_id)
            if agent_uri:
                query += " AND (from_uri = ? OR to_uri = ?)"
                params.extend([agent_uri, agent_uri])
            query += " ORDER BY seq DESC LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(query, params).fetchall()
            out = []
            for r in rows:
                env = json.loads(r["envelope_json"])
                env["_meta"] = {
                    "seq": r["seq"],
                    "status": r["status"],
                    "created_at": r["created_at"],
                    "expires_at": r["expires_at"],
                    "delivered_at": r["delivered_at"],
                    "acked_at": r["acked_at"],
                    "ack_reason": r["ack_reason"],
                }
                out.append(env)
            return out

    # ------------------------------------------------------------------
    # Workspace File Leases (Coding Agent Concurrency Guard)
    # ------------------------------------------------------------------

    def manage_lease(
        self,
        action: str,
        owner_uri: str,
        paths: Optional[List[str]] = None,
        mode: str = "exclusive_write",
        ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
    ) -> Tuple[bool, Dict[str, Any]]:
        now = epoch_now()
        paths = [_normalize_path(p) for p in (paths or []) if p and p.strip()]
        ttl_sec = max(10, min(int(ttl_sec or DEFAULT_LEASE_TTL_SEC), 1800))

        with self._lock:
            # Purge expired leases
            self._conn.execute("DELETE FROM leases WHERE expires_at < ?", (now,))

            if action == "list":
                rows = self._conn.execute("SELECT * FROM leases ORDER BY expires_at ASC").fetchall()
                self._conn.commit()
                return True, {
                    "ok": True,
                    "leases": [self._deserialize_lease(dict(r), now) for r in rows],
                }

            if action == "release":
                if paths:
                    for p in paths:
                        self._conn.execute(
                            "DELETE FROM leases WHERE owner_uri = ? AND path_pattern = ?",
                            (owner_uri, p),
                        )
                else:
                    self._conn.execute("DELETE FROM leases WHERE owner_uri = ?", (owner_uri,))
                self._conn.commit()
                return True, {"ok": True, "action": "released", "owner_uri": owner_uri, "paths": paths}

            if action in {"acquire", "renew"}:
                if not paths:
                    return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": "paths[] is required"}

                active_rows = self._conn.execute("SELECT * FROM leases").fetchall()
                conflicts = []
                for req_path in paths:
                    for r in active_rows:
                        if r["owner_uri"] == owner_uri:
                            continue
                        if not paths_overlap(req_path, r["path_pattern"]):
                            continue
                        if mode == "exclusive_write" or r["mode"] == "exclusive_write":
                            conflicts.append(
                                {
                                    "requested_path": req_path,
                                    "held_by": r["owner_uri"],
                                    "held_pattern": r["path_pattern"],
                                    "mode": r["mode"],
                                    "expires_in_sec": max(0, int(r["expires_at"] - now)),
                                }
                            )
                if conflicts:
                    self._conn.commit()
                    return False, {
                        "ok": False,
                        "error_code": "ERR_LEASE_CONFLICT",
                        "error": "One or more requested paths are locked by another AGY instance",
                        "conflicts": conflicts,
                    }

                granted = []
                for req_path in paths:
                    fencing_token = self._next_sequence("lease_fencing")
                    existing = self._conn.execute(
                        "SELECT lease_id FROM leases WHERE owner_uri = ? AND path_pattern = ?",
                        (owner_uri, req_path),
                    ).fetchone()
                    if existing:
                        lease_id = existing["lease_id"]
                        self._conn.execute(
                            "UPDATE leases SET mode = ?, expires_at = ?, fencing_token = ? WHERE lease_id = ?",
                            (mode, now + ttl_sec, fencing_token, lease_id),
                        )
                    else:
                        lease_id = f"lse_{uuid.uuid4().hex[:10]}"
                        self._conn.execute(
                            """
                            INSERT INTO leases (lease_id, owner_uri, path_pattern, mode, created_at, expires_at, fencing_token)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (lease_id, owner_uri, req_path, mode, now, now + ttl_sec, fencing_token),
                        )
                    granted.append(
                        {
                            "lease_id": lease_id,
                            "owner_uri": owner_uri,
                            "path_pattern": req_path,
                            "mode": mode,
                            "fencing_token": fencing_token,
                            "expires_in_sec": ttl_sec,
                        }
                    )
                self._conn.commit()
                return True, {"ok": True, "action": action, "leases": granted}

            if action == "check":
                if not paths:
                    return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": "paths[] is required"}
                active_rows = self._conn.execute("SELECT * FROM leases").fetchall()
                conflicts = []
                for req_path in paths:
                    for r in active_rows:
                        if r["owner_uri"] == owner_uri:
                            continue
                        if not paths_overlap(req_path, r["path_pattern"]):
                            continue
                        if mode == "exclusive_write" or r["mode"] == "exclusive_write":
                            conflicts.append(
                                {
                                    "requested_path": req_path,
                                    "held_by": r["owner_uri"],
                                    "held_pattern": r["path_pattern"],
                                    "mode": r["mode"],
                                    "fencing_token": int(r["fencing_token"] or 1),
                                    "expires_in_sec": max(0, int(r["expires_at"] - now)),
                                }
                            )
                # Advisory check against other online agents' declared owned_paths
                owned_advisories = []
                for ag in self.list_agents(status="online", exclude_uri=owner_uri):
                    for owned_pat in ag.get("owned_paths") or []:
                        for req_path in paths:
                            if paths_overlap(req_path, owned_pat):
                                owned_advisories.append(
                                    {
                                        "requested_path": req_path,
                                        "owner_uri": ag["agent_uri"],
                                        "display_name": ag.get("display_name", ""),
                                        "owned_pattern": owned_pat,
                                    }
                                )
                self._conn.commit()
                return True, {
                    "ok": True,
                    "action": "check",
                    "can_acquire": len(conflicts) == 0,
                    "conflicts": conflicts,
                    "owned_path_advisories": owned_advisories,
                }

            if action == "force_release":
                removed = 0
                if paths:
                    for p in paths:
                        cur = self._conn.execute("DELETE FROM leases WHERE path_pattern = ? OR lease_id = ?", (p, p))
                        removed += int(cur.rowcount or 0)
                else:
                    cur = self._conn.execute("DELETE FROM leases WHERE owner_uri = ?", (owner_uri,))
                    removed = int(cur.rowcount or 0)
                self._conn.commit()
                return True, {"ok": True, "action": "force_released", "removed_count": removed, "paths": paths}

            return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": f"Unknown lease action: {action}"}

    def check_write_conflict(
        self,
        actor_uri: str,
        target_path: str,
        fencing_token: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return conflicting lease dict if another AGY holds an exclusive lock OR caller presents a stale fencing_token."""
        if not target_path:
            return None
        now = epoch_now()
        norm = _normalize_path(target_path)
        with self._lock:
            self._conn.execute("DELETE FROM leases WHERE expires_at < ?", (now,))
            rows = self._conn.execute("SELECT * FROM leases").fetchall()
            self._conn.commit()
            for r in rows:
                if not paths_overlap(norm, r["path_pattern"]):
                    continue
                if r["owner_uri"] != actor_uri and r["mode"] == "exclusive_write":
                    return self._deserialize_lease(dict(r), now)
                if fencing_token is not None and int(fencing_token) < int(r["fencing_token"]):
                    stale = self._deserialize_lease(dict(r), now)
                    stale["error_code"] = "ERR_STALE_FENCING_TOKEN"
                    return stale
            if fencing_token is not None:
                # Check against global highest issued fencing_token if caller's lease already expired!
                seq_row = self._conn.execute(
                    "SELECT val FROM sequences WHERE name = 'lease_fencing'"
                ).fetchone()
                latest_tok = int(seq_row["val"]) if seq_row else 0
                if int(fencing_token) < latest_tok:
                    return {
                        "lease_id": "expired",
                        "owner_uri": "system",
                        "path_pattern": norm,
                        "mode": "exclusive_write",
                        "fencing_token": latest_tok,
                        "expires_in_sec": 0,
                        "error_code": "ERR_STALE_FENCING_TOKEN",
                    }
            return None

    def force_release_lease(self, lease_id: str) -> bool:
        """Allow human operator from the monitor UI to forcefully release a stuck lease."""
        with self._lock:
            cur = self._conn.execute("DELETE FROM leases WHERE lease_id = ?", (lease_id,))
            self._conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def _deserialize_lease(row: Dict[str, Any], now: float) -> Dict[str, Any]:
        return {
            "lease_id": row["lease_id"],
            "owner_uri": row["owner_uri"],
            "path_pattern": row["path_pattern"],
            "mode": row["mode"],
            "fencing_token": int(row.get("fencing_token") or 1),
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "expires_in_sec": max(0, int(row["expires_at"] - now)),
        }

    # ------------------------------------------------------------------
    # Pass-by-Reference Artifact Store
    # ------------------------------------------------------------------

    def put_artifact(
        self,
        kind: str,
        owner_uri: str,
        content: Optional[str] = None,
        source_path: Optional[str] = None,
        mime_type: str = "text/plain",
        redact: bool = True,
    ) -> Tuple[bool, Dict[str, Any]]:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        redacted_fields: List[str] = []
        if source_path:
            src = Path(source_path)
            if not src.exists() or not src.is_file():
                return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"File not found: {source_path}"}
            raw_bytes = src.read_bytes()
            is_text_mime = (
                mime_type.startswith("text/")
                or mime_type in {"application/json", "application/xml", "application/yaml", "application/x-yaml"}
            )
            if redact and is_text_mime and b"\x00" not in raw_bytes[:4096]:
                try:
                    decoded = raw_bytes.decode("utf-8")
                    clean_text, redacted_fields = redact_secrets(decoded, "artifact")
                    raw_bytes = clean_text.encode("utf-8")
                except UnicodeDecodeError:
                    pass
        elif content is not None:
            if redact:
                clean_text, redacted_fields = redact_secrets(content, "artifact")
            else:
                clean_text = content
            raw_bytes = clean_text.encode("utf-8")
        else:
            return False, {"ok": False, "error_code": "ERR_SCHEMA_INVALID", "error": "Provide content or source_path"}

        digest = hashlib.sha256(raw_bytes).hexdigest()
        artifact_id = f"art_{digest[:12]}"
        dest_path = self.artifacts_dir / f"{artifact_id}.bin"
        dest_path.write_bytes(raw_bytes)
        excerpt = raw_bytes[:400].decode("utf-8", errors="replace")
        now = epoch_now()

        with self._lock:
            self._conn.execute(
                """
                INSERT INTO artifacts (artifact_id, kind, owner_uri, mime_type, sha256, size_bytes, file_path, excerpt, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET created_at=excluded.created_at
                """,
                (artifact_id, kind, owner_uri, mime_type, digest, len(raw_bytes), str(dest_path), excerpt, now),
            )
            self._conn.commit()

        return True, {
            "ok": True,
            "redacted_fields": redacted_fields,
            "artifact": {
                "artifact_id": artifact_id,
                "kind": kind,
                "uri": f"agp-blob://local/{artifact_id}",
                "sha256": digest,
                "size_bytes": len(raw_bytes),
                "mime_type": mime_type,
                "excerpt": excerpt,
            },
        }

    def get_artifact(
        self,
        artifact_id: str,
        max_bytes: int = 65536,
        offset_bytes: int = 0,
    ) -> Tuple[bool, Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,)
            ).fetchone()
        if not row:
            return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": f"Artifact {artifact_id} not found"}
        p = Path(row["file_path"])
        if not p.exists():
            return False, {"ok": False, "error_code": "ERR_NOT_FOUND", "error": "Artifact blob missing on disk"}
        raw = p.read_bytes()
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != row["sha256"]:
            return False, {"ok": False, "error_code": "ERR_INTEGRITY", "error": "SHA-256 checksum mismatch"}
        start = max(0, min(int(offset_bytes or 0), len(raw)))
        limit_b = max(1, int(max_bytes or 65536))
        chunk = raw[start : start + limit_b]
        next_offset = start + len(chunk)
        has_more = next_offset < len(raw)
        truncated = has_more or start > 0
        text = chunk.decode("utf-8", errors="replace")
        return True, {
            "ok": True,
            "artifact_id": artifact_id,
            "kind": row["kind"],
            "mime_type": row["mime_type"],
            "sha256": actual_sha,
            "size_bytes": len(raw),
            "offset_bytes": start,
            "chunk_bytes": len(chunk),
            "next_offset": next_offset if has_more else None,
            "has_more": has_more,
            "truncated": truncated,
            "content": text,
        }

    def list_artifacts(
        self,
        limit: int = 30,
        kind: Optional[str] = None,
        owner_uri: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            q = "SELECT * FROM artifacts WHERE 1=1"
            params: List[Any] = []
            if kind:
                q += " AND kind = ?"
                params.append(kind)
            if owner_uri:
                q += " AND owner_uri = ?"
                params.append(owner_uri)
            q += " ORDER BY created_at DESC LIMIT ?"
            params.append(max(1, min(int(limit or 30), 200)))
            rows = self._conn.execute(q, params).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Observability Traces & Events
    # ------------------------------------------------------------------

    def append_trace(self, trace_id: str, entry: Dict[str, Any]) -> None:
        self.traces_dir.mkdir(parents=True, exist_ok=True)
        safe_id = "".join(c for c in trace_id if c.isalnum() or c in "-_")[:64] or "default"
        trace_file = self.traces_dir / f"{safe_id}.jsonl"
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        with self._lock:
            with trace_file.open("a", encoding="utf-8") as fh:
                fh.write(line)

    def inspect_trace(self, trace_id: str) -> Dict[str, Any]:
        safe_id = "".join(c for c in trace_id if c.isalnum() or c in "-_")[:64]
        trace_file = self.traces_dir / f"{safe_id}.jsonl"
        spans: List[Dict[str, Any]] = []
        if trace_file.exists():
            for line in trace_file.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    spans.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        messages = self.list_recent_messages(limit=100, trace_id=trace_id)
        return {
            "trace_id": trace_id,
            "span_count": len(spans),
            "spans": spans,
            "messages": list(reversed(messages)),
        }

    def list_traces(self, limit: int = 30) -> Dict[str, Any]:
        """List recent distributed traces with message counts and latest summaries."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT trace_id, COUNT(*) AS message_count,
                       MAX(created_at) AS last_at,
                       MIN(created_at) AS first_at,
                       GROUP_CONCAT(DISTINCT msg_type) AS msg_types
                FROM messages
                GROUP BY trace_id
                ORDER BY last_at DESC
                LIMIT ?
                """,
                (max(1, min(int(limit or 30), 100)),),
            ).fetchall()
            traces = []
            for r in rows:
                traces.append(
                    {
                        "trace_id": r["trace_id"],
                        "message_count": int(r["message_count"]),
                        "first_at": r["first_at"],
                        "last_at": r["last_at"],
                        "msg_types": (r["msg_types"] or "").split(",") if r["msg_types"] else [],
                    }
                )
            return {"ok": True, "count": len(traces), "traces": traces}

    def record_event(
        self,
        event_type: str,
        detail: Dict[str, Any],
        trace_id: Optional[str] = None,
        agent_uri: Optional[str] = None,
        conversation_id: Optional[str] = None,
    ) -> int:
        now = epoch_now()
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO events (trace_id, agent_uri, conversation_id, event_type, detail_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    agent_uri,
                    conversation_id,
                    event_type,
                    json.dumps(detail, ensure_ascii=False),
                    now,
                ),
            )
            # Keep event log bounded at 1500 rows
            self._conn.execute(
                "DELETE FROM events WHERE event_id NOT IN (SELECT event_id FROM events ORDER BY event_id DESC LIMIT 1500)"
            )
            self._conn.commit()
            return int(cur.lastrowid or 0)

    def list_events(self, limit: int = 80) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY event_id DESC LIMIT ?", (limit,)
            ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "event_id": r["event_id"],
                        "trace_id": r["trace_id"],
                        "agent_uri": r["agent_uri"],
                        "conversation_id": r["conversation_id"],
                        "event_type": r["event_type"],
                        "detail": json.loads(r["detail_json"]),
                        "created_at": r["created_at"],
                    }
                )
            return out

    # ------------------------------------------------------------------
    # Unified Durable Tasks (A2A v1.0 `Task` & MCP `io.modelcontextprotocol/tasks`)
    # ------------------------------------------------------------------

    def upsert_task(
        self,
        task_id: str,
        context_id: Optional[str] = None,
        protocol: Optional[str] = None,
        from_uri: Optional[str] = None,
        to_uri: Optional[str] = None,
        state: Optional[str] = None,
        title: Optional[str] = None,
        append_message: Optional[Dict[str, Any]] = None,
        append_artifacts: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        result: Optional[Dict[str, Any]] = None,
        elicitation: Optional[Dict[str, Any]] = None,
        status_reason: str = "",
    ) -> Dict[str, Any]:
        now = epoch_now()
        with self._inbox_cond:
            existing = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            ex = dict(existing) if existing else {}
            history: List[Dict[str, Any]] = json.loads(ex.get("history_json") or "[]")
            if append_message:
                history.append(append_message)
            artifacts: List[Dict[str, Any]] = json.loads(ex.get("artifacts_json") or "[]")
            if append_artifacts:
                artifacts.extend(append_artifacts)
            meta_obj: Dict[str, Any] = json.loads(ex.get("metadata_json") or "{}")
            if metadata:
                meta_obj.update(metadata)
            if status_reason:
                meta_obj["status_reason"] = status_reason

            res_json = (
                json.dumps(result, ensure_ascii=False)
                if result is not None
                else ex.get("result_json")
            )
            elic_json = (
                json.dumps(elicitation, ensure_ascii=False)
                if elicitation is not None
                else ex.get("elicitation_json")
            )
            new_state = state or ex.get("state") or "TASK_STATE_SUBMITTED"
            ctx = context_id or ex.get("context_id") or f"ctx_{uuid.uuid4().hex[:16]}"
            proto = protocol or ex.get("protocol") or "a2a"
            f_uri = from_uri or ex.get("from_uri") or "agp://local/unknown/caller"
            t_uri = to_uri or ex.get("to_uri") or "agp://local/channel/all"
            ttl_title = title if title is not None else (ex.get("title") or "")
            created = ex.get("created_at") or now

            self._conn.execute(
                """
                INSERT INTO tasks (
                    task_id, context_id, protocol, from_uri, to_uri, state, title,
                    history_json, artifacts_json, metadata_json, result_json, elicitation_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    context_id=excluded.context_id,
                    state=excluded.state,
                    title=excluded.title,
                    history_json=excluded.history_json,
                    artifacts_json=excluded.artifacts_json,
                    metadata_json=excluded.metadata_json,
                    result_json=excluded.result_json,
                    elicitation_json=excluded.elicitation_json,
                    updated_at=excluded.updated_at
                """,
                (
                    task_id,
                    ctx,
                    proto,
                    f_uri,
                    t_uri,
                    new_state,
                    ttl_title,
                    json.dumps(history, ensure_ascii=False),
                    json.dumps(artifacts, ensure_ascii=False),
                    json.dumps(meta_obj, ensure_ascii=False),
                    res_json,
                    elic_json,
                    created,
                    now,
                ),
            )
            self._conn.commit()
            self._inbox_cond.notify_all()
            row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            deserialized = self._deserialize_task(dict(row))

        self._emit_task_updated(deserialized)
        return deserialized

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            return self._deserialize_task(dict(row)) if row else None

    def wait_for_task(
        self,
        task_id: str,
        wait_timeout_sec: float = 25.0,
        cancel_event: Optional[threading.Event] = None,
    ) -> Optional[Dict[str, Any]]:
        """Wait until a durable task leaves SUBMITTED/WORKING or times out (cross-process safe)."""
        active_states = {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}
        deadline = epoch_now() + min(120.0, max(0.0, float(wait_timeout_sec or 0.0)))
        step_wait = 0.05
        with self._inbox_cond:
            while True:
                task = self.get_task(task_id)
                if not task:
                    return None
                cur_state = (task.get("status") or {}).get("state", "")
                remaining = deadline - epoch_now()
                if cur_state not in active_states or remaining <= 0 or (cancel_event and cancel_event.is_set()):
                    return task
                self._inbox_cond.wait(timeout=min(remaining, step_wait))
                step_wait = min(0.18, step_wait * 1.35)

    def list_tasks(
        self,
        context_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            q = "SELECT * FROM tasks WHERE 1=1"
            params: List[Any] = []
            if context_id:
                q += " AND context_id = ?"
                params.append(context_id)
            if state:
                norm_state = state.upper()
                if not norm_state.startswith("TASK_STATE_"):
                    norm_state = f"TASK_STATE_{norm_state}"
                q += " AND state = ?"
                params.append(norm_state)
            q += " ORDER BY updated_at DESC LIMIT ?"
            params.append(max(1, min(int(limit or 50), 200)))
            rows = self._conn.execute(q, params).fetchall()
            return [self._deserialize_task(dict(r)) for r in rows]

    def cancel_task(self, task_id: str, reason: str = "Task canceled") -> Tuple[bool, Dict[str, Any]]:
        existing = self.get_task(task_id)
        if not existing:
            return False, {"ok": False, "error": f"Task '{task_id}' not found"}
        cur_state = (existing.get("status") or {}).get("state", "")
        if cur_state in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED"}:
            return False, {"ok": False, "error": f"Task '{task_id}' is already in terminal state {cur_state}"}
        updated = self.upsert_task(task_id=task_id, state="TASK_STATE_CANCELED", status_reason=reason)
        self.send_message(
            {
                "trace_id": existing.get("contextId"),
                "correlation_id": task_id,
                "from": {"uri": existing.get("_from_uri") or "agp://local/system/broker"},
                "to": {"uri": existing.get("_to_uri") or "agp://local/channel/all"},
                "type": "task.cancel",
                "priority": "high",
                "summary": f"Cancel task {task_id}: {reason}"[:200],
                "payload": {"task_id": task_id, "reason": reason},
            }
        )
        return True, {"ok": True, "task": updated}

    def update_task_input(
        self, task_id: str, input_data: Dict[str, Any], actor_uri: str = "agp://local/mcp/client"
    ) -> Tuple[bool, Dict[str, Any]]:
        """Supply mid-flight input (`tasks/update` in MCP `io.modelcontextprotocol/tasks`)."""
        existing = self.get_task(task_id)
        if not existing:
            return False, {"ok": False, "error": f"Task '{task_id}' not found"}
        msg_id = f"upd_{uuid.uuid4().hex[:12]}"
        summary = str(input_data.get("summary") or input_data.get("answer") or "Supplied task input")[:200]
        self.send_message(
            {
                "msg_id": msg_id,
                "trace_id": existing.get("contextId"),
                "correlation_id": task_id,
                "from": {"uri": actor_uri},
                "to": {"uri": existing.get("_to_uri") or "agp://local/channel/all"},
                "type": "chat.note",
                "priority": "high",
                "summary": summary,
                "payload": {"input": input_data},
            }
        )
        updated = self.upsert_task(
            task_id=task_id,
            state="TASK_STATE_WORKING",
            elicitation={},
            append_message={
                "messageId": msg_id,
                "contextId": existing.get("contextId"),
                "taskId": task_id,
                "role": "ROLE_USER",
                "parts": [{"text": summary}, {"data": input_data}],
            },
        )
        return True, {"ok": True, "task": updated}

    def _sync_task_from_envelope(self, envelope: Dict[str, Any]) -> None:
        corr_id = envelope.get("correlation_id")
        if not corr_id:
            return
        existing = self.get_task(corr_id)
        if not existing:
            return
        msg_type = envelope.get("type", "")
        payload = envelope.get("payload") or {}
        summary = envelope.get("summary") or ""
        from_uri = (envelope.get("from") or {}).get("uri", "")

        a2a_artifacts: List[Dict[str, Any]] = []
        for art in envelope.get("artifacts") or []:
            a2a_artifacts.append(
                {
                    "artifactId": art.get("artifact_id") or f"art_{uuid.uuid4().hex[:10]}",
                    "name": art.get("kind") or "artifact",
                    "parts": [
                        {
                            "url": art.get("uri") or "",
                            "mediaType": art.get("mime_type") or "text/plain",
                            "metadata": {"sha256": art.get("sha256", ""), "size_bytes": art.get("size_bytes", 0)},
                        }
                    ],
                }
            )

        agent_msg = {
            "messageId": envelope.get("msg_id"),
            "contextId": existing.get("contextId"),
            "taskId": corr_id,
            "role": "ROLE_AGENT",
            "parts": [{"text": summary}, {"data": payload}],
            "metadata": {"from_uri": from_uri, "agp_type": msg_type},
        }

        if msg_type == "task.result":
            res_status = str(payload.get("status") or "success").lower()
            target_state = "TASK_STATE_FAILED" if res_status == "failed" else "TASK_STATE_COMPLETED"
            if payload:
                a2a_artifacts.append(
                    {
                        "artifactId": f"res_{uuid.uuid4().hex[:10]}",
                        "name": "structured_output",
                        "parts": [{"text": summary}, {"data": payload}],
                    }
                )
            self.upsert_task(
                task_id=corr_id,
                state=target_state,
                append_message=agent_msg,
                append_artifacts=a2a_artifacts,
                result={"summary": summary, "payload": payload, "artifacts": envelope.get("artifacts") or []},
            )
        elif msg_type == "task.clarify":
            self.upsert_task(
                task_id=corr_id,
                state="TASK_STATE_INPUT_REQUIRED",
                append_message=agent_msg,
                elicitation={
                    "mode": "form",
                    "message": summary,
                    "requestedSchema": payload.get("schema")
                    or {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]},
                },
            )
        elif msg_type == "hitl.escalate":
            self.upsert_task(
                task_id=corr_id,
                state="TASK_STATE_AUTH_REQUIRED",
                append_message=agent_msg,
                elicitation={"mode": "approval", "message": summary, "reason": payload.get("reason", "")},
            )
        elif msg_type == "task.progress":
            self.upsert_task(
                task_id=corr_id,
                state="TASK_STATE_WORKING",
                append_message=agent_msg,
            )

    @staticmethod
    def _deserialize_task(row: Dict[str, Any]) -> Dict[str, Any]:
        history = json.loads(row.get("history_json") or "[]")
        artifacts = json.loads(row.get("artifacts_json") or "[]")
        meta = json.loads(row.get("metadata_json") or "{}")
        result = json.loads(row["result_json"]) if row.get("result_json") else None
        elicitation = json.loads(row["elicitation_json"]) if row.get("elicitation_json") else None
        ts_iso = datetime.fromtimestamp(float(row["updated_at"]), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        status_block: Dict[str, Any] = {
            "state": row["state"],
            "timestamp": ts_iso,
        }
        if history:
            status_block["message"] = history[-1]
        out: Dict[str, Any] = {
            "id": row["task_id"],
            "contextId": row["context_id"],
            "status": status_block,
            "history": history,
            "artifacts": artifacts,
            "metadata": meta,
            "_protocol": row.get("protocol", "a2a"),
            "_from_uri": row.get("from_uri", ""),
            "_to_uri": row.get("to_uri", ""),
        }
        if result is not None:
            out["result"] = result
        if elicitation:
            out["elicitation"] = elicitation
        return out

    # ------------------------------------------------------------------
    # PushNotificationConfig Store (A2A v1.0 Webhooks)
    # ------------------------------------------------------------------

    def create_push_config(
        self,
        task_id: str,
        url: str,
        token: str = "",
        authentication: Optional[Dict[str, Any]] = None,
        config_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        cid = config_id or f"pnc_{uuid.uuid4().hex[:12]}"
        now = epoch_now()
        auth_obj = authentication or {}
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO push_configs (config_id, task_id, url, token, auth_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(config_id) DO UPDATE SET
                    url=excluded.url,
                    token=excluded.token,
                    auth_json=excluded.auth_json
                """,
                (cid, task_id, url, token, json.dumps(auth_obj, ensure_ascii=False), now),
            )
            self._conn.commit()
        return {
            "id": cid,
            "taskId": task_id,
            "pushNotificationConfig": {
                "id": cid,
                "url": url,
                "token": token,
                "authentication": auth_obj,
            },
            "url": url,
            "token": token,
            "authentication": auth_obj,
        }

    def get_push_config(self, task_id: str, config_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            if config_id:
                row = self._conn.execute(
                    "SELECT * FROM push_configs WHERE task_id = ? AND config_id = ?",
                    (task_id, config_id),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM push_configs WHERE task_id = ? ORDER BY created_at DESC LIMIT 1",
                    (task_id,),
                ).fetchone()
            return self._deserialize_push_config(dict(row)) if row else None

    def list_push_configs(self, task_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM push_configs WHERE task_id = ? ORDER BY created_at ASC",
                (task_id,),
            ).fetchall()
            return [self._deserialize_push_config(dict(r)) for r in rows]

    def delete_push_config(self, task_id: str, config_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM push_configs WHERE task_id = ? AND config_id = ?",
                (task_id, config_id),
            )
            self._conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def _deserialize_push_config(row: Dict[str, Any]) -> Dict[str, Any]:
        auth_obj = json.loads(row.get("auth_json") or "{}")
        return {
            "id": row["config_id"],
            "taskId": row["task_id"],
            "url": row["url"],
            "token": row.get("token") or "",
            "authentication": auth_obj,
            "pushNotificationConfig": {
                "id": row["config_id"],
                "url": row["url"],
                "token": row.get("token") or "",
                "authentication": auth_obj,
            },
        }
