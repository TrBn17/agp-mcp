"""Incremental live scanner for Antigravity JSONL transcripts across CLI, App 2.0, and IDE."""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import (
    SURFACE_BRAIN_DIRS,
    SURFACE_PRESENCE_DIRS,
    SURFACE_SUMMARIES_DBS,
    estimate_cost_usd,
)

USER_REQ_RE = re.compile(r"<USER_REQUEST>\s*([\s\S]*?)\s*</USER_REQUEST>")
MODEL_CHANGE_RE = re.compile(r"changed setting `Model Selection` from .*? to ([^.\n]+)")
WORKSPACE_PATH_RE = re.compile(r"([A-Za-z]:/(?:[^/\s\"'<>|\\;,)]+/)*[^/\s\"'<>|\\;,)]+)")
SUBAGENT_SENDER_RE = re.compile(r"sender=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?=\s|$)", re.I)
CREATED_SUBAGENT_RE = re.compile(r'Created the following subagent:\s*\{[\s\S]*?"conversationId"\s*:\s*"([^"]+)"')


def decode_workspace_uri(raw_uris: str) -> str:
    """Extract first local filesystem path from conversation_summaries.workspace_uris JSON."""
    if not raw_uris:
        return ""
    try:
        arr = json.loads(raw_uris)
        if isinstance(arr, list) and arr:
            u = str(arr[0])
            if u.startswith("file:///"):
                return urllib.parse.unquote(u[8:])
            if u.startswith("file://"):
                return urllib.parse.unquote(u[7:])
            return u
    except Exception:
        pass
    return ""


def parse_iso_epoch(ts: Optional[str]) -> Optional[float]:
    if not ts:
        return None
    try:
        clean = ts.strip()
        if clean.endswith("Z"):
            clean = clean[:-1] + "+00:00"
        return datetime.fromisoformat(clean).timestamp()
    except Exception:
        return None


def clean_user_prompt(raw: str) -> str:
    if not raw:
        return ""
    m = USER_REQ_RE.search(raw)
    text = m.group(1) if m else raw
    text = " ".join(text.strip().split())
    return text[:260] + ("..." if len(text) > 260 else "")


def extract_workspace_label(raw_args: Dict[str, Any]) -> str:
    """Safely extract a clean project root path from tool call arguments."""
    if not isinstance(raw_args, dict):
        return ""
    for key in ("Path", "path", "TargetFile", "Cwd", "cwd", "CommandLine"):
        val = raw_args.get(key)
        if not val or not isinstance(val, str):
            continue
        # Unwrap nested JSON string quotes / escaped backslashes from transcript args
        cleaned = val.strip().strip('"').strip("'")
        cleaned = cleaned.replace("\\\\", "/").replace("\\", "/")
        cleaned = re.sub(r"/[nrt]\b", " ", cleaned)
        for m in WORKSPACE_PATH_RE.finditer(cleaned):
            cand = m.group(1).rstrip("/.")
            low = cand.lower()
            if ".gemini" in low or "/windows/" in low or "/program files" in low:
                continue
            parts = [p for p in cand.split("/") if p]
            # Prefer "<drive>/projects/<project-name>" if present
            for idx, part in enumerate(parts):
                if part.lower() in {"projects", "repos", "workspace", "workspaces", "dev", "src"} and idx + 1 < len(parts):
                    return "/".join(parts[: idx + 2])
            p = Path(cand)
            parent = p.parent if p.suffix else p
            if parent.name and len(parent.name) > 1:
                return str(parent).replace("\\", "/")
    return ""


def summarize_tool_call(call: Dict[str, Any]) -> str:
    name = call.get("name", "tool")
    args = call.get("args") or {}
    if not isinstance(args, dict):
        return name
    for key in ("toolSummary", "Path", "path", "CommandLine", "query", "to_uri", "Role", "Prompt"):
        if key in args and args[key]:
            val = str(args[key]).strip().strip('"').strip("'")
            val = val.replace("\\n", " ").replace("\\\\", "\\")
            val = " ".join(val.split())
            if len(val) > 64:
                val = val[:61] + "..."
            return f"{name}({val})"
    return name


@dataclass
class TranscriptState:
    conversation_id: str
    surface: str
    transcript_path: Path
    byte_offset: int = 0
    mtime: float = 0.0
    file_size: int = 0

    model: str = "auto"
    started_at: Optional[float] = None
    last_activity_at: Optional[float] = None
    turn_started_at: Optional[float] = None

    input_tokens: int = 0
    cache_read_tokens: int = 0
    output_tokens: int = 0
    llm_calls: int = 0
    tool_call_count: int = 0
    step_count: int = 0

    first_user_prompt: str = ""
    last_user_prompt: str = ""
    last_assistant_reply: str = ""
    last_thinking: str = ""
    last_tool_summary: str = ""
    last_has_tool_calls: bool = False
    last_step_type: str = ""
    last_step_status: str = ""
    inferred_workspace: str = ""
    inferred_parent_id: str = ""
    spawned_children: List[str] = field(default_factory=list)
    spawned_child_meta: Dict[str, Dict[str, str]] = field(default_factory=dict)
    pending_subagent_invocations: List[Dict[str, str]] = field(default_factory=list)
    inferred_subagent_role: str = ""
    inferred_subagent_task: str = ""
    recent_steps: List[Dict[str, Any]] = field(default_factory=list)
    token_history: List[Dict[str, Any]] = field(default_factory=list)

    def to_summary(
        self,
        now: float,
        presence_active: bool = False,
        db_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta = db_meta or {}
        total_tokens = self.input_tokens + self.cache_read_tokens + self.output_tokens
        billable_input = self.input_tokens + self.cache_read_tokens
        cache_hit_pct = (
            round((self.cache_read_tokens / billable_input) * 100.0, 1)
            if billable_input > 0
            else 0.0
        )
        cost_usd = estimate_cost_usd(
            self.model, self.input_tokens, self.cache_read_tokens, self.output_tokens
        )
        start = self.started_at or self.mtime or now
        last = self.last_activity_at or self.mtime or start
        age_sec = max(0.0, now - last)

        cascade_status = str(meta.get("status") or "")
        killed = bool(meta.get("killed"))
        not_fully_idle = bool(meta.get("not_fully_idle"))
        # Trust conversation_summaries.db when present; fallback to transcript heuristic only if absent
        if db_meta is not None:
            parent_id = str(meta.get("parent_conversation_id") or "")
            nesting_depth = int(meta.get("nesting_depth") or (1 if parent_id else 0))
        else:
            parent_id = str(self.inferred_parent_id or "")
            nesting_depth = 1 if parent_id else 0
        if parent_id == self.conversation_id:
            parent_id = ""
            nesting_depth = 0
        agent_name = str(meta.get("agent_name") or "")
        db_title = str(meta.get("title") or "").strip()
        db_workspace = str(meta.get("workspace_path") or "").strip()

        # Infer live state accurately from Cascade DB status + last transcript step semantics.
        # Bound `not_fully_idle` and `presence_active` by recency so dead/abandoned sessions never stay zombie-online!
        recent_open = (not_fully_idle and age_sec < 1800) or (presence_active and age_sec < 1200)
        if killed:
            activity_state = "offline"
            status = "offline"
        elif cascade_status == "CASCADE_RUN_STATUS_RUNNING" and age_sec < 600:
            if self.last_step_status == "RUNNING" or (self.last_step_type == "PLANNER_RESPONSE" and self.last_has_tool_calls):
                activity_state = "running_tool"
            else:
                activity_state = "thinking"
            status = "online"
        elif self.last_step_status == "RUNNING" and age_sec < 180:
            activity_state = "running_tool"
            status = "online"
        elif self.last_step_type == "PLANNER_RESPONSE" and self.last_has_tool_calls and age_sec < 60:
            activity_state = "running_tool"
            status = "online"
        elif self.last_step_type in {"USER_INPUT", "GENERIC"} and age_sec < 45:
            activity_state = "thinking"
            status = "online"
        elif self.last_step_type == "PLANNER_RESPONSE" and not self.last_has_tool_calls and self.last_step_status == "DONE":
            if recent_open or age_sec < 300:
                activity_state = "idle"
                status = "online"
            else:
                activity_state = "offline"
                status = "offline"
        elif recent_open or age_sec < 180:
            activity_state = "idle"
            status = "online"
        else:
            activity_state = "offline"
            status = "offline"

        active_duration_sec = max(
            0.0,
            (now - start) if status == "online" and activity_state in {"thinking", "running_tool"} else (last - start),
        )
        turn_elapsed_sec = (
            max(0.0, (now - self.turn_started_at) if activity_state in {"thinking", "running_tool"} else (last - self.turn_started_at))
            if self.turn_started_at
            else active_duration_sec
        )
        burn_rate = (
            round(total_tokens / max(active_duration_sec / 60.0, 0.25), 1)
            if total_tokens > 0
            else 0.0
        )

        best_workspace = db_workspace or self.inferred_workspace
        best_title = db_title or self.inferred_subagent_role or agent_name or self.first_user_prompt or f"Session {self.conversation_id[:8]}"

        return {
            "conversation_id": self.conversation_id,
            "agent_uri": f"agp://local/{self.surface}/{self.conversation_id}",
            "surface": self.surface,
            "transcript_path": str(self.transcript_path),
            "model": self.model,
            "status": status,
            "activity_state": activity_state,
            "parent_conversation_id": parent_id,
            "nesting_depth": nesting_depth,
            "is_subagent": bool(parent_id or nesting_depth > 0),
            "agent_name": agent_name,
            "subagent_role": self.inferred_subagent_role,
            "subagent_task": self.inferred_subagent_task,
            "not_fully_idle": not_fully_idle,
            "killed": killed,
            "started_at": start,
            "last_activity_at": last,
            "turn_started_at": self.turn_started_at or start,
            "elapsed_sec": round(active_duration_sec, 1),
            "turn_elapsed_sec": round(turn_elapsed_sec, 1),
            "age_sec": round(age_sec, 1),
            "input_tokens": self.input_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": total_tokens,
            "cache_hit_pct": cache_hit_pct,
            "estimated_cost_usd": cost_usd,
            "tokens_per_min": burn_rate,
            "llm_calls": self.llm_calls,
            "tool_call_count": self.tool_call_count,
            "step_count": self.step_count,
            "title": best_title,
            "first_user_prompt": self.first_user_prompt,
            "last_user_prompt": self.last_user_prompt,
            "last_assistant_reply": self.last_assistant_reply,
            "last_thinking": self.last_thinking,
            "current_action": self.last_tool_summary or self.last_thinking or self.last_user_prompt,
            "inferred_workspace": best_workspace,
            "recent_steps": self.recent_steps[-25:],
            "token_history": self.token_history[-30:],
        }


class TranscriptScanner:
    """Scans Antigravity transcript.jsonl files incrementally with byte-offset caching."""

    def __init__(
        self,
        brain_dirs: Optional[Dict[str, Path]] = None,
        presence_dirs: Optional[Dict[str, Path]] = None,
        summaries_dbs: Optional[Dict[str, Path]] = None,
        max_sessions: int = 180,
    ) -> None:
        self.brain_dirs = brain_dirs if brain_dirs is not None else SURFACE_BRAIN_DIRS
        self.presence_dirs = presence_dirs if presence_dirs is not None else SURFACE_PRESENCE_DIRS
        self.summaries_dbs = summaries_dbs if summaries_dbs is not None else SURFACE_SUMMARIES_DBS
        self.max_sessions = max_sessions
        self._lock = threading.RLock()
        self._cache: Dict[str, TranscriptState] = {}
        self._last_db_meta: Dict[str, Dict[str, Any]] = {}

    def get_db_meta_map(self) -> Dict[str, Dict[str, Any]]:
        """Return most recent conversation_summaries metadata map (loaded if empty)."""
        if not self._last_db_meta:
            self._last_db_meta = self._load_summaries_metadata()
        return self._last_db_meta

    def _load_summaries_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Read-only lookup of title, parent_conversation_id, agent_name, workspace & status."""
        meta_by_id: Dict[str, Dict[str, Any]] = {}
        for surface, db_path in self.summaries_dbs.items():
            if not db_path.exists():
                continue
            try:
                uri = f"file:{db_path.as_posix()}?mode=ro"
                con = sqlite3.connect(uri, uri=True, timeout=1.0)
                con.row_factory = sqlite3.Row
                rows = con.execute(
                    """
                    SELECT conversation_id, title, workspace_uris, status,
                           agent_name, parent_conversation_id, nesting_depth,
                           not_fully_idle, killed
                    FROM conversation_summaries
                    ORDER BY last_modified_time DESC
                    LIMIT 500
                    """
                ).fetchall()
                con.close()
                for r in rows:
                    cid = r["conversation_id"]
                    meta_by_id[cid] = {
                        "surface": surface,
                        "title": r["title"] or "",
                        "workspace_path": decode_workspace_uri(r["workspace_uris"] or ""),
                        "status": r["status"] or "",
                        "agent_name": r["agent_name"] or "",
                        "parent_conversation_id": r["parent_conversation_id"] or "",
                        "nesting_depth": int(r["nesting_depth"] or 0),
                        "not_fully_idle": bool(r["not_fully_idle"]),
                        "killed": bool(r["killed"]),
                    }
            except Exception:
                continue
        return meta_by_id

    def _load_active_presence(self, now: float) -> Dict[str, bool]:
        active_presence: Dict[str, bool] = {}
        for _, pdir in self.presence_dirs.items():
            if not pdir.exists():
                continue
            try:
                for lock_file in pdir.glob("*.lock"):
                    try:
                        if (now - lock_file.stat().st_mtime) < 600:
                            active_presence[lock_file.stem] = True
                    except OSError:
                        pass
            except OSError:
                pass
        return active_presence

    def scan_metadata_only(self, max_age_hours: float = 24.0) -> Dict[str, Dict[str, Any]]:
        """Ultra-fast (<5ms) session discovery from conversation_summaries.db without parsing transcript JSONLs."""
        now = time.time()
        db_meta_map = self._load_summaries_metadata()
        self._last_db_meta = db_meta_map
        active_presence = self._load_active_presence(now)
        summaries: Dict[str, Dict[str, Any]] = {}
        for cid, meta in db_meta_map.items():
            surface = str(meta.get("surface") or "cli")
            killed = bool(meta.get("killed"))
            cascade_status = str(meta.get("status") or "")
            not_fully_idle = bool(meta.get("not_fully_idle"))
            pres = active_presence.get(cid, False)
            if killed:
                status, act = "offline", "offline"
            elif cascade_status == "CASCADE_RUN_STATUS_RUNNING":
                status, act = "online", "running_tool"
            elif not_fully_idle or pres:
                status, act = "online", "idle"
            else:
                status, act = "offline", "offline"
            parent_id = str(meta.get("parent_conversation_id") or "")
            if parent_id == cid:
                parent_id = ""
            nesting_depth = int(meta.get("nesting_depth") or (1 if parent_id else 0))
            summaries[cid] = {
                "conversation_id": cid,
                "agent_uri": f"agp://local/{surface}/{cid}",
                "surface": surface,
                "transcript_path": "",
                "model": "auto",
                "status": status,
                "activity_state": act,
                "parent_conversation_id": parent_id,
                "nesting_depth": nesting_depth,
                "is_subagent": bool(parent_id or nesting_depth > 0),
                "agent_name": str(meta.get("agent_name") or ""),
                "subagent_role": "",
                "subagent_task": "",
                "not_fully_idle": not_fully_idle,
                "killed": killed,
                "started_at": now,
                "last_activity_at": now if status == "online" else (now - 3600.0),
                "turn_started_at": now,
                "elapsed_sec": 0.0,
                "turn_elapsed_sec": 0.0,
                "age_sec": 0.0 if status == "online" else 3600.0,
                "input_tokens": 0,
                "cache_read_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "cache_hit_pct": 0.0,
                "estimated_cost_usd": 0.0,
                "tokens_per_min": 0.0,
                "llm_calls": 0,
                "tool_call_count": 0,
                "step_count": 0,
                "title": str(meta.get("title") or meta.get("agent_name") or f"Session {cid[:8]}"),
                "first_user_prompt": "",
                "last_user_prompt": "",
                "last_assistant_reply": "",
                "last_thinking": "",
                "current_action": "",
                "inferred_workspace": str(meta.get("workspace_path") or ""),
                "recent_steps": [],
                "token_history": [],
            }
        return summaries

    def scan(self, max_age_hours: float = 72.0) -> Dict[str, Dict[str, Any]]:
        """Discover and incrementally parse recent transcripts across all AGY surfaces."""
        now = time.time()
        cutoff = now - (max_age_hours * 3600.0)
        db_meta_map = self._load_summaries_metadata()
        self._last_db_meta = db_meta_map
        candidates: List[Tuple[int, float, str, str, Path]] = []

        for surface, brain_root in self.brain_dirs.items():
            if not brain_root.exists():
                continue
            try:
                for conv_dir in brain_root.iterdir():
                    if not conv_dir.is_dir():
                        continue
                    tpath = conv_dir / ".system_generated" / "logs" / "transcript.jsonl"
                    if not tpath.exists():
                        continue
                    try:
                        st = tpath.stat()
                    except OSError:
                        continue
                    if st.st_mtime < cutoff:
                        continue
                    cid = conv_dir.name
                    meta = db_meta_map.get(cid)
                    # Prioritize root sessions so heavy subagent bursts never crowd out root sessions!
                    is_sub = 1 if (meta and meta.get("parent_conversation_id")) else 0
                    candidates.append((is_sub, st.st_mtime, surface, cid, tpath))
            except OSError:
                continue

        # Sort roots first (newest first), then subagents (newest first), then keep top max_sessions
        roots_cands = sorted([c for c in candidates if c[0] == 0], key=lambda x: x[1], reverse=True)
        subs_cands = sorted([c for c in candidates if c[0] == 1], key=lambda x: x[1], reverse=True)
        root_budget = min(len(roots_cands), max(50, self.max_sessions // 2))
        sub_budget = max(0, self.max_sessions - root_budget)
        selected = roots_cands[:root_budget] + subs_cands[:sub_budget]

        active_presence = self._load_active_presence(now)

        summaries: Dict[str, Dict[str, Any]] = {}
        inferred_parent_from_parent_log: Dict[str, str] = {}
        inferred_child_meta: Dict[str, Dict[str, str]] = {}
        with self._lock:
            for _, mtime, surface, conv_id, tpath in selected:
                state = self._cache.get(conv_id)
                if state is None or state.transcript_path != tpath:
                    state = TranscriptState(
                        conversation_id=conv_id,
                        surface=surface,
                        transcript_path=tpath,
                    )
                    self._cache[conv_id] = state
                self._update_state(state, mtime)
                for child_cid in state.spawned_children:
                    inferred_parent_from_parent_log[child_cid] = conv_id
                for child_cid, cmeta in state.spawned_child_meta.items():
                    inferred_child_meta[child_cid] = cmeta

            for _, _, _, conv_id, _ in selected:
                state = self._cache[conv_id]
                if not state.inferred_parent_id and conv_id in inferred_parent_from_parent_log:
                    state.inferred_parent_id = inferred_parent_from_parent_log[conv_id]
                if conv_id in inferred_child_meta:
                    cmeta = inferred_child_meta[conv_id]
                    if cmeta.get("role"):
                        state.inferred_subagent_role = cmeta["role"]
                    if cmeta.get("task") and not state.inferred_subagent_task:
                        state.inferred_subagent_task = cmeta["task"]
                summaries[conv_id] = state.to_summary(
                    now,
                    presence_active=active_presence.get(conv_id, False),
                    db_meta=db_meta_map.get(conv_id),
                )
        return summaries

    def scan_single(
        self,
        transcript_path: str,
        surface: str = "cli",
        conv_id: str = "",
        presence_active: bool = False,
        db_meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Incrementally parse a specific transcript file by path without faking online presence."""
        p = Path(transcript_path)
        if not p.exists():
            return None
        try:
            mtime = p.stat().st_mtime
        except OSError:
            return None
        cid = conv_id or p.parent.parent.parent.name
        now = time.time()
        meta = db_meta if db_meta is not None else self._last_db_meta.get(cid)
        with self._lock:
            state = self._cache.get(cid)
            if state is None or state.transcript_path != p:
                state = TranscriptState(conversation_id=cid, surface=surface, transcript_path=p)
                self._cache[cid] = state
            self._update_state(state, mtime)
            return state.to_summary(now, presence_active=presence_active, db_meta=meta)

    def _update_state(self, state: TranscriptState, mtime: float) -> None:
        try:
            size = state.transcript_path.stat().st_size
        except OSError:
            return
        if size < state.byte_offset:
            # File was recreated/truncated
            state.byte_offset = 0
            state.input_tokens = 0
            state.cache_read_tokens = 0
            state.output_tokens = 0
            state.llm_calls = 0
            state.tool_call_count = 0
            state.step_count = 0
            state.recent_steps.clear()
            state.token_history.clear()

        if size == state.byte_offset and mtime == state.mtime:
            return

        state.mtime = mtime
        state.file_size = size

        try:
            with state.transcript_path.open("rb") as fh:
                fh.seek(state.byte_offset)
                chunk = fh.read()
                new_offset = fh.tell()
        except OSError:
            return

        if not chunk:
            return

        # Only consume up to the last newline so we never split a partial JSON line mid-write
        last_nl = chunk.rfind(b"\n")
        if last_nl == -1:
            return
        complete_bytes = chunk[: last_nl + 1]
        state.byte_offset += len(complete_bytes)

        prev_ts = state.last_activity_at
        for raw_line in complete_bytes.decode("utf-8", errors="replace").splitlines():
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                obj = json.loads(raw_line)
            except json.JSONDecodeError:
                continue

            state.step_count += 1
            step_idx = obj.get("step_index", state.step_count)
            step_type = obj.get("type", "")
            step_status = obj.get("status", "")
            created_epoch = parse_iso_epoch(obj.get("created_at"))
            if created_epoch:
                if state.started_at is None or created_epoch < state.started_at:
                    state.started_at = created_epoch
                if state.last_activity_at is None or created_epoch > state.last_activity_at:
                    state.last_activity_at = created_epoch

            step_latency_sec = (
                round(max(0.0, created_epoch - prev_ts), 1)
                if (created_epoch and prev_ts)
                else 0.0
            )
            if created_epoch:
                prev_ts = created_epoch

            state.last_step_type = step_type
            state.last_step_status = step_status

            if step_type == "SYSTEM_MESSAGE" and step_idx == 0 and not state.inferred_parent_id:
                raw_sys = obj.get("content", "")
                m_sender = SUBAGENT_SENDER_RE.search(raw_sys)
                if m_sender and m_sender.group(1) != state.conversation_id:
                    state.inferred_parent_id = m_sender.group(1)
                    if not state.first_user_prompt:
                        # Extract brief task line after content=
                        idx_c = raw_sys.find("content=")
                        if idx_c != -1:
                            brief = " ".join(raw_sys[idx_c + 8 :].strip().split())
                            state.first_user_prompt = brief[:180]
                            state.last_user_prompt = brief[:180]
            elif step_type == "USER_INPUT":
                raw_content = obj.get("content", "")
                prompt = clean_user_prompt(raw_content)
                if prompt:
                    if not state.first_user_prompt:
                        state.first_user_prompt = prompt
                    state.last_user_prompt = prompt
                if created_epoch:
                    state.turn_started_at = created_epoch
                m_change = MODEL_CHANGE_RE.search(raw_content)
                if m_change:
                    state.model = m_change.group(1).strip()
                state.recent_steps.append(
                    {
                        "step_index": step_idx,
                        "kind": "user",
                        "label": prompt[:140] or "User prompt",
                        "status": step_status,
                        "timestamp": obj.get("created_at"),
                        "duration_sec": step_latency_sec,
                        "tokens": 0,
                    }
                )
            elif step_type == "PLANNER_RESPONSE":
                state.llm_calls += 1
                if obj.get("model"):
                    state.model = obj["model"]
                in_tok = int(obj.get("input_tokens") or 0)
                cache_tok = int(obj.get("cache_read_tokens") or 0)
                out_tok = int(obj.get("output_tokens") or 0)
                state.input_tokens += in_tok
                state.cache_read_tokens += cache_tok
                state.output_tokens += out_tok

                thinking = " ".join((obj.get("thinking") or "").strip().split())
                if thinking:
                    state.last_thinking = thinking[:240] + ("..." if len(thinking) > 240 else "")

                raw_reply = (obj.get("content") or "").strip()
                if raw_reply:
                    state.last_assistant_reply = raw_reply[:3500]

                tool_calls = obj.get("tool_calls") or []
                tool_summaries = [summarize_tool_call(tc) for tc in tool_calls if isinstance(tc, dict)]
                state.last_has_tool_calls = bool(tool_summaries)
                if tool_summaries:
                    state.tool_call_count += len(tool_summaries)
                    state.last_tool_summary = ", ".join(tool_summaries[:2])
                    # Capture invoke_subagent Role & brief Prompt so child subagents get human names!
                    for tc in tool_calls:
                        if not isinstance(tc, dict):
                            continue
                        if tc.get("name") == "invoke_subagent":
                            tc_args = tc.get("args") or {}
                            raw_role = str(tc_args.get("Role") or "").strip().strip('"').strip("'")
                            raw_prompt = str(tc_args.get("Prompt") or "").strip().strip('"').strip("'")
                            raw_prompt = raw_prompt.replace("\\n", " ").replace("\\\\", "\\")
                            raw_prompt = " ".join(raw_prompt.split())
                            state.pending_subagent_invocations.append(
                                {"role": raw_role[:60], "task": raw_prompt[:140]}
                            )
                    # Try inferring workspace path cleanly from file read/write/command args
                    if not state.inferred_workspace:
                        for tc in tool_calls:
                            if not isinstance(tc, dict):
                                continue
                            ws = extract_workspace_label(tc.get("args") or {})
                            if ws:
                                state.inferred_workspace = ws
                                break
                else:
                    state.last_tool_summary = "Replied to user" if step_status == "DONE" else "Responding to user"

                step_total = in_tok + cache_tok + out_tok
                state.token_history.append(
                    {
                        "step": step_idx,
                        "input": in_tok,
                        "cache": cache_tok,
                        "output": out_tok,
                        "total": step_total,
                        "cumulative": state.input_tokens + state.cache_read_tokens + state.output_tokens,
                        "timestamp": obj.get("created_at"),
                    }
                )
                state.recent_steps.append(
                    {
                        "step_index": step_idx,
                        "kind": "reply" if (not tool_summaries and raw_reply) else "planner",
                        "label": state.last_tool_summary or "Model response",
                        "reply": raw_reply[:1200] if raw_reply else "",
                        "thinking": state.last_thinking[:140],
                        "status": step_status,
                        "timestamp": obj.get("created_at"),
                        "duration_sec": step_latency_sec,
                        "tokens": step_total,
                        "input_tokens": in_tok,
                        "cache_read_tokens": cache_tok,
                        "output_tokens": out_tok,
                    }
                )
            elif step_type == "GENERIC":
                raw_gen = obj.get("content", "")
                if "Created the following subagent:" in raw_gen:
                    for m_sub in CREATED_SUBAGENT_RE.finditer(raw_gen):
                        child_cid = m_sub.group(1)
                        if child_cid:
                            if child_cid not in state.spawned_children:
                                state.spawned_children.append(child_cid)
                            if state.pending_subagent_invocations:
                                inv = state.pending_subagent_invocations.pop(0)
                                state.spawned_child_meta[child_cid] = inv
                if step_status in {"RUNNING", "ERROR"}:
                    state.recent_steps.append(
                        {
                            "step_index": step_idx,
                            "kind": "tool_result",
                            "label": f"Step {step_idx} ({step_status})",
                            "status": step_status,
                            "timestamp": obj.get("created_at"),
                            "duration_sec": step_latency_sec,
                            "tokens": 0,
                        }
                    )

            if len(state.recent_steps) > 40:
                state.recent_steps = state.recent_steps[-40:]
            if len(state.token_history) > 50:
                state.token_history = state.token_history[-50:]
