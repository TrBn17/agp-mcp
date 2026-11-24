"""AGP v1.0 Local Broker, MCP Streamable HTTP Server, Hook Receiver & Live HTML Monitor."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import queue
import signal
import socket
import sqlite3
import sys
import threading
import time
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import __protocol_version__, __version__
from .a2a_gateway import A2AGateway
from .config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    MAX_STOP_CONTINUATIONS,
    PID_FILE,
    WEB_DIR,
    ensure_data_dirs,
)
from .identity import IdentityRegistry, build_conversations_and_graph
from .listen import build_listen_command, drain_unified_inbox
from .mcp_protocol import MCPHandler
from .security import format_team_injection, format_untrusted_injection
from .storage import AGPStore, epoch_now, utc_now_iso
from .teams import TeamManager
from .transcripts import TranscriptScanner, clean_user_prompt
from .worker import run_agent_wakeup, spawn_detached_worker


class SSEHub:
    """Thread-safe Server-Sent Events fan-out hub for real-time HTML monitor updates."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: List[queue.Queue[str]] = []

    def subscribe(self) -> queue.Queue[str]:
        q: queue.Queue[str] = queue.Queue(maxsize=64)
        with self._lock:
            self._clients.append(q)
        return q

    def unsubscribe(self, q: queue.Queue[str]) -> None:
        with self._lock:
            if q in self._clients:
                self._clients.remove(q)

    def broadcast(self, event_name: str, payload: Dict[str, Any]) -> None:
        frame = f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
        with self._lock:
            dead = []
            for q in self._clients:
                try:
                    q.put_nowait(frame)
                except queue.Full:
                    try:
                        q.get_nowait()
                        q.put_nowait(frame)
                    except queue.Empty:
                        dead.append(q)
            for q in dead:
                self._clients.remove(q)


class AGPBrokerRuntime:
    """Shared runtime state combining SQLite store, transcript scanner, MCP handler, and SSE hub."""

    def __init__(
        self,
        store: Optional[AGPStore] = None,
        scanner: Optional[TranscriptScanner] = None,
        teams: Optional[TeamManager] = None,
        web_dir: Path = WEB_DIR,
        agent_runner_fn: Optional[Any] = None,
        detached_wakeup: bool = False,
        default_agent_uri: Optional[str] = None,
        fast_mode: bool = False,
    ) -> None:
        self.store = store or AGPStore()
        self.scanner = scanner or TranscriptScanner()
        self.web_dir = web_dir
        self.fast_mode = bool(fast_mode)
        self.sse = SSEHub()
        self.started_at = epoch_now()
        self._stop_event = threading.Event()
        self._last_registry: Optional[IdentityRegistry] = None
        self._has_custom_runner = agent_runner_fn is not None
        self._agent_runner_fn = agent_runner_fn or TeamManager._run_agy_cli_turn
        self._detached_wakeup = bool(detached_wakeup and not self._has_custom_runner)
        self._wake_meta_lock = threading.Lock()
        self._agent_wake_locks: Dict[str, threading.Lock] = {}
        self._agent_wake_pending: Dict[str, bool] = {}
        self.teams = teams or TeamManager(
            notify_fn=self.notify,
            hook_deliver_fn=self._bridge_team_msg_to_hook,
            detached_wakeup=self._detached_wakeup,
            locks_dir=self.store.locks_dir,
        )
        if teams is not None and teams._hook_deliver_fn is None:
            teams._hook_deliver_fn = self._bridge_team_msg_to_hook
        self.mcp = MCPHandler(
            store=self.store,
            snapshot_fn=self.build_snapshot,
            notify_fn=self.notify,
            teams=self.teams,
            resolve_target_fn=self.resolve_target_uri,
            wakeup_agent_fn=self.wakeup_agent,
            default_agent_uri=default_agent_uri,
        )
        self.a2a = A2AGateway(
            store=self.store,
            notify_fn=self.notify,
            resolve_target_fn=self.resolve_target_uri,
            wakeup_agent_fn=self.wakeup_agent,
        )
        self._bg_thread: Optional[threading.Thread] = None

    def _bridge_team_msg_to_hook(
        self,
        cfg: Dict[str, Any],
        rec_info: Dict[str, Any],
        msg: Dict[str, Any],
        wake_mode: str = "auto",
    ) -> Dict[str, Any]:
        """Mirror `team_send` / `team_join` messages into SQLite AGP inbox for live sessions & hooks."""
        cid = str(rec_info.get("conversation_id") or "").strip()
        if not cid:
            return {"bridged": False, "will_auto_receive": False, "mode": "team_mailbox_only"}
        surface = str(rec_info.get("surface") or "cli").strip() or "cli"
        to_uri = f"agp://local/{surface}/{cid}"
        team_slug = str(cfg.get("team") or msg.get("team") or "")
        sender_handle = str(msg.get("from") or "lead")
        sender_mem = (cfg.get("members") or {}).get(sender_handle) or {}
        sender_cid = str(sender_mem.get("conversation_id") or "").strip()
        sender_surf = str(sender_mem.get("surface") or "cli").strip() or "cli"
        from_uri = (
            f"agp://local/{sender_surf}/{sender_cid}"
            if sender_cid
            else f"agp://local/team/{team_slug}/{sender_handle}"
        )
        rec_handle = str(rec_info.get("name") or "teammate")
        reply_hint = (
            f"[Team #{team_slug} · from @{sender_handle} -> @{rec_handle}] "
            f"Reply via `team_send(team='{team_slug}', from_name='{rec_handle}', "
            f"to='{sender_handle}', message='...')`."
        )
        ok, res = self.store.send_message(
            {
                "from": {"uri": from_uri, "role": sender_mem.get("role") or sender_handle},
                "to": {"uri": to_uri},
                "type": "chat.note",
                "priority": "high",
                "summary": f"[#{team_slug} @{sender_handle}] {str(msg.get('text') or '')[:180]}",
                "payload": {
                    "_team_msg_id": msg.get("id", ""),
                    "team": team_slug,
                    "from_handle": sender_handle,
                    "to_handle": rec_handle,
                    "message": msg.get("text", ""),
                    "reply_instructions": reply_hint,
                },
                "idempotency_key": f"team_bridge_{msg.get('id', '')}_{cid}",
            }
        )
        if not ok:
            return {"bridged": False, "will_auto_receive": False, "error": res.get("error", "")}
        self.notify("agp_message", res.get("envelope", {}))
        delivery = self.mcp._assess_delivery(to_uri, wake=(wake_mode != "none"), wake_mode=wake_mode)
        delivery["bridged"] = True
        delivery["to_uri"] = to_uri
        return delivery

    def wakeup_agent(self, target_uri: str, wake_mode: str = "auto") -> bool:
        """Unified auto-wakeup dispatcher for `auto_cli` agents (or explicit `wake_mode='force_cli'`)."""
        uri = self.resolve_target_uri(target_uri)
        if not uri or "/channel/" in uri or wake_mode == "none":
            return False
        agent = self.store.get_agent(uri)
        force = wake_mode == "force_cli"
        if not agent:
            if not force:
                return False
            cid = uri.rsplit("/", 1)[-1] if uri.startswith("agp://local/") else ""
            if not cid:
                return False
            agent = self.store.upsert_agent(
                {"agent_uri": uri, "conversation_id": cid, "status": "online", "wakeup_mode": "hook"}
            )
        if not force and agent.get("wakeup_mode") != "auto_cli":
            return False

        if self._detached_wakeup:
            return spawn_detached_worker(
                agent_uri=uri,
                data_dir=self.teams.data_dir,
                force=force,
            )

        with self._wake_meta_lock:
            self._agent_wake_pending[uri] = True
            if uri not in self._agent_wake_locks:
                self._agent_wake_locks[uri] = threading.Lock()
        threading.Thread(
            target=self._agent_wakeup_worker,
            args=(uri, force),
            name=f"agp-auto-wake-{uri.split('/')[-1][:12]}",
            daemon=True,
        ).start()
        return True

    def _agent_wakeup_worker(self, agent_uri: str, force: bool = False) -> None:
        with self._wake_meta_lock:
            wlock = self._agent_wake_locks.setdefault(agent_uri, threading.Lock())
        if not wlock.acquire(blocking=False):
            return
        try:
            run_agent_wakeup(
                store=self.store,
                teams_dir=self.teams.teams_dir,
                agent_uri=agent_uri,
                runner_fn=self._agent_runner_fn,
                teams=self.teams,
                force=force,
            )
            if not self._stop_event.is_set() and not self.store.is_closed:
                ag = self.store.get_agent(agent_uri)
                if ag:
                    self.notify("agent_updated", ag)
        except sqlite3.ProgrammingError:
            return
        finally:
            wlock.release()

    def resolve_target_uri(self, target: str) -> str:
        t = (target or "").strip()
        if not t or t.startswith("agp://"):
            return t
        if t in {"all", "@all"}:
            return "agp://local/channel/all"
        if t.startswith("channel:") or (t.startswith("#") and len(t) > 1):
            return self.store._normalize_target_uri(t)
        # Fast-path 1: direct SQLite lookup by conversation_id (avoids heavy transcript scan!)
        ag = self.store.get_agent_by_conversation(t)
        if ag:
            return ag["agent_uri"]
        # Fast-path 2: already-built identity registry
        reg = self._last_registry
        if reg:
            resolved = reg.resolve_target_to_uri(t)
            if resolved:
                return resolved
        # Fallback: build snapshot (lightweight in fast_mode) to resolve @handle or transcript session
        self.build_snapshot()
        reg = self._last_registry
        if reg:
            resolved = reg.resolve_target_to_uri(t)
            if resolved:
                return resolved
        return t

    def notify(self, event_name: str, data: Dict[str, Any]) -> None:
        self.sse.broadcast(event_name, {"event": event_name, "timestamp": utc_now_iso(), "data": data})

    def start_background_scanner(self, interval_sec: float = 1.5) -> None:
        if self._bg_thread and self._bg_thread.is_alive():
            return
        self._bg_thread = threading.Thread(
            target=self._scanner_loop,
            args=(interval_sec,),
            name="agp-transcript-scanner",
            daemon=True,
        )
        self._bg_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._wake_meta_lock:
            locks = list(self._agent_wake_locks.values())
        for lk in locks:
            if lk.acquire(timeout=0.5):
                lk.release()
        self.store.close()

    def _scanner_loop(self, interval_sec: float) -> None:
        last_digest = ""
        last_data_ver = -1
        while not self._stop_event.wait(interval_sec):
            try:
                cur_data_ver = self.store.get_data_version()
                snap = self.build_snapshot()
                # Compact fingerprint covering external SQLite writes, teams, tasks, acks & leases
                fingerprint = json.dumps(
                    {
                        "dv": cur_data_ver,
                        "kpis": snap["kpis"],
                        "sessions": [
                            (
                                s["conversation_id"],
                                s.get("identity", {}).get("full_label", ""),
                                s["status"],
                                s["activity_state"],
                                s["total_tokens"],
                                s["current_action"],
                                s["step_count"],
                            )
                            for s in snap["sessions"][:24]
                        ],
                        "msgs": [
                            (m.get("msg_id"), (m.get("_meta") or {}).get("status"))
                            for m in snap["messages"][:15]
                        ],
                        "teams": [
                            (
                                t.get("team"),
                                len(t.get("messages") or []),
                                [(tk.get("id"), tk.get("status"), tk.get("owner")) for tk in (t.get("tasks") or [])],
                                [(mb.get("name"), mb.get("status")) for mb in (t.get("members") or [])],
                            )
                            for t in snap.get("teams") or []
                        ],
                        "tasks": [
                            (tk.get("id"), (tk.get("status") or {}).get("state"))
                            for tk in (snap.get("tasks") or [])[:15]
                        ],
                        "conv_cnt": len(snap.get("conversations", [])),
                        "leases": [(l.get("lease_id"), l.get("fencing_token")) for l in snap["leases"]],
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                )
                if fingerprint != last_digest or cur_data_ver != last_data_ver:
                    last_digest = fingerprint
                    last_data_ver = cur_data_ver
                    self.sse.broadcast("snapshot", snap)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Unified Live Snapshot (Merges Hook Registry + Live Transcripts)
    # ------------------------------------------------------------------

    def build_snapshot(self) -> Dict[str, Any]:
        now = epoch_now()
        transcripts = self.scanner.scan_metadata_only() if self.fast_mode else self.scanner.scan()
        db_meta_map = self.scanner.get_db_meta_map()
        registered = self.store.list_agents()
        reg_by_conv: Dict[str, Dict[str, Any]] = {}
        reg_by_uri: Dict[str, Dict[str, Any]] = {}
        for agent in registered:
            reg_by_uri[agent["agent_uri"]] = agent
            if agent.get("conversation_id"):
                reg_by_conv[agent["conversation_id"]] = agent

        merged: List[Dict[str, Any]] = []
        seen_conv: set[str] = set()

        # 1. Start with hook-registered agents enriched with transcript token telemetry
        for agent in registered:
            cid = agent.get("conversation_id") or ""
            t_info = transcripts.get(cid)
            if not t_info and agent.get("transcript_path"):
                t_info = self.scanner.scan_single(
                    agent["transcript_path"],
                    surface=agent.get("surface", "cli"),
                    conv_id=cid,
                    presence_active=False,
                    db_meta=db_meta_map.get(cid),
                )
            if cid:
                seen_conv.add(cid)

            item = self._merge_agent_and_transcript(agent, t_info, now)
            merged.append(item)

        # 2. Add transcript-discovered sessions that haven't fired a hook yet
        for cid, t_info in transcripts.items():
            if cid in seen_conv:
                continue
            item = self._merge_agent_and_transcript(None, t_info, now)
            merged.append(item)

        # Sort: active working first, then online idle, then most recently active
        state_rank = {
            "running_tool": 0,
            "thinking": 1,
            "communicating": 2,
            "waiting_hitl": 3,
            "idle": 4,
            "offline": 5,
        }
        merged.sort(
            key=lambda s: (
                0 if s["status"] == "online" else 1,
                state_rank.get(s["activity_state"], 9),
                -(s.get("last_activity_at") or 0),
            )
        )

        # Auto-sync active Antigravity `/teamwork` hierarchies and enrich live member statuses!
        self.teams.sync_teamwork_sessions(merged)
        teams_snap = self.teams.list_teams_snapshot(
            live_sessions_by_cid={s["conversation_id"]: s for s in merged if s.get("conversation_id")}
        )
        registry = IdentityRegistry(
            sessions=merged,
            teams=teams_snap,
            db_agents=registered,
            db_summaries=db_meta_map,
        )
        self._last_registry = registry

        for s in merged:
            ident = registry.get_by_session(s)
            s["identity"] = ident
            s["canonical_name"] = ident["name"]
            s["task_label"] = ident["task_label"]
            s["full_label"] = ident["full_label"]
            s["handle"] = ident["handle"]
            s["avatar"] = ident["avatar"]
            s["color"] = ident["color"]
            s["team"] = ident["team"]
            s["team_handle"] = ident["team_handle"]

        # Build parent -> subagents hierarchy & tree rollups
        by_cid: Dict[str, Dict[str, Any]] = {s["conversation_id"]: s for s in merged}
        children_by_parent: Dict[str, List[Dict[str, Any]]] = {}
        for s in merged:
            s.setdefault("parent_conversation_id", "")
            s.setdefault("nesting_depth", 0)
            s.setdefault("is_subagent", bool(s["parent_conversation_id"]))
            s.setdefault("agent_name", "")
            s.setdefault("killed", False)
            s["subagents"] = []
            s["subagent_count"] = 0
            s["active_subagent_count"] = 0
            s["tree_total_tokens"] = s["total_tokens"]
            s["tree_estimated_cost_usd"] = s["estimated_cost_usd"]
            pid = s["parent_conversation_id"]
            if pid and pid in by_cid and pid != s["conversation_id"]:
                children_by_parent.setdefault(pid, []).append(s)

        def _subtree_totals(node: Dict[str, Any], visited: set[str]) -> Tuple[int, float, int]:
            cid = node["conversation_id"]
            if cid in visited:
                return node["total_tokens"], node["estimated_cost_usd"], 0
            visited.add(cid)
            kids = children_by_parent.get(cid, [])
            kids.sort(
                key=lambda c: (
                    0 if c["activity_state"] in {"running_tool", "thinking", "communicating"} else 1,
                    0 if c["status"] == "online" else 1,
                    -(c.get("last_activity_at") or 0),
                )
            )
            tot_tok = int(node["total_tokens"])
            tot_cost = float(node["estimated_cost_usd"])
            active_kids = 0
            compact_kids: List[Dict[str, Any]] = []
            for k in kids:
                ktok, kcost, kactive = _subtree_totals(k, visited)
                tot_tok += ktok
                tot_cost += kcost
                if k["activity_state"] in {"running_tool", "thinking", "communicating"}:
                    active_kids += 1
                active_kids += kactive
                k_ident = k.get("identity") or registry.get_by_session(k)
                compact_kids.append(
                    {
                        "conversation_id": k["conversation_id"],
                        "agent_uri": k["agent_uri"],
                        "agent_name": k_ident["name"],
                        "task_label": k_ident["task_label"],
                        "full_label": k_ident["full_label"],
                        "handle": k_ident["handle"],
                        "avatar": k_ident["avatar"],
                        "color": k_ident["color"],
                        "title": k.get("title") or k_ident["full_label"],
                        "model": k.get("model", "auto"),
                        "status": k["status"],
                        "activity_state": k["activity_state"],
                        "current_action": k.get("current_action", ""),
                        "last_assistant_reply": k.get("last_assistant_reply", ""),
                        "total_tokens": k["total_tokens"],
                        "estimated_cost_usd": k["estimated_cost_usd"],
                        "elapsed_sec": k.get("elapsed_sec", 0.0),
                        "started_at": k.get("started_at"),
                        "last_activity_at": k.get("last_activity_at"),
                        "killed": bool(k.get("killed")),
                        "nesting_depth": int(k.get("nesting_depth", 1)),
                    }
                )
            node["subagents"] = compact_kids
            node["subagent_count"] = len(compact_kids)
            node["active_subagent_count"] = active_kids
            node["tree_total_tokens"] = tot_tok
            node["tree_estimated_cost_usd"] = round(tot_cost, 6)
            # Keep parent marked online while any subagent is actively working
            if active_kids > 0 and node["status"] != "online":
                node["status"] = "online"
                if node["activity_state"] == "offline":
                    node["activity_state"] = "idle"
                node["identity"]["status"] = node["status"]
                node["identity"]["activity_state"] = node["activity_state"]
            return tot_tok, tot_cost, active_kids

        for s in merged:
            if not s["parent_conversation_id"] or s["parent_conversation_id"] not in by_cid:
                _subtree_totals(s, set())

        # Re-sort after active_subagent_count promotion
        merged.sort(
            key=lambda s: (
                0 if s["status"] == "online" else 1,
                0 if s["active_subagent_count"] > 0 else 1,
                state_rank.get(s["activity_state"], 9),
                -(s.get("last_activity_at") or 0),
            )
        )

        # Compute global KPIs (avoiding double-counting tokens across parent/child)
        root_sessions = [s for s in merged if not s["is_subagent"] or s["parent_conversation_id"] not in by_cid]
        subagent_sessions = [s for s in merged if s["is_subagent"]]
        active_roots = [s for s in root_sessions if s["status"] == "online"]
        working_roots = [
            s for s in active_roots
            if s["activity_state"] in {"running_tool", "thinking", "communicating"} or s["active_subagent_count"] > 0
        ]
        working_subagents = [
            s for s in subagent_sessions if s["activity_state"] in {"running_tool", "thinking", "communicating"}
        ]
        today_cutoff = now - 86400.0
        today_sessions = [s for s in merged if (s.get("last_activity_at") or 0) >= today_cutoff]

        total_input = sum(s["input_tokens"] for s in today_sessions)
        total_cache = sum(s["cache_read_tokens"] for s in today_sessions)
        total_output = sum(s["output_tokens"] for s in today_sessions)
        total_tokens = total_input + total_cache + total_output
        total_cost = round(sum(s["estimated_cost_usd"] for s in today_sessions), 4)
        avg_cache_pct = (
            round((total_cache / (total_input + total_cache)) * 100.0, 1)
            if (total_input + total_cache) > 0
            else 0.0
        )

        messages = self.store.list_recent_messages(limit=120)
        for m in messages:
            from_obj = m.get("from") or {}
            to_obj = m.get("to") or {}
            m["_resolved"] = {
                "from": registry.resolve_uri(from_obj.get("uri", ""), from_obj.get("role", "")),
                "to": registry.resolve_uri(to_obj.get("uri", "")),
            }

        for t in teams_snap:
            tname = t.get("team", "")
            for mem in t.get("members") or []:
                mem["identity"] = registry.resolve_team_member(tname, mem.get("name", ""), mem)
            for tm in t.get("messages") or []:
                tm["_resolved"] = {
                    "from": registry.resolve_team_member(tname, tm.get("from", "")),
                    "to": registry.resolve_team_member(tname, tm.get("to", "")),
                }

        _, lease_res = self.store.manage_lease("list", owner_uri="agp://local/system/monitor")
        leases = lease_res.get("leases", [])
        for lse in leases:
            lse["owner_identity"] = registry.resolve_uri(lse.get("owner_uri", ""))

        conversations, graph = build_conversations_and_graph(merged, messages, teams_snap, registry)

        # Attach per-session peer communication summary so Fleet cards show live AI-to-AI links!
        peer_summary_by_cid: Dict[str, List[Dict[str, Any]]] = {}
        for ch in conversations:
            if ch["kind"] == "direct" and len(ch["participants"]) == 2:
                p1, p2 = ch["participants"]
                for me, peer in ((p1, p2), (p2, p1)):
                    mcid = me.get("conversation_id")
                    if mcid:
                        peer_summary_by_cid.setdefault(mcid, []).append(
                            {
                                "channel_id": ch["channel_id"],
                                "peer_name": peer["name"],
                                "peer_full_label": peer["full_label"],
                                "peer_avatar": peer["avatar"],
                                "peer_color": peer["color"],
                                "count": ch["message_count"],
                                "unread": ch["unread_count"],
                                "last_at": ch["last_message_at"],
                            }
                        )
        for s in merged:
            peers = peer_summary_by_cid.get(s["conversation_id"], [])
            peers.sort(key=lambda x: x["last_at"], reverse=True)
            s["peer_comms"] = peers[:4]

        circuits = self.store.list_circuit_breakers()
        events = self.store.list_events(limit=60)
        artifacts = self.store.list_artifacts(limit=25)
        durable_tasks = self.store.list_tasks(limit=40)
        team_msg_total = sum(len(t.get("messages", [])) for t in teams_snap)
        pending_msgs = sum(1 for m in messages if (m.get("_meta") or {}).get("status") == "queued")

        return {
            "version": __version__,
            "protocol_version": __protocol_version__,
            "server_time": now,
            "server_time_iso": utc_now_iso(),
            "broker_uptime_sec": round(now - self.started_at, 1),
            "kpis": {
                "online_agents": len(active_roots),
                "working_agents": len(working_roots),
                "total_subagents": len(subagent_sessions),
                "working_subagents": len(working_subagents),
                "total_sessions_tracked": len(root_sessions),
                "today_sessions": len(today_sessions),
                "total_tokens_24h": total_tokens,
                "input_tokens_24h": total_input,
                "cache_read_tokens_24h": total_cache,
                "output_tokens_24h": total_output,
                "cache_hit_pct_24h": avg_cache_pct,
                "estimated_cost_usd_24h": total_cost,
                "messages_count": len(messages) + team_msg_total,
                "pending_messages": pending_msgs,
                "conversation_count": len(conversations),
                "team_count": len(teams_snap),
                "active_leases": len(leases),
                "a2a_tasks_count": len(durable_tasks),
                "open_circuits": sum(1 for c in circuits if c["state"] != "CLOSED"),
            },
            "sessions": merged,
            "teams": teams_snap,
            "messages": messages,
            "tasks": durable_tasks,
            "conversations": conversations,
            "graph": graph,
            "leases": leases,
            "circuit_breakers": circuits,
            "artifacts": artifacts,
            "events": events,
        }

    def _merge_agent_and_transcript(
        self,
        agent: Optional[Dict[str, Any]],
        t_info: Optional[Dict[str, Any]],
        now: float,
    ) -> Dict[str, Any]:
        if agent and t_info:
            status = "online" if (agent["status"] == "online" or t_info["status"] == "online") else "offline"
            # Hook state takes precedence when recently updated (< 25s)
            hook_fresh = (now - (agent.get("last_seen_at") or 0)) < 25
            activity_state = agent["activity_state"] if (hook_fresh and agent["activity_state"] != "idle") else t_info["activity_state"]
            if status == "offline":
                activity_state = "offline"

            current_action = (
                agent["current_action"]
                if (hook_fresh and agent.get("current_action"))
                else t_info.get("current_action", "")
            )
            current_tool = agent.get("current_tool", "") if hook_fresh else ""
            tool_elapsed_sec = (
                round(max(0.0, now - agent["current_tool_started_at"]), 1)
                if (current_tool and agent.get("current_tool_started_at"))
                else 0.0
            )
            model = t_info["model"] if t_info.get("model") not in (None, "", "auto") else agent.get("model", "auto")
            cwd = agent.get("cwd") or t_info.get("inferred_workspace") or ""
            folder = Path(cwd).name if cwd else ""
            display_name = agent["display_name"]
            if folder and folder not in display_name and display_name.startswith("AGY-"):
                display_name = f"{display_name} · {folder}"

            return {
                **t_info,
                "agent_uri": agent["agent_uri"],
                "display_name": display_name,
                "custom_name": agent.get("custom_name", ""),
                "role": agent["role"],
                "description": agent.get("description", ""),
                "model": model,
                "cwd": cwd,
                "workspace_paths": agent.get("workspace_paths") or ([cwd] if cwd else []),
                "owned_paths": agent.get("owned_paths", []),
                "capabilities": agent.get("capabilities", []),
                "trust_tier": agent.get("trust_tier", "local-same-user"),
                "status": status,
                "activity_state": activity_state,
                "current_action": current_action,
                "current_tool": current_tool,
                "tool_elapsed_sec": tool_elapsed_sec,
                "last_activity_at": max(agent.get("last_seen_at") or 0, t_info.get("last_activity_at") or 0),
                "hook_connected": True,
            }

        if agent:
            started = agent.get("started_at") or now
            last_seen = agent.get("last_seen_at") or started
            turn_start = agent.get("turn_started_at") or started
            active = agent["status"] == "online"
            tool_elapsed = (
                round(max(0.0, now - agent["current_tool_started_at"]), 1)
                if (agent.get("current_tool") and agent.get("current_tool_started_at"))
                else 0.0
            )
            return {
                "conversation_id": agent.get("conversation_id") or agent["agent_uri"].split("/")[-1],
                "agent_uri": agent["agent_uri"],
                "display_name": agent["display_name"],
                "custom_name": agent.get("custom_name", ""),
                "surface": agent["surface"],
                "role": agent["role"],
                "description": agent.get("description", ""),
                "model": agent.get("model", "auto"),
                "cwd": agent.get("cwd", ""),
                "workspace_paths": agent.get("workspace_paths", []),
                "owned_paths": agent.get("owned_paths", []),
                "capabilities": agent.get("capabilities", []),
                "trust_tier": agent.get("trust_tier", "local-same-user"),
                "status": agent["status"],
                "activity_state": agent["activity_state"],
                "started_at": started,
                "last_activity_at": last_seen,
                "turn_started_at": turn_start,
                "elapsed_sec": round(max(0.0, (now - started) if active else (last_seen - started)), 1),
                "turn_elapsed_sec": round(max(0.0, (now - turn_start) if active else (last_seen - turn_start)), 1),
                "age_sec": round(max(0.0, now - last_seen), 1),
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
                "title": agent.get("last_user_input") or agent["display_name"],
                "last_user_prompt": agent.get("last_user_input", ""),
                "last_thinking": "",
                "current_action": agent.get("current_action", ""),
                "current_tool": agent.get("current_tool", ""),
                "tool_elapsed_sec": tool_elapsed,
                "recent_steps": [],
                "token_history": [],
                "hook_connected": True,
            }

        # Transcript-only session (automatically discovered!)
        assert t_info is not None
        cwd = t_info.get("inferred_workspace") or ""
        folder = Path(cwd).name if cwd else ""
        surf = t_info["surface"].upper()
        cid_short = t_info["conversation_id"][:8]
        display = f"AGY-{surf} ({folder or cid_short})"
        return {
            **t_info,
            "display_name": display,
            "role": "detected-session",
            "description": "Tự động phát hiện qua Antigravity transcript log.",
            "cwd": cwd,
            "workspace_paths": [cwd] if cwd else [],
            "owned_paths": [],
            "capabilities": ["transcript.monitored"],
            "trust_tier": "local-same-user",
            "current_tool": "",
            "tool_elapsed_sec": 0.0,
            "hook_connected": False,
        }

    # ------------------------------------------------------------------
    # Antigravity Lifecycle Hook Handlers
    # ------------------------------------------------------------------

    def _drain_and_format_hook_inbox(self, agent_uri: str, conv_id: str) -> Tuple[int, str]:
        """Drain BOTH AGP SQLite messages and Team mailboxes for a live session and format with reply routes."""
        agp_msgs, team_msgs = drain_unified_inbox(
            store=self.store,
            teams_dir=self.teams.teams_dir,
            agent_uri=agent_uri,
            conversation_id=conv_id,
            mark_read=True,
            limit=12,
        )
        total = len(agp_msgs) + len(team_msgs)
        if total == 0:
            return 0, ""
        blocks: List[str] = []
        if agp_msgs:
            sender_labels: Dict[str, str] = {}
            reg = self._last_registry
            for msg in agp_msgs:
                furi = (msg.get("from") or {}).get("uri", "")
                if furi and furi not in sender_labels:
                    if reg:
                        ident = reg.resolve_uri(furi, (msg.get("from") or {}).get("role", ""))
                        sender_labels[furi] = f"{ident['full_label']} ({ident['at_handle']})"
                    else:
                        fa = self.store.get_agent(furi)
                        if fa and fa.get("display_name"):
                            sender_labels[furi] = fa["display_name"]
            blocks.append(format_untrusted_injection(agp_msgs, sender_labels=sender_labels, recipient_uri=agent_uri))
        if team_msgs:
            blocks.append(format_team_injection(team_msgs))
        return total, "\n\n".join(blocks)

    def handle_hook(self, hook_event: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = epoch_now()
        conv_id = payload.get("conversationId") or ""
        transcript_path = payload.get("transcriptPath") or ""
        workspace_paths = payload.get("workspacePaths") or []
        cwd = payload.get("cwd") or (workspace_paths[0] if workspace_paths else "")
        model = payload.get("modelName") or "auto"
        last_input = clean_user_prompt(payload.get("lastUserInput") or "")
        surface = AGPStore._infer_surface(transcript_path)
        agent_uri = f"agp://local/{surface}/{conv_id}" if conv_id else f"agp://local/{surface}/session"

        if hook_event == "session_start":
            agent = self.store.upsert_agent(
                {
                    "agent_uri": agent_uri,
                    "conversation_id": conv_id,
                    "surface": surface,
                    "model": model,
                    "cwd": cwd,
                    "workspace_paths": workspace_paths,
                    "transcript_path": transcript_path,
                    "status": "online",
                    "activity_state": "idle",
                    "current_action": "Session started",
                    "started_at": now,
                    "turn_started_at": now,
                    "stop_continuations": 0,
                }
            )
            self.store.record_event(
                "hook.session_start",
                {"cwd": cwd, "model": model, "surface": surface},
                agent_uri=agent_uri,
                conversation_id=conv_id,
            )
            self.notify("agent_updated", agent)
            listen_cmd = build_listen_command(agent_uri, data_dir=self.teams.data_dir)
            return {
                "injectSteps": [
                    {
                        "systemMessage": {
                            "systemMessage": (
                                f"[AGP v2.0 Active — Serverless Background MCP] Your agent_uri is `{agent_uri}`. "
                                f"Hooks deliver incoming AGP/Team messages automatically mid-turn (no HTTP server required). "
                                f"When collaborating or waiting for peer replies while idle (0 tokens), run in background: `{listen_cmd}`. "
                                f"Use `agp_register`, `agp_discover`, `agp_ask`/`agp_send`, `agp_inbox`, `agp_lease`, or `team_*` tools."
                            ),
                            "metadata": {
                                "user_markdown": (
                                    f"**AGP Mesh v2.0 active (background MCP)** — URI: `{agent_uri}`"
                                )
                            },
                        }
                    }
                ]
            }

        if hook_event == "pre_invocation":
            inv_num = int(payload.get("invocationNum") or 0)
            update: Dict[str, Any] = {
                "agent_uri": agent_uri,
                "conversation_id": conv_id,
                "surface": surface,
                "model": model,
                "cwd": cwd,
                "workspace_paths": workspace_paths,
                "transcript_path": transcript_path,
                "last_user_input": last_input,
                "status": "online",
                "activity_state": "thinking",
                "current_action": f"Reasoning (invocation #{inv_num})",
                "current_tool": "",
                "current_tool_started_at": None,
            }
            if inv_num <= 1:
                update["turn_started_at"] = now
                update["stop_continuations"] = 0
            agent = self.store.upsert_agent(update)
            self.notify("agent_updated", agent)

            cnt, injected = self._drain_and_format_hook_inbox(agent_uri, conv_id)
            if cnt > 0:
                return {
                    "injectSteps": [
                        {
                            "systemMessage": {
                                "systemMessage": injected,
                            }
                        }
                    ]
                }
            return {}

        if hook_event == "post_invocation":
            cnt, injected = self._drain_and_format_hook_inbox(agent_uri, conv_id)
            if cnt > 0:
                self.store.upsert_agent(
                    {
                        "agent_uri": agent_uri,
                        "conversation_id": conv_id,
                        "surface": surface,
                        "status": "online",
                        "activity_state": "communicating",
                        "current_action": f"Received {cnt} peer message(s) mid-turn",
                    }
                )
                return {
                    "injectSteps": [
                        {
                            "systemMessage": {
                                "systemMessage": injected,
                            }
                        }
                    ],
                    "terminationBehavior": "force_continue",
                }
            return {}

        if hook_event == "pre_tool_use":
            tool_call = payload.get("toolCall") or {}
            tool_name = tool_call.get("name", "unknown_tool")
            tool_args = tool_call.get("args") or {}
            target_path = (
                tool_args.get("Path")
                or tool_args.get("path")
                or tool_args.get("TargetFile")
                or ""
            )

            # Enforce workspace exclusive_write file leases (+ monotonic fencing_token) on edit/write tools!
            if tool_name in {"edit_file", "write_file", "replace_file_content", "multi_replace_file_content"} and target_path:
                raw_fence = tool_args.get("fencing_token") if "fencing_token" in tool_args else payload.get("fencingToken")
                fence_val = int(raw_fence) if raw_fence is not None else None
                conflict = self.store.check_write_conflict(agent_uri, str(target_path), fencing_token=fence_val)
                if conflict:
                    self.store.record_event(
                        "lease.blocked_write",
                        {"tool": tool_name, "path": target_path, "conflict": conflict},
                        agent_uri=agent_uri,
                        conversation_id=conv_id,
                    )
                    self.notify("lease_conflict", {"actor": agent_uri, "path": target_path, "conflict": conflict})
                    if conflict.get("error_code") == "ERR_STALE_FENCING_TOKEN":
                        return {
                            "decision": "deny",
                            "reason": (
                                f"[AGP Lease Guard — ERR_STALE_FENCING_TOKEN] Stale fencing_token ({fence_val}) "
                                f"for '{target_path}' (current epoch is {conflict['fencing_token']}). Re-acquire lease via agp_lease."
                            ),
                        }
                    return {
                        "decision": "deny",
                        "reason": (
                            f"[AGP Lease Guard] Cannot edit '{target_path}': exclusive write lease held by "
                            f"{conflict['owner_uri']} ({conflict['path_pattern']}, expires in {conflict['expires_in_sec']}s). "
                            "Coordinate via agp_send(type='task.request') or wait for lease release."
                        ),
                    }

            action_label = f"{tool_name}"
            if target_path:
                action_label = f"{tool_name}({Path(str(target_path)).name})"
            elif tool_args.get("toolSummary"):
                action_label = f"{tool_name}: {str(tool_args['toolSummary'])[:60]}"
            elif tool_args.get("CommandLine"):
                action_label = f"{tool_name}: {str(tool_args['CommandLine'])[:60]}"

            state = "communicating" if tool_name.startswith("mcp_agp") or "agp_" in tool_name else "running_tool"
            agent = self.store.upsert_agent(
                {
                    "agent_uri": agent_uri,
                    "conversation_id": conv_id,
                    "surface": surface,
                    "model": model,
                    "cwd": cwd,
                    "workspace_paths": workspace_paths,
                    "transcript_path": transcript_path,
                    "status": "online",
                    "activity_state": state,
                    "current_tool": tool_name,
                    "current_tool_started_at": now,
                    "current_action": action_label,
                }
            )
            self.store.record_event(
                "hook.pre_tool_use",
                {"tool": tool_name, "action": action_label},
                agent_uri=agent_uri,
                conversation_id=conv_id,
            )
            self.notify("agent_updated", agent)
            return {"decision": "allow"}

        if hook_event == "post_tool_use":
            tool_call = payload.get("toolCall") or {}
            tool_name = tool_call.get("name", "unknown_tool")
            err = payload.get("error") or ""
            agent = self.store.upsert_agent(
                {
                    "agent_uri": agent_uri,
                    "conversation_id": conv_id,
                    "surface": surface,
                    "status": "online",
                    "activity_state": "thinking",
                    "current_tool": "",
                    "current_tool_started_at": None,
                    "current_action": f"Finished {tool_name}" + (f" (err: {err[:40]})" if err else ""),
                }
            )
            self.store.record_event(
                "hook.post_tool_use",
                {"tool": tool_name, "error": err[:200] if err else ""},
                agent_uri=agent_uri,
                conversation_id=conv_id,
            )
            self.notify("agent_updated", agent)
            return {}

        if hook_event == "stop":
            continuation_active = bool(payload.get("continuationActive", False))
            term_reason = payload.get("terminationReason", "NO_TOOL_CALL")
            existing = self.store.get_agent(agent_uri)
            cont_count = int(existing["stop_continuations"]) if existing else 0

            # Check ALL unread messages (any priority) across BOTH AGP SQLite inbox and Team mailboxes!
            if (
                not continuation_active
                and term_reason == "NO_TOOL_CALL"
                and cont_count < MAX_STOP_CONTINUATIONS
            ):
                unread_cnt, injected_body = self._drain_and_format_hook_inbox(agent_uri, conv_id)
                if unread_cnt > 0:
                    self.store.upsert_agent(
                        {
                            "agent_uri": agent_uri,
                            "conversation_id": conv_id,
                            "surface": surface,
                            "status": "online",
                            "activity_state": "communicating",
                            "current_action": f"Processing {unread_cnt} incoming AGP/Team message(s)",
                            "stop_continuations": cont_count + 1,
                        }
                    )
                    return {
                        "decision": "continue",
                        "reason": (
                            f"[AGP Broker] You have {unread_cnt} unread peer message(s) — process and reply before finishing:\n\n"
                            + injected_body
                        ),
                    }

            agent = self.store.upsert_agent(
                {
                    "agent_uri": agent_uri,
                    "conversation_id": conv_id,
                    "surface": surface,
                    "status": "online",
                    "activity_state": "idle",
                    "current_tool": "",
                    "current_tool_started_at": None,
                    "current_action": f"Idle ({term_reason})",
                }
            )
            self.store.record_event(
                "hook.stop",
                {"termination_reason": term_reason, "fully_idle": payload.get("fullyIdle", True)},
                agent_uri=agent_uri,
                conversation_id=conv_id,
            )
            self.notify("agent_updated", agent)
            return {}

        return {}


def make_handler_class(runtime: AGPBrokerRuntime):
    class AGPHTTPRequestHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            # Keep stdout/stderr clean unless AGP_DEBUG=1
            if os.environ.get("AGP_DEBUG") == "1":
                super().log_message(format, *args)

        def _send_json(
            self,
            status: int,
            data: Any,
            extra_headers: Optional[Dict[str, str]] = None,
        ) -> None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _send_empty(self, status: int, extra_headers: Optional[Dict[str, str]] = None) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.send_header("Access-Control-Allow-Origin", "*")
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()

        def _read_json_body(self) -> Tuple[bool, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                return False, None
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                return True, json.loads(raw.decode("utf-8"))
            except Exception:
                return False, None

        def _base_url(self) -> str:
            host_hdr = self.headers.get("Host")
            if host_hdr:
                return f"http://{host_hdr}"
            addr = self.server.server_address
            return f"http://{addr[0]}:{addr[1]}"

        def do_OPTIONS(self) -> None:
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, Accept, Authorization, Mcp-Session-Id, MCP-Protocol-Version, Mcp-Method, Mcp-Name, A2A-Version, A2A-Extensions",
            )
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)

            if path == "/api/health":
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "service": "agp-broker",
                        "version": __version__,
                        "protocol": __protocol_version__,
                        "mcp_versions": sorted(runtime.mcp.SUPPORTED_PROTOCOL_VERSIONS)
                        if hasattr(runtime.mcp, "SUPPORTED_PROTOCOL_VERSIONS")
                        else ["2026-07-28", "2025-03-26"],
                        "a2a_version": "1.0",
                        "pid": os.getpid(),
                        "uptime_sec": round(epoch_now() - runtime.started_at, 1),
                    },
                )
                return

            # A2A v1.0 RFC 8615 Discovery & JWKS endpoints
            if path in {"/.well-known/agent-card.json", "/.well-known/agent.json"}:
                card = runtime.a2a.build_agent_card(self._base_url(), extended=False)
                self._send_json(200, card, {"A2A-Version": "1.0"})
                return

            if path == "/.well-known/jwks.json":
                self._send_json(200, runtime.a2a.build_jwks())
                return

            if path in {"/extendedAgentCard", "/v1/card"}:
                auth_hdr = (self.headers.get("Authorization") or "").strip()
                if not auth_hdr.lower().startswith("bearer ") or not auth_hdr[7:].strip():
                    self._send_json(
                        401,
                        {"error": "Unauthorized: Bearer token required for GetExtendedAgentCard"},
                        {"WWW-Authenticate": 'Bearer realm="agp-a2a"', "A2A-Version": "1.0"},
                    )
                    return
                card = runtime.a2a.build_agent_card(self._base_url(), extended=True)
                self._send_json(200, card, {"A2A-Version": "1.0"})
                return

            # A2A v1.0 HTTP+JSON Task GET endpoints
            if path in {"/tasks", "/v1/tasks"}:
                ctx_id = (qs.get("contextId") or [None])[0]
                state = (qs.get("status") or qs.get("state") or [None])[0]
                lim = int((qs.get("pageSize") or qs.get("limit") or [50])[0])
                _, res = runtime.a2a.list_tasks(context_id=ctx_id, state=state, limit=lim)
                self._send_json(200, res, {"A2A-Version": "1.0"})
                return

            if path.startswith("/tasks/") or path.startswith("/v1/tasks/"):
                tail = path.split("/tasks/", 1)[1].strip("/")
                if "/pushNotificationConfigs" in tail:
                    tid, _, cid_part = tail.partition("/pushNotificationConfigs")
                    cid = cid_part.strip("/") or None
                    if cid:
                        ok_p, res_p = runtime.a2a.get_push_config(urllib.parse.unquote(tid), config_id=urllib.parse.unquote(cid))
                        self._send_json(200 if ok_p else 404, res_p, {"A2A-Version": "1.0"})
                    else:
                        _, res_p = runtime.a2a.list_push_configs(urllib.parse.unquote(tid))
                        self._send_json(200, res_p, {"A2A-Version": "1.0"})
                    return
                ok_t, res_t = runtime.a2a.get_task(urllib.parse.unquote(tail))
                self._send_json(200 if ok_t else 404, res_t, {"A2A-Version": "1.0"})
                return

            if path == "/api/snapshot":
                self._send_json(200, runtime.build_snapshot())
                return

            if path == "/api/events" or path == "/mcp":
                self._handle_sse(is_mcp=(path == "/mcp"))
                return

            if path.startswith("/api/traces/"):
                trace_id = urllib.parse.unquote(path.split("/api/traces/", 1)[1])
                self._send_json(200, runtime.store.inspect_trace(trace_id))
                return

            if path.startswith("/api/artifacts/"):
                artifact_id = urllib.parse.unquote(path.split("/api/artifacts/", 1)[1])
                ok, res = runtime.store.get_artifact(artifact_id)
                self._send_json(200 if ok else 404, res)
                return

            # Static HTML Monitor UI
            self._serve_static(path)

        def do_POST(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            ok_body, body = self._read_json_body()
            if not ok_body:
                self._send_json(400, {"ok": False, "error": "Invalid JSON body"})
                return

            if path == "/mcp":
                sid = self.headers.get("Mcp-Session-Id")
                proto_hdr = self.headers.get("MCP-Protocol-Version") or "2026-07-28"
                qs = urllib.parse.parse_qs(parsed.query)
                cm_raw = (qs.get("codemode") or [None])[0]
                cm_val = None if cm_raw is None else (cm_raw.strip().lower() not in {"0", "false", "no"})
                tr_raw = (qs.get("truncateToolResult") or qs.get("truncate") or [None])[0]
                tr_val = None if tr_raw is None else (tr_raw.strip().lower() not in {"0", "false", "no"})
                status, resp, out_sid = runtime.mcp.handle_jsonrpc(
                    body,
                    session_id=sid,
                    codemode=cm_val,
                    truncate_tool_result=tr_val,
                )
                headers: Dict[str, str] = {"MCP-Protocol-Version": proto_hdr}
                if out_sid:
                    headers["Mcp-Session-Id"] = out_sid
                if isinstance(body, dict) and body.get("method"):
                    headers["Mcp-Method"] = str(body["method"])
                if resp is None:
                    self._send_empty(status, headers)
                else:
                    self._send_json(status, resp, headers)
                return

            # A2A v1.0 JSON-RPC 2.0 Endpoint (`POST /a2a`)
            if path == "/a2a":
                req_method = str(body.get("method") or "") if isinstance(body, dict) else ""
                if req_method in {"SendStreamingMessage", "message/stream"}:
                    ok_s, res_s = runtime.a2a.send_message(body.get("params") or {})
                    if not ok_s:
                        self._send_json(200, {"jsonrpc": "2.0", "id": body.get("id"), "error": res_s}, {"A2A-Version": "1.0"})
                        return
                    self._handle_a2a_task_sse(res_s["task"]["id"], jsonrpc_id=body.get("id"))
                    return
                if req_method in {"SubscribeToTask", "tasks/resubscribe"}:
                    params = body.get("params") or {}
                    tid = str(params.get("id") or params.get("taskId") or "")
                    self._handle_a2a_task_sse(tid, jsonrpc_id=body.get("id"))
                    return
                st_code, rpc_out = runtime.a2a.handle_jsonrpc(body, base_url=self._base_url())
                self._send_json(st_code, rpc_out, {"A2A-Version": "1.0"})
                return

            # A2A v1.0 HTTP+JSON REST Bindings
            if path in {"/message:send", "/v1/message:send"} or (
                path.endswith("/message:send") and path.count("/") == 2
            ):
                tenant = ""
                if path.endswith("/message:send") and path not in {"/message:send", "/v1/message:send"}:
                    tenant = urllib.parse.unquote(path.strip("/").split("/", 1)[0])
                ok_a2a, res_a2a = runtime.a2a.send_message(body if isinstance(body, dict) else {}, tenant=tenant)
                self._send_json(200 if ok_a2a else 400, res_a2a, {"A2A-Version": "1.0"})
                return

            if path in {"/message:stream", "/v1/message:stream"}:
                ok_a2a, res_a2a = runtime.a2a.send_message(body if isinstance(body, dict) else {})
                if not ok_a2a:
                    self._send_json(400, res_a2a, {"A2A-Version": "1.0"})
                    return
                self._handle_a2a_task_sse(res_a2a["task"]["id"])
                return

            if path.startswith("/tasks/") or path.startswith("/v1/tasks/"):
                tail = path.split("/tasks/", 1)[1].strip("/")
                if tail.endswith(":cancel"):
                    tid = urllib.parse.unquote(tail[: -len(":cancel")])
                    ok_c, res_c = runtime.a2a.cancel_task(tid)
                    self._send_json(200 if ok_c else 400, res_c, {"A2A-Version": "1.0"})
                    return
                if tail.endswith(":subscribe"):
                    tid = urllib.parse.unquote(tail[: -len(":subscribe")])
                    self._handle_a2a_task_sse(tid)
                    return
                if tail.endswith("/pushNotificationConfigs"):
                    tid = urllib.parse.unquote(tail[: -len("/pushNotificationConfigs")])
                    ok_p, res_p = runtime.a2a.create_push_config(tid, body if isinstance(body, dict) else {})
                    self._send_json(200 if ok_p else 400, res_p, {"A2A-Version": "1.0"})
                    return

            if path.startswith("/hooks/"):
                hook_name = path.split("/hooks/", 1)[1].strip("/")
                result = runtime.handle_hook(hook_name, body if isinstance(body, dict) else {})
                self._send_json(200, result)
                return

            if path == "/api/send":
                raw_body = dict(body) if isinstance(body, dict) else {}
                to_obj = dict(raw_body.get("to") or {})
                raw_to = to_obj.get("uri") or raw_body.get("to_uri") or ""
                resolved_to = ""
                if raw_to:
                    resolved_to = runtime.resolve_target_uri(str(raw_to))
                    to_obj["uri"] = resolved_to
                    raw_body["to"] = to_obj
                ok, res = runtime.store.send_message(raw_body)
                if ok:
                    runtime.notify("agp_message", res.get("envelope", {}))
                    if raw_body.get("wake", True) and resolved_to:
                        runtime.wakeup_agent(resolved_to)
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/agents/rename":
                ok, res = runtime.store.rename_agent(
                    agent_uri=body.get("agent_uri", ""),
                    conversation_id=body.get("conversation_id", ""),
                    custom_name=body.get("custom_name", body.get("display_name", "")),
                    role=body.get("role"),
                    surface=body.get("surface", "cli"),
                )
                if ok:
                    runtime.notify("agent_updated", res.get("agent", {}))
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/ack":
                ok, res = runtime.store.ack_message(
                    msg_id=body.get("msg_id", ""),
                    status=body.get("status", "accepted"),
                    reason=body.get("reason", ""),
                    actor_uri=body.get("actor_uri", "agp://local/human/monitor"),
                )
                if ok:
                    runtime.notify("agp_ack", res)
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/leases":
                ok, res = runtime.store.manage_lease(
                    action=body.get("action", "list"),
                    owner_uri=body.get("owner_uri", "agp://local/human/monitor"),
                    paths=body.get("paths"),
                    mode=body.get("mode", "exclusive_write"),
                    ttl_sec=int(body.get("ttl_sec") or 120),
                )
                if ok:
                    runtime.notify("lease_updated", res)
                self._send_json(200 if ok else 409, res)
                return

            # Claude-style AGP Teams REST API (used by HTML Monitor UI)
            if path == "/api/teams":
                ok, res = runtime.teams.create_team(
                    team=body.get("team", ""),
                    description=body.get("description", ""),
                    lead_name=body.get("lead_name", "lead"),
                    lead_conversation_id=body.get("lead_conversation_id", ""),
                    cwd=body.get("cwd", ""),
                )
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/teams/join":
                ok, res = runtime.teams.join_team(
                    team=body.get("team", ""),
                    name=body.get("name", ""),
                    role=body.get("role", "teammate"),
                    conversation_id=body.get("conversation_id", ""),
                    cwd=body.get("cwd", ""),
                    managed=False,
                )
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/teams/spawn":
                ok, res = runtime.teams.spawn_teammate(
                    team=body.get("team", ""),
                    name=body.get("name", ""),
                    role=body.get("role", "teammate"),
                    prompt=body.get("prompt", ""),
                    cwd=body.get("cwd", ""),
                    model=body.get("model", ""),
                    effort=body.get("effort", "low"),
                )
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/teams/send":
                ok, res = runtime.teams.send_message(
                    team=body.get("team", ""),
                    to=body.get("to", ""),
                    message=body.get("message", ""),
                    from_name=body.get("from_name", "lead"),
                    kind=body.get("kind", "message"),
                    wake=bool(body.get("wake", True)),
                    hop=int(body.get("hop", 1)),
                )
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/teams/task":
                ok, res = runtime.teams.manage_task(
                    team=body.get("team", ""),
                    action=body.get("action", "list"),
                    task_id=body.get("task_id"),
                    title=body.get("title", ""),
                    description=body.get("description", ""),
                    owner=body.get("owner", ""),
                    status=body.get("status", ""),
                    blocked_by=body.get("blocked_by"),
                    result_summary=body.get("result_summary", ""),
                )
                self._send_json(200 if ok else 400, res)
                return

            if path == "/api/teams/shutdown":
                ok, res = runtime.teams.shutdown_teammate(
                    team=body.get("team", ""),
                    name=body.get("name", ""),
                    reason=body.get("reason", "Stopped from Monitor UI"),
                )
                self._send_json(200 if ok else 400, res)
                return

            self._send_json(404, {"ok": False, "error": f"Unknown endpoint: {path}"})

        def do_DELETE(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            if path == "/mcp":
                runtime.mcp.delete_session(self.headers.get("Mcp-Session-Id"))
                self._send_empty(204)
                return

            if path.startswith("/api/leases/"):
                lease_id = urllib.parse.unquote(path.split("/api/leases/", 1)[1])
                removed = runtime.store.force_release_lease(lease_id)
                if removed:
                    runtime.notify("lease_updated", {"released": lease_id})
                self._send_json(200 if removed else 404, {"ok": removed, "lease_id": lease_id})
                return

            if (path.startswith("/tasks/") or path.startswith("/v1/tasks/")) and "/pushNotificationConfigs/" in path:
                tail = path.split("/tasks/", 1)[1].strip("/")
                tid, _, cid = tail.partition("/pushNotificationConfigs/")
                ok_d, res_d = runtime.a2a.delete_push_config(
                    urllib.parse.unquote(tid), urllib.parse.unquote(cid.strip("/"))
                )
                if ok_d:
                    self._send_empty(204, {"A2A-Version": "1.0"})
                else:
                    self._send_json(404, res_d, {"A2A-Version": "1.0"})
                return

            self._send_json(404, {"ok": False, "error": f"Unknown endpoint: {path}"})

        def _handle_a2a_task_sse(self, task_id: str, jsonrpc_id: Any = None) -> None:
            """Stream A2A v1.0 `StreamResponse` frames for `SendStreamingMessage` & `SubscribeToTask`."""
            task = runtime.store.get_task(task_id)
            if not task:
                self._send_json(404, {"code": -32001, "message": f"Task '{task_id}' not found"}, {"A2A-Version": "1.0"})
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("A2A-Version", "1.0")
            self.end_headers()

            def _wrap(frame_obj: Dict[str, Any]) -> bytes:
                if jsonrpc_id is not None:
                    frame_obj = {"jsonrpc": "2.0", "id": jsonrpc_id, "result": frame_obj}
                return f"data: {json.dumps(frame_obj, ensure_ascii=False)}\n\n".encode("utf-8")

            q = runtime.sse.subscribe()
            try:
                self.wfile.write(_wrap({"task": task}))
                self.wfile.flush()
                terminal = {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED"}
                if (task.get("status") or {}).get("state") in terminal:
                    return
                deadline = time.time() + 30.0
                while not runtime._stop_event.is_set() and time.time() < deadline:
                    try:
                        raw_ev = q.get(timeout=2.0)
                    except queue.Empty:
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
                        continue
                    if "event: a2a_task_update\n" not in raw_ev:
                        continue
                    for line in raw_ev.splitlines():
                        if line.startswith("data: "):
                            ev_obj = json.loads(line[6:])
                            ev_data = ev_obj.get("data") or {}
                            upd = ev_data.get("statusUpdate") or {}
                            if upd.get("taskId") == task_id:
                                self.wfile.write(_wrap({"statusUpdate": upd}))
                                t_now = ev_data.get("task") or {}
                                for art in t_now.get("artifacts") or []:
                                    self.wfile.write(
                                        _wrap(
                                            {
                                                "artifactUpdate": {
                                                    "taskId": task_id,
                                                    "contextId": t_now.get("contextId", ""),
                                                    "artifact": art,
                                                    "append": False,
                                                    "lastChunk": True,
                                                }
                                            }
                                        )
                                    )
                                self.wfile.flush()
                                if upd.get("final"):
                                    return
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                runtime.sse.unsubscribe(q)

        def _handle_sse(self, is_mcp: bool = False) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if is_mcp:
                # MCP Streamable HTTP GET keepalive stream
                try:
                    while not runtime._stop_event.is_set():
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
                        time.sleep(15)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    return
                return

            q = runtime.sse.subscribe()
            try:
                # Immediately push initial snapshot on connect
                init_frame = (
                    f"event: snapshot\ndata: {json.dumps(runtime.build_snapshot(), ensure_ascii=False)}\n\n"
                )
                self.wfile.write(init_frame.encode("utf-8"))
                self.wfile.flush()

                while not runtime._stop_event.is_set():
                    try:
                        frame = q.get(timeout=10.0)
                        self.wfile.write(frame.encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                runtime.sse.unsubscribe(q)

        def _serve_static(self, req_path: str) -> None:
            rel = "index.html" if req_path in ("", "/") else req_path.lstrip("/")
            candidate = (runtime.web_dir / rel).resolve()
            if not str(candidate).startswith(str(runtime.web_dir.resolve())) or not candidate.is_file():
                self._send_json(404, {"ok": False, "error": "Not found"})
                return
            content_type, _ = mimetypes.guess_type(str(candidate))
            if candidate.suffix == ".js":
                content_type = "application/javascript; charset=utf-8"
            elif candidate.suffix == ".css":
                content_type = "text/css; charset=utf-8"
            elif candidate.suffix == ".html":
                content_type = "text/html; charset=utf-8"
            data = candidate.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data)

    return AGPHTTPRequestHandler


class _SingleInstanceHTTPServer(ThreadingHTTPServer):
    """Prevent duplicate brokers from double-binding the same port on Windows."""

    allow_reuse_address = os.name != "nt"

    def server_bind(self) -> None:
        if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            try:
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except OSError:
                pass
        super().server_bind()


def create_server(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    runtime: Optional[AGPBrokerRuntime] = None,
) -> Tuple[ThreadingHTTPServer, AGPBrokerRuntime]:
    rt = runtime or AGPBrokerRuntime()
    handler_cls = make_handler_class(rt)
    server = _SingleInstanceHTTPServer((host, port), handler_cls)
    server.daemon_threads = True
    return server, rt


def main() -> None:
    parser = argparse.ArgumentParser(description="AGP v1.0 Local Broker & AGY Live Monitor")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--open-browser", action="store_true", help="Open HTML monitor in default browser once started")
    args = parser.parse_args()

    ensure_data_dirs()
    server, runtime = create_server(host=args.host, port=args.port)
    runtime.start_background_scanner(interval_sec=1.0)

    PID_FILE.write_text(str(os.getpid()), encoding="utf-8")

    def _shutdown(*_args: Any) -> None:
        runtime.stop()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    url = f"http://{args.host}:{args.port}"
    print(
        f"[AGY Monitor UI v{__version__}] Viewing dashboard at {url} "
        f"(Optional UI & HTTP/A2A Gateway — MCP runs silently even when this is closed)",
        flush=True,
    )
    if args.open_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    finally:
        runtime.stop()
        try:
            if PID_FILE.exists() and PID_FILE.read_text(encoding="utf-8").strip() == str(os.getpid()):
                PID_FILE.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()
