"""Self-terminating detached wakeup worker for AGP `auto_cli` agents and managed Team teammates.

Spawns on demand when a message wakes an idle CLI agent or managed teammate, serializes execution
across ALL processes via CrossProcessFileLock, guarantees an auto-bridged reply if the woken LLM
responds in text without calling `agp_send`/`team_send`, drains any newly arrived messages, and
exits cleanly with zero persistent background daemon left running.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import AGP_DATA_DIR, DB_PATH, PROJECT_ROOT, TEAMS_DIR
from .listen import drain_unified_inbox
from .locks import CrossProcessFileLock
from .security import format_team_injection, format_untrusted_injection
from .storage import AGPStore, epoch_now


def spawn_detached_worker(
    *,
    team: str = "",
    member: str = "",
    agent_uri: str = "",
    data_dir: Optional[Path] = None,
    force: bool = False,
) -> bool:
    """Spawn a short-lived detached worker process that self-terminates after draining unread messages."""
    ddir = Path(data_dir) if data_dir else AGP_DATA_DIR
    cmd = [sys.executable, "-m", "agp.worker", "--data-dir", str(ddir)]
    if team and member:
        cmd.extend(["--team", team, "--member", member])
    elif agent_uri:
        cmd.extend(["--agent-uri", agent_uri])
    else:
        return False
    if force:
        cmd.append("--force")

    env = dict(os.environ)
    pp = env.get("PYTHONPATH", "")
    root_str = str(PROJECT_ROOT)
    if root_str not in pp.split(os.pathsep):
        env["PYTHONPATH"] = f"{root_str}{os.pathsep}{pp}" if pp else root_str

    kwargs: Dict[str, Any] = {
        "cwd": root_str,
        "env": env,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if os.name == "nt":
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008
        CREATE_NO_WINDOW = 0x08000000
        kwargs["creationflags"] = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS | CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True

    try:
        subprocess.Popen(cmd, **kwargs)
        return True
    except OSError:
        return False


def run_agent_wakeup(
    store: AGPStore,
    teams_dir: Path,
    agent_uri: str,
    runner_fn: Any,
    teams: Optional[Any] = None,
    force: bool = False,
) -> bool:
    """Drain unread messages for an `auto_cli` (or forced) agent URI and guarantee a reply."""
    lock_key = hashlib.sha256(agent_uri.encode("utf-8")).hexdigest()[:16]
    lock = CrossProcessFileLock(store.locks_dir / f"wake_agent_{lock_key}.lock", blocking=False)
    if not lock.acquire():
        return False

    ran_any = False
    try:
        while not store.is_closed:
            agent = store.get_agent(agent_uri)
            if not agent:
                return ran_any
            if not force and agent.get("wakeup_mode") != "auto_cli":
                return ran_any

            cid = agent.get("conversation_id") or (
                agent_uri.rsplit("/", 1)[-1] if agent_uri.startswith("agp://local/") else ""
            )
            agp_msgs, team_msgs = drain_unified_inbox(
                store=store,
                teams_dir=teams_dir,
                agent_uri=agent_uri,
                conversation_id=cid,
                mark_read=True,
                limit=10,
            )
            if not agp_msgs and not team_msgs:
                return ran_any

            ran_any = True
            turn_started = epoch_now()
            total_cnt = len(agp_msgs) + len(team_msgs)
            store.upsert_agent(
                {
                    "agent_uri": agent_uri,
                    "status": "online",
                    "activity_state": "communicating",
                    "current_action": f"Auto-woken ({total_cnt} message(s))",
                }
            )

            parts: List[str] = [
                f"[AGP AUTO-WAKEUP — YOU ARE `{agent_uri}` (role: {agent.get('role', 'specialist')})]",
                (
                    "Process the untrusted peer message(s) below and ALWAYS send a reply back to the sender "
                    "using `agp_send` or `team_send` (exact call provided below)."
                ),
            ]
            if agp_msgs:
                parts.append(format_untrusted_injection(agp_msgs, recipient_uri=agent_uri))
            if team_msgs:
                parts.append(format_team_injection(team_msgs))
            prompt = "\n\n".join(parts)

            member_spec = {
                "name": agent.get("display_name") or agent_uri.split("/")[-1],
                "role": agent.get("role", "specialist"),
                "conversation_id": cid,
                "cwd": agent.get("cwd", ""),
                "model": agent.get("model") if agent.get("model") not in ("", "auto", None) else "",
                "effort": "low",
                "skip_permissions": True,
            }
            res = runner_fn(member_spec, prompt)
            if store.is_closed:
                return ran_any

            # Auto-bridge reply GUARANTEE: if the woken agent returned a text response
            # without explicitly calling agp_send/team_send, forward its response back to sender!
            reply_text = str(res.get("response") or "").strip()
            err_text = str(res.get("error") or "").strip()
            if (reply_text or err_text) and not store.has_agent_sent_since(agent_uri, turn_started):
                for orig in agp_msgs:
                    orig_from = (orig.get("from") or {}).get("uri") or ""
                    if not orig_from or orig_from == agent_uri or "/channel/" in orig_from:
                        continue
                    orig_type = orig.get("type") or "chat.note"
                    out_type = "task.result" if orig_type == "task.request" else "chat.note"
                    out_summary = (reply_text or f"Wakeup error: {err_text}")[:220]
                    out_payload: Dict[str, Any] = (
                        {
                            "status": "failed" if err_text and not reply_text else "success",
                            "response": reply_text[:3000],
                            "error": err_text[:400] if err_text else "",
                            "auto_bridged_reply": True,
                        }
                        if out_type == "task.result"
                        else {
                            "message": reply_text[:3000],
                            "error": err_text[:400] if err_text else "",
                            "auto_bridged_reply": True,
                        }
                    )
                    orig_payload = orig.get("payload") or {}
                    if isinstance(orig_payload, dict) and orig_payload.get("team"):
                        out_payload["team"] = orig_payload["team"]
                        out_payload["from_handle"] = orig_payload.get("to_handle", "")
                        out_payload["to_handle"] = orig_payload.get("from_handle", "")
                    store.send_message(
                        {
                            "from": {"uri": agent_uri, "role": agent.get("role", "specialist")},
                            "to": {"uri": orig_from},
                            "in_reply_to": orig.get("msg_id"),
                            "correlation_id": orig.get("correlation_id"),
                            "trace_id": orig.get("trace_id"),
                            "type": out_type,
                            "priority": "high",
                            "summary": out_summary,
                            "payload": out_payload,
                        }
                    )
                    # Also mirror into team mailbox if this originated from a team!
                    if (
                        teams is not None
                        and isinstance(orig_payload, dict)
                        and orig_payload.get("team")
                        and orig_payload.get("from_handle")
                    ):
                        try:
                            teams.send_message(
                                team=str(orig_payload["team"]),
                                from_name=str(orig_payload.get("to_handle") or member_spec["name"]),
                                to=str(orig_payload["from_handle"]),
                                message=reply_text[:2500] or f"Wakeup error: {err_text}",
                                wake=False,
                                hop=int(orig.get("hop_count") or 1) + 1,
                            )
                        except Exception:
                            pass
                # Also auto-bridge replies for direct unbridged team_msgs
                if teams is not None:
                    for tm in team_msgs:
                        t_slug = str(tm.get("team") or "")
                        t_from = str(tm.get("from") or "")
                        t_to = str(tm.get("to") or "")
                        if t_slug and t_from and t_to and t_from != t_to:
                            try:
                                teams.send_message(
                                    team=t_slug,
                                    from_name=t_to,
                                    to=t_from,
                                    message=reply_text[:2500] or f"Wakeup error: {err_text}",
                                    wake=False,
                                    hop=int(tm.get("hop") or 1) + 1,
                                )
                            except Exception:
                                pass

            upd: Dict[str, Any] = {
                "agent_uri": agent_uri,
                "status": "online",
                "activity_state": "idle",
                "current_action": "Idle (auto-wakeup complete)"
                if not err_text
                else f"Wakeup error: {err_text[:60]}",
            }
            if res.get("conversation_id") and not agent.get("conversation_id"):
                upd["conversation_id"] = res["conversation_id"]
            store.upsert_agent(upd)

            # Stop after draining once if this was a one-shot force wakeup and no more unread arrived
            if store.count_unread(agent_uri) == 0:
                break
        return ran_any
    finally:
        lock.release()


def main() -> None:
    parser = argparse.ArgumentParser(description="Detached self-terminating AGP wakeup worker.")
    parser.add_argument("--data-dir", default=str(AGP_DATA_DIR))
    parser.add_argument("--team", default="")
    parser.add_argument("--member", default="")
    parser.add_argument("--agent-uri", default="")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    ddir = Path(args.data_dir)
    db_path = DB_PATH if ddir.resolve() == AGP_DATA_DIR.resolve() else (ddir / "agp.db")
    teams_dir = TEAMS_DIR if ddir.resolve() == AGP_DATA_DIR.resolve() else (ddir / "teams")

    from .teams import TeamManager

    store = AGPStore(db_path=db_path)
    tm = TeamManager(teams_dir=teams_dir, detached_wakeup=True)
    try:
        if args.team and args.member:
            tm.run_wakeup_sync(args.team, args.member)
        elif args.agent_uri:
            run_agent_wakeup(
                store=store,
                teams_dir=teams_dir,
                agent_uri=args.agent_uri,
                runner_fn=TeamManager._run_agy_cli_turn,
                teams=tm,
                force=args.force,
            )
    finally:
        store.close()


if __name__ == "__main__":
    main()
