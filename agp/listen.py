"""Zero-token reactive background inbox listener for live interactive AGY sessions.

How it works:
  1. An interactive AGY session (CLI / IDE / Antigravity 2.0) launches this script as a
     non-blocking background task (`run_command`, `WaitMsBeforeAsync=500`, `IsDaemon=false`)
     and ends its turn (0 LLM tokens while sleeping).
  2. `agp.listen` registers `listener_pid` + heartbeat in SQLite WAL so every peer
     immediately sees `listening: true` (`delivery.will_auto_receive: true`).
  3. It watches BOTH SQLite AGP messages (`PRAGMA data_version`) AND all Team mailboxes
     under `~/.gemini/agp/teams/*/inboxes/*.json` matching this session's `conversation_id`
     or explicit `--team`/`--name` — dynamically picking up NEW teams the agent is joined to!
  4. The instant ANY message arrives, it atomically drains & deduplicates the messages,
     prints them with unambiguous reply routing instructions to stdout, and EXITS with code 0.
  5. Antigravity's Reactive Wakeup immediately resumes the live interactive session with the
     received messages, zero wasted polling tokens!
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import AGP_DATA_DIR, DB_PATH, PROJECT_ROOT, SURFACE_PRESENCE_DIRS, TEAMS_DIR
from .locks import CrossProcessFileLock, atomic_write_json, read_json_safe
from .security import format_team_injection, format_untrusted_injection
from .storage import AGPStore, epoch_now

if os.name == "nt":
    import msvcrt
else:
    import fcntl


def is_conversation_presence_locked(
    conversation_id: str,
    presence_dirs: Optional[Dict[str, Path]] = None,
) -> bool:
    """Return True if an active interactive AGY process currently holds the OS presence lock."""
    cid = (conversation_id or "").strip()
    if not cid:
        return False
    dirs = presence_dirs if presence_dirs is not None else SURFACE_PRESENCE_DIRS
    for pdir in dirs.values():
        lock_file = Path(pdir) / f"{cid}.lock"
        if not lock_file.exists():
            continue
        try:
            with lock_file.open("r+b") as fh:
                try:
                    if os.name == "nt":
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    return False
                except OSError:
                    return True
        except OSError:
            return True
    return False


def build_listen_command(
    agent_uri: str,
    team: str = "",
    member_name: str = "",
    data_dir: Optional[Path] = None,
) -> str:
    """Build a portable zero-token background listener command for an AGY session."""
    script_path = (PROJECT_ROOT / "agp" / "listen.py").resolve()
    parts = [f'python "{script_path.as_posix()}"', f'--agent "{agent_uri}"']
    if team and member_name:
        parts.append(f'--team "{team}" --name "{member_name}"')
    if data_dir and Path(data_dir).resolve() != AGP_DATA_DIR.resolve():
        parts.append(f'--data-dir "{Path(data_dir).resolve().as_posix()}"')
    return " ".join(parts)


def collect_unread_team_messages_for_agent(
    teams_dir: Path,
    conversation_id: str = "",
    explicit_team: str = "",
    explicit_name: str = "",
    mark_read: bool = True,
    exclude_ids: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    """Scan all team mailboxes linked to conversation_id (or explicit team/name) and collect unread messages."""
    tdir_root = Path(teams_dir)
    if not tdir_root.exists():
        return []
    cid = (conversation_id or "").strip()
    skip_ids = exclude_ids or set()
    targets: List[Tuple[Path, str, str]] = []

    if explicit_team and explicit_name:
        tdir = tdir_root / explicit_team
        if tdir.is_dir():
            targets.append((tdir, explicit_team, explicit_name))

    if cid:
        try:
            for tdir in tdir_root.iterdir():
                if not tdir.is_dir():
                    continue
                cfg = read_json_safe(tdir / "config.json", None)
                if not isinstance(cfg, dict):
                    continue
                tslug = str(cfg.get("team") or tdir.name)
                for mname, minfo in (cfg.get("members") or {}).items():
                    if str((minfo or {}).get("conversation_id") or "").strip() == cid:
                        pair = (tdir, tslug, str(mname))
                        if pair not in targets:
                            targets.append(pair)
        except OSError:
            pass

    collected: List[Dict[str, Any]] = []
    is_default_teams = tdir_root.resolve() == TEAMS_DIR.resolve()
    locks_dir = (AGP_DATA_DIR / "locks") if is_default_teams else (tdir_root.parent / "locks")
    for tdir, tslug, mname in targets:
        inbox_path = tdir / "inboxes" / f"{mname}.json"
        if not inbox_path.exists():
            continue
        lock = CrossProcessFileLock(locks_dir / f"team_{tslug}.lock", timeout=3.0)
        try:
            with lock:
                inbox = read_json_safe(inbox_path, [])
                if not isinstance(inbox, list):
                    continue
                unread = [
                    m for m in inbox
                    if isinstance(m, dict)
                    and not m.get("read")
                    and m.get("kind") not in {"shutdown_request"}
                ]
                if not unread:
                    continue
                if mark_read:
                    u_ids = {m.get("id") for m in unread}
                    for item in inbox:
                        if isinstance(item, dict) and item.get("id") in u_ids:
                            item["read"] = True
                    atomic_write_json(inbox_path, inbox)
                for m in unread:
                    mid = str(m.get("id") or "")
                    if mid and mid in skip_ids:
                        continue
                    item_copy = dict(m)
                    item_copy.setdefault("team", tslug)
                    item_copy.setdefault("to", mname)
                    collected.append(item_copy)
        except TimeoutError:
            continue
    return collected


def drain_unified_inbox(
    store: AGPStore,
    teams_dir: Path,
    agent_uri: str,
    conversation_id: str = "",
    explicit_team: str = "",
    explicit_name: str = "",
    mark_read: bool = True,
    limit: int = 20,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Atomically drain both AGP SQLite messages and Team mailbox messages without duplicates."""
    agp_msgs = store.poll_inbox(
        agent_uri=agent_uri,
        include_delivered=False,
        limit=limit,
        mark_delivered=mark_read,
    )
    bridged_team_ids: set[str] = set()
    for m in agp_msgs:
        p = m.get("payload") or {}
        if isinstance(p, dict) and p.get("_team_msg_id"):
            bridged_team_ids.add(str(p["_team_msg_id"]))
        idem = str(m.get("idempotency_key") or "")
        if idem.startswith("team_bridge_"):
            parts = idem.split("_")
            if len(parts) >= 4:
                bridged_team_ids.add(f"{parts[2]}_{parts[3]}")

    cid = conversation_id or (agent_uri.rsplit("/", 1)[-1] if agent_uri.startswith("agp://local/") else "")
    team_msgs = collect_unread_team_messages_for_agent(
        teams_dir=teams_dir,
        conversation_id=cid,
        explicit_team=explicit_team,
        explicit_name=explicit_name,
        mark_read=mark_read,
        exclude_ids=bridged_team_ids,
    )
    return agp_msgs, team_msgs


def format_unified_wakeup_output(
    agent_uri: str,
    agp_msgs: List[Dict[str, Any]],
    team_msgs: List[Dict[str, Any]],
    listen_cmd: str = "",
) -> str:
    sections: List[str] = [
        f"[AGP REACTIVE WAKEUP — NEW PEER MESSAGE(S) FOR `{agent_uri}`]",
    ]
    if agp_msgs:
        sections.append(format_untrusted_injection(agp_msgs, recipient_uri=agent_uri))
    if team_msgs:
        sections.append(format_team_injection(team_msgs))
    if listen_cmd:
        sections.append(
            "[STAY REACHABLE AT 0 TOKENS]\n"
            "After processing & replying above, if you expect further peer replies while idle, "
            f"re-launch the background listener (`run_command`, `WaitMsBeforeAsync=500`, `IsDaemon=false`):\n"
            f"  {listen_cmd}"
        )
    return "\n\n".join(sections)


def listen_once(
    agent_uri: str,
    data_dir: Path = AGP_DATA_DIR,
    team: str = "",
    member_name: str = "",
    timeout_sec: float = 1800.0,
    poll_interval_sec: float = 0.25,
    output_json: bool = False,
) -> int:
    """Wait efficiently at 0 token cost until any AGP or Team message arrives, print it, and exit."""
    ddir = Path(data_dir)
    db_path = DB_PATH if ddir.resolve() == AGP_DATA_DIR.resolve() else (ddir / "agp.db")
    teams_dir = TEAMS_DIR if ddir.resolve() == AGP_DATA_DIR.resolve() else (ddir / "teams")
    store = AGPStore(db_path=db_path)
    cid = agent_uri.rsplit("/", 1)[-1] if agent_uri.startswith("agp://local/") and "/mcp/" not in agent_uri else ""

    lock_key = hashlib.sha256(agent_uri.encode("utf-8")).hexdigest()[:16]
    singleton_lock = CrossProcessFileLock(store.locks_dir / f"listen_{lock_key}.lock", blocking=False)
    if not singleton_lock.acquire():
        msg = {"ok": False, "already_listening": True, "agent_uri": agent_uri}
        if output_json:
            print(json.dumps(msg, ensure_ascii=False), flush=True)
        else:
            print(f"[AGP Listen] Another listener is already active for {agent_uri}.", flush=True)
        store.close()
        return 0

    stop_requested = False

    def _handle_sig(*_args: Any) -> None:
        nonlocal stop_requested
        stop_requested = True

    try:
        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)
    except Exception:
        pass

    pid = os.getpid()
    store.set_listener_state(agent_uri, pid=pid)
    last_hb = time.monotonic()
    deadline = (time.monotonic() + timeout_sec) if timeout_sec > 0 else None
    listen_cmd = build_listen_command(agent_uri, team=team, member_name=member_name, data_dir=ddir)

    try:
        while not stop_requested:
            agp_msgs, team_msgs = drain_unified_inbox(
                store=store,
                teams_dir=teams_dir,
                agent_uri=agent_uri,
                conversation_id=cid,
                explicit_team=team,
                explicit_name=member_name,
                mark_read=True,
            )
            if agp_msgs or team_msgs:
                if output_json:
                    print(
                        json.dumps(
                            {
                                "ok": True,
                                "event": "messages_received",
                                "agent_uri": agent_uri,
                                "agp_messages": agp_msgs,
                                "team_messages": team_msgs,
                                "listen_command": listen_cmd,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        flush=True,
                    )
                else:
                    print(
                        format_unified_wakeup_output(agent_uri, agp_msgs, team_msgs, listen_cmd=listen_cmd),
                        flush=True,
                    )
                return 0

            now_m = time.monotonic()
            if deadline is not None and now_m >= deadline:
                if output_json:
                    print(
                        json.dumps(
                            {"ok": True, "event": "timeout", "agent_uri": agent_uri, "agp_messages": [], "team_messages": []},
                            ensure_ascii=False,
                        ),
                        flush=True,
                    )
                else:
                    print(
                        f"[AGP Listen] Timeout ({int(timeout_sec)}s) with no incoming messages for {agent_uri}. "
                        f"Re-run if you still want to stay reachable:\n  {listen_cmd}",
                        flush=True,
                    )
                return 0

            if (now_m - last_hb) >= 10.0:
                store.set_listener_state(agent_uri, pid=pid, heartbeat_at=epoch_now())
                last_hb = now_m

            time.sleep(max(0.05, float(poll_interval_sec)))
    finally:
        try:
            store.set_listener_state(agent_uri, pid=None)
        except Exception:
            pass
        store.close()
        singleton_lock.release()
    return 0


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(
        description="Zero-token reactive background inbox listener for AGP & Agent Teams."
    )
    parser.add_argument("--agent", required=True, help="Your AGP URI (e.g. agp://local/cli/<conversation_id>)")
    parser.add_argument("--team", default="", help="Optional team slug to also watch explicitly")
    parser.add_argument("--name", default="", help="Optional member handle in --team")
    parser.add_argument("--timeout", type=float, default=1800.0, help="Max seconds to wait (0 = indefinite, default 1800)")
    parser.add_argument("--poll-interval", type=float, default=0.25, help="Poll interval in seconds (default 0.25)")
    parser.add_argument("--data-dir", default=str(AGP_DATA_DIR), help="AGP data directory")
    parser.add_argument("--json", action="store_true", help="Output received messages as JSON")
    args = parser.parse_args()

    code = listen_once(
        agent_uri=args.agent,
        data_dir=Path(args.data_dir),
        team=args.team,
        member_name=args.name,
        timeout_sec=args.timeout,
        poll_interval_sec=args.poll_interval,
        output_json=args.json,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    # Allow running directly via `python D:/.../agp/listen.py` as well as `python -m agp.listen`
    if __package__ in (None, ""):
        root = str(Path(__file__).resolve().parent.parent)
        if root not in sys.path:
            sys.path.insert(0, root)
        from agp.listen import main as _pkg_main  # type: ignore
        _pkg_main()
    else:
        main()
