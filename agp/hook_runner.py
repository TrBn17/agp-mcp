"""Serverless in-process Antigravity Lifecycle Hook Runner (`agp-hook`).

Executes `session_start`, `pre_invocation`, `post_invocation`, `pre_tool_use`, `post_tool_use`,
and `stop` directly against shared local SQLite WAL (`~/.gemini/agp/agp.db`) and Team mailboxes
(`~/.gemini/agp/teams/`) WITHOUT starting any HTTP server or popping open any browser window!

Fail-open by design: any unexpected exception outputs the safe default JSON so an AGY session
is never disrupted.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

from .broker import AGPBrokerRuntime
from .config import AGP_DATA_DIR, DB_PATH, TEAMS_DIR
from .storage import AGPStore
from .teams import TeamManager
from .transcripts import TranscriptScanner


def run_hook_event(
    event_name: str,
    payload: Dict[str, Any],
    data_dir: Path = AGP_DATA_DIR,
) -> Dict[str, Any]:
    """Execute a single Antigravity lifecycle hook directly in-process against SQLite WAL."""
    ddir = Path(data_dir)
    ddir.mkdir(parents=True, exist_ok=True)
    is_default = ddir.resolve() == AGP_DATA_DIR.resolve()
    db_path = DB_PATH if is_default else (ddir / "agp.db")
    teams_dir = TEAMS_DIR if is_default else (ddir / "teams")

    store = AGPStore(db_path=db_path)
    teams = TeamManager(teams_dir=teams_dir, detached_wakeup=True, locks_dir=store.locks_dir)
    scanner = TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={})
    runtime = AGPBrokerRuntime(store=store, scanner=scanner, teams=teams, detached_wakeup=True, fast_mode=True)
    try:
        return runtime.handle_hook(event_name, payload)
    finally:
        runtime.stop()


def main() -> None:
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    event_name = sys.argv[1] if len(sys.argv) > 1 else "session_start"
    try:
        raw_stdin = sys.stdin.read()
        payload = json.loads(raw_stdin) if raw_stdin.strip() else {}
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    default_out: Dict[str, Any] = {"decision": "allow"} if event_name == "pre_tool_use" else {}
    data_dir = Path(os.environ.get("AGP_DATA_DIR", str(AGP_DATA_DIR)))

    try:
        result = run_hook_event(event_name, payload, data_dir=data_dir)
        sys.stdout.write(json.dumps(result, ensure_ascii=False))
        sys.stdout.flush()
    except Exception:
        # Fail-open so a hook error never blocks or breaks an AGY session
        sys.stdout.write(json.dumps(default_out))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
