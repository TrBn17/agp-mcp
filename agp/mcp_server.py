"""Standard Serverless MCP Server for AGP v2.0 & Agent Teams (`agp-mcp`).

Runs silently in the background over standard MCP `stdio` transport directly against shared
SQLite WAL (`~/.gemini/agp/agp.db`) and file-backed teams (`~/.gemini/agp/teams/`) — NO HTTP
server needs to be opened or kept running! Opening the web monitor (`agp-monitor` /
`python -m agp.broker`) is strictly optional and ONLY needed when a human wants to view the UI.

Also supports:
  - `agp-mcp` (default: standard stdio MCP server)
  - `agp-mcp --transport http [--host 127.0.0.1] [--port 47821] [--open-browser]`
  - `agp-mcp --print-config [antigravity|claude-desktop|claude-code|cursor|vscode|windsurf|generic]`
  - `agp-mcp --doctor`
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from . import __protocol_version__, __version__
from .broker import AGPBrokerRuntime, create_server
from .config import (
    AGP_DATA_DIR,
    DB_PATH,
    DEFAULT_HOST,
    DEFAULT_PORT,
    PROJECT_ROOT,
    TEAMS_DIR,
    ensure_data_dirs,
)
from .locks import CrossProcessFileLock
from .mcp_protocol import (
    BUNDLED_SKILL_NAMES,
    CODEMODE_TOOLS,
    MCP_PROMPTS,
    MCP_RESOURCE_TEMPLATES,
    MCP_STATIC_RESOURCES,
    MCP_TOOLS,
    _check_optional_monitor_up,
)
from .storage import AGPStore
from .teams import TeamManager
from .transcripts import TranscriptScanner


def _build_runtime(
    data_dir: Path,
    *,
    fast_mode: bool = True,
    codemode: bool = False,
    truncate_by_default: bool = True,
) -> AGPBrokerRuntime:
    ddir = Path(data_dir)
    ddir.mkdir(parents=True, exist_ok=True)
    is_default = ddir.resolve() == AGP_DATA_DIR.resolve()
    db_path = DB_PATH if is_default else (ddir / "agp.db")
    teams_dir = TEAMS_DIR if is_default else (ddir / "teams")
    store = AGPStore(db_path=db_path)
    teams = TeamManager(
        teams_dir=teams_dir,
        detached_wakeup=True,
        locks_dir=store.locks_dir,
    )
    scanner = (
        TranscriptScanner()
        if is_default
        else TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={})
    )
    rt = AGPBrokerRuntime(
        store=store,
        scanner=scanner,
        teams=teams,
        detached_wakeup=True,
        default_agent_uri=os.environ.get("AGP_AGENT_URI") or None,
        fast_mode=fast_mode,
    )
    rt.mcp.codemode_default = bool(codemode)
    rt.mcp.truncate_by_default = bool(truncate_by_default)
    return rt


def run_stdio(
    data_dir: Path = AGP_DATA_DIR,
    *,
    codemode: bool = False,
    truncate_by_default: bool = True,
) -> int:
    """Run the standard embedded MCP server over UTF-8 newline-delimited JSON-RPC 2.0 stdio."""
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    env_cm = os.environ.get("AGP_CODEMODE", "").strip().lower() in {"1", "true", "yes"}
    eff_cm = bool(codemode or env_cm)
    runtime = _build_runtime(data_dir, codemode=eff_cm, truncate_by_default=truncate_by_default)
    session_id = runtime.mcp.create_session(client_surface="mcp")
    stdout_lock = threading.Lock()
    active_workers: set[threading.Thread] = set()
    workers_lock = threading.Lock()
    stop_ev = threading.Event()

    def _write_json(obj: Any) -> None:
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        with stdout_lock:
            try:
                sys.stdout.write(line + "\n")
                sys.stdout.flush()
            except (BrokenPipeError, OSError):
                stop_ev.set()

    def _dispatch_request(payload: Any) -> None:
        try:
            _, resp, _ = runtime.mcp.handle_jsonrpc(payload, session_id=session_id)
            if resp is not None:
                _write_json(resp)
        except Exception as exc:
            req_id = payload.get("id") if isinstance(payload, dict) else None
            if req_id is not None:
                _write_json(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32603, "message": f"Internal MCP server error: {exc}"},
                    }
                )

    def _handle_sig(*_args: Any) -> None:
        stop_ev.set()

    try:
        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)
    except Exception:
        pass

    try:
        for raw_line in sys.stdin:
            if stop_ev.is_set():
                break
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                _write_json(
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": -32700, "message": f"Parse error: {exc.msg}"},
                    }
                )
                continue

            # Process notifications/cancelled & fast control requests inline so cancellations
            # immediately interrupt blocking tools (agp_inbox, agp_ask, agp_task(wait), team_inbox)!
            method = payload.get("method", "") if isinstance(payload, dict) else ""
            if method in {"notifications/cancelled", "notifications/initialized", "initialize", "ping", "logging/setLevel"}:
                _dispatch_request(payload)
                continue

            # Run potentially long-polling tool calls on a worker thread so stdin stays responsive
            def _worker(p: Any) -> None:
                try:
                    _dispatch_request(p)
                finally:
                    with workers_lock:
                        active_workers.discard(threading.current_thread())

            t = threading.Thread(target=_worker, args=(payload,), daemon=True)
            with workers_lock:
                active_workers.add(t)
            t.start()

        # Wait briefly for in-flight requests to finish cleanly on EOF
        with workers_lock:
            pending = list(active_workers)
        for t in pending:
            t.join(timeout=2.0)
    finally:
        # Cleanly sign off ephemeral stdio agent (releases held file leases; keeps auto_cli agents intact)
        if os.environ.get("AGP_AUTO_CLEANUP_ON_EXIT", "1") != "0":
            try:
                bound_uri = runtime.mcp.get_session_agent_uri(session_id)
                if bound_uri:
                    ag = runtime.store.get_agent(bound_uri)
                    if ag and ag.get("wakeup_mode") != "auto_cli":
                        runtime.store.unregister_agent(
                            bound_uri,
                            release_leases=True,
                            reason="MCP stdio session closed",
                        )
            except Exception:
                pass
        runtime.stop()
    return 0


def build_client_config(
    client: str = "generic",
    use_installed_bin: bool = False,
    codemode: bool = False,
    npx_spec: str = "github:TrBn17/agp-mcp",
) -> Dict[str, Any]:
    """Generate standard copy-paste-ready MCP client config JSON for any major MCP host."""
    extra_args = ["--codemode"] if codemode else []
    if use_installed_bin:
        server_entry: Dict[str, Any] = {
            "command": "agp-mcp",
            "args": extra_args,
        }
    else:
        server_entry = {
            "command": "npx",
            "args": ["-y", npx_spec, *extra_args],
        }

    c = (client or "generic").strip().lower()
    if c == "vscode":
        return {"servers": {"agp-broker": {"type": "stdio", **server_entry}}}
    return {"mcpServers": {"agp-broker": server_entry}}


def run_doctor(data_dir: Path = AGP_DATA_DIR) -> Dict[str, Any]:
    """Run full environment & storage health check."""
    ddir = Path(data_dir)
    ddir.mkdir(parents=True, exist_ok=True)
    is_default = ddir.resolve() == AGP_DATA_DIR.resolve()
    db_path = DB_PATH if is_default else (ddir / "agp.db")
    teams_dir = TEAMS_DIR if is_default else (ddir / "teams")

    store = AGPStore(db_path=db_path)
    try:
        wal_mode = store._conn.execute("PRAGMA journal_mode;").fetchone()[0]
        data_ver = store.get_data_version()
        agents_count = len(store.list_agents())
        online_count = len(store.list_agents(status="online"))
        _, lease_res = store.manage_lease("list", owner_uri="agp://local/system/doctor")
        lock_ok = False
        lk = CrossProcessFileLock(store.locks_dir / ".doctor.lock", timeout=2.0)
        if lk.acquire():
            lock_ok = True
            lk.release()
    finally:
        store.close()

    teams_count = sum(1 for p in teams_dir.iterdir() if p.is_dir() and (p / "config.json").exists()) if teams_dir.exists() else 0
    monitor_running = _check_optional_monitor_up()
    agy_path = shutil.which(os.environ.get("AGP_AGENT_CLI") or "agy") or ""
    npx_path = shutil.which("npx") or ""

    return {
        "ok": True,
        "package": "agp-mcp",
        "version": __version__,
        "protocol_version": __protocol_version__,
        "python": sys.version.split()[0],
        "transport_default": "stdio (serverless, zero HTTP daemon required)",
        "storage": {
            "data_dir": str(ddir),
            "db_path": str(db_path),
            "sqlite_journal_mode": wal_mode,
            "sqlite_data_version": data_ver,
            "cross_process_file_lock": lock_ok,
        },
        "mcp_surface": {
            "direct_mode_tools": len(MCP_TOOLS),
            "codemode_tools": len(CODEMODE_TOOLS),
            "tools": len(MCP_TOOLS),
            "skills": len(BUNDLED_SKILL_NAMES),
            "static_resources": len(MCP_STATIC_RESOURCES),
            "resource_templates": len(MCP_RESOURCE_TEMPLATES),
            "prompts": len(MCP_PROMPTS),
            "extensions": ["io.modelcontextprotocol/tasks", "io.modelcontextprotocol/skills"],
            "protocol_versions": sorted(["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25", "2026-07-28"]),
        },
        "live_state": {
            "tracked_agents": agents_count,
            "online_agents": online_count,
            "teams": teams_count,
            "active_leases": len(lease_res.get("leases") or []),
        },
        "cli_runtime": {
            "agy_cli_found": bool(agy_path),
            "agy_cli_path": agy_path,
            "npx_found": bool(npx_path),
            "npx_path": npx_path,
        },
        "optional_monitor_ui": {
            "running": monitor_running,
            "url": f"http://{DEFAULT_HOST}:{DEFAULT_PORT}" if monitor_running else "",
            "note": "Optional — start via `npx -y github:TrBn17/agp-mcp monitor` or `agp-monitor` only when viewing UI.",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="agp-mcp",
        description="Standard Serverless MCP Server for AGP v2.0 Inter-Agent Mesh & Agent Teams.",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="MCP transport mode: 'stdio' (default, serverless background) or 'http' (optional Monitor UI + Streamable HTTP).",
    )
    parser.add_argument("--data-dir", default=str(AGP_DATA_DIR), help="Persistent state directory (default ~/.gemini/agp).")
    parser.add_argument("--host", default=DEFAULT_HOST, help="HTTP host (only used with --transport http).")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="HTTP port (only used with --transport http).")
    parser.add_argument("--open-browser", action="store_true", help="Open browser UI (only used with --transport http).")
    parser.add_argument(
        "--codemode",
        action="store_true",
        help="Enable ultra-compact 4-tool Code Mode (whoami, docs, search, execute — ~900 tokens vs ~6.5k direct).",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Disable default ~6,000-token structural JSON truncation in Code Mode.",
    )
    parser.add_argument(
        "--print-config",
        nargs="?",
        const="generic",
        choices=["generic", "antigravity", "claude-desktop", "claude-code", "cursor", "vscode", "windsurf"],
        help="Print ready-to-use standard MCP client JSON config and exit.",
    )
    parser.add_argument(
        "--use-bin",
        action="store_true",
        help="With --print-config, emit globally installed `agp-mcp` instead of `npx`.",
    )
    parser.add_argument("--doctor", action="store_true", help="Run health diagnostics and exit.")
    parser.add_argument("--version", action="store_true", help="Print version and exit.")
    args = parser.parse_args()

    if args.version:
        print(f"agp-mcp {__version__} (AGP v{__protocol_version__}, MCP 2024-11-05..2026-07-28)")
        return

    if args.print_config:
        print(
            json.dumps(
                build_client_config(
                    args.print_config,
                    use_installed_bin=args.use_bin,
                    codemode=args.codemode,
                ),
                indent=2,
            )
        )
        return

    if args.doctor:
        print(json.dumps(run_doctor(Path(args.data_dir)), ensure_ascii=False, indent=2))
        return

    if args.transport == "http":
        ensure_data_dirs()
        rt = _build_runtime(
            Path(args.data_dir),
            fast_mode=False,
            codemode=args.codemode,
            truncate_by_default=not args.no_truncate,
        )
        server, runtime = create_server(host=args.host, port=args.port, runtime=rt)
        runtime.start_background_scanner(interval_sec=1.0)
        url = f"http://{args.host}:{args.port}"
        print(f"[AGP MCP HTTP & Monitor v{__version__}] Listening on {url} (MCP: {url}/mcp)", flush=True)
        if args.open_browser:
            import webbrowser

            threading.Timer(0.5, lambda: webbrowser.open(url)).start()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            runtime.stop()
            server.server_close()
        return

    raise SystemExit(
        run_stdio(
            data_dir=Path(args.data_dir),
            codemode=args.codemode,
            truncate_by_default=not args.no_truncate,
        )
    )


if __name__ == "__main__":
    main()
