"""Standard Serverless MCP Stdio Entrypoint for the `agp-mesh` plugin.

Runs the full AGP v2.1 + Agent Teams MCP server directly in-process over stdio against shared
SQLite WAL (`~/.gemini/agp/agp.db`) and `~/.gemini/agp/teams/` — NO HTTP broker server is
spawned and NO browser window is opened. Opening the Monitor UI (`agp-monitor` /
`npx -y agp-mcp monitor`) is completely optional and only for viewing the dashboard.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _bootstrap_agp_import() -> None:
    candidates = []
    env_home = os.environ.get("AGP_HOME")
    if env_home:
        candidates.append(Path(env_home))
    real = Path(os.path.realpath(__file__))
    # 1. Offline runtime bundled inside installed plugin (`plugins/agp-mesh/runtime/agp`)
    if len(real.parents) >= 2:
        candidates.append(real.parents[1] / "runtime")
    # 2. Direct repo checkout (`<repo>/plugins/agp-mesh/scripts/agp_mcp_stdio.py`)
    if len(real.parents) >= 4:
        candidates.append(real.parents[3])

    for candidate in candidates:
        if (candidate / "agp" / "mcp_server.py").exists():
            cand_str = str(candidate)
            if cand_str not in sys.path:
                sys.path.insert(0, cand_str)
            return


def main() -> None:
    _bootstrap_agp_import()
    from agp.mcp_server import main as mcp_main

    mcp_main()


if __name__ == "__main__":
    main()
