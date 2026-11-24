"""Serverless Antigravity Lifecycle Hook Entrypoint for `agp-mesh`.

Executes lifecycle hooks (`session_start`, `pre_invocation`, `post_invocation`,
`pre_tool_use`, `post_tool_use`, `stop`) directly against local SQLite WAL + Team mailboxes.
Never spawns an HTTP broker and never opens a browser window!
"""

from __future__ import annotations

import json
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
    # 2. Direct repo checkout (`<repo>/plugins/agp-mesh/scripts/agp_hook.py`)
    if len(real.parents) >= 4:
        candidates.append(real.parents[3])

    for candidate in candidates:
        if (candidate / "agp" / "hook_runner.py").exists():
            cand_str = str(candidate)
            if cand_str not in sys.path:
                sys.path.insert(0, cand_str)
            return


def main() -> None:
    event_name = sys.argv[1] if len(sys.argv) > 1 else "session_start"
    default_out = {"decision": "allow"} if event_name == "pre_tool_use" else {}
    try:
        _bootstrap_agp_import()
        from agp.hook_runner import main as hook_main

        hook_main()
    except Exception:
        sys.stdout.write(json.dumps(default_out))
        sys.stdout.flush()


if __name__ == "__main__":
    main()
