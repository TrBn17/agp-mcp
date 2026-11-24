"""Configuration and runtime paths for AGP Broker & AGY Monitoring."""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Dict, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = PROJECT_ROOT / "web"
PLUGIN_DIR = PROJECT_ROOT / "plugins" / "agp-mesh"

DEFAULT_HOST = os.environ.get("AGP_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("AGP_PORT", "47821"))
BROKER_URL = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"

GEMINI_HOME = Path(os.environ.get("GEMINI_HOME", str(Path.home() / ".gemini")))
AGP_DATA_DIR = Path(os.environ.get("AGP_DATA_DIR", str(GEMINI_HOME / "agp")))
DB_PATH = AGP_DATA_DIR / "agp.db"
ARTIFACT_DIR = AGP_DATA_DIR / "artifacts"
TRACE_DIR = AGP_DATA_DIR / "traces"
KEYS_DIR = AGP_DATA_DIR / "keys"
LOCKS_DIR = AGP_DATA_DIR / "locks"
TEAMS_DIR = AGP_DATA_DIR / "teams"
KEY_FILE = AGP_DATA_DIR / "broker.key"
PID_FILE = AGP_DATA_DIR / "broker.pid"
LOG_FILE = AGP_DATA_DIR / "broker.log"
SETTINGS_FILE = AGP_DATA_DIR / "monitor_settings.json"
ALLOW_LOOPBACK_WEBHOOKS = os.environ.get("AGP_ALLOW_LOOPBACK_WEBHOOKS", "1") != "0"

# Antigravity surface directories under ~/.gemini
SURFACE_BRAIN_DIRS: Dict[str, Path] = {
    "cli": GEMINI_HOME / "antigravity-cli" / "brain",
    "app": GEMINI_HOME / "antigravity" / "brain",
    "ide": GEMINI_HOME / "antigravity-ide" / "brain",
}

SURFACE_PRESENCE_DIRS: Dict[str, Path] = {
    "cli": GEMINI_HOME / "antigravity-cli" / "presence",
    "app": GEMINI_HOME / "antigravity" / "presence",
    "ide": GEMINI_HOME / "antigravity-ide" / "presence",
}

SURFACE_SUMMARIES_DBS: Dict[str, Path] = {
    "cli": GEMINI_HOME / "antigravity-cli" / "conversation_summaries.db",
    "app": GEMINI_HOME / "antigravity" / "conversation_summaries.db",
    "ide": GEMINI_HOME / "antigravity-ide" / "conversation_summaries.db",
}

# Protocol governance defaults
DEFAULT_HEARTBEAT_TTL_SEC = 90
DEFAULT_LEASE_TTL_SEC = 120
DEFAULT_MESSAGE_TTL_SEC = 86400  # 24h so unread messages never silently expire while a peer is busy/idle
DEFAULT_MAX_HOPS = 4
HARD_MAX_HOPS = 6
DEFAULT_MAX_CONTEXT_TOKENS_PER_MSG = 4000
DEFAULT_SUMMARY_MAX_TOKENS = 220
CIRCUIT_FAILURE_WINDOW = 5
CIRCUIT_FAILURE_THRESHOLD = 3
CIRCUIT_COOLDOWN_SEC = 60
MAX_STOP_CONTINUATIONS = 3
INLINE_ARTIFACT_MAX_BYTES = 16384
LISTENER_STALE_SEC = 45.0

# Estimated USD cost per 1M tokens: (input_per_1m, cache_read_per_1m, output_per_1m)
# Clearly surfaced as estimates in the UI.
MODEL_PRICING_PER_1M: Dict[str, Tuple[float, float, float]] = {
    "flash_lite": (0.10, 0.025, 0.40),
    "flash": (0.30, 0.075, 2.50),
    "carbon": (1.25, 0.31, 10.00),
    "pro": (1.25, 0.31, 10.00),
    "neon": (2.50, 0.625, 15.00),
    "opus": (5.00, 0.50, 25.00),
    "sonnet": (3.00, 0.30, 15.00),
    "default": (1.25, 0.31, 10.00),
}


def ensure_data_dirs() -> None:
    """Create local AGP state directories if missing."""
    for directory in (AGP_DATA_DIR, ARTIFACT_DIR, TRACE_DIR, KEYS_DIR, LOCKS_DIR, TEAMS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def get_or_create_broker_key() -> str:
    """Load or generate the local same-user shared secret key."""
    ensure_data_dirs()
    if KEY_FILE.exists():
        key = KEY_FILE.read_text(encoding="utf-8").strip()
        if key:
            return key
    key = secrets.token_urlsafe(32)
    KEY_FILE.write_text(key, encoding="utf-8")
    try:
        os.chmod(KEY_FILE, 0o600)
    except OSError:
        pass
    return key


def estimate_cost_usd(
    model: str,
    input_tokens: int,
    cache_read_tokens: int,
    output_tokens: int,
) -> float:
    """Estimate USD cost for a given model name and token counts."""
    m = (model or "").lower()
    rates = MODEL_PRICING_PER_1M["default"]
    for key, candidate in MODEL_PRICING_PER_1M.items():
        if key != "default" and key in m:
            rates = candidate
            break
    in_rate, cache_rate, out_rate = rates
    cost = (
        (max(0, input_tokens) / 1_000_000.0) * in_rate
        + (max(0, cache_read_tokens) / 1_000_000.0) * cache_rate
        + (max(0, output_tokens) / 1_000_000.0) * out_rate
    )
    return round(cost, 6)
