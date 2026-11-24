"""Structural valid-JSON tool result truncation (inspired by cloudflare/mcp).

Caps oversized MCP tool outputs at ~6,000 tokens while keeping JSON syntactically valid:
  - Largest values shrink first so small metadata fields stay intact
  - Lists preserve complete leading items and append `"--- TRUNCATED --- N more items"`
  - Objects preserve keys where possible or add a `"--- TRUNCATED ---"` summary entry
  - Plain strings clip with `--- TRUNCATED --- N chars`
  - Callers piping straight to files can pass `truncate=False` (or `?truncateToolResult=false`)
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List

CHARS_PER_TOKEN = 4
MAX_TOKENS = 6000
MAX_CHARS = MAX_TOKENS * CHARS_PER_TOKEN
TRUNCATION_MARKER = "--- TRUNCATED ---"
MIN_SLOT = 128

TRUNCATE_ARGUMENT_DESCRIPTION = (
    "Cap the result at ~6,000 tokens using structural valid-JSON truncation. "
    "Set to false only when piping the full output outside the model context."
)


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


def _size(value: Any) -> int:
    return len(_compact_json(value))


def _clip_string(text: str, max_chars: int) -> str:
    if len(_compact_json(text)) <= max_chars:
        return text
    if max_chars <= 24:
        return TRUNCATION_MARKER[: max(0, max_chars - 2)]
    budget = max(8, max_chars - len(TRUNCATION_MARKER) - 24)
    if len(text) <= budget:
        return text
    omitted = len(text) - budget
    return f"{text[:budget]} {TRUNCATION_MARKER} {omitted:,} chars"


def _shrink_value(value: Any, max_chars: int) -> Any:
    if _size(value) <= max_chars:
        return value
    if isinstance(value, str):
        return _clip_string(value, max_chars)
    if isinstance(value, list):
        return _shrink_list(value, max_chars)
    if isinstance(value, dict):
        return _shrink_dict(value, max_chars)
    return value


def _allocate_budgets(sizes: List[int], total_budget: int) -> List[int]:
    n = len(sizes)
    if n == 0:
        return []
    alloc = [0] * n
    remaining = max(0, total_budget)
    order = sorted(range(n), key=lambda i: sizes[i])
    left = n
    for idx in order:
        fair = remaining // left if left > 0 else 0
        take = min(sizes[idx], fair)
        alloc[idx] = take
        remaining -= take
        left -= 1
    return alloc


def _shrink_list(items: List[Any], max_chars: int) -> List[Any]:
    if not items:
        return items
    # Reserve space for trailing truncation marker
    tail_budget = len(_compact_json(f"{TRUNCATION_MARKER} {len(items):,} more items")) + 4
    avail = max(16, max_chars - tail_budget)
    kept: List[Any] = []
    used = 2  # []
    for item in items:
        item_sz = _size(item) + (1 if kept else 0)
        if used + item_sz <= avail:
            kept.append(item)
            used += item_sz
        elif not kept and avail >= MIN_SLOT:
            # Keep at least 1 shrunk item so caller sees record structure
            kept.append(_shrink_value(item, avail - 2))
            break
        else:
            break
    dropped = len(items) - len(kept)
    if dropped > 0:
        kept.append(f"{TRUNCATION_MARKER} {dropped:,} more items")
    return kept


def _shrink_dict(obj: Dict[str, Any], max_chars: int) -> Dict[str, Any]:
    if not obj:
        return obj
    entries = list(obj.items())
    # Account for keys and JSON punctuation
    key_overhead = sum(len(_compact_json(str(k))) + 2 for k, _ in entries) + 2
    if key_overhead + len(entries) * 16 <= max_chars:
        val_budget = max(0, max_chars - key_overhead)
        sizes = [_size(v) for _, v in entries]
        allocs = _allocate_budgets(sizes, val_budget)
        return {k: _shrink_value(v, max(16, allocs[i])) for i, (k, v) in enumerate(entries)}

    # Too many keys: drop largest entries first and list omitted keys cleanly
    by_size = sorted(range(len(entries)), key=lambda i: _size(entries[i][1]), reverse=True)
    dropped_idx: set[int] = set()
    est = _size(obj)
    for idx in by_size:
        if est <= max_chars - 64:
            break
        dropped_idx.add(idx)
        est -= len(_compact_json(str(entries[idx][0]))) + _size(entries[idx][1]) + 3

    out: Dict[str, Any] = {}
    omitted_keys: List[str] = []
    for i, (k, v) in enumerate(entries):
        if i in dropped_idx:
            omitted_keys.append(str(k))
        else:
            out[k] = v

    if omitted_keys:
        marker_key = TRUNCATION_MARKER
        while marker_key in out:
            marker_key += " "
        note = f"{len(omitted_keys):,} keys omitted: {', '.join(omitted_keys[:12])}"
        if len(omitted_keys) > 12:
            note += f" (+{len(omitted_keys) - 12} more)"
        out[marker_key] = note

    # Final safety shrink if still slightly over budget
    if _size(out) > max_chars:
        remaining_entries = list(out.items())
        key_ov = sum(len(_compact_json(str(k))) + 2 for k, _ in remaining_entries) + 2
        val_b = max(0, max_chars - key_ov)
        allocs = _allocate_budgets([_size(v) for _, v in remaining_entries], val_b)
        out = {k: _shrink_value(v, max(16, allocs[i])) for i, (k, v) in enumerate(remaining_entries)}
    return out


def format_tool_result(
    content: Any,
    *,
    truncate: bool = True,
    max_tokens: int = MAX_TOKENS,
) -> str:
    """Render a tool result as text; when `truncate=True`, structurally cap at `max_tokens`."""
    max_chars = max(512, int(max_tokens) * CHARS_PER_TOKEN)

    if isinstance(content, str):
        if not truncate or len(content) <= max_chars:
            return content
        stripped = content.lstrip()
        if stripped.startswith(("{", "[")):
            try:
                parsed = json.loads(content)
                return _compact_json(_shrink_value(parsed, max_chars))
            except json.JSONDecodeError:
                pass
        est_tokens = math.ceil(len(content) / CHARS_PER_TOKEN)
        return (
            f"{content[:max_chars]}\n\n{TRUNCATION_MARKER}\n"
            f"Response was ~{est_tokens:,} tokens (limit: {max_tokens:,}). "
            "Pass truncate=false only if piping outside model context, or narrow your query."
        )

    pretty = _pretty_json(content)
    if not truncate or len(pretty) <= max_chars:
        return pretty

    compact = _compact_json(content)
    if len(compact) <= max_chars:
        return compact

    shrunk = _shrink_value(content, max_chars)
    return _compact_json(shrunk)
