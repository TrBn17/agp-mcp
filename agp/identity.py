"""Unified Human-Readable Agent Identity, Task-Based Naming, Threads & Graph Builder.

Core design principles:
- Name every AI by its role/name + specific task: `Name (Specific Task)` — NEVER raw hex session IDs
  and NEVER identical folder names ("acme-app -> acme-app")
- Automatically link Sessions <-> Team Members <-> Shared Tasks <-> Subagent Roles
- Provide deterministic Avatar monogram + Color + unique @handle across all views
- Group scattered AGP & Team messages into clean Conversation Threads and a Live Communication Graph
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

# Curated accessible 12-color palette (crisp contrast on dark surfaces)
AGENT_PALETTE: List[str] = [
    "#38bdf8",  # sky
    "#34d399",  # emerald
    "#a78bfa",  # violet
    "#fbbf24",  # amber
    "#fb7185",  # rose
    "#2dd4bf",  # teal
    "#818cf8",  # indigo
    "#f97316",  # orange
    "#e879f9",  # fuchsia
    "#4ade80",  # green
    "#60a5fa",  # blue
    "#facc15",  # yellow
]

AUTO_DISPLAY_RE = re.compile(r"^AGY-(CLI|APP|IDE|SDK|SUBAGENT)\b", re.I)
GENERIC_SUBAGENT_NAMES = {"", "self", "research", "subagent", "general-engineer", "detected-session"}

PROMPT_PREFIX_PATTERNS = [
    re.compile(r"^\[AGP TEAM\s+'[^']+'\s*[—\-]+\s*YOU ARE\s+'@([^']+)'[^\]]*\]\s*", re.I),
    re.compile(
        r"^(?:bạn có nhiệm vụ(?:\s+sau)?|nhiệm vụ của bạn|giúp tôi(?:\s+như sau)?|hãy giúp tôi|"
        r"bạn chỉnh sửa lại|bạn hãy|hiện tại,?\s*|hiện\s+thông\s+qua|trên\s+prod\s+đang\s+dính\s+lỗi\s+sau|"
        r"đã\s+implement)\s*[:,\-]?\s*",
        re.I,
    ),
    re.compile(r"^(?:your mission|mission|you are acting as|you are the|perform a thorough,?\s*evidence-backed)\s*[:,\-]?\s*", re.I),
]

TEAM_PROMPT_HANDLE_RE = re.compile(r"\[AGP TEAM\s+'([^']+)'\s*[—\-]+\s*YOU ARE\s+'@([^']+)'", re.I)
PERSONA_BOLD_RE = re.compile(r"(?:you are(?: acting as| the)?|role\s*:)\s*\*\*([^*\n]{3,60})\*\*", re.I)
MISSION_HEADING_RE = re.compile(r"###?\s*Mission\s*\(([^)\n]{4,68})\)", re.I)
MISSION_LINE_RE = re.compile(r"###?\s*Mission\s*\n+([^\n]{6,80})", re.I)
VIET_D_MAP = str.maketrans({"đ": "d", "Đ": "D"})


def ascii_slug(text: str, max_len: int = 24) -> str:
    """Convert Vietnamese/Unicode text to a clean lowercase @handle slug."""
    raw = (text or "").translate(VIET_D_MAP)
    norm = unicodedata.normalize("NFKD", raw)
    stripped = "".join(ch for ch in norm if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", stripped.lower()).strip("-")
    if len(slug) > max_len:
        cut = slug[:max_len]
        if "-" in cut and len(cut) > 10:
            cut = cut.rsplit("-", 1)[0]
        slug = cut.strip("-")
    return slug


def pick_color(seed: str) -> str:
    digest = hashlib.sha256((seed or "agent").encode("utf-8")).digest()
    return AGENT_PALETTE[digest[0] % len(AGENT_PALETTE)]


def make_avatar(name: str) -> str:
    cleaned = re.sub(r"^@", "", (name or "").strip())
    words = [w for w in re.split(r"[\s\-_/·:&()]+", cleaned) if w and any(c.isalnum() for c in w)]
    if len(words) >= 2:
        a = next((c for c in words[0] if c.isalnum()), "")
        b = next((c for c in words[1] if c.isalnum()), "")
        if a and b:
            return (a + b).upper()
    if words:
        alnum = "".join(c for c in words[0] if c.isalnum())
        if len(alnum) >= 2:
            return alnum[:2].upper()
        if alnum:
            return alnum.upper()
    return "AI"


def clean_human_title(raw: str, max_len: int = 44) -> str:
    """Turn a raw session title or user prompt into a clean, concise task phrase."""
    text = " ".join((raw or "").strip().split())
    if not text:
        return ""

    # Strip [AGP TEAM ...] preamble if present
    text = TEAM_PROMPT_HANDLE_RE.sub("", text).strip()
    if text.startswith("]"):
        text = text[1:].strip()

    for pat in PROMPT_PREFIX_PATTERNS:
        text = pat.sub("", text).strip()

    # Remove markdown formatting & noisy path prefixes
    text = re.sub(r"[*_`#]+", "", text).strip()
    text = re.sub(r"^(?:là\s+|để\s+|cho\s+|về\s+)", "", text, flags=re.I).strip()
    if not text:
        return ""

    if len(text) > max_len:
        for sep in (" — ", " - ", ". ", "; ", "\n"):
            idx = text.find(sep)
            if 10 <= idx <= max_len:
                text = text[:idx].strip()
                break
    if len(text) > max_len:
        cut = text[: max_len - 1]
        if " " in cut:
            cut = cut.rsplit(" ", 1)[0]
        text = cut.rstrip(",:;- ") + "…"

    # Capitalize first letter gently
    if text and text[0].islower():
        text = text[0].upper() + text[1:]
    return text


def extract_subagent_role_from_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    m_persona = PERSONA_BOLD_RE.search(prompt)
    if m_persona:
        return clean_human_title(m_persona.group(1), max_len=36)
    m_mission = MISSION_HEADING_RE.search(prompt)
    if m_mission:
        return clean_human_title(m_mission.group(1), max_len=36)
    return ""


def extract_subagent_task_from_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    m_mission = MISSION_HEADING_RE.search(prompt)
    if m_mission:
        return clean_human_title(m_mission.group(1), max_len=40)
    m_line = MISSION_LINE_RE.search(prompt)
    if m_line:
        return clean_human_title(m_line.group(1), max_len=40)
    return clean_human_title(prompt, max_len=40)


def is_generic_display_name(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return True
    if AUTO_DISPLAY_RE.match(s):
        return True
    if s.lower() in {"session", "agent", "unknown", "anonymous"}:
        return True
    return False


def project_from_path(cwd: str) -> str:
    if not cwd:
        return ""
    norm = cwd.replace("\\", "/").rstrip("/")
    parts = [p for p in norm.split("/") if p]
    if not parts:
        return ""
    last = parts[-1]
    return "" if last.endswith(":") else last


def format_role_label(role: str) -> str:
    r = (role or "").strip()
    if not r or r in {"general-engineer", "detected-session", "unknown", "teammate"}:
        return ""
    return r


NOISY_SUBAGENT_TASK_RE = re.compile(
    r"^(?:read\b|first,?\s*read\b|act\s+as\b|you\s+are\b|both\s+the\b|the\s+(?:code|technical|frontend|backend|security)|"
    r"agency\s+|code\s+reviewer\b|perform\s+a\s+thorough\b|final\s+cross-pillar\b)",
    re.I,
)


def _texts_overlap_heavily(a: str, b: str) -> bool:
    """Avoid redundant labels like `Tiến Độ Nhánh Toán (Tiến Độ Nhánh Toán)` while allowing rich task elaboration."""
    sa = ascii_slug(a, max_len=60)
    sb = ascii_slug(b, max_len=60)
    if not sa or not sb:
        return True
    if sa == sb:
        return True
    wa = set(sa.split("-"))
    wb = set(sb.split("-"))
    if not wa or not wb:
        return False
    jaccard = len(wa & wb) / len(wa | wb)
    return jaccard >= 0.65


def compose_full_label(base_name: str, task_label: str) -> str:
    """Build `Name (Specific Task)` cleanly without redundancy, nested parens, or raw session IDs."""
    b = (base_name or "").strip()
    t = (task_label or "").strip()
    # If base_name already carries a specific parenthetical scope, don't stack a second (...)
    if "(" in b and ")" in b:
        return b
    if b and t and not _texts_overlap_heavily(b, t):
        short_t = t if len(t) <= 38 else (t[:35].rsplit(" ", 1)[0].rstrip(",:;- ") + "…")
        return f"{b} ({short_t})"
    return b or t or "AI Agent"


class IdentityRegistry:
    """Resolves every AGP URI, conversation_id, and team member to a task-aware human Identity."""

    def __init__(
        self,
        sessions: List[Dict[str, Any]],
        teams: List[Dict[str, Any]],
        db_agents: Optional[List[Dict[str, Any]]] = None,
        db_summaries: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self._by_uri: Dict[str, Dict[str, Any]] = {}
        self._by_cid: Dict[str, Dict[str, Any]] = {}
        self._by_handle: Dict[str, Dict[str, Any]] = {}
        self._team_member_by_cid: Dict[str, Tuple[str, Dict[str, Any]]] = {}
        self._team_member_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._team_task_by_owner: Dict[Tuple[str, str], str] = {}
        self._used_handles: set[str] = {"you", "all", "human", "monitor", "system"}

        # 1. Index team memberships & active/latest assigned task per team member
        for t in teams:
            tname = t.get("team", "")
            for m in t.get("members") or []:
                mname = m.get("name", "")
                if mname:
                    self._team_member_by_key[(tname, mname)] = m
                cid = (m.get("conversation_id") or "").strip()
                if cid and cid not in self._team_member_by_cid:
                    self._team_member_by_cid[cid] = (tname, m)
            # Pick best task title per owner (prefer in_progress > pending > completed)
            status_prio = {"in_progress": 0, "pending": 1, "blocked": 2, "completed": 3}
            sorted_tasks = sorted(
                t.get("tasks") or [],
                key=lambda tk: (status_prio.get(tk.get("status", ""), 9), -(tk.get("updated_at") or 0)),
            )
            for tk in sorted_tasks:
                owner = (tk.get("owner") or "").strip()
                title = (tk.get("title") or "").strip()
                if owner and title and (tname, owner) not in self._team_task_by_owner:
                    self._team_task_by_owner[(tname, owner)] = clean_human_title(title, max_len=38)

        # 2. Index live/merged sessions (roots first so root sessions claim cleanest handles)
        ordered = sorted(
            sessions,
            key=lambda s: (1 if s.get("is_subagent") else 0, -(s.get("last_activity_at") or 0)),
        )
        for s in ordered:
            ident = self._build_session_identity(s)
            self._register(ident)

        # 3. Index historical SQLite agents not in current live sessions
        for ag in db_agents or []:
            uri = ag.get("agent_uri") or ""
            cid = ag.get("conversation_id") or ""
            if uri in self._by_uri or (cid and cid in self._by_cid):
                continue
            sum_meta = (db_summaries or {}).get(cid) if cid else None
            pseudo = {
                "agent_uri": uri,
                "conversation_id": cid,
                "display_name": ag.get("display_name", ""),
                "custom_name": ag.get("custom_name", ""),
                "role": ag.get("role", ""),
                "surface": ag.get("surface", "cli"),
                "cwd": ag.get("cwd") or (sum_meta.get("workspace_path", "") if sum_meta else ""),
                "title": (sum_meta.get("title", "") if sum_meta else "") or ag.get("last_user_input", ""),
                "last_user_prompt": ag.get("last_user_input", ""),
                "agent_name": sum_meta.get("agent_name", "") if sum_meta else "",
                "is_subagent": bool(sum_meta.get("parent_conversation_id")) if sum_meta else False,
                "status": "offline",
                "activity_state": "offline",
            }
            ident = self._build_session_identity(pseudo)
            self._register(ident)

        self._db_summaries = db_summaries or {}

    def _allocate_handle(self, base: str, cid: str) -> str:
        slug = ascii_slug(base, max_len=22)
        if not slug:
            slug = f"agent-{cid[:4]}" if cid else "agent"
        if slug not in self._used_handles:
            self._used_handles.add(slug)
            return slug
        for i in range(2, 99):
            cand = f"{slug}-{i}"
            if cand not in self._used_handles:
                self._used_handles.add(cand)
                return cand
        fallback = f"{slug}-{cid[:4]}" if cid else f"{slug}-x"
        self._used_handles.add(fallback)
        return fallback

    def _build_session_identity(self, s: Dict[str, Any]) -> Dict[str, Any]:
        cid = (s.get("conversation_id") or "").strip()
        uri = (s.get("agent_uri") or "").strip() or (f"agp://local/{s.get('surface', 'cli')}/{cid}" if cid else "")
        cwd = s.get("cwd") or s.get("inferred_workspace") or ""
        project = project_from_path(cwd)
        surface = (s.get("surface") or "cli").lower()
        is_sub = bool(s.get("is_subagent"))

        team_info = self._team_member_by_cid.get(cid)
        team_name = team_info[0] if team_info else ""
        team_member = team_info[1] if team_info else None
        team_handle = team_member.get("name", "") if team_member else ""
        team_role = team_member.get("role", "") if team_member else ""

        custom_name = (s.get("custom_name") or "").strip()
        explicit_display = (s.get("display_name") or "").strip()
        if is_generic_display_name(explicit_display):
            explicit_display = ""
        else:
            # Strip noisy "(acme-app)" suffix if project is already shown as a badge
            if project and explicit_display.endswith(f"({project})"):
                trimmed = explicit_display[: -(len(project) + 2)].strip()
                if trimmed:
                    explicit_display = trimmed

        subagent_role = (s.get("subagent_role") or "").strip()
        subagent_task = (s.get("subagent_task") or "").strip()
        agent_name = (s.get("agent_name") or "").strip()
        if agent_name.lower() in GENERIC_SUBAGENT_NAMES:
            agent_name = ""

        raw_title = (s.get("title") or "").strip()
        if raw_title.lower().startswith("session ") and len(raw_title) <= 20:
            raw_title = ""

        # Detect managed team wakeup prompt inside title
        m_team = TEAM_PROMPT_HANDLE_RE.search(raw_title)
        if m_team and not team_handle:
            team_name = team_name or m_team.group(1)
            team_handle = m_team.group(2)

        team_task_title = self._team_task_by_owner.get((team_name, team_handle), "") if (team_name and team_handle) else ""
        clean_title = clean_human_title(raw_title, max_len=42)
        clean_prompt = clean_human_title(s.get("first_user_prompt") or s.get("last_user_prompt") or "", max_len=42)

        # Specific task label for this AI
        if is_sub:
            cand_task = (
                extract_subagent_task_from_prompt(s.get("first_user_prompt") or s.get("last_user_prompt") or "")
                or clean_human_title(subagent_task, max_len=40)
                or clean_title
            )
            # Drop boilerplate instruction fragments ("Read...", "You are...", "Agency ...") when Role already names the subagent
            if subagent_role and (not cand_task or len(cand_task) < 8 or NOISY_SUBAGENT_TASK_RE.match(cand_task)):
                task_label = ""
            else:
                task_label = cand_task
        else:
            task_label = team_task_title or clean_title or clean_prompt

        role = format_role_label(s.get("role", "")) or format_role_label(team_role)

        # Base name priority: Custom Rename > Registered Name > Subagent Role > Team Handle > Task-Based AI Name
        name_source = "fallback"
        if custom_name:
            base_name = custom_name
            name_source = "custom"
        elif explicit_display:
            base_name = explicit_display
            name_source = "registered"
        elif is_sub and subagent_role:
            base_name = subagent_role
            name_source = "subagent_role"
        elif team_handle:
            base_name = f"@{team_handle}"
            name_source = "team"
        elif is_sub and agent_name:
            base_name = agent_name
            name_source = "subagent_name"
        elif is_sub:
            inferred = extract_subagent_role_from_prompt(s.get("first_user_prompt") or s.get("last_user_prompt") or raw_title)
            if inferred:
                base_name = inferred
                name_source = "subagent_prompt"
            elif task_label:
                base_name = task_label
                name_source = "task"
            else:
                base_name = f"Subagent ({s.get('agent_name') or 'worker'})"
                name_source = "fallback"
        elif role and task_label:
            base_name = role.replace("-", " ").title()
            name_source = "role_task"
        elif task_label:
            # No explicit agent nickname: name the AI directly by its specific task!
            base_name = task_label
            name_source = "task"
        elif role:
            base_name = role.replace("-", " ").title()
            name_source = "role"
        elif project:
            base_name = f"AI {project}"
            name_source = "project"
        else:
            base_name = f"AI {surface.upper()}"
            name_source = "fallback"

        # If user set a custom name explicitly, respect it as full_label unless it's super short and has a distinct task
        if name_source == "custom":
            full_label = base_name
        else:
            full_label = compose_full_label(base_name, task_label)

        # Clean unique @handle
        if team_handle:
            handle = self._allocate_handle(team_handle, cid)
        elif custom_name:
            handle = self._allocate_handle(custom_name, cid)
        elif explicit_display:
            handle = self._allocate_handle(explicit_display, cid)
        elif is_sub and subagent_role:
            handle = self._allocate_handle(subagent_role, cid)
        elif role:
            handle = self._allocate_handle(role, cid)
        elif task_label:
            handle = self._allocate_handle(task_label, cid)
        else:
            handle = self._allocate_handle(project or surface, cid)

        seed = cid or uri or handle
        color = pick_color(seed)
        avatar = make_avatar(team_handle or base_name)

        return {
            "agent_uri": uri,
            "conversation_id": cid,
            "short_id": cid[:6] if cid else uri.split("/")[-1][:6],
            "name": base_name,
            "task_label": task_label,
            "full_label": full_label,
            "handle": handle,
            "at_handle": f"@{handle}",
            "role": role,
            "project": project,
            "surface": surface,
            "avatar": avatar,
            "color": color,
            "team": team_name,
            "team_handle": team_handle,
            "is_subagent": is_sub,
            "is_custom_name": bool(custom_name),
            "name_source": name_source,
            "status": s.get("status", "offline"),
            "activity_state": s.get("activity_state", "offline"),
        }

    def _register(self, ident: Dict[str, Any]) -> None:
        if ident.get("agent_uri"):
            self._by_uri[ident["agent_uri"]] = ident
        if ident.get("conversation_id"):
            self._by_cid[ident["conversation_id"]] = ident
        if ident.get("handle"):
            self._by_handle[ident["handle"]] = ident

    def get_by_session(self, s: Dict[str, Any]) -> Dict[str, Any]:
        cid = s.get("conversation_id") or ""
        uri = s.get("agent_uri") or ""
        if cid and cid in self._by_cid:
            return self._by_cid[cid]
        if uri and uri in self._by_uri:
            return self._by_uri[uri]
        ident = self._build_session_identity(s)
        self._register(ident)
        return ident

    def resolve_uri(self, uri: str, role_hint: str = "") -> Dict[str, Any]:
        """Resolve ANY AGP URI to a clean task-aware Identity."""
        u = (uri or "").strip()
        if not u:
            return self._special_identity("", "Không rõ", "unknown", "#64748b", "??")

        if "/human/" in u or u in {"human", "you", "user"}:
            return self._special_identity(u, "Bạn (Operator)", "you", "#38bdf8", "ME", is_human=True)

        if "/channel/all" in u or u in {"all", "@all"}:
            return self._special_identity(u, "Tất cả AGY", "all", "#a78bfa", "ALL", is_channel=True)

        if "/channel/" in u:
            ch = u.split("/channel/", 1)[1].strip("/")
            return self._special_identity(u, f"Kênh #{ch}", ch, "#a78bfa", "#", is_channel=True)

        if "/system/" in u:
            return self._special_identity(u, "Hệ thống Monitor", "system", "#94a3b8", "SY")

        if u in self._by_uri:
            return self._by_uri[u]

        tail = u.split("/")[-1]
        if tail in self._by_cid:
            return self._by_cid[tail]
        clean_h = tail.lstrip("@").lower()
        if clean_h in self._by_handle:
            return self._by_handle[clean_h]

        if tail in self._db_summaries:
            meta = self._db_summaries[tail]
            pseudo = {
                "agent_uri": u,
                "conversation_id": tail,
                "surface": meta.get("surface", "cli"),
                "title": meta.get("title", ""),
                "cwd": meta.get("workspace_path", ""),
                "role": role_hint,
                "agent_name": meta.get("agent_name", ""),
                "is_subagent": bool(meta.get("parent_conversation_id")),
                "status": "offline",
                "activity_state": "offline",
            }
            ident = self._build_session_identity(pseudo)
            self._register(ident)
            return ident

        if tail in self._team_member_by_cid:
            tname, m = self._team_member_by_cid[tail]
            return self.resolve_team_member(tname, m.get("name", ""), m)

        if role_hint and role_hint not in {"unknown", "general-engineer"}:
            label = role_hint.replace("-", " ").title()
            return self._special_identity(
                u, label, ascii_slug(role_hint) or tail[:6], pick_color(u), make_avatar(label), role=role_hint, cid=tail
            )

        return self._special_identity(
            u,
            f"AI Agent ({tail[:6]})",
            f"agy-{tail[:6]}",
            pick_color(u),
            make_avatar(tail[:2]),
            cid=tail,
        )

    def resolve_team_member(
        self,
        team: str,
        member_name: str,
        member_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mname = (member_name or "").strip().lstrip("@")
        if mname in {"human", "user", "you"}:
            return self._special_identity("agp://local/human/monitor", "Bạn (Operator)", "you", "#38bdf8", "ME", is_human=True)
        if mname == "all":
            return self._special_identity("agp://local/channel/all", f"Cả team #{team}", "all", "#a78bfa", "ALL", is_channel=True)

        info = member_info or self._team_member_by_key.get((team, mname)) or {}
        cid = (info.get("conversation_id") or "").strip()
        task_title = self._team_task_by_owner.get((team, mname), "")
        if cid and cid in self._by_cid:
            base = dict(self._by_cid[cid])
            base["team"] = team
            base["team_handle"] = mname
            if task_title and not base.get("task_label"):
                base["task_label"] = task_title
                base["full_label"] = compose_full_label(base["name"], task_title)
            return base

        role = format_role_label(info.get("role", ""))
        display = f"@{mname}" if mname else "Teammate"
        task_or_role = task_title or (role.replace("-", " ").title() if role else "")
        full_label = compose_full_label(display, task_or_role)
        seed = cid or f"team:{team}:{mname}"
        return {
            "agent_uri": f"agp://local/cli/{cid}" if cid else f"agp://local/team/{team}/{mname}",
            "conversation_id": cid,
            "short_id": cid[:6] if cid else mname[:6],
            "name": display,
            "task_label": task_or_role,
            "full_label": full_label,
            "handle": mname or "member",
            "at_handle": f"@{mname}" if mname else "@member",
            "role": role,
            "project": project_from_path(info.get("cwd", "")),
            "surface": "cli",
            "avatar": make_avatar(mname),
            "color": pick_color(seed),
            "team": team,
            "team_handle": mname,
            "is_subagent": False,
            "is_custom_name": False,
            "name_source": "team",
            "status": "online" if info.get("status") in {"working", "starting", "idle"} else "offline",
            "activity_state": "running_tool" if info.get("status") in {"working", "starting"} else info.get("status", "idle"),
        }

    def resolve_target_to_uri(self, target: str) -> Optional[str]:
        """Resolve an agent URI, @handle, or conversation_id (full or prefix) to a canonical AGP URI."""
        t = (target or "").strip()
        if not t:
            return None
        if t.startswith("agp://"):
            return t
        if t in {"all", "@all"}:
            return "agp://local/channel/all"
        if t in self._by_cid:
            return self._by_cid[t]["agent_uri"]
        slug = t.lstrip("@").lower()
        if slug in self._by_handle:
            return self._by_handle[slug]["agent_uri"]
        if len(t) >= 6:
            matches = [v["agent_uri"] for k, v in self._by_cid.items() if k.startswith(t)]
            if len(matches) == 1:
                return matches[0]
        return None

    @staticmethod
    def _special_identity(
        uri: str,
        name: str,
        handle: str,
        color: str,
        avatar: str,
        role: str = "",
        cid: str = "",
        is_human: bool = False,
        is_channel: bool = False,
    ) -> Dict[str, Any]:
        return {
            "agent_uri": uri,
            "conversation_id": cid,
            "short_id": cid[:6] if cid else handle[:6],
            "name": name,
            "task_label": "",
            "full_label": name,
            "handle": handle,
            "at_handle": f"@{handle}",
            "role": role,
            "project": "",
            "surface": "human" if is_human else ("channel" if is_channel else "cli"),
            "avatar": avatar,
            "color": color,
            "team": "",
            "team_handle": "",
            "is_subagent": False,
            "is_custom_name": False,
            "name_source": "special",
            "is_human": is_human,
            "is_channel": is_channel,
            "status": "online" if (is_human or is_channel) else "offline",
            "activity_state": "idle" if (is_human or is_channel) else "offline",
        }


def build_conversations_and_graph(
    sessions: List[Dict[str, Any]],
    messages: List[Dict[str, Any]],
    teams: List[Dict[str, Any]],
    registry: IdentityRegistry,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build unified Conversation Threads (direct pairs, broadcast, teams) and Live Communication Graph."""
    threads: Dict[str, Dict[str, Any]] = {}
    edge_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    node_stats: Dict[str, Dict[str, int]] = {}
    referenced_identities: Dict[str, Dict[str, Any]] = {}

    def _node_key(ident: Dict[str, Any]) -> str:
        return ident.get("conversation_id") or ident.get("agent_uri") or ident.get("handle") or "unknown"

    def _record_edge(
        src_id: Dict[str, Any],
        dst_id: Dict[str, Any],
        msg_type: str,
        summary: str,
        ts: float,
        pending: bool,
        is_conflict: bool,
    ) -> None:
        sk = _node_key(src_id)
        dk = _node_key(dst_id)
        if sk == dk or dst_id.get("is_channel"):
            return
        referenced_identities[sk] = src_id
        referenced_identities[dk] = dst_id
        node_stats.setdefault(sk, {"sent": 0, "recv": 0})["sent"] += 1
        node_stats.setdefault(dk, {"sent": 0, "recv": 0})["recv"] += 1

        ek = (sk, dk)
        edge = edge_map.get(ek)
        if not edge:
            edge = {
                "from_key": sk,
                "to_key": dk,
                "from_name": src_id["full_label"],
                "to_name": dst_id["full_label"],
                "count": 0,
                "last_at": 0.0,
                "last_type": msg_type,
                "last_summary": "",
                "has_pending": False,
                "has_conflict": False,
            }
            edge_map[ek] = edge
        edge["count"] += 1
        if ts >= edge["last_at"]:
            edge["last_at"] = ts
            edge["last_type"] = msg_type
            edge["last_summary"] = summary[:120]
        if pending:
            edge["has_pending"] = True
        if is_conflict:
            edge["has_conflict"] = True

    # 1. Process AGP bus messages chronologically
    for m in sorted(messages, key=lambda x: (x.get("_meta") or {}).get("created_at") or 0):
        meta = m.get("_meta") or {}
        resolved = m.get("_resolved") or {}
        src = resolved.get("from") or registry.resolve_uri(
            (m.get("from") or {}).get("uri", ""), (m.get("from") or {}).get("role", "")
        )
        dst = resolved.get("to") or registry.resolve_uri((m.get("to") or {}).get("uri", ""))
        ts = float(meta.get("created_at") or 0.0)
        status = meta.get("status", "queued")
        msg_type = m.get("type", "chat.note")
        summary = m.get("summary", "")

        is_broadcast = bool(dst.get("is_channel"))
        if is_broadcast:
            ch_id = f"channel:{dst['handle']}"
            ch_title = dst["full_label"]
            ch_kind = "broadcast"
            participants = [dst]
        else:
            sk = _node_key(src)
            dk = _node_key(dst)
            pair = sorted([sk, dk])
            ch_id = f"direct:{pair[0]}::{pair[1]}"
            p1 = src if sk == pair[0] else dst
            p2 = dst if sk == pair[0] else src
            ch_title = f"{p1['name']} ↔ {p2['name']}"
            ch_kind = "direct"
            participants = [p1, p2]

        th = threads.get(ch_id)
        if not th:
            th = {
                "channel_id": ch_id,
                "kind": ch_kind,
                "title": ch_title,
                "participants": participants,
                "team": "",
                "message_count": 0,
                "unread_count": 0,
                "conflict_count": 0,
                "last_message_at": 0.0,
                "last_preview": "",
                "last_sender_name": "",
                "messages": [],
            }
            threads[ch_id] = th

        entry = {
            "id": m.get("msg_id") or f"agp_{meta.get('seq', 0)}",
            "source": "agp",
            "seq": meta.get("seq"),
            "from": src,
            "to": dst,
            "type": msg_type,
            "priority": m.get("priority", "normal"),
            "status": status,
            "summary": summary,
            "payload": m.get("payload") or {},
            "verbatim_constraints": m.get("verbatim_constraints") or [],
            "artifacts": m.get("artifacts") or [],
            "trace_id": m.get("trace_id", ""),
            "correlation_id": m.get("correlation_id", ""),
            "in_reply_to": m.get("in_reply_to"),
            "requires_ack": bool(m.get("requires_ack")),
            "injection_flagged": bool((m.get("security") or {}).get("injection_flagged")),
            "created_at": ts,
            "timestamp": m.get("timestamp", ""),
            "is_system_noise": False,
        }
        th["messages"].append(entry)
        th["message_count"] += 1
        if status == "queued":
            th["unread_count"] += 1
        if msg_type in {"conflict.report", "hitl.escalate"}:
            th["conflict_count"] += 1
        if ts >= th["last_message_at"]:
            th["last_message_at"] = ts
            th["last_preview"] = summary[:120]
            th["last_sender_name"] = src["name"]

        _record_edge(
            src,
            dst,
            msg_type,
            summary,
            ts,
            pending=(status == "queued"),
            is_conflict=(msg_type in {"conflict.report", "hitl.escalate"}),
        )

    # 2. Process Team channels
    for t in teams:
        tname = t.get("team", "")
        t_msgs = t.get("messages") or []
        members_resolved = [
            registry.resolve_team_member(tname, m.get("name", ""), m) for m in (t.get("members") or [])
        ]
        ch_id = f"team:{tname}"
        th = {
            "channel_id": ch_id,
            "kind": "team",
            "title": f"#{tname}",
            "description": t.get("description", ""),
            "team": tname,
            "participants": members_resolved,
            "message_count": 0,
            "unread_count": 0,
            "conflict_count": 0,
            "last_message_at": float(t.get("created_at") or 0.0),
            "last_preview": t.get("description", ""),
            "last_sender_name": "",
            "messages": [],
        }
        for tm in sorted(t_msgs, key=lambda x: x.get("created_at") or 0):
            src = (tm.get("_resolved") or {}).get("from") or registry.resolve_team_member(tname, tm.get("from", ""))
            dst = (tm.get("_resolved") or {}).get("to") or registry.resolve_team_member(tname, tm.get("to", ""))
            ts = float(tm.get("created_at") or 0.0)
            kind = tm.get("kind", "message")
            text = tm.get("text", "")
            is_noise = kind == "idle_notification"
            entry = {
                "id": tm.get("id", ""),
                "source": "team",
                "team": tname,
                "from": src,
                "to": dst,
                "type": kind,
                "priority": "normal",
                "status": "read" if tm.get("read") else "queued",
                "summary": text,
                "payload": {},
                "verbatim_constraints": [],
                "artifacts": [],
                "created_at": ts,
                "timestamp": tm.get("timestamp", ""),
                "is_system_noise": is_noise,
            }
            th["messages"].append(entry)
            th["message_count"] += 1
            if not tm.get("read") and not is_noise:
                th["unread_count"] += 1
            if ts >= th["last_message_at"] and not is_noise:
                th["last_message_at"] = ts
                th["last_preview"] = text[:120]
                th["last_sender_name"] = src["name"]

            if not is_noise:
                _record_edge(src, dst, kind, text, ts, pending=not tm.get("read"), is_conflict=False)

        threads[ch_id] = th

    thread_list = sorted(
        threads.values(),
        key=lambda x: (x["last_message_at"], x["message_count"]),
        reverse=True,
    )
    for th in thread_list:
        if len(th["messages"]) > 100:
            th["messages"] = th["messages"][-100:]

    # 3. Build Graph Nodes
    sess_by_key: Dict[str, Dict[str, Any]] = {}
    for s in sessions:
        ident = s.get("identity") or registry.get_by_session(s)
        k = _node_key(ident)
        sess_by_key[k] = s
        if not s.get("is_subagent") and s.get("status") == "online":
            referenced_identities.setdefault(k, ident)

    graph_nodes: List[Dict[str, Any]] = []
    for k, ident in referenced_identities.items():
        s = sess_by_key.get(k)
        stats = node_stats.get(k, {"sent": 0, "recv": 0})
        graph_nodes.append(
            {
                "key": k,
                "identity": ident,
                "status": s["status"] if s else ident.get("status", "offline"),
                "activity_state": s["activity_state"] if s else ident.get("activity_state", "offline"),
                "current_action": s.get("current_action", "") if s else "",
                "total_tokens": (s.get("tree_total_tokens") or s.get("total_tokens", 0)) if s else 0,
                "subagent_count": s.get("subagent_count", 0) if s else 0,
                "msg_sent": stats["sent"],
                "msg_recv": stats["recv"],
                "msg_total": stats["sent"] + stats["recv"],
            }
        )

    graph_nodes.sort(
        key=lambda n: (
            0 if n["msg_total"] > 0 else 1,
            0 if n["status"] == "online" else 1,
            -n["msg_total"],
            -n["total_tokens"],
        )
    )
    kept_nodes = graph_nodes[:12]
    kept_keys = {n["key"] for n in kept_nodes}
    kept_edges = [e for e in edge_map.values() if e["from_key"] in kept_keys and e["to_key"] in kept_keys]
    kept_edges.sort(key=lambda e: (-e["last_at"], -e["count"]))

    return thread_list, {"nodes": kept_nodes, "edges": kept_edges}
