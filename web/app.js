(() => {
  "use strict";

  // Calm, high-contrast pastel avatar palettes (paired bg + ink for Light & Dark modes)
  const AVATAR_SWATCHES_LIGHT = [
    { bg: "#e0e7ff", fg: "#3730a3" }, // indigo
    { bg: "#d1fae5", fg: "#065f46" }, // emerald
    { bg: "#fef3c7", fg: "#92400e" }, // amber
    { bg: "#ffe4e6", fg: "#9f1239" }, // rose
    { bg: "#e0f2fe", fg: "#075985" }, // sky
    { bg: "#f3e8ff", fg: "#6b21a8" }, // purple
    { bg: "#ccfbf1", fg: "#115e59" }, // teal
    { bg: "#ffedd5", fg: "#9a3412" }, // orange
  ];

  const AVATAR_SWATCHES_DARK = [
    { bg: "rgba(99, 102, 241, 0.22)", fg: "#a5b4fc" },
    { bg: "rgba(16, 185, 129, 0.22)", fg: "#6ee7b7" },
    { bg: "rgba(245, 158, 11, 0.22)", fg: "#fcd34d" },
    { bg: "rgba(244, 63, 94, 0.22)", fg: "#fda4af" },
    { bg: "rgba(56, 189, 248, 0.22)", fg: "#7dd3fc" },
    { bg: "rgba(168, 85, 247, 0.22)", fg: "#d8b4fe" },
    { bg: "rgba(20, 184, 166, 0.22)", fg: "#5eead4" },
    { bg: "rgba(249, 115, 22, 0.22)", fg: "#fdba74" },
  ];

  const state = {
    snapshot: null,
    theme: localStorage.getItem("agy_monitor_theme") || "light",
    mode: "overview", // overview | agent | chat | teams
    fleetFilter: "active", // active | working | all
    projectFilter: "all",
    search: "",
    // Agent detail
    selectedAgentId: null,
    agentTab: "overview", // overview | chat | subagents | steps
    // Chat
    selectedChannelId: "__all__",
    hideNoise: true,
    newestFirst: false,
    showMap: false,
    // Teams
    selectedTeam: "",
    // Render stability keys (prevent scroll/details flicker on SSE)
    lastChatKey: "",
    lastComposerCh: null,
    lastSyncMs: Date.now(),
    serverTimeAtSync: Date.now() / 1000,
    sse: null,
    autoSwitchedOnce: false,
  };

  const STATUS_TEXT = {
    running_tool: "Đang chạy",
    thinking: "Đang suy luận",
    communicating: "Đang trao đổi",
    waiting_hitl: "Cần duyệt",
    idle: "Đang chờ",
    offline: "Đã xong",
    working: "Đang làm",
    starting: "Đang bật",
    shutdown: "Đã tắt",
    error: "Lỗi",
  };

  const MSG_TYPE_TEXT = {
    "task.request": "Giao việc",
    "task.ack": "Xác nhận",
    "task.result": "Kết quả",
    "task.progress": "Tiến độ",
    "task.clarify": "Làm rõ",
    "task.cancel": "Hủy việc",
    "review.request": "Nhờ review",
    "review.verdict": "Đánh giá",
    "sync.git_handoff": "Bàn giao Git",
    "conflict.report": "Cảnh báo xung đột",
    "conflict.arbitrate": "Phân xử",
    "hitl.escalate": "Cần duyệt gấp",
    "hitl.decision": "Đã duyệt",
    "chat.note": "Trao đổi",
    message: "Tin Team",
    broadcast: "Thông báo nhóm",
    spawn_brief: "Giao nhiệm vụ",
    task_completed: "Hoàn thành task",
    idle_notification: "Nghỉ chờ lệnh",
    shutdown_request: "Yêu cầu tắt",
  };

  const DELIVERY_TEXT = {
    queued: "Chờ đọc",
    delivered: "Đã giao",
    accepted: "Đã nhận",
    completed: "Hoàn tất",
    rejected: "Từ chối",
    expired: "Hết hạn",
    read: "Đã đọc",
  };

  const TASK_STATUS_TEXT = {
    pending: "Chờ làm",
    blocked: "Bị chặn",
    in_progress: "Đang làm",
    completed: "Hoàn thành",
  };

  const $ = (id) => document.getElementById(id);

  function esc(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function hashIdx(seed, mod) {
    let h = 0;
    const s = String(seed || "ai");
    for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
    return h % mod;
  }

  function avatarStyleFor(ident) {
    const palette = state.theme === "dark" ? AVATAR_SWATCHES_DARK : AVATAR_SWATCHES_LIGHT;
    const key = ident?.conversation_id || ident?.agent_uri || ident?.handle || ident?.name || "ai";
    const sw = palette[hashIdx(key, palette.length)];
    return `background:${sw.bg};color:${sw.fg}`;
  }

  function renderAvatar(ident, sizeClass = "", showDot = false) {
    const st = ident?.activity_state || "offline";
    return `
      <span class="avatar ${sizeClass}" style="${avatarStyleFor(ident)}">
        ${esc(ident?.avatar || "AI")}
        ${showDot ? `<span class="avatar-status dot-${esc(st)}"></span>` : ""}
      </span>
    `;
  }

  function applyTheme(theme) {
    state.theme = theme === "dark" ? "dark" : "light";
    document.documentElement.setAttribute("data-theme", state.theme);
    localStorage.setItem("agy_monitor_theme", state.theme);
    $("themeIcon").textContent = state.theme === "dark" ? "☀️" : "🌙";
    $("themeLabel").textContent = state.theme === "dark" ? "Sáng" : "Tối";
    if (state.snapshot) {
      state.lastChatKey = "";
      renderAll();
    }
  }

  function fmtTokens(n) {
    const v = Number(n || 0);
    if (v >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M";
    if (v >= 1_000) return (v / 1_000).toFixed(1) + "k";
    return String(Math.round(v));
  }

  function fmtCost(c) {
    const v = Number(c || 0);
    if (v >= 1) return "$" + v.toFixed(2);
    if (v >= 0.01) return "$" + v.toFixed(3);
    return "$" + v.toFixed(4);
  }

  function fmtDuration(sec) {
    const s = Math.max(0, Math.floor(Number(sec || 0)));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const r = s % 60;
    if (h > 0) return `${h}g ${String(m).padStart(2, "0")}p`;
    if (m > 0) return `${m}p ${String(r).padStart(2, "0")}s`;
    return `${r}s`;
  }

  function serverNow() {
    return state.serverTimeAtSync + Math.max(0, (Date.now() - state.lastSyncMs) / 1000);
  }

  function fmtAgo(epochSec) {
    if (!epochSec) return "";
    const d = Math.max(0, Math.floor(serverNow() - epochSec));
    if (d < 10) return "vừa xong";
    if (d < 60) return `${d}s trước`;
    if (d < 3600) return `${Math.floor(d / 60)}p trước`;
    if (d < 86400) return `${Math.floor(d / 3600)}g trước`;
    return `${Math.floor(d / 86400)} ngày trước`;
  }

  function fmtClock(epochSec) {
    if (!epochSec) return "";
    try {
      return new Date(epochSec * 1000).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });
    } catch {
      return "";
    }
  }

  function isWorking(s) {
    return ["running_tool", "thinking", "communicating", "waiting_hitl"].includes(s?.activity_state);
  }

  function liveElapsed(s) {
    if (isWorking(s) && s.started_at) return Math.max(0, serverNow() - s.started_at);
    return Number(s.elapsed_sec || 0);
  }

  function cleanPreview(text, maxLen = 84) {
    if (!text) return "";
    const flat = String(text)
      .replace(/```[\s\S]*?```/g, " [code] ")
      .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
      .replace(/[*_`#>]/g, "")
      .replace(/\s+/g, " ")
      .trim();
    return flat.length > maxLen ? flat.slice(0, maxLen - 1) + "…" : flat;
  }

  function humanAction(rawAction, stateCode, lastReply = "") {
    const raw = String(rawAction || "").trim();
    if (!raw || raw.startsWith("Idle (") || raw === "Session started") {
      if (lastReply) return `Đã trả lời: ${cleanPreview(lastReply, 64)}`;
      return stateCode === "offline" ? "Đã hoàn tất" : "Đang chờ lệnh mới";
    }
    if (raw === "Replied to user" || (raw === "Responding to user" && !["running_tool", "thinking", "communicating"].includes(stateCode))) {
      return lastReply ? `Đã trả lời: ${cleanPreview(lastReply, 64)}` : "Đã trả lời";
    }
    if (raw === "Responding to user") return "Đang trả lời...";
    if (raw.startsWith("Reasoning")) return "Đang suy luận...";

    const first = raw.split(", ")[0];
    const m = first.match(/^([a-zA-Z0-9_.-]+)\(([\s\S]*)\)?$/);
    if (m) {
      const tool = m[1];
      const arg = m[2].replace(/\)$/, "").trim();
      const base = arg.replace(/\\/g, "/").split("/").pop() || arg;
      if (tool === "read_file") return `Đọc file ${base}`;
      if (tool === "edit_file" || tool === "replace_file_content") return `Sửa file ${base}`;
      if (tool === "write_file") return `Ghi file ${base}`;
      if (tool === "run_command") return `Chạy lệnh: ${arg.slice(0, 52)}`;
      if (tool === "search_web") return `Tìm kiếm web: ${arg.slice(0, 46)}`;
      if (tool === "invoke_subagent") return `Gọi subagent: ${arg.slice(0, 44)}`;
      if (tool.includes("agp_send") || tool.includes("team_send")) return "Gửi tin cho AI khác";
      if (tool.includes("agp_inbox") || tool.includes("team_inbox")) return "Đọc tin nhắn mới";
      return `${tool}: ${arg.slice(0, 46)}`;
    }
    return raw.slice(0, 70);
  }

  // Separate crisp Name vs Task subtitle cleanly (never duplicate or clutter!)
  function agentNameAndTask(ident) {
    const name = (ident?.name || "AI Agent").trim();
    const task = (ident?.task_label || "").trim();
    const showTask = task && task.toLowerCase() !== name.toLowerCase() && !name.toLowerCase().includes(task.toLowerCase());
    return { name, task: showTask ? task : "" };
  }

  function getRootSessions() {
    const all = state.snapshot?.sessions || [];
    const byId = new Set(all.map((x) => x.conversation_id));
    return all.filter((s) => !s.is_subagent || !s.parent_conversation_id || !byId.has(s.parent_conversation_id));
  }

  function getAllUnifiedMessages() {
    const convs = state.snapshot?.conversations || [];
    const seen = new Set();
    const out = [];
    convs.forEach((ch) => {
      (ch.messages || []).forEach((m) => {
        const k = `${m.source}:${m.id}:${m.created_at}`;
        if (!seen.has(k)) {
          seen.add(k);
          out.push(m);
        }
      });
    });
    return out;
  }

  // =========================================================================
  // Mode Switching
  // =========================================================================
  function setMode(mode) {
    state.mode = mode;
    $("paneOverview").classList.toggle("active", mode === "overview");
    $("paneAgent").classList.toggle("active", mode === "agent");
    $("paneChat").classList.toggle("active", mode === "chat");
    $("paneTeams").classList.toggle("active", mode === "teams");

    $("navOverview").classList.toggle("active", mode === "overview");
    $("navAllChat").classList.toggle("active", mode === "chat" && state.selectedChannelId === "__all__");
    $("navTeams").classList.toggle("active", mode === "teams");

    renderAll();
  }

  function openAgent(convId, opts = {}) {
    state.selectedAgentId = convId;
    state.agentTab = opts.tab || "overview";
    $("renameForm").classList.toggle("hidden", !opts.openRename);
    $("agPingStatus").textContent = "";
    setMode("agent");
    if (opts.openRename) {
      $("rnName").focus();
      $("rnName").select();
    }
  }

  function openChannel(channelId) {
    state.selectedChannelId = channelId || "__all__";
    state.lastChatKey = "";
    setMode("chat");
  }

  // =========================================================================
  // Header & Left Sidebar
  // =========================================================================
  function renderHeader(snap) {
    const k = snap.kpis || {};
    const workingTotal = (k.working_agents ?? 0) + (k.working_subagents ?? 0);
    $("kpiWorking").textContent = String(workingTotal);
    $("kpiOnline").textContent = String(k.online_agents ?? 0);
    $("kpiTokens").textContent = fmtTokens(k.total_tokens_24h);
    $("kpiCache").textContent = `${k.cache_hit_pct_24h ?? 0}%`;
    $("kpiCost").textContent = fmtCost(k.estimated_cost_usd_24h);

    const leases = Number(k.active_leases || 0);
    $("kpiLeasesWrap").classList.toggle("hidden", leases === 0);
    $("kpiLeases").textContent = String(leases);

    $("cntActiveNav").textContent = String(k.online_agents ?? 0);
    $("cntMsgNav").textContent = String(k.messages_count ?? 0);
    $("cntTeamNav").textContent = String(k.team_count ?? 0);

    const pending = Number(k.pending_messages || 0);
    $("cntPendingNav").textContent = String(pending);
    $("cntPendingNav").classList.toggle("hidden", pending === 0);
  }

  function renderSidebar(snap) {
    const roots = getRootSessions();
    const activeRoots = roots.filter((s) => s.status === "online");
    const q = state.search.trim().toLowerCase();

    // 1. Active AIs in Sidebar
    const shownAgents = (activeRoots.length ? activeRoots : roots.slice(0, 6)).filter((s) => {
      if (!q) return true;
      const idn = s.identity || {};
      return `${idn.full_label} ${idn.name} ${idn.task_label} ${idn.project}`.toLowerCase().includes(q);
    });

    const agList = $("sideAgentsList");
    if (!shownAgents.length) {
      agList.innerHTML = `<div class="side-empty">Không có AI nào đang mở</div>`;
    } else {
      agList.innerHTML = shownAgents
        .map((s) => {
          const idn = s.identity || {};
          const { name, task } = agentNameAndTask(idn);
          const isSel = state.mode === "agent" && state.selectedAgentId === s.conversation_id;
          const st = s.activity_state || "offline";
          return `
            <button class="side-item ${isSel ? "active" : ""}" type="button" data-side-agent="${esc(s.conversation_id)}">
              ${renderAvatar(idn, "avatar-sm", true)}
              <div class="side-item-body">
                <div class="side-item-top">
                  <span class="side-item-title">${esc(name)}</span>
                  <span class="side-item-meta">${esc(STATUS_TEXT[st] || st)}</span>
                </div>
                <span class="side-item-sub">${esc(task || humanAction(s.current_action, st, s.last_assistant_reply))}</span>
              </div>
            </button>
          `;
        })
        .join("");

      agList.querySelectorAll("[data-side-agent]").forEach((btn) => {
        btn.addEventListener("click", () => openAgent(btn.getAttribute("data-side-agent")));
      });
    }

    // 2. Recent AI Conversations in Sidebar
    const convs = (snap.conversations || []).filter((ch) => {
      if (!q) return true;
      return `${ch.title} ${ch.last_preview} ${(ch.participants || []).map((p) => p.full_label).join(" ")}`
        .toLowerCase()
        .includes(q);
    });
    $("sideConvCount").textContent = convs.length ? `${convs.length} luồng` : "";

    const thList = $("sideThreadsList");
    if (!convs.length) {
      thList.innerHTML = `<div class="side-empty">Chưa có hội thoại AI nào</div>`;
    } else {
      thList.innerHTML = convs
        .slice(0, 14)
        .map((ch) => {
          const isSel = state.mode === "chat" && state.selectedChannelId === ch.channel_id;
          const p0 = (ch.participants || [])[0] || { avatar: "AI" };
          const avHtml =
            ch.kind === "team"
              ? `<span class="avatar avatar-sm" style="background:var(--accent-soft);color:var(--accent)">#</span>`
              : renderAvatar(p0, "avatar-sm");
          return `
            <button class="side-item ${isSel ? "active" : ""}" type="button" data-side-ch="${esc(ch.channel_id)}">
              ${avHtml}
              <div class="side-item-body">
                <div class="side-item-top">
                  <span class="side-item-title" title="${esc(ch.title)}">${esc(ch.title)}</span>
                  <span class="side-item-meta">${ch.message_count} tin</span>
                </div>
                <span class="side-item-sub">${esc(ch.last_preview || "Chưa có tin nhắn")}</span>
              </div>
            </button>
          `;
        })
        .join("");

      thList.querySelectorAll("[data-side-ch]").forEach((btn) => {
        btn.addEventListener("click", () => openChannel(btn.getAttribute("data-side-ch")));
      });
    }
  }

  // =========================================================================
  // MODE A: OVERVIEW (CLEAN AGENT CARDS)
  // =========================================================================
  function renderOverview() {
    const roots = getRootSessions();
    const activeCnt = roots.filter((s) => s.status === "online").length;
    const workingCnt = roots.filter((s) => isWorking(s) || (s.active_subagent_count || 0) > 0).length;

    $("segCntActive").textContent = String(activeCnt);
    $("segCntWorking").textContent = String(workingCnt);
    $("segCntAll").textContent = String(roots.length);
    $("segActive").classList.toggle("active", state.fleetFilter === "active");
    $("segWorking").classList.toggle("active", state.fleetFilter === "working");
    $("segAll").classList.toggle("active", state.fleetFilter === "all");

    $("overviewHeading").textContent =
      state.fleetFilter === "active"
        ? "AI đang hoạt động"
        : state.fleetFilter === "working"
        ? "AI đang xử lý tác vụ"
        : "Tất cả phiên AI gần đây";

    // Project chips (only when >1 project exists)
    const projCounts = new Map();
    roots.forEach((s) => {
      const p = s.identity?.project || "";
      if (p) projCounts.set(p, (projCounts.get(p) || 0) + 1);
    });
    const pBar = $("projectBar");
    if (projCounts.size <= 1) {
      pBar.classList.add("hidden");
      state.projectFilter = "all";
    } else {
      pBar.classList.remove("hidden");
      pBar.innerHTML = [
        `<button class="chip-btn ${state.projectFilter === "all" ? "active" : ""}" data-proj="all" type="button">Tất cả dự án</button>`,
        ...[...projCounts.entries()].map(
          ([p, cnt]) =>
            `<button class="chip-btn ${state.projectFilter === p ? "active" : ""}" data-proj="${esc(p)}" type="button">${esc(p)} (${cnt})</button>`
        ),
      ].join("");
      pBar.querySelectorAll("[data-proj]").forEach((btn) => {
        btn.addEventListener("click", () => {
          state.projectFilter = btn.getAttribute("data-proj");
          renderOverview();
        });
      });
    }

    const q = state.search.trim().toLowerCase();
    const list = roots.filter((s) => {
      if (state.fleetFilter === "active" && s.status !== "online") return false;
      if (state.fleetFilter === "working" && !isWorking(s) && !(s.active_subagent_count > 0)) return false;
      if (state.projectFilter !== "all" && (s.identity?.project || "") !== state.projectFilter) return false;
      if (!q) return true;
      const idn = s.identity || {};
      return `${idn.full_label} ${idn.name} ${idn.task_label} ${idn.project} ${s.current_action} ${s.last_user_prompt}`
        .toLowerCase()
        .includes(q);
    });

    const grid = $("agentsGrid");
    if (!list.length) {
      grid.innerHTML = `<div class="empty-state">${
        state.fleetFilter === "active"
          ? "Hiện không có AI nào đang mở. Chuyển sang <strong>Tất cả</strong> để xem lịch sử gần đây."
          : "Không có AI nào khớp bộ lọc."
      }</div>`;
      return;
    }

    grid.innerHTML = list
      .map((s) => {
        const idn = s.identity || {};
        const { name, task } = agentNameAndTask(idn);
        const st = s.activity_state || "offline";
        const act = humanAction(s.current_action, st, s.last_assistant_reply);
        const tokens = s.subagent_count > 0 ? s.tree_total_tokens || s.total_tokens : s.total_tokens;
        const cost = s.subagent_count > 0 ? s.tree_estimated_cost_usd || s.estimated_cost_usd : s.estimated_cost_usd;
        const topPeer = (s.peer_comms || [])[0];
        const totalPeerMsgs = (s.peer_comms || []).reduce((acc, p) => acc + (p.count || 0), 0);

        return `
          <article class="agent-card ${st === "offline" && !s.active_subagent_count ? "is-offline" : ""}" data-open-agent="${esc(s.conversation_id)}">
            <div class="ac-head">
              <div class="ac-id">
                ${renderAvatar(idn, "", true)}
                <div class="ac-titles">
                  <div class="ac-name">${esc(name)}</div>
                  <div class="ac-task" title="${esc(task || idn.project || "")}">${esc(
                    task ? `Task: ${task}` : idn.project ? `Dự án: ${idn.project}` : ""
                  )}</div>
                </div>
              </div>
              <span class="pill pill-${esc(st)}">
                <span class="dot dot-${esc(st)}"></span>
                ${esc(STATUS_TEXT[st] || st)}
              </span>
            </div>

            <div class="ac-activity">
              <span class="ac-act-text" title="${esc(s.current_action || "")}">${esc(act)}</span>
              <span class="ac-act-time">${fmtAgo(s.last_activity_at)}</span>
            </div>

            <div class="ac-foot">
              <div class="ac-nums">
                <span>${fmtTokens(tokens)} tok</span>
                <span>·</span>
                <span data-tick="${esc(s.conversation_id)}">${fmtDuration(liveElapsed(s))}</span>
                <span>·</span>
                <span>${fmtCost(cost)}</span>
              </div>
              <div class="ac-actions">
                ${
                  topPeer
                    ? `<button class="ac-link" type="button" data-card-chat="${esc(topPeer.channel_id)}" title="Mở hội thoại với ${esc(topPeer.peer_name)}">💬 ${totalPeerMsgs} tin</button>`
                    : ""
                }
                ${
                  s.subagent_count > 0
                    ? `<button class="ac-link" type="button" data-card-subs="${esc(s.conversation_id)}">${s.subagent_count} subagent</button>`
                    : ""
                }
              </div>
            </div>
          </article>
        `;
      })
      .join("");

    grid.querySelectorAll("[data-open-agent]").forEach((card) => {
      card.addEventListener("click", () => openAgent(card.getAttribute("data-open-agent")));
    });
    grid.querySelectorAll("[data-card-chat]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        openChannel(btn.getAttribute("data-card-chat"));
      });
    });
    grid.querySelectorAll("[data-card-subs]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        openAgent(btn.getAttribute("data-card-subs"), { tab: "subagents" });
      });
    });
  }

  // =========================================================================
  // MODE B: SINGLE AGENT DETAIL VIEW
  // =========================================================================
  function renderAgentDetail() {
    if (!state.selectedAgentId || !state.snapshot) return;
    const s = (state.snapshot.sessions || []).find((x) => x.conversation_id === state.selectedAgentId);
    if (!s) {
      setMode("overview");
      return;
    }

    const idn = s.identity || {};
    const { name, task } = agentNameAndTask(idn);
    const st = s.activity_state || "offline";
    const parent = s.parent_conversation_id
      ? (state.snapshot.sessions || []).find((x) => x.conversation_id === s.parent_conversation_id)
      : null;

    const av = $("agAvatar");
    av.textContent = idn.avatar || "AI";
    av.setAttribute("style", avatarStyleFor(idn));
    $("agName").textContent = name;

    const badge = $("agStatusBadge");
    badge.className = `pill pill-${st}`;
    badge.innerHTML = `<span class="dot dot-${esc(st)}"></span> ${esc(STATUS_TEXT[st] || st)}`;

    const metaBits = [];
    if (task) metaBits.push(`<span><strong>Task:</strong> ${esc(task)}</span>`);
    if (idn.project) metaBits.push(`<span>Dự án: <strong>${esc(idn.project)}</strong></span>`);
    if (idn.team) metaBits.push(`<span>Team: <strong>#${esc(idn.team)}</strong></span>`);
    metaBits.push(`<span>Hoạt động: ${fmtAgo(s.last_activity_at)}</span>`);
    $("agTaskLine").innerHTML = metaBits.join(" · ");

    $("agParentWrap").innerHTML = parent
      ? `<button id="btnGoParent" class="btn-subtle btn-sm" type="button">↑ Về AI cha (${esc(parent.identity?.name || "Cha")})</button>`
      : "";
    const pBtn = $("btnGoParent");
    if (pBtn && parent) pBtn.addEventListener("click", () => openAgent(parent.conversation_id));

    if ($("renameForm").classList.contains("hidden")) {
      $("rnName").value = idn.is_custom_name ? idn.name : idn.full_label || name;
      $("rnRole").value = idn.role || "";
    }

    $("agStatsRow").innerHTML = `
      <div class="dstat">
        <div class="dstat-k">Tổng Tokens</div>
        <div class="dstat-v">${fmtTokens(s.total_tokens)}</div>
        <div class="dstat-s">${s.cache_hit_pct || 0}% cache · Output ${fmtTokens(s.output_tokens)}</div>
      </div>
      <div class="dstat">
        <div class="dstat-k">Thời gian hoạt động</div>
        <div class="dstat-v" data-tick="${esc(s.conversation_id)}">${fmtDuration(liveElapsed(s))}</div>
        <div class="dstat-s">${s.llm_calls || 0} lượt gọi AI · ${s.tool_call_count || 0} lệnh tool</div>
      </div>
      <div class="dstat">
        <div class="dstat-k">Chi phí ước tính</div>
        <div class="dstat-v">${fmtCost(s.estimated_cost_usd)}</div>
        <div class="dstat-s">${fmtTokens(s.tokens_per_min)} tokens/phút</div>
      </div>
    `;

    const myMsgs = getAllUnifiedMessages()
      .filter((m) => m.from?.conversation_id === s.conversation_id || m.to?.conversation_id === s.conversation_id)
      .sort((a, b) => (b.created_at || 0) - (a.created_at || 0));
    const subs = s.subagents || [];
    const steps = [...(s.recent_steps || [])].reverse();

    $("agTabChatCnt").textContent = String(myMsgs.length);
    $("agTabSubCnt").textContent = String(subs.length);
    $("agTabStepsCnt").textContent = String(steps.length);

    document.querySelectorAll("[data-ag-tab]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-ag-tab") === state.agentTab);
    });

    const body = $("agBody");

    if (state.agentTab === "chat") {
      if (!myMsgs.length) {
        body.innerHTML = `<div class="empty-state">AI này chưa có trao đổi nào với các AI khác.</div>`;
      } else {
        body.innerHTML = myMsgs.slice(0, 40).map((m) => renderChatBubble(m, null)).join("");
        bindChatBubbleEvents(body);
      }
      return;
    }

    if (state.agentTab === "subagents") {
      if (!subs.length) {
        body.innerHTML = `<div class="empty-state">AI này không tạo subagent nào.</div>`;
      } else {
        body.innerHTML = `<div class="list-stack">${subs
          .map((c) => {
            const cst = c.killed ? "offline" : c.activity_state || "offline";
            return `
              <div class="simple-row clickable" data-open-sub="${esc(c.conversation_id)}">
                <div class="simple-row-main">
                  <div class="simple-row-title">${esc(c.full_label || c.agent_name || "Subagent")}</div>
                  <div class="simple-row-sub">${esc(c.killed ? "Đã dừng" : humanAction(c.current_action, cst, c.last_assistant_reply))}</div>
                </div>
                <span class="pill pill-${esc(cst)}">${esc(STATUS_TEXT[cst] || cst)}</span>
                <span class="muted">${fmtTokens(c.total_tokens)} · ${fmtDuration(c.elapsed_sec)}</span>
              </div>
            `;
          })
          .join("")}</div>`;
        body.querySelectorAll("[data-open-sub]").forEach((r) => {
          r.addEventListener("click", () => openAgent(r.getAttribute("data-open-sub")));
        });
      }
      return;
    }

    if (state.agentTab === "steps") {
      if (!steps.length) {
        body.innerHTML = `<div class="empty-state">Chưa ghi nhận bước thực thi nào.</div>`;
      } else {
        body.innerHTML = `<div class="list-stack">${steps
          .slice(0, 30)
          .map(
            (stItem) => `
            <div class="simple-row">
              <div class="simple-row-main">
                <div class="simple-row-title">${esc(
                  stItem.kind === "user"
                    ? `Người dùng: ${stItem.label}`
                    : stItem.kind === "reply"
                    ? `AI trả lời: ${cleanPreview(stItem.reply, 88) || "Xong"}`
                    : humanAction(stItem.label, "idle", stItem.reply)
                )}</div>
              </div>
              <span class="muted">${stItem.tokens ? `${fmtTokens(stItem.tokens)} tok · ` : ""}${Number(stItem.duration_sec || 0).toFixed(0)}s</span>
            </div>`
          )
          .join("")}</div>`;
      }
      return;
    }

    // Overview Tab
    body.innerHTML = `
      ${
        isWorking(s)
          ? `<div class="card-section">
              <h3>Đang thực hiện</h3>
              <div class="text-panel">${esc(humanAction(s.current_action, st, s.last_assistant_reply))}</div>
            </div>`
          : ""
      }
      ${
        s.last_user_prompt
          ? `<div class="card-section">
              <h3>Yêu cầu gần nhất</h3>
              <div class="text-panel">${esc(s.last_user_prompt)}</div>
            </div>`
          : ""
      }
      ${
        s.last_assistant_reply
          ? `<div class="card-section">
              <h3>Phản hồi gần nhất</h3>
              <div class="text-panel reply">${esc(s.last_assistant_reply)}</div>
            </div>`
          : ""
      }
      <div class="meta-footer">
        <div><strong>Thư mục làm việc:</strong> <code>${esc(s.cwd || "Không rõ")}</code></div>
        <div><strong>Định danh AGP:</strong> <code>${esc(idn.at_handle || "")}</code> · <code>${esc(s.agent_uri || "")}</code></div>
      </div>
    `;
  }

  // =========================================================================
  // MODE C: CLEAN CHAT VIEW
  // =========================================================================
  function renderChatBubble(m, activeCh) {
    const src = m.from || {};
    const dst = m.to || {};
    const isDirect = activeCh && activeCh.kind === "direct";
    const firstKey = activeCh?.participants?.[0]?.conversation_id || activeCh?.participants?.[0]?.agent_uri;
    const isSideB = isDirect && (src.conversation_id || src.agent_uri) !== firstKey;

    const typeText = MSG_TYPE_TEXT[m.type] || m.type || "Trao đổi";
    const delivText = DELIVERY_TEXT[m.status] || "";
    const isConflict = m.type === "conflict.report" || m.type === "hitl.escalate" || m.injection_flagged;

    const payloadKeys = m.payload && typeof m.payload === "object" ? Object.keys(m.payload) : [];
    const hasDetails = payloadKeys.length > 0 || (m.artifacts || []).length > 0;
    const canAck = m.source === "agp" && ["queued", "delivered"].includes(m.status);

    const detailObj = {};
    if (payloadKeys.length) detailObj.payload = m.payload;
    if ((m.artifacts || []).length) detailObj.artifacts = m.artifacts;

    return `
      <article class="chat-bubble ${m.is_system_noise ? "is-noise" : ""} ${isConflict ? "is-conflict" : ""} ${isSideB ? "side-b" : ""}">
        ${renderAvatar(src, "avatar-sm")}
        <div class="cb-content">
          <div class="cb-head">
            <div class="cb-who">
              <button class="cb-actor" type="button" ${src.conversation_id ? `data-cid="${esc(src.conversation_id)}"` : ""}>${esc(src.name || "AI")}</button>
              <span class="cb-arrow">→</span>
              <button class="cb-target" type="button" ${dst.conversation_id ? `data-cid="${esc(dst.conversation_id)}"` : ""}>${esc(dst.name || "AI")}</button>
              ${m.team ? `<span class="soft-tag">#${esc(m.team)}</span>` : ""}
              ${m.type !== "chat.note" && m.type !== "message" ? `<span class="soft-tag ${isConflict ? "err" : ""}">${esc(typeText)}</span>` : ""}
              ${m.priority === "urgent" || m.priority === "high" ? `<span class="soft-tag warn">${esc(m.priority.toUpperCase())}</span>` : ""}
              ${delivText && m.status === "queued" ? `<span class="soft-tag warn">${esc(delivText)}</span>` : ""}
            </div>
            <span class="cb-time" title="${esc(m.timestamp || "")}">${fmtClock(m.created_at)} · ${fmtAgo(m.created_at)}</span>
          </div>

          <div class="cb-text">${esc(m.summary || "")}</div>

          ${
            (m.verbatim_constraints || []).length
              ? `<div class="cb-constraints"><strong>Ràng buộc bắt buộc:</strong><ul>${m.verbatim_constraints
                  .map((c) => `<li>${esc(c)}</li>`)
                  .join("")}</ul></div>`
              : ""
          }

          ${
            hasDetails || canAck
              ? `<div class="cb-foot">
                  ${
                    hasDetails
                      ? `<details class="cb-details">
                          <summary>Xem dữ liệu đính kèm (${payloadKeys.length} mục)</summary>
                          <pre class="cb-json">${esc(JSON.stringify(detailObj, null, 2))}</pre>
                        </details>`
                      : `<span></span>`
                  }
                  ${
                    canAck
                      ? `<div class="cb-actions">
                          <button class="btn-subtle btn-sm btn-ok" type="button" data-ack-id="${esc(m.id)}" data-ack-st="accepted">✓ Xác nhận</button>
                          <button class="btn-subtle btn-sm btn-danger" type="button" data-ack-id="${esc(m.id)}" data-ack-st="rejected">Từ chối</button>
                        </div>`
                      : ""
                  }
                </div>`
              : ""
          }
        </div>
      </article>
    `;
  }

  function bindChatBubbleEvents(container) {
    container.querySelectorAll("[data-cid]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const cid = btn.getAttribute("data-cid");
        if (cid) openAgent(cid);
      });
    });
    container.querySelectorAll("[data-ack-id]").forEach((btn) => {
      btn.addEventListener("click", async () => {
        await fetch("/api/ack", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            msg_id: btn.getAttribute("data-ack-id"),
            status: btn.getAttribute("data-ack-st"),
            reason: "Updated from AGY Monitor",
          }),
        });
        fetchSnapshot();
      });
    });
  }

  function renderChat(snap) {
    const convs = snap.conversations || [];
    const activeCh = convs.find((c) => c.channel_id === state.selectedChannelId) || null;
    if (state.selectedChannelId !== "__all__" && !activeCh) {
      state.selectedChannelId = "__all__";
    }

    $("chatHeading").textContent = activeCh ? activeCh.title : "Tất cả tin nhắn giữa các AI";
    $("btnSortChat").textContent = state.newestFirst ? "Mới nhất trước" : "Cũ → Mới";
    $("chkHideNoise").checked = state.hideNoise;
    $("mapDrawer").classList.toggle("hidden", !state.showMap);

    // Participants bar with clear Name + Task
    const partsEl = $("chatParticipants");
    if (activeCh && (activeCh.participants || []).length) {
      partsEl.innerHTML = activeCh.participants
        .slice(0, 6)
        .map((p) => {
          const { name, task } = agentNameAndTask(p);
          return `
            <button class="part-btn" type="button" ${p.conversation_id ? `data-part-cid="${esc(p.conversation_id)}"` : ""}>
              ${renderAvatar(p, "avatar-xs", true)}
              <span>${esc(name)}</span>
              ${task ? `<span class="part-task">(${esc(task)})</span>` : ""}
            </button>
          `;
        })
        .join("");
      partsEl.querySelectorAll("[data-part-cid]").forEach((btn) => {
        btn.addEventListener("click", () => openAgent(btn.getAttribute("data-part-cid")));
      });
    } else {
      partsEl.innerHTML = `<span class="muted">Chọn một cặp hội thoại ở cột trái để đọc riêng từng cuộc trao đổi.</span>`;
    }

    // Optional Connection Pairs Map (Clean pills instead of messy SVG arrows)
    if (state.showMap) {
      const pairs = convs.filter((c) => c.kind === "direct" || c.kind === "team");
      $("mapPairs").innerHTML = pairs.length
        ? pairs
            .map(
              (c) => `
              <button class="pair-pill" type="button" data-map-ch="${esc(c.channel_id)}">
                <span>${esc(c.title)}</span>
                <span class="soft-tag">${c.message_count} tin</span>
              </button>`
            )
            .join("")
        : `<span class="muted">Chưa có kết nối nào.</span>`;
      $("mapPairs").querySelectorAll("[data-map-ch]").forEach((btn) => {
        btn.addEventListener("click", () => {
          state.showMap = false;
          openChannel(btn.getAttribute("data-map-ch"));
        });
      });
    }

    // Filter & sort messages
    const q = state.search.trim().toLowerCase();
    let msgs = activeCh ? [...(activeCh.messages || [])] : getAllUnifiedMessages();
    if (state.hideNoise) msgs = msgs.filter((m) => !m.is_system_noise);
    if (q) {
      msgs = msgs.filter((m) =>
        `${m.summary} ${m.from?.full_label} ${m.to?.full_label} ${m.team || ""}`.toLowerCase().includes(q)
      );
    }
    msgs.sort((a, b) => (state.newestFirst ? (b.created_at || 0) - (a.created_at || 0) : (a.created_at || 0) - (b.created_at || 0)));

    const renderKey = [
      state.selectedChannelId,
      state.hideNoise,
      state.newestFirst,
      state.theme,
      q,
      msgs.slice(-100).map((m) => `${m.id}:${m.status}:${m.from?.name}:${m.to?.name}`).join("|"),
    ].join("::");

    const box = $("chatMessages");
    if (renderKey !== state.lastChatKey) {
      state.lastChatKey = renderKey;
      const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 80;
      box.innerHTML = msgs.length
        ? msgs.slice(-100).map((m) => renderChatBubble(m, activeCh)).join("")
        : `<div class="empty-state">Không có tin nhắn nào trong luồng này.</div>`;
      bindChatBubbleEvents(box);
      if (!state.newestFirst && atBottom) box.scrollTop = box.scrollHeight;
    }

    // Active file leases banner
    const leases = snap.leases || [];
    const lStrip = $("leasesStrip");
    if (!leases.length) {
      lStrip.classList.add("hidden");
    } else {
      lStrip.classList.remove("hidden");
      lStrip.innerHTML = leases
        .map(
          (l) => `
          <div class="lease-item">
            <span>🔒 <strong>${esc(l.owner_identity?.name || l.owner_uri)}</strong> đang khóa <code>${esc(l.path_pattern)}</code> (còn ${l.expires_in_sec}s)</span>
            <button class="btn-subtle btn-sm btn-danger" type="button" data-unlease="${esc(l.lease_id)}">Gỡ khóa</button>
          </div>`
        )
        .join("");
      lStrip.querySelectorAll("[data-unlease]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          await fetch(`/api/leases/${encodeURIComponent(btn.getAttribute("data-unlease"))}`, { method: "DELETE" });
          fetchSnapshot();
        });
      });
    }

    // Unified Simple Target Dropdown (AGP sessions + Teams in one clean grouped <select>!)
    populateComposerSelect(snap, activeCh);
  }

  function populateComposerSelect(snap, activeCh) {
    const sel = $("cmpTo");
    const prev = sel.value;
    const chKey = activeCh ? activeCh.channel_id : "__all__";
    const changed = chKey !== state.lastComposerCh;
    state.lastComposerCh = chKey;

    const roots = getRootSessions();
    const teams = snap.teams || [];

    const agpOpts = [
      `<option value="agp::agp://local/channel/all">Gửi tất cả AI đang mở (@all)</option>`,
      ...roots.map((s) => {
        const idn = s.identity || {};
        const { name, task } = agentNameAndTask(idn);
        const dot = s.status === "online" ? "● " : "○ ";
        const lbl = task ? `${name} (${task})` : name;
        return `<option value="agp::${esc(s.agent_uri)}">${dot}${esc(lbl)}</option>`;
      }),
    ];

    const teamOpts = [];
    teams.forEach((t) => {
      teamOpts.push(`<option value="team::${esc(t.team)}::all">#${esc(t.team)} → Cả team (@all)</option>`);
      (t.members || [])
        .filter((m) => m.status !== "shutdown")
        .forEach((m) => {
          teamOpts.push(`<option value="team::${esc(t.team)}::${esc(m.name)}">#${esc(t.team)} → @${esc(m.name)}</option>`);
        });
    });

    sel.innerHTML =
      `<optgroup label="Nhắn trực tiếp AI">${agpOpts.join("")}</optgroup>` +
      (teamOpts.length ? `<optgroup label="Nhắn Agent Team (Tự đánh thức)">${teamOpts.join("")}</optgroup>` : "");

    if (changed && activeCh) {
      if (activeCh.kind === "team") {
        const v = `team::${activeCh.team}::all`;
        if ([...sel.options].some((o) => o.value === v)) {
          sel.value = v;
          return;
        }
      } else if (activeCh.kind === "direct") {
        const pref = (activeCh.participants || []).find((p) => !p.is_human && p.agent_uri);
        const v = pref ? `agp::${pref.agent_uri}` : "";
        if (v && [...sel.options].some((o) => o.value === v)) {
          sel.value = v;
          return;
        }
      }
    }
    if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;
  }

  // =========================================================================
  // MODE D: AGENT TEAMS
  // =========================================================================
  function renderTeams(snap) {
    const teams = snap.teams || [];
    const sel = $("teamSelect");
    const openChatBtn = $("btnOpenTeamChat");

    if (!teams.length) {
      sel.innerHTML = `<option value="">Chưa có Team nào</option>`;
      state.selectedTeam = "";
      openChatBtn.classList.add("hidden");
    } else {
      if (!state.selectedTeam || !teams.some((t) => t.team === state.selectedTeam)) {
        state.selectedTeam = teams[0].team;
      }
      sel.innerHTML = teams
        .map((t) => `<option value="${esc(t.team)}">#${esc(t.team)} (${(t.members || []).length} thành viên)</option>`)
        .join("");
      sel.value = state.selectedTeam;
      openChatBtn.classList.remove("hidden");
    }

    const activeTeam = teams.find((t) => t.team === state.selectedTeam) || null;
    const members = activeTeam?.members || [];
    const tasks = activeTeam?.tasks || [];

    $("rosterCount").textContent = activeTeam ? `${members.length} thành viên` : "";
    const tkOwner = $("tkOwner");
    const prevOwner = tkOwner.value;
    tkOwner.innerHTML = [
      `<option value="">Chưa giao</option>`,
      ...members.filter((m) => m.status !== "shutdown").map((m) => `<option value="${esc(m.name)}">@${esc(m.name)}</option>`),
    ].join("");
    if (prevOwner && [...tkOwner.options].some((o) => o.value === prevOwner)) tkOwner.value = prevOwner;

    const jnSel = $("jnSession");
    if (jnSel) {
      const prevJn = jnSel.value;
      const existingCids = new Set(members.map((m) => m.conversation_id).filter(Boolean));
      const candidateSessions = (snap.sessions || []).filter((s) => s.conversation_id && !existingCids.has(s.conversation_id));
      jnSel.innerHTML = candidateSessions.length
        ? candidateSessions
            .slice(0, 30)
            .map((s) => {
              const lbl = s.identity?.full_label || s.display_name || s.conversation_id.slice(0, 8);
              const tag = s.status === "online" ? "🟢" : "⚪";
              return `<option value="${esc(s.conversation_id)}" data-handle="${esc(s.identity?.handle || "")}" data-cwd="${esc(s.cwd || s.inferred_workspace || "")}">${tag} ${esc(lbl)} (${esc(s.conversation_id.slice(0, 8))})</option>`;
            })
            .join("")
        : `<option value="">Tất cả phiên đang mở đã nằm trong Team</option>`;
      if (prevJn && [...jnSel.options].some((o) => o.value === prevJn)) jnSel.value = prevJn;
    }

    const rosterEl = $("teamRoster");
    if (!activeTeam) {
      rosterEl.innerHTML = `<div class="empty-state">Nhấn <strong>+ Tạo Team</strong> để bắt đầu điều phối nhóm AI.</div>`;
    } else {
      rosterEl.innerHTML = members
        .map((m) => {
          const idn = m.identity || { name: `@${m.name}`, avatar: m.name.slice(0, 2).toUpperCase() };
          const { name, task } = agentNameAndTask(idn);
          const stKey = m.status === "working" || m.status === "starting" ? "running_tool" : m.status === "idle" ? "idle" : "offline";
          return `
            <div class="simple-row">
              ${renderAvatar(idn, "avatar-sm", true)}
              <div class="simple-row-main">
                <div class="simple-row-title">${esc(name)}</div>
                <div class="simple-row-sub">@${esc(m.name)}${task ? ` · ${esc(task)}` : m.role ? ` · ${esc(m.role)}` : ""}</div>
              </div>
              <span class="pill pill-${esc(stKey)}">${esc(STATUS_TEXT[m.status] || m.status)}</span>
              ${m.conversation_id ? `<button class="btn-subtle btn-sm" type="button" data-tm-view="${esc(m.conversation_id)}">Xem</button>` : ""}
              ${m.managed && m.status !== "shutdown" ? `<button class="btn-subtle btn-sm btn-danger" type="button" data-tm-shut="${esc(m.name)}">Tắt</button>` : ""}
            </div>
          `;
        })
        .join("");

      rosterEl.querySelectorAll("[data-tm-view]").forEach((btn) => {
        btn.addEventListener("click", () => openAgent(btn.getAttribute("data-tm-view")));
      });
      rosterEl.querySelectorAll("[data-tm-shut]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          await fetch("/api/teams/shutdown", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ team: activeTeam.team, name: btn.getAttribute("data-tm-shut") }),
          });
          fetchSnapshot();
        });
      });
    }

    const doneCnt = tasks.filter((t) => t.status === "completed").length;
    $("tasksCount").textContent = tasks.length ? `${doneCnt}/${tasks.length} hoàn thành` : "";
    const tasksEl = $("teamTasks");
    if (!tasks.length) {
      tasksEl.innerHTML = `<div class="empty-state">Chưa có công việc nào.</div>`;
    } else {
      tasksEl.innerHTML = tasks
        .map((t) => {
          const deps = (t.blocked_by || []).length ? ` · chờ ${t.blocked_by.join(", ")}` : "";
          const own = t.owner ? `@${t.owner}` : "Chưa giao";
          return `
            <div class="simple-row">
              <div class="simple-row-main">
                <div class="simple-row-title">${esc(t.id)} · ${esc(t.title)}</div>
                <div class="simple-row-sub">${esc(own)}${esc(deps)}${t.result_summary ? ` — ${esc(t.result_summary)}` : ""}</div>
              </div>
              <span class="soft-tag ${t.status === "completed" ? "ok" : t.status === "blocked" ? "warn" : ""}">${esc(TASK_STATUS_TEXT[t.status] || t.status)}</span>
              ${
                t.status !== "completed"
                  ? `<button class="btn-subtle btn-sm btn-ok" type="button" data-done-tk="${esc(t.id)}">✓ Xong</button>`
                  : ""
              }
            </div>
          `;
        })
        .join("");

      tasksEl.querySelectorAll("[data-done-tk]").forEach((btn) => {
        btn.addEventListener("click", async () => {
          await fetch("/api/teams/task", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ team: activeTeam.team, action: "complete", task_id: btn.getAttribute("data-done-tk") }),
          });
          fetchSnapshot();
        });
      });
    }
  }

  // =========================================================================
  // Sync & SSE
  // =========================================================================
  function renderAll() {
    if (!state.snapshot) return;
    renderHeader(state.snapshot);
    renderSidebar(state.snapshot);
    if (state.mode === "overview") renderOverview();
    else if (state.mode === "agent") renderAgentDetail();
    else if (state.mode === "chat") renderChat(state.snapshot);
    else if (state.mode === "teams") renderTeams(state.snapshot);
  }

  function applySnapshot(snap) {
    state.snapshot = snap;
    state.serverTimeAtSync = Number(snap.server_time || Date.now() / 1000);
    state.lastSyncMs = Date.now();

    if (!state.autoSwitchedOnce) {
      state.autoSwitchedOnce = true;
      if ((snap.kpis?.online_agents || 0) === 0 && (snap.sessions || []).length > 0) {
        state.fleetFilter = "all";
      }
    }
    renderAll();
  }

  async function fetchSnapshot() {
    try {
      const r = await fetch("/api/snapshot", { cache: "no-store" });
      if (r.ok) applySnapshot(await r.json());
    } catch (_) {}
  }

  function connectSSE() {
    if (state.sse) state.sse.close();
    const dot = $("liveDot");
    const es = new EventSource("/api/events");
    state.sse = es;

    es.onopen = () => {
      dot.className = "live-dot live";
      dot.title = "Đã kết nối trực tiếp";
    };
    es.addEventListener("snapshot", (e) => {
      try {
        applySnapshot(JSON.parse(e.data));
      } catch (_) {}
    });
    ["agent_updated", "agp_message", "agp_ack", "lease_updated", "team_updated", "team_message", "team_task_updated"].forEach(
      (ev) => es.addEventListener(ev, () => fetchSnapshot())
    );
    es.onerror = () => {
      dot.className = "live-dot error";
      dot.title = "Mất kết nối — đang tự kết nối lại...";
    };
  }

  // 1s live timer tick for working agents
  setInterval(() => {
    if (!state.snapshot) return;
    (state.snapshot.sessions || []).forEach((s) => {
      if (!isWorking(s)) return;
      document.querySelectorAll(`[data-tick="${CSS.escape(s.conversation_id)}"]`).forEach((el) => {
        el.textContent = fmtDuration(liveElapsed(s));
      });
    });
  }, 1000);

  // =========================================================================
  // Event Wiring
  // =========================================================================
  $("btnThemeToggle").addEventListener("click", () => applyTheme(state.theme === "dark" ? "light" : "dark"));
  $("searchInput").addEventListener("input", (e) => {
    state.search = e.target.value;
    state.lastChatKey = "";
    renderAll();
  });

  $("navOverview").addEventListener("click", () => setMode("overview"));
  $("navAllChat").addEventListener("click", () => openChannel("__all__"));
  $("navTeams").addEventListener("click", () => setMode("teams"));
  $("btnShowHistory").addEventListener("click", () => {
    state.fleetFilter = "all";
    setMode("overview");
  });

  $("segActive").addEventListener("click", () => {
    state.fleetFilter = "active";
    renderOverview();
  });
  $("segWorking").addEventListener("click", () => {
    state.fleetFilter = "working";
    renderOverview();
  });
  $("segAll").addEventListener("click", () => {
    state.fleetFilter = "all";
    renderOverview();
  });

  // Agent Detail controls
  $("btnBackOverview").addEventListener("click", () => setMode("overview"));
  $("btnToggleRename").addEventListener("click", () => {
    const f = $("renameForm");
    f.classList.toggle("hidden");
    if (!f.classList.contains("hidden")) {
      $("rnName").focus();
      $("rnName").select();
    }
  });
  $("btnCancelRename").addEventListener("click", () => $("renameForm").classList.add("hidden"));

  $("renameForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const s = (state.snapshot?.sessions || []).find((x) => x.conversation_id === state.selectedAgentId);
    if (!s) return;
    const r = await fetch("/api/agents/rename", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        agent_uri: s.agent_uri,
        conversation_id: s.conversation_id,
        custom_name: $("rnName").value.trim(),
        role: $("rnRole").value.trim(),
        surface: s.surface || "cli",
      }),
    });
    if (r.ok) {
      $("renameForm").classList.add("hidden");
      fetchSnapshot();
    }
  });

  document.querySelectorAll("[data-ag-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.agentTab = btn.getAttribute("data-ag-tab");
      renderAgentDetail();
    });
  });

  $("agPingForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const s = (state.snapshot?.sessions || []).find((x) => x.conversation_id === state.selectedAgentId);
    const text = $("agPingInput").value.trim();
    const stEl = $("agPingStatus");
    if (!s || !text) return;
    stEl.className = "form-note";
    stEl.textContent = "Đang gửi...";
    const r = await fetch("/api/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        from: { uri: "agp://local/human/monitor", role: "human-operator" },
        to: { uri: s.agent_uri },
        type: "chat.note",
        priority: "high",
        summary: text,
        payload: { message: text },
      }),
    });
    const d = await r.json();
    if (r.ok && d.ok) {
      $("agPingInput").value = "";
      stEl.textContent = "Đã gửi ✓";
      fetchSnapshot();
    } else {
      stEl.className = "form-note err";
      stEl.textContent = d.error || "Lỗi gửi tin";
    }
  });

  // Chat controls
  $("chkHideNoise").addEventListener("change", (e) => {
    state.hideNoise = e.target.checked;
    state.lastChatKey = "";
    renderChat(state.snapshot || {});
  });
  $("btnSortChat").addEventListener("click", () => {
    state.newestFirst = !state.newestFirst;
    state.lastChatKey = "";
    renderChat(state.snapshot || {});
  });
  $("btnToggleMap").addEventListener("click", () => {
    state.showMap = !state.showMap;
    renderChat(state.snapshot || {});
  });
  $("btnCloseMap").addEventListener("click", () => {
    state.showMap = false;
    renderChat(state.snapshot || {});
  });
  $("btnCmpOpts").addEventListener("click", () => {
    $("cmpAdvanced").classList.toggle("hidden");
  });

  $("chatComposer").addEventListener("submit", async (e) => {
    e.preventDefault();
    const st = $("cmpStatus");
    st.className = "form-note";
    const val = $("cmpTo").value;
    const text = $("cmpText").value.trim();
    if (!val || !text) return;

    st.textContent = "Đang gửi...";
    try {
      if (val.startsWith("team::")) {
        const [, team, to] = val.split("::");
        const r = await fetch("/api/teams/send", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ team, from_name: "lead", to, message: text, wake: true }),
        });
        const d = await r.json();
        if (!r.ok || !d.ok) {
          st.className = "form-note err";
          st.textContent = d.error || "Gửi thất bại";
          return;
        }
        const woken = (d.auto_woken || []).map((x) => `@${x}`).join(", ");
        st.textContent = woken ? `Đã gửi · tự đánh thức ${woken}` : "Đã gửi ✓";
      } else {
        const uri = val.replace(/^agp::/, "");
        const msgType = $("cmpType").value || "chat.note";
        const prio = $("cmpPrio").value || "normal";
        const payload =
          msgType === "task.request"
            ? { goal: text }
            : msgType === "review.request"
            ? { target: text }
            : msgType === "sync.git_handoff"
            ? { branch: text }
            : { message: text };
        const r = await fetch("/api/send", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            from: { uri: "agp://local/human/monitor", role: "human-operator" },
            to: { uri },
            type: msgType,
            priority: prio,
            summary: text,
            payload,
          }),
        });
        const d = await r.json();
        if (!r.ok || !d.ok) {
          st.className = "form-note err";
          st.textContent = d.error || "Gửi thất bại";
          return;
        }
        st.textContent = "Đã gửi ✓";
      }
      $("cmpText").value = "";
      fetchSnapshot();
    } catch (err) {
      st.className = "form-note err";
      st.textContent = String(err);
    }
  });

  // Teams controls
  $("teamSelect").addEventListener("change", (e) => {
    state.selectedTeam = e.target.value;
    renderTeams(state.snapshot || {});
  });
  $("btnOpenTeamChat").addEventListener("click", () => {
    if (state.selectedTeam) openChannel(`team:${state.selectedTeam}`);
  });
  $("btnNewTeam").addEventListener("click", () => {
    $("newTeamForm").classList.toggle("hidden");
    $("spawnForm").classList.add("hidden");
    $("joinForm")?.classList.add("hidden");
  });
  $("btnJoinMember")?.addEventListener("click", () => {
    $("joinForm")?.classList.toggle("hidden");
    $("newTeamForm").classList.add("hidden");
    $("spawnForm").classList.add("hidden");
  });
  $("btnSpawnMember").addEventListener("click", () => {
    $("spawnForm").classList.toggle("hidden");
    $("newTeamForm").classList.add("hidden");
    $("joinForm")?.classList.add("hidden");
  });

  $("joinForm")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const team = state.selectedTeam || "default-team";
    const selOpt = $("jnSession").selectedOptions?.[0];
    const cid = $("jnSession").value;
    if (!cid) return;
    const r = await fetch("/api/teams/join", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        team,
        name: $("jnName").value.trim(),
        role: $("jnRole").value.trim() || "peer-agent",
        conversation_id: cid,
        cwd: selOpt?.getAttribute("data-cwd") || "",
      }),
    });
    const d = await r.json();
    if (d.ok) {
      state.selectedTeam = d.team;
      $("jnName").value = "";
      $("jnRole").value = "";
      $("joinForm").classList.add("hidden");
      $("teamNotice").textContent = `Đã gắn @${d.member.name} vào #${d.team} ✓`;
      fetchSnapshot();
    } else {
      $("teamNotice").className = "form-note err";
      $("teamNotice").textContent = d.error || "Lỗi gắn phiên AI";
    }
  });

  $("newTeamForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const r = await fetch("/api/teams", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ team: $("ntName").value.trim(), description: $("ntDesc").value.trim() }),
    });
    const d = await r.json();
    if (d.ok) {
      state.selectedTeam = d.team.team;
      $("ntName").value = "";
      $("ntDesc").value = "";
      $("newTeamForm").classList.add("hidden");
      $("teamNotice").textContent = `Đã tạo team #${state.selectedTeam}`;
      fetchSnapshot();
    } else {
      $("teamNotice").className = "form-note err";
      $("teamNotice").textContent = d.error || "Lỗi tạo team";
    }
  });

  $("spawnForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const team = state.selectedTeam || "default-team";
    const r = await fetch("/api/teams/spawn", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        team,
        name: $("spName").value.trim(),
        role: $("spRole").value.trim(),
        prompt: $("spPrompt").value.trim(),
      }),
    });
    const d = await r.json();
    if (d.ok) {
      state.selectedTeam = d.team;
      $("spName").value = "";
      $("spRole").value = "";
      $("spPrompt").value = "";
      $("spawnForm").classList.add("hidden");
      $("teamNotice").textContent = `Đang khởi chạy @${d.member.name}...`;
      fetchSnapshot();
    } else {
      $("teamNotice").className = "form-note err";
      $("teamNotice").textContent = d.error || "Lỗi gọi teammate";
    }
  });

  $("addTaskForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!state.selectedTeam) return;
    const blocked = $("tkBlocked").value.split(",").map((x) => x.trim()).filter(Boolean);
    await fetch("/api/teams/task", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        team: state.selectedTeam,
        action: "create",
        title: $("tkTitle").value.trim(),
        owner: $("tkOwner").value || "",
        blocked_by: blocked,
      }),
    });
    $("tkTitle").value = "";
    $("tkBlocked").value = "";
    fetchSnapshot();
  });

  applyTheme(state.theme);
  fetchSnapshot();
  connectSSE();
})();
