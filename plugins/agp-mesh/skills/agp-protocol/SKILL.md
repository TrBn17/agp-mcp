---
name: agp-protocol
description: Complete playbook for serverless AGP v2.1 MCP inter-agent collaboration, Claude-Style Agent Teams, two-way reactive receive/reply channels, workspace file leases, SHA-256 artifacts, and distributed tracing.
---

# AGP v2.1 Serverless MCP & Bidirectional Collaboration Playbook

`agp-broker` runs **100% silently in the background** as a standard stdio MCP server backed by shared SQLite WAL (`~/.gemini/agp/agp.db`) and cross-process locked Team mailboxes (`~/.gemini/agp/teams/`). **No HTTP server needs to be running** — opening the HTML Monitor UI (`agp-monitor` / `scripts/open-monitor.ps1`) is strictly optional and only for viewing the dashboard.

## How Target Sessions Actually Receive & Reply (Zero Token Waste)

1. **Active Turn (Automatic Mid-Turn Delivery)**:
   - `PreInvocation`, `PostInvocation`, and `Stop` hooks automatically drain **both** AGP SQLite messages (24h TTL) and Team mailboxes, injecting them with exact `[REPLY ROUTING]` calls.
2. **Idle Interactive Session (Zero-Token Reactive Listener — `agp.listen`)**:
   - Launch the `listen_command` returned by `SessionStart` / `agp_status` / `team_join` (e.g. `npx -y agp-mcp listen --agent <your_agent_uri>` or `agp-listen --agent <your_agent_uri>`) via `run_command` (`WaitMsBeforeAsync=500`, `IsDaemon=false`) and end your turn.
   - Consumes **0 LLM tokens** while waiting; watches BOTH direct AGP messages AND every Team you are joined to (even teams created later!).
   - Exits with the incoming messages the instant a peer writes -> Antigravity Reactive Wakeup wakes your live interactive session immediately!
3. **Autonomous / Detached CLI Teammates (`team_spawn` / `wakeup_mode='auto_cli'` / `wake_mode='force_cli'`)**:
   - Spawns a short-lived, self-terminating detached `agp.worker` (`agy --conversation <id>`) that drains unread messages, executes the turn, **auto-bridges a reply back if the LLM forgot to call `team_send`/`agp_send`**, and exits cleanly!
4. **Honest Delivery Diagnostics (`delivery.will_auto_receive`)**:
   - Every `agp_send`, `agp_ask`, and `team_send` reports whether the target will auto-receive. Never loop calling `agp_inbox` when `will_auto_receive` is `false`!

## Mode 1 (Recommended): Claude-Style Agent Teams (`team_*`)

1. **Create team**: `team_create(team="feature-x", description="Build API + UI")`
2. **Define task DAG**:
   - `team_task(team="feature-x", action="create", title="Build API", owner="backend")` -> `T1`
   - `team_task(team="feature-x", action="create", title="Wire UI", owner="frontend", blocked_by=["T1"])` -> `T2`
3. **Add live sessions OR spawn autonomous teammates**:
   - Existing live session: `team_join(team="feature-x", name="frontend", role="frontend-dev", conversation_id="<cid>", notify_member=True, welcome_note="...")` — opens its channel immediately and returns `channel.listen_command` + `channel.reply_call`.
   - Autonomous worker: `team_spawn(team="feature-x", name="backend", role="backend-dev", prompt="...")`.
4. **Communicate & inspect**:
   - `team_send(team="feature-x", from_name="lead", to="frontend", message="...")`
   - `team_list(team="feature-x")` — inspect roster, live status, unread counts, task board.
   - `team_inbox(team="feature-x", name="lead", wait_sec=25)` (or launch `agp.listen` at 0 tokens!).
5. **Clean shutdown**:
   - `team_shutdown(team="feature-x", name="all")` (and optionally `team_delete(team="feature-x")`).

## Mode 2: Cross-Session AGP v2.0 Bus (`agp_*`)

- **Orient**: `agp_status()` -> see your URI, unread counts, leases, open circuits, `listen_command`.
- **Identify**: `agp_register(agent_uri=..., display_name="Backend API (Auth Fix)", role="backend", owned_paths=["ai-platform/**"], channels=["api-contract"])`.
- **Discover**: `agp_discover(role=..., path_filter=..., query=..., exclude_self=True)`.
- **One-Call Ask**: `agp_ask(to_uri="@backend", summary="...", payload={"goal": "..."}, wait_timeout_sec=35)` (fails fast in 0ms if target is unreachable idle!).
- **Manual Send/Inbox/Ack/Task**: `agp_send`, `agp_inbox(mark_delivered=False)` (peek), `agp_ack`, `agp_task(action="list"|"get"|"wait"|"update"|"cancel")`.
- **Workspace Concurrency**: `agp_lease(action="check"|"acquire"|"renew"|"release"|"list"|"force_release", paths=[...])`.
- **Artifacts & Traces**: `agp_artifact_put`, `agp_artifact_get(offset_bytes=...)`, `agp_artifact_list`, `agp_trace_inspect()`.
- **Sign off**: `agp_unregister()` (releases held leases cleanly).
