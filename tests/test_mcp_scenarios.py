"""Comprehensive end-to-end scenario tests for standard serverless AGP MCP v2.1.

Covers:
  - Standalone stdio MCP subprocesses with ZERO HTTP server running
  - True multi-process AI-to-AI communication over shared SQLite WAL & cross-process locked Teams
  - Bidirectional receive/reply mechanisms:
      * PreInvocation / PostInvocation / Stop hooks (AGP + Team unified inbox, normal & high priority, 24h TTL)
      * Zero-token reactive background listener (`agp.listen`) waking on direct AGP, broadcast, & dynamic Team join
      * `team_join` channel opening handshake + bidirectional team <-> AGP reply mirroring
      * Detached/worker auto-bridged reply guarantee when woken LLM responds in plain text
      * `agp_ask` one-call request/reply + 0ms fail-fast anti-token-burn guard on unreachable idle peers
  - Full MCP spec compliance:
      * Protocol versions 2024-11-05 .. 2026-07-28, parse errors (-32700), unknown methods (-32601), invalid params (-32602), JSON-RPC batches
      * Tool annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`)
      * Static resources (6) + URI templates (5) + prompts (6) + completions + logging/setLevel + notifications/cancelled
      * Durable tasks (`agp_task` + `tasks/list`, `tasks/get`, `tasks/update`, `tasks/cancel`)
  - Multi-subscriber channel broadcasts (`channel_reads`), lease dry-run `check` + relative/absolute Windows overlap,
    artifact chunk pagination + `source_path` DLP redaction, trace listing, identity isolation, EOF lease cleanup
  - Optional Monitor UI sync (`PRAGMA data_version`) when opened after silent background work and closed again
  - Packaging & CLI (`--doctor`, `--print-config`, `--version`, `pyproject.toml`, `server.json`)
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agp.broker import AGPBrokerRuntime, create_server
from agp.storage import AGPStore, paths_overlap
from agp.teams import TeamManager
from agp.transcripts import TranscriptScanner


class StdioMCPClient:
    """Test harness driving a real standalone `agp-mcp` stdio subprocess."""

    def __init__(
        self,
        data_dir: Path,
        script_mode: bool = False,
        extra_env: Dict[str, str] | None = None,
        extra_args: List[str] | None = None,
        use_node_cli: bool = False,
    ) -> None:
        env = dict(os.environ)
        env["AGP_DATA_DIR"] = str(data_dir)
        env["AGP_HOME"] = str(PROJECT_ROOT)
        env["PYTHONPATH"] = str(PROJECT_ROOT)
        if extra_env:
            env.update(extra_env)
        extras = list(extra_args or [])
        if use_node_cli:
            cmd = ["node", str(PROJECT_ROOT / "bin" / "agp-mcp.mjs"), "--data-dir", str(data_dir), *extras]
        elif script_mode:
            cmd = [sys.executable, str(PROJECT_ROOT / "plugins" / "agp-mesh" / "scripts" / "agp_mcp_stdio.py"), *extras]
        else:
            cmd = [sys.executable, "-m", "agp.mcp_server", "--data-dir", str(data_dir), *extras]
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=env,
        )
        self._next_id = 1

    def send_raw(self, text: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(text + "\n")
        self.proc.stdin.flush()

    def recv_json(self, timeout: float = 8.0) -> Any:
        assert self.proc.stdout is not None
        result: List[Any] = []
        err: List[BaseException] = []

        def _reader() -> None:
            try:
                line = self.proc.stdout.readline()
                if line:
                    result.append(json.loads(line))
            except BaseException as exc:
                err.append(exc)

        t = threading.Thread(target=_reader, daemon=True)
        t.start()
        t.join(timeout=timeout)
        if err:
            raise err[0]
        if not result:
            stderr_out = ""
            if self.proc.poll() is not None and self.proc.stderr:
                stderr_out = self.proc.stderr.read()
            raise TimeoutError(f"Timed out waiting for stdio MCP response (stderr={stderr_out!r})")
        return result[0]

    def request(self, method: str, params: Dict[str, Any] | None = None, req_id: int | None = None) -> Dict[str, Any]:
        rid = req_id if req_id is not None else self._next_id
        self._next_id = max(self._next_id + 1, rid + 1)
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            payload["params"] = params
        self.send_raw(json.dumps(payload))
        return self.recv_json()

    def notify(self, method: str, params: Dict[str, Any] | None = None) -> None:
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self.send_raw(json.dumps(payload))

    def call_tool(self, name: str, arguments: Dict[str, Any] | None = None) -> tuple[bool, Dict[str, Any]]:
        resp = self.request("tools/call", {"name": name, "arguments": arguments or {}})
        res = resp["result"]
        body = json.loads(res["content"][0]["text"])
        return (not res.get("isError", False)), body

    def close(self) -> None:
        if self.proc.stdin and not self.proc.stdin.closed:
            try:
                self.proc.stdin.close()
            except OSError:
                pass
        try:
            self.proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=2.0)
        for stream in (self.proc.stdout, self.proc.stderr):
            if stream and not stream.closed:
                try:
                    stream.close()
                except OSError:
                    pass


class TestServerlessMCPScenarios(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    # 1. Standalone stdio MCP (plugin script + module) works completely without HTTP server
    def test_01_standalone_stdio_protocol_full_spec_no_http_server(self) -> None:
        client = StdioMCPClient(self.data_dir, script_mode=True)
        try:
            init = client.request(
                "initialize",
                {
                    "protocolVersion": "2025-03-26",
                    "clientInfo": {"name": "cursor-test", "version": "1.0"},
                },
            )
            self.assertEqual(init["result"]["protocolVersion"], "2025-03-26")
            caps = init["result"]["capabilities"]
            for cap_key in ("tools", "resources", "prompts", "logging", "completions", "extensions"):
                self.assertIn(cap_key, caps)
            client.notify("notifications/initialized")

            # Ping & logging/setLevel
            self.assertEqual(client.request("ping")["result"], {})
            self.assertEqual(client.request("logging/setLevel", {"level": "warning"})["result"], {})
            bad_lvl = client.request("logging/setLevel", {"level": "verbose"})
            self.assertEqual(bad_lvl["error"]["code"], -32602)

            # Parse error (-32700) & unknown method (-32601)
            client.send_raw("{not valid json")
            parse_err = client.recv_json()
            self.assertEqual(parse_err["error"]["code"], -32700)

            unk = client.request("nonexistent/method")
            self.assertEqual(unk["error"]["code"], -32601)

            # Batch JSON-RPC request
            client.send_raw(
                json.dumps(
                    [
                        {"jsonrpc": "2.0", "id": 501, "method": "ping"},
                        {"jsonrpc": "2.0", "method": "notifications/initialized"},
                        {"jsonrpc": "2.0", "id": 502, "method": "tools/list"},
                    ]
                )
            )
            batch_resp = client.recv_json()
            self.assertIsInstance(batch_resp, list)
            self.assertEqual(len(batch_resp), 2)
            tools = batch_resp[1]["result"]["tools"]
            tool_names = {t["name"] for t in tools}
            self.assertGreaterEqual(len(tools), 23)
            self.assertTrue({"agp_status", "agp_ask", "agp_task", "agp_unregister", "agp_artifact_list", "team_list", "team_delete"} <= tool_names)
            # Every tool carries standard MCP annotations
            for t in tools:
                self.assertIn("annotations", t)
                self.assertIn("readOnlyHint", t["annotations"])

            # Static resources + URI templates + Prompts + Completions
            res_list = client.request("resources/list")["result"]["resources"]
            self.assertGreaterEqual(len(res_list), 6)
            tpl_list = client.request("resources/templates/list")["result"]["resourceTemplates"]
            self.assertGreaterEqual(len(tpl_list), 5)

            prompts = client.request("prompts/list")["result"]["prompts"]
            self.assertGreaterEqual(len(prompts), 6)
            p_get = client.request(
                "prompts/get",
                {"name": "agp_coordinate_team", "arguments": {"team": "alpha", "goal": "Ship v2"}},
            )
            self.assertIn("alpha", p_get["result"]["messages"][0]["content"]["text"])
            # Missing required prompt argument returns -32602
            p_err = client.request("prompts/get", {"name": "agp_delegate_task", "arguments": {}})
            self.assertEqual(p_err["error"]["code"], -32602)

            # Register agent, read resource template agp://agents/{id}, verify completion/complete
            ok_reg, reg = client.call_tool(
                "agp_register",
                {"agent_uri": "agp://local/cursor/sess-1", "display_name": "Cursor Reviewer", "role": "reviewer"},
            )
            self.assertTrue(ok_reg)
            self.assertIn("listen_command", reg)

            read_ag = client.request("resources/read", {"uri": "agp://agents/sess-1"})
            ag_data = json.loads(read_ag["result"]["contents"][0]["text"])
            self.assertEqual(ag_data["agent"]["role"], "reviewer")

            comp = client.request(
                "completion/complete",
                {"ref": {"type": "ref/resource", "uri": "agp://agents/{agent_id}"}, "argument": {"name": "agent_id", "value": "sess"}},
            )
            self.assertTrue(any("sess-1" in v for v in comp["result"]["completion"]["values"]))
        finally:
            client.close()

    # 2. True multi-process serverless AI-to-AI messaging + fast cross-process wait + in_reply_to inheritance
    def test_02_multiprocess_two_stdio_agents_roundtrip_without_http(self) -> None:
        agent_a = StdioMCPClient(self.data_dir)
        agent_b = StdioMCPClient(self.data_dir)
        try:
            agent_a.request("initialize", {"protocolVersion": "2026-07-28", "clientInfo": {"name": "agy-cli"}})
            agent_b.request("initialize", {"protocolVersion": "2026-07-28", "clientInfo": {"name": "agy-ide"}})

            # Register both peers with distinct identities (verify zero identity hijacking!)
            agent_a.call_tool("agp_register", {"agent_uri": "agp://local/cli/agent-a", "display_name": "Lead A", "role": "coordinator"})
            agent_b.call_tool("agp_register", {"agent_uri": "agp://local/ide/agent-b", "display_name": "Worker B", "role": "backend"})

            # Agent B starts cross-process blocking wait in a background thread
            inbox_holder: List[Dict[str, Any]] = []

            def _wait_b() -> None:
                _, body = agent_b.call_tool("agp_inbox", {"agent_uri": "agp://local/ide/agent-b", "wait_timeout_sec": 8})
                inbox_holder.append(body)

            t_b = threading.Thread(target=_wait_b, daemon=True)
            t_b.start()
            time.sleep(0.15)

            t0 = time.perf_counter()
            ok_s, sent = agent_a.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/cli/agent-a",
                    "to_uri": "agp://local/ide/agent-b",
                    "type": "task.request",
                    "priority": "high",
                    "summary": "Check schema compatibility",
                    "payload": {"goal": "Verify v2 schema"},
                },
            )
            self.assertTrue(ok_s)
            self.assertIn("delivery", sent)

            t_b.join(timeout=4.0)
            elapsed = time.perf_counter() - t0
            self.assertEqual(len(inbox_holder), 1)
            self.assertEqual(inbox_holder[0]["count"], 1)
            self.assertLess(elapsed, 2.0)  # Fast cross-process wakeup!
            incoming = inbox_holder[0]["messages"][0]

            # Agent B replies using ONLY `in_reply_to` (omitting to_uri, trace_id, correlation_id!)
            ok_r, reply = agent_b.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/ide/agent-b",
                    "in_reply_to": incoming["msg_id"],
                    "type": "task.result",
                    "summary": "Schema verified green",
                    "payload": {"status": "success", "compatible": True},
                },
            )
            self.assertTrue(ok_r)
            self.assertEqual(reply["envelope"]["to"]["uri"], "agp://local/cli/agent-a")
            self.assertEqual(reply["trace_id"], sent["trace_id"])
            self.assertEqual(reply["correlation_id"], sent["correlation_id"])

            # Agent A filters inbox by correlation_id (peek first, then consume)
            _, peeked = agent_a.call_tool(
                "agp_inbox",
                {"agent_uri": "agp://local/cli/agent-a", "correlation_id": sent["correlation_id"], "mark_delivered": False},
            )
            self.assertEqual(peeked["count"], 1)
            _, consumed = agent_a.call_tool(
                "agp_inbox",
                {"agent_uri": "agp://local/cli/agent-a", "correlation_id": sent["correlation_id"], "mark_delivered": True},
            )
            self.assertEqual(consumed["count"], 1)
            self.assertEqual(consumed["messages"][0]["payload"]["compatible"], True)
        finally:
            agent_a.close()
            agent_b.close()

    # 3. Zero-token reactive background listener (`agp.listen`) wakes on dynamic Team join + AGP send + Broadcast!
    def test_03_zero_token_reactive_listener_wakes_on_team_join_and_message(self) -> None:
        env = dict(os.environ)
        env["AGP_DATA_DIR"] = str(self.data_dir)
        env["PYTHONPATH"] = str(PROJECT_ROOT)

        target_cid = "live-target-999"
        target_uri = f"agp://local/cli/{target_cid}"

        # Launch `agp.listen` BEFORE the team even exists!
        listen_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agp.listen",
                "--agent",
                target_uri,
                "--data-dir",
                str(self.data_dir),
                "--timeout",
                "10",
                "--poll-interval",
                "0.08",
                "--json",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
        )

        # Wait until listener registers heartbeat in SQLite
        lead = StdioMCPClient(self.data_dir)
        try:
            lead.request("initialize", {"protocolVersion": "2026-07-28"})
            deadline = time.time() + 4.0
            listening = False
            while time.time() < deadline:
                _, disc = lead.call_tool("agp_discover", {"query": target_cid})
                if disc["agents"] and disc["agents"][0].get("listening"):
                    listening = True
                    break
                time.sleep(0.08)
            self.assertTrue(listening, "agp.listen did not register active listener heartbeat")

            # Lead creates a team and adds target via `team_join` (which sends Team Welcome handshake!)
            lead.call_tool("team_create", {"team": "release-squad", "description": "Coordinate release", "lead_name": "release-lead"})
            ok_j, joined = lead.call_tool(
                "team_join",
                {
                    "team": "release-squad",
                    "name": "projects-dev",
                    "role": "frontend-dev",
                    "conversation_id": target_cid,
                    "notify_member": True,
                    "welcome_note": "Fix duplicate i18n key before deploy",
                },
            )
            self.assertTrue(ok_j)
            self.assertTrue(joined["channel"]["delivery"]["will_auto_receive"])
            self.assertEqual(joined["channel"]["delivery"]["mode"], "reactive_listener")

            out, err = listen_proc.communicate(timeout=5.0)
            self.assertEqual(listen_proc.returncode, 0, msg=f"stderr={err}")
            event = json.loads(out)
            self.assertEqual(event["event"], "messages_received")
            all_text = json.dumps(event, ensure_ascii=False)
            self.assertIn("release-squad", all_text)
            self.assertIn("Fix duplicate i18n key", all_text)
        finally:
            lead.close()
            if listen_proc.poll() is None:
                listen_proc.kill()
                listen_proc.wait(timeout=2.0)

    # 4. Headless lifecycle hooks (`agp_hook.py`) work WITHOUT HTTP server & deliver BOTH AGP and Team messages!
    def test_04_headless_hooks_deliver_team_and_normal_messages_without_http(self) -> None:
        env = dict(os.environ)
        env["AGP_DATA_DIR"] = str(self.data_dir)
        env["AGP_HOME"] = str(PROJECT_ROOT)
        hook_script = str(PROJECT_ROOT / "plugins" / "agp-mesh" / "scripts" / "agp_hook.py")

        def run_hook(event: str, payload: Dict[str, Any]) -> Dict[str, Any]:
            proc = subprocess.run(
                [sys.executable, hook_script, event],
                input=json.dumps(payload),
                capture_output=True,
                text=True,
                encoding="utf-8",
                env=env,
                timeout=8,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            return json.loads(proc.stdout)

        cid = "hook-live-session-1"
        base_payload = {
            "conversationId": cid,
            "transcriptPath": f"C:/Users/Test/.gemini/antigravity-cli/brain/{cid}/transcript.jsonl",
            "cwd": "D:/projects/demo",
            "modelName": "gemini-4-neon",
        }

        # 1. SessionStart registers agent silently (NO HTTP server!) and provides zero-token listen command
        ss = run_hook("session_start", base_payload)
        self.assertIn("listen.py", ss["injectSteps"][0]["systemMessage"]["systemMessage"])

        # 2. Another process sends a NORMAL priority message + acquires a file lease
        sender = StdioMCPClient(self.data_dir)
        try:
            sender.request("initialize", {"protocolVersion": "2026-07-28"})
            sender.call_tool("agp_register", {"agent_uri": "agp://local/cli/sender-1", "display_name": "Sender"})
            sender.call_tool("agp_lease", {"action": "acquire", "owner_uri": "agp://local/cli/sender-1", "paths": ["src/locked.py"]})

            # PreToolUse blocks edit on relative/absolute overlapping path WITHOUT HTTP server!
            denied = run_hook(
                "pre_tool_use",
                {
                    **base_payload,
                    "toolCall": {"id": "tc1", "name": "edit_file", "args": {"Path": "D:\\projects\\demo\\src\\locked.py"}},
                },
            )
            self.assertEqual(denied["decision"], "deny")
            self.assertIn("AGP Lease Guard", denied["reason"])

            # Send NORMAL priority message -> PostInvocation catches & forces continuation!
            sender.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/cli/sender-1",
                    "to_uri": f"agp://local/cli/{cid}",
                    "type": "chat.note",
                    "priority": "normal",
                    "summary": "Mid-turn normal priority heads-up",
                    "payload": {"info": "api changed"},
                },
            )
            post_inv = run_hook("post_invocation", {**base_payload, "invocationNum": 2})
            self.assertEqual(post_inv.get("terminationBehavior"), "force_continue")
            self.assertIn("Mid-turn normal priority heads-up", post_inv["injectSteps"][0]["systemMessage"]["systemMessage"])
            self.assertIn("REPLY ROUTING", post_inv["injectSteps"][0]["systemMessage"]["systemMessage"])

            # Send ANOTHER normal priority message -> Stop hook catches ANY priority (not just urgent!) and injects content directly!
            sender.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/cli/sender-1",
                    "to_uri": f"agp://local/cli/{cid}",
                    "type": "chat.note",
                    "priority": "normal",
                    "summary": "End-of-turn check before sleeping",
                    "payload": {"info": "please reply"},
                },
            )
            stop_out = run_hook("stop", {**base_payload, "terminationReason": "NO_TOOL_CALL", "continuationActive": False})
            self.assertEqual(stop_out.get("decision"), "continue")
            self.assertIn("End-of-turn check before sleeping", stop_out.get("reason", ""))
        finally:
            sender.close()

    # 5. `agp_ask` fail-fast anti-token-burn guard on unreachable idle peer vs instant reply when reachable
    def test_05_agp_ask_fail_fast_vs_correlated_reply(self) -> None:
        store = AGPStore(db_path=self.data_dir / "agp.db")
        tm = TeamManager(teams_dir=self.data_dir / "teams", locks_dir=store.locks_dir)
        rt = AGPBrokerRuntime(store=store, scanner=TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={}), teams=tm)
        try:
            # Target is idle without listener and wakeup_mode='hook' -> agp_ask MUST fail fast in < 0.3s!
            store.upsert_agent(
                {
                    "agent_uri": "agp://local/cli/asleep-peer",
                    "conversation_id": "asleep-peer",
                    "status": "online",
                    "activity_state": "idle",
                    "wakeup_mode": "hook",
                }
            )
            t0 = time.perf_counter()
            _, resp = rt.mcp._execute_tool(
                "agp_ask",
                {
                    "from_uri": "agp://local/cli/caller",
                    "to_uri": "agp://local/cli/asleep-peer",
                    "summary": "Can you check tests?",
                    "wait_timeout_sec": 30,
                },
                session_id=None,
            )
            elapsed = time.perf_counter() - t0
            self.assertFalse(resp["replied"])
            self.assertEqual(resp["status"], "target_unreachable_idle")
            self.assertFalse(resp["delivery"]["will_auto_receive"])
            self.assertLess(elapsed, 0.5)

            # Confirm the fail-fast message was durably queued, then clear it before testing the live listener roundtrip
            queued_msgs = store.poll_inbox("agp://local/cli/asleep-peer")
            self.assertEqual(len(queued_msgs), 1)

            # Now simulate an active listener on target + responder thread -> agp_ask succeeds in 1 call!
            store.set_listener_state("agp://local/cli/asleep-peer", pid=os.getpid())

            def _peer_reply() -> None:
                msgs = store.wait_for_inbox("agp://local/cli/asleep-peer", wait_timeout_sec=3.0)
                if msgs:
                    m = msgs[0]
                    store.send_message(
                        {
                            "from": {"uri": "agp://local/cli/asleep-peer"},
                            "to": {"uri": "agp://local/cli/caller"},
                            "in_reply_to": m["msg_id"],
                            "type": "task.result",
                            "summary": "All 42 tests green",
                            "payload": {"status": "success", "passed": 42},
                        }
                    )

            tr = threading.Thread(target=_peer_reply, daemon=True)
            tr.start()
            _, resp2 = rt.mcp._execute_tool(
                "agp_ask",
                {
                    "from_uri": "agp://local/cli/caller",
                    "to_uri": "agp://local/cli/asleep-peer",
                    "summary": "Run test suite now",
                    "wait_timeout_sec": 5,
                },
                session_id=None,
            )
            tr.join(timeout=2.0)
            self.assertTrue(resp2["replied"])
            self.assertEqual(resp2["status"], "replied")
            self.assertEqual(resp2["replies"][0]["payload"]["passed"], 42)
        finally:
            rt.stop()

    # 6. Auto-bridged reply guarantee: woken auto_cli agent replies in plain text -> worker still sends reply back!
    def test_06_auto_cli_worker_auto_bridges_plain_text_reply(self) -> None:
        store = AGPStore(db_path=self.data_dir / "agp.db")
        tm = TeamManager(teams_dir=self.data_dir / "teams", locks_dir=store.locks_dir)

        def silent_llm_runner(member: dict, prompt: str) -> dict:
            # Simulates an LLM that answers in text but FORGETS to call agp_send!
            return {"conversation_id": member.get("conversation_id", "auto-1"), "response": "Fixed auth bug in routes.py!", "total_tokens": 120}

        rt = AGPBrokerRuntime(
            store=store,
            scanner=TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={}),
            teams=tm,
            agent_runner_fn=silent_llm_runner,
        )
        try:
            store.upsert_agent(
                {
                    "agent_uri": "agp://local/cli/auto-dev",
                    "conversation_id": "auto-dev",
                    "role": "backend",
                    "wakeup_mode": "auto_cli",
                    "status": "online",
                    "activity_state": "idle",
                }
            )
            _, ask_res = rt.mcp._execute_tool(
                "agp_ask",
                {
                    "from_uri": "agp://local/cli/lead-1",
                    "to_uri": "agp://local/cli/auto-dev",
                    "summary": "Fix auth 500",
                    "wait_timeout_sec": 5,
                },
                session_id=None,
            )
            self.assertTrue(ask_res["replied"])
            self.assertTrue(ask_res["replies"][0]["payload"].get("auto_bridged_reply"))
            self.assertIn("Fixed auth bug", ask_res["replies"][0]["payload"]["response"])
        finally:
            rt.stop()

    # 7. Multi-subscriber broadcast (`channel_reads`) & custom pub/sub channel subscriptions
    def test_07_multi_subscriber_broadcast_and_custom_channels(self) -> None:
        store = AGPStore(db_path=self.data_dir / "agp.db")
        try:
            store.upsert_agent({"agent_uri": "agp://local/cli/sub-a", "channels": ["api-contract"]})
            store.upsert_agent({"agent_uri": "agp://local/cli/sub-b", "channels": ["api-contract"]})
            store.upsert_agent({"agent_uri": "agp://local/cli/sub-c", "channels": []})

            # Broadcast to all: BOTH sub-a AND sub-b AND sub-c must receive it independently!
            ok1, _ = store.send_message(
                {
                    "from": {"uri": "agp://local/cli/lead"},
                    "to": {"uri": "all"},
                    "type": "chat.note",
                    "summary": "Release freeze at 18:00",
                    "payload": {"freeze": True},
                }
            )
            self.assertTrue(ok1)

            # Custom channel message: ONLY sub-a and sub-b receive it, NOT sub-c!
            ok2, _ = store.send_message(
                {
                    "from": {"uri": "agp://local/cli/lead"},
                    "to": {"uri": "channel:api-contract"},
                    "type": "chat.note",
                    "summary": "OpenAPI v3.1 updated",
                    "payload": {"ver": "3.1"},
                }
            )
            self.assertTrue(ok2)

            msgs_a = store.poll_inbox("agp://local/cli/sub-a")
            msgs_b = store.poll_inbox("agp://local/cli/sub-b")
            msgs_c = store.poll_inbox("agp://local/cli/sub-c")

            self.assertEqual(len(msgs_a), 2)
            self.assertEqual(len(msgs_b), 2)
            self.assertEqual(len(msgs_c), 1)
            # Second poll for sub-a returns 0 (already read by sub-a!)
            self.assertEqual(len(store.poll_inbox("agp://local/cli/sub-a")), 0)
        finally:
            store.close()

    # 8. Cross-process atomic team_task race + full team lifecycle (list/claim/unclaim/fail/cancel/delete)
    def test_08_cross_process_team_lifecycle_and_atomic_task_claim(self) -> None:
        p1 = StdioMCPClient(self.data_dir)
        p2 = StdioMCPClient(self.data_dir)
        try:
            p1.request("initialize", {"protocolVersion": "2026-07-28"})
            p2.request("initialize", {"protocolVersion": "2026-07-28"})

            p1.call_tool("team_create", {"team": "atomic-team", "description": "Race test"})
            _, t1 = p1.call_tool("team_task", {"team": "atomic-team", "action": "create", "title": "Single hot task"})
            _, t2 = p1.call_tool("team_task", {"team": "atomic-team", "action": "create", "title": "Blocked task", "blocked_by": ["T1"]})
            self.assertEqual(t1["task"]["id"], "T1")
            self.assertEqual(t2["task"]["status"], "blocked")

            # Two separate processes race to claim T1 simultaneously -> strictly ONE wins!
            outcomes: List[bool] = []

            def _claim(cli: StdioMCPClient, who: str) -> None:
                ok, _ = cli.call_tool("team_task", {"team": "atomic-team", "action": "claim", "owner": who})
                outcomes.append(ok)

            th1 = threading.Thread(target=_claim, args=(p1, "worker-1"))
            th2 = threading.Thread(target=_claim, args=(p2, "worker-2"))
            th1.start()
            th2.start()
            th1.join(timeout=5.0)
            th2.join(timeout=5.0)
            self.assertEqual(sorted(outcomes), [False, True])

            # Unclaim -> Complete -> Verify T2 auto-unblocked -> Fail T2 -> team_list -> team_shutdown(all) -> team_delete
            ok_u, u_res = p1.call_tool("team_task", {"team": "atomic-team", "action": "unclaim", "task_id": "T1"})
            self.assertTrue(ok_u)
            self.assertEqual(u_res["task"]["status"], "pending")

            ok_c, c_res = p2.call_tool("team_task", {"team": "atomic-team", "action": "complete", "task_id": "T1", "owner": "worker-2"})
            self.assertTrue(ok_c)
            self.assertIn("T2", c_res["unblocked"])

            ok_f, f_res = p1.call_tool("team_task", {"team": "atomic-team", "action": "fail", "task_id": "T2", "result_summary": "Missing fixture"})
            self.assertTrue(ok_f)
            self.assertEqual(f_res["task"]["status"], "failed")

            ok_tl, tl = p1.call_tool("team_list", {"team": "atomic-team"})
            self.assertTrue(ok_tl)
            self.assertEqual(tl["task_counts"]["completed"], 1)
            self.assertEqual(tl["task_counts"]["failed"], 1)

            ok_sd, _ = p1.call_tool("team_shutdown", {"team": "atomic-team", "name": "all"})
            self.assertTrue(ok_sd)
            ok_del, del_res = p1.call_tool("team_delete", {"team": "atomic-team"})
            self.assertTrue(ok_del and del_res["deleted"])
        finally:
            p1.close()
            p2.close()

    # 9. `notifications/cancelled` interrupts blocking `agp_inbox` immediately + `agp_task` + lease check + artifact pagination + EOF cleanup
    def test_09_cancellation_tasks_leases_artifacts_traces_and_eof_cleanup(self) -> None:
        client = StdioMCPClient(self.data_dir)
        try:
            client.request("initialize", {"protocolVersion": "2026-07-28"})
            client.call_tool("agp_register", {"agent_uri": "agp://local/mcp/worker-x", "owned_paths": ["backend/**"]})

            # Start long-poll agp_inbox (20s) on req_id=777, then send notifications/cancelled!
            client.send_raw(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 777,
                        "method": "tools/call",
                        "params": {"name": "agp_inbox", "arguments": {"agent_uri": "agp://local/mcp/worker-x", "wait_timeout_sec": 20}},
                    }
                )
            )
            time.sleep(0.15)
            t0 = time.perf_counter()
            client.notify("notifications/cancelled", {"requestId": 777, "reason": "user aborted"})
            cancel_resp = client.recv_json(timeout=3.0)
            self.assertEqual(cancel_resp["id"], 777)
            self.assertLess(time.perf_counter() - t0, 1.5)

            # Relative vs absolute Windows path overlap + dry-run lease check + force_release
            self.assertTrue(paths_overlap("src/**", "D:\\repo\\src\\api\\auth.py"))
            self.assertTrue(paths_overlap("src/api.py", "D:/repo/src/api.py"))
            client.call_tool("agp_lease", {"action": "acquire", "owner_uri": "agp://local/mcp/worker-x", "paths": ["backend/app.py"]})
            ok_chk, chk = client.call_tool(
                "agp_lease",
                {"action": "check", "owner_uri": "agp://local/mcp/other-y", "paths": ["D:/workspace/backend/app.py"]},
            )
            self.assertTrue(ok_chk)
            self.assertFalse(chk["can_acquire"])
            self.assertEqual(len(chk["conflicts"]), 1)

            # Artifact DLP on source_path + chunk pagination + agp_artifact_list
            secret_file = self.data_dir / "sample_log.txt"
            secret_file.write_text("HEADER_0123456789\napi_key=my_super_secret_token_999999\n" + ("A" * 500), encoding="utf-8")
            ok_ap, ap = client.call_tool("agp_artifact_put", {"kind": "test_log", "source_path": str(secret_file)})
            self.assertTrue(ok_ap)
            art_id = ap["artifact"]["artifact_id"]
            self.assertTrue(len(ap["redacted_fields"]) >= 1)

            ok_g1, g1 = client.call_tool("agp_artifact_get", {"artifact_id": art_id, "offset_bytes": 0, "max_bytes": 60})
            self.assertTrue(ok_g1 and g1["has_more"])
            self.assertNotIn("my_super_secret_token_999999", g1["content"])
            ok_g2, g2 = client.call_tool("agp_artifact_get", {"artifact_id": art_id, "offset_bytes": g1["next_offset"], "max_bytes": 1000})
            self.assertTrue(ok_g2 and not g2["has_more"])

            ok_al, al = client.call_tool("agp_artifact_list", {"kind": "test_log"})
            self.assertTrue(ok_al and al["count"] >= 1)

            # Durable async task via agp_send + agp_task(list/get/cancel) + agp_trace_inspect(recent)
            _, sent_t = client.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/mcp/worker-x",
                    "to_uri": "agp://local/mcp/other-y",
                    "type": "task.request",
                    "summary": "Async build",
                    "payload": {"goal": "build"},
                    "async_task": True,
                },
            )
            tid = sent_t["task"]["taskId"]
            _, t_list = client.call_tool("agp_task", {"action": "list"})
            self.assertGreaterEqual(t_list["count"], 1)
            _, t_can = client.call_tool("agp_task", {"action": "cancel", "task_id": tid})
            self.assertEqual(t_can["task"]["status"], "cancelled")

            _, recent_traces = client.call_tool("agp_trace_inspect", {})
            self.assertGreaterEqual(recent_traces["count"], 1)

            # agp_status reports leases, unread, and optional monitor status
            _, st = client.call_tool("agp_status", {})
            self.assertEqual(st["mode"], "embedded_serverless_mcp")
            self.assertEqual(len(st["my_leases"]), 1)
        finally:
            # Closing stdio EOF must automatically release worker-x's lease and mark it offline!
            client.close()

        verify_store = AGPStore(db_path=self.data_dir / "agp.db")
        try:
            ag = verify_store.get_agent("agp://local/mcp/worker-x")
            self.assertEqual(ag["status"], "offline")
            _, leases_after = verify_store.manage_lease("list", owner_uri="agp://local/system/check")
            self.assertEqual(len(leases_after["leases"]), 0)
        finally:
            verify_store.close()

    # 10. Optional Monitor UI opens AFTER silent MCP activity -> immediately shows all state -> closes without breaking MCP!
    def test_10_optional_monitor_ui_sees_silent_mcp_work_and_closes_cleanly(self) -> None:
        mcp_cli = StdioMCPClient(self.data_dir)
        try:
            mcp_cli.request("initialize", {"protocolVersion": "2026-07-28"})
            mcp_cli.call_tool("agp_register", {"agent_uri": "agp://local/cli/silent-agent", "display_name": "Silent Worker", "role": "backend"})
            mcp_cli.call_tool("team_create", {"team": "silent-team", "description": "Created while UI closed"})
            mcp_cli.call_tool(
                "agp_send",
                {
                    "from_uri": "agp://local/cli/silent-agent",
                    "to_uri": "all",
                    "type": "chat.note",
                    "summary": "Worked silently with UI closed!",
                    "payload": {"silent": True},
                },
            )

            # Now human opens the optional Monitor UI on a random port pointing at the SAME data_dir!
            ui_store = AGPStore(db_path=self.data_dir / "agp.db")
            ui_teams = TeamManager(teams_dir=self.data_dir / "teams", locks_dir=ui_store.locks_dir)
            ui_rt = AGPBrokerRuntime(
                store=ui_store,
                scanner=TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={}),
                teams=ui_teams,
            )
            server, _ = create_server(host="127.0.0.1", port=0, runtime=ui_rt)
            port = server.server_address[1]
            t = threading.Thread(target=server.serve_forever, daemon=True)
            t.start()
            try:
                dv_before = ui_store.get_data_version()
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/snapshot", timeout=3) as r:
                    snap = json.loads(r.read().decode("utf-8"))
                self.assertGreaterEqual(snap["kpis"]["messages_count"], 1)
                self.assertEqual(snap["kpis"]["team_count"], 1)
                self.assertEqual(snap["teams"][0]["team"], "silent-team")

                # Write another message from headless stdio MCP while Monitor UI is open -> PRAGMA data_version advances!
                mcp_cli.call_tool(
                    "agp_send",
                    {
                        "from_uri": "agp://local/cli/silent-agent",
                        "to_uri": "all",
                        "type": "chat.note",
                        "summary": "Live update from headless MCP",
                        "payload": {"step": 2},
                    },
                )
                dv_after = ui_store.get_data_version()
                self.assertNotEqual(dv_before, dv_after)
            finally:
                server.shutdown()
                server.server_close()
                ui_rt.stop()

            # After Monitor UI is CLOSED, headless MCP continues operating seamlessly!
            ok_after, res_after = mcp_cli.call_tool("agp_status", {})
            self.assertTrue(ok_after)
            self.assertEqual(res_after["mode"], "embedded_serverless_mcp")
        finally:
            mcp_cli.close()

    # 11. Standard MCP CLI flags (`--doctor`, `--print-config`, `--version`) & packaging files
    def test_11_standard_packaging_and_cli_diagnostics(self) -> None:
        env = dict(os.environ)
        env["PYTHONPATH"] = str(PROJECT_ROOT)

        doc = subprocess.run(
            [sys.executable, "-m", "agp.mcp_server", "--doctor", "--data-dir", str(self.data_dir)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            timeout=8,
        )
        self.assertEqual(doc.returncode, 0, msg=doc.stderr)
        doc_json = json.loads(doc.stdout)
        self.assertTrue(doc_json["ok"])
        self.assertEqual(doc_json["storage"]["sqlite_journal_mode"].lower(), "wal")
        self.assertTrue(doc_json["storage"]["cross_process_file_lock"])
        self.assertGreaterEqual(doc_json["mcp_surface"]["tools"], 23)

        cfg = subprocess.run(
            [sys.executable, "-m", "agp.mcp_server", "--print-config", "vscode"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            timeout=8,
        )
        self.assertEqual(cfg.returncode, 0, msg=cfg.stderr)
        cfg_json = json.loads(cfg.stdout)
        self.assertIn("servers", cfg_json)
        self.assertEqual(cfg_json["servers"]["agp-broker"]["type"], "stdio")

        self.assertTrue((PROJECT_ROOT / "pyproject.toml").is_file())
        self.assertTrue((PROJECT_ROOT / "package.json").is_file())
        self.assertTrue((PROJECT_ROOT / "server.json").is_file())
        manifest = json.loads((PROJECT_ROOT / "server.json").read_text(encoding="utf-8"))
        self.assertFalse(manifest["transports"][0]["requiresDaemon"])
        self.assertEqual(doc_json["mcp_surface"]["codemode_tools"], 4)
        self.assertGreaterEqual(doc_json["mcp_surface"]["skills"], 2)

    # 12. Structural valid-JSON ~6,000-token truncation (cloudflare/mcp parity)
    def test_12_structural_json_truncation_always_valid_json(self) -> None:
        from agp.truncate import TRUNCATION_MARKER, format_tool_result

        small = {"ok": True, "items": [1, 2, 3]}
        s_out = format_tool_result(small)
        self.assertEqual(json.loads(s_out), small)
        self.assertNotIn(TRUNCATION_MARKER, s_out)

        huge = {
            "ok": True,
            "summary": "A" * 10000,
            "items": [{"id": i, "body": f"payload-{i}-" + ("x" * 600)} for i in range(80)],
            "extra": {f"k{i}": "v" * 400 for i in range(60)},
        }
        truncated = format_tool_result(huge, truncate=True, max_tokens=1200)
        parsed = json.loads(truncated)  # MUST remain 100% valid JSON!
        self.assertIsInstance(parsed, dict)
        self.assertIn(TRUNCATION_MARKER, truncated)
        self.assertLessEqual(len(truncated), 1200 * 4 + 400)

        # Opt-out `truncate=False` preserves full JSON untouched
        full_out = format_tool_result(huge, truncate=False, max_tokens=1200)
        self.assertNotIn(TRUNCATION_MARKER, full_out)
        self.assertEqual(len(json.loads(full_out)["items"]), 80)

    # 13. 4-tool Code Mode (`whoami`, `docs`, `search`, `execute` chained pipeline + truncation opt-out)
    def test_13_codemode_4_tools_chained_execute_whoami_docs_and_truncation(self) -> None:
        cli = StdioMCPClient(self.data_dir, extra_args=["--codemode"])
        try:
            init = cli.request("initialize", {"protocolVersion": "2026-07-28"})
            self.assertIn("io.modelcontextprotocol/skills", init["result"]["capabilities"]["extensions"])

            tools_res = cli.request("tools/list")
            tools = tools_res["result"]["tools"]
            names = {t["name"] for t in tools}
            self.assertEqual(names, {"whoami", "docs", "search", "execute"})

            # Verify OpenAI profile metadata & outputSchema on whoami
            whoami_def = next(t for t in tools if t["name"] == "whoami")
            self.assertTrue(whoami_def.get("_meta", {}).get("openai/profile"))
            self.assertIn("outputSchema", whoami_def)

            # Call whoami -> returns structuredContent alongside text content
            w_raw = cli.request("tools/call", {"name": "whoami", "arguments": {}})
            self.assertFalse(w_raw["result"].get("isError"))
            self.assertIn("structuredContent", w_raw["result"])
            self.assertTrue(w_raw["result"]["structuredContent"]["id"].startswith("agp://"))

            # Call docs -> returns topic guidance
            ok_d, d_res = cli.call_tool("docs", {"query": "wakeup"})
            self.assertTrue(ok_d)
            self.assertGreaterEqual(d_res["count"], 1)

            # Call search -> finds team_task schema
            ok_s, s_res = cli.call_tool("search", {"scope": "tools", "query": "team_task"})
            self.assertTrue(ok_s)
            self.assertTrue(any(t["name"] == "team_task" for t in s_res["tools"]))

            # Call execute with 3-step chained pipeline using `$prev.task.id`!
            ok_e, e_res = cli.call_tool(
                "execute",
                {
                    "steps": [
                        {"action": "team_create", "args": {"team": "cm-pipe", "description": "Code Mode pipeline"}},
                        {"action": "team_task", "args": {"team": "cm-pipe", "action": "create", "title": "Step 1 API", "owner": "be"}},
                        {
                            "action": "team_task",
                            "args": {
                                "team": "cm-pipe",
                                "action": "create",
                                "title": "Step 2 UI",
                                "owner": "fe",
                                "blocked_by": ["$prev.task.id"],
                            },
                        },
                    ]
                },
            )
            self.assertTrue(ok_e)
            self.assertTrue(e_res["ok"])
            self.assertEqual(e_res["steps_executed"], 3)
            self.assertEqual(e_res["results"][2]["task"]["blocked_by"], ["T1"])

            # Put a 32KB artifact, then read via execute with default truncation vs `truncate: false`
            big_blob = ("LINE_DATA_0123456789\n" * 1600)
            ok_put, put_res = cli.call_tool(
                "execute",
                {
                    "action": "agp_artifact_put",
                    "args": {"kind": "log", "content": big_blob},
                },
            )
            self.assertTrue(ok_put)
            art_id = put_res["artifact"]["artifact_id"]

            # Default truncated read -> valid JSON containing TRUNCATED marker
            trunc_raw = cli.request(
                "tools/call",
                {
                    "name": "execute",
                    "arguments": {"action": "agp_artifact_get", "args": {"artifact_id": art_id, "max_bytes": 65536}},
                },
            )
            trunc_text = trunc_raw["result"]["content"][0]["text"]
            self.assertIn("--- TRUNCATED ---", trunc_text)
            json.loads(trunc_text)  # Valid JSON!

            # Opt-out `truncate: false` -> full content returned untouched
            full_raw = cli.request(
                "tools/call",
                {
                    "name": "execute",
                    "arguments": {
                        "action": "agp_artifact_get",
                        "args": {"artifact_id": art_id, "max_bytes": 65536},
                        "truncate": False,
                    },
                },
            )
            full_text = full_raw["result"]["content"][0]["text"]
            self.assertNotIn("--- TRUNCATED ---", full_text)
            full_obj = json.loads(full_text)
            self.assertEqual(len(full_obj["content"]), len(big_blob))
        finally:
            cli.close()

        # Also verify HTTP Streamable `/mcp?codemode=true&truncateToolResult=false`
        ui_store = AGPStore(db_path=self.data_dir / "agp.db")
        ui_teams = TeamManager(teams_dir=self.data_dir / "teams", locks_dir=ui_store.locks_dir)
        ui_rt = AGPBrokerRuntime(
            store=ui_store,
            scanner=TranscriptScanner(brain_dirs={}, presence_dirs={}, summaries_dbs={}),
            teams=ui_teams,
        )
        server, _ = create_server(host="127.0.0.1", port=0, runtime=ui_rt)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/mcp?codemode=true&truncateToolResult=false",
                data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=4) as r:
                body = json.loads(r.read().decode("utf-8"))
            self.assertEqual({tool["name"] for tool in body["result"]["tools"]}, {"whoami", "docs", "search", "execute"})
        finally:
            server.shutdown()
            server.server_close()
            ui_rt.stop()

    # 14. MCP Skills Extension (`io.modelcontextprotocol/skills`: skills/list, skills/get, skill:// resources)
    def test_14_mcp_skills_extension_list_get_and_resource_digest(self) -> None:
        cli = StdioMCPClient(self.data_dir)
        try:
            init = cli.request("initialize", {"protocolVersion": "2026-07-28"})
            self.assertIn("io.modelcontextprotocol/skills", init["result"]["capabilities"]["extensions"])

            s_list = cli.request("skills/list")
            skills = s_list["result"]["skills"]
            skill_names = {s["frontmatter"]["name"] for s in skills}
            self.assertIn("agp-protocol", skill_names)
            self.assertIn("two-way-wakeup", skill_names)

            s_get = cli.request("skills/get", {"uri": "skill://agp-protocol/SKILL.md"})
            skill_obj = s_get["result"]["skill"]
            self.assertEqual(skill_obj["frontmatter"]["name"], "agp-protocol")
            entry = next(f for f in skill_obj["files"] if f["path"] == "SKILL.md")
            expected_digest = entry["digest"]

            res_read = cli.request("resources/read", {"uri": "skill://agp-protocol/SKILL.md"})
            content = res_read["result"]["contents"][0]["text"]
            actual_digest = "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
            self.assertEqual(actual_digest, expected_digest)
            self.assertIn("agp-protocol", content)
        finally:
            cli.close()

    # 15. Real Node.js npm/npx CLI wrapper (`bin/agp-mcp.mjs`): stdio, doctor, config, and offline install
    def test_15_npm_node_cli_wrapper_stdio_doctor_config_and_offline_install(self) -> None:
        if not shutil.which("node"):
            self.skipTest("node not installed in test environment")

        # 1. `node bin/agp-mcp.mjs doctor` & `config cursor --codemode`
        doc = subprocess.run(
            ["node", str(PROJECT_ROOT / "bin" / "agp-mcp.mjs"), "doctor", "--data-dir", str(self.data_dir)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
        self.assertEqual(doc.returncode, 0, msg=doc.stderr)
        doc_json = json.loads(doc.stdout)
        self.assertTrue(doc_json["ok"])
        self.assertEqual(doc_json["mcp_surface"]["codemode_tools"], 4)

        cfg = subprocess.run(
            ["node", str(PROJECT_ROOT / "bin" / "agp-mcp.mjs"), "config", "cursor", "--codemode"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
        self.assertEqual(cfg.returncode, 0, msg=cfg.stderr)
        cfg_json = json.loads(cfg.stdout)
        self.assertEqual(cfg_json["mcpServers"]["agp-broker"]["command"], "npx")
        self.assertEqual(cfg_json["mcpServers"]["agp-broker"]["args"], ["-y", "agp-mcp", "--codemode"])

        # 2. Live stdio JSON-RPC session through `node bin/agp-mcp.mjs --codemode` (ZERO stdout noise!)
        node_cli = StdioMCPClient(self.data_dir, use_node_cli=True, extra_args=["--codemode"])
        try:
            init = node_cli.request("initialize", {"protocolVersion": "2026-07-28"})
            self.assertEqual(init["result"]["serverInfo"]["name"], "agp-broker")
            tlist = node_cli.request("tools/list")
            self.assertEqual({t["name"] for t in tlist["result"]["tools"]}, {"whoami", "docs", "search", "execute"})
        finally:
            node_cli.close()

        # 3. `node bin/agp-mcp.mjs install --no-agy --target-dir <tmp>` bundles offline Python runtime
        plugin_dst = self.data_dir / "installed-plugin"
        inst = subprocess.run(
            [
                "node",
                str(PROJECT_ROOT / "bin" / "agp-mcp.mjs"),
                "install",
                "--no-agy",
                "--target-dir",
                str(plugin_dst),
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=15,
        )
        self.assertEqual(inst.returncode, 0, msg=inst.stderr)
        self.assertTrue((plugin_dst / "runtime" / "agp" / "mcp_server.py").is_file())

        # Execute bundled hook with ZERO AGP_HOME / PYTHONPATH to verify offline self-contained runtime!
        clean_env = {k: v for k, v in os.environ.items() if k not in {"AGP_HOME", "PYTHONPATH"}}
        clean_env["AGP_DATA_DIR"] = str(self.data_dir)
        hook_proc = subprocess.run(
            [sys.executable, str(plugin_dst / "scripts" / "agp_hook.py"), "session_start"],
            input=json.dumps({"conversationId": "offline-hook-test", "cwd": str(self.data_dir)}),
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=clean_env,
            timeout=10,
        )
        self.assertEqual(hook_proc.returncode, 0, msg=hook_proc.stderr)
        hook_out = json.loads(hook_proc.stdout)
        self.assertIn("injectSteps", hook_out)


if __name__ == "__main__":
    unittest.main()
