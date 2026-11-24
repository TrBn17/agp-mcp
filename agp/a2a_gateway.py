"""Normative A2A Protocol v1.0 (+ v0.3 legacy compatible) Gateway Adapter for AGP Mesh.

Exposes:
  - `GET /.well-known/agent-card.json` (RFC 8615) signed with RFC 7515 EdDSA JWS over RFC 8785 JCS
  - `GET /.well-known/jwks.json` (RFC 7517 public verification keys)
  - `GET /extendedAgentCard` & `GET /v1/card` (`GetExtendedAgentCard`)
  - JSON-RPC 2.0 endpoint (`POST /a2a`) supporting both v1.0 PascalCase (`SendMessage`, `GetTask`, ...)
    and v0.3 slash methods (`message/send`, `tasks/get`, ...)
  - HTTP+JSON REST bindings (`POST /message:send`, `POST /message:stream`, `GET /tasks/{id}`,
    `GET /tasks`, `POST /tasks/{id}:cancel`, `POST /tasks/{id}:subscribe`, and
    `PushNotificationConfig` CRUD under `/tasks/{id}/pushNotificationConfigs`)
  - SSRF-guarded asynchronous webhook push dispatcher signed with EdDSA Bearer JWT + `X-A2A-Notification-Token`
"""

from __future__ import annotations

import base64
import ipaddress
import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import __version__
from .config import ALLOW_LOOPBACK_WEBHOOKS
from .crypto import (
    get_or_create_agent_keypair,
    public_jwk_from_keypair,
    sign_jws_detached,
    sign_jwt,
)
from .storage import AGPStore, utc_now_iso

AGP_WORKSPACE_EXTENSION_URI = "https://agp.antigravity.dev/extensions/workspace-governance/v1"

TERMINAL_TASK_STATES = {
    "TASK_STATE_COMPLETED",
    "TASK_STATE_FAILED",
    "TASK_STATE_CANCELED",
    "TASK_STATE_REJECTED",
}


def validate_webhook_url(url: str, allow_loopback: bool = ALLOW_LOOPBACK_WEBHOOKS) -> Tuple[bool, str]:
    """Validate PushNotificationConfig URL against SSRF vectors."""
    try:
        parsed = urllib.parse.urlparse((url or "").strip())
    except Exception:
        return False, "Malformed webhook URL"

    if parsed.scheme not in {"https", "http"}:
        return False, "Webhook URL must use https:// or http://"
    if parsed.username or parsed.password:
        return False, "Webhook URL must not embed credentials"
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False, "Webhook URL missing hostname"

    # Resolve literal IP or hostname to inspect IP ranges
    ips: List[ipaddress._BaseAddress] = []
    try:
        ips.append(ipaddress.ip_address(host))
    except ValueError:
        if host == "localhost":
            ips.append(ipaddress.ip_address("127.0.0.1"))
        else:
            try:
                infos = socket.getaddrinfo(host, parsed.port or 443, proto=socket.IPPROTO_TCP)
                for info in infos:
                    sockaddr = info[4]
                    if sockaddr and sockaddr[0]:
                        ips.append(ipaddress.ip_address(sockaddr[0]))
            except OSError:
                # Unresolvable external host at config time: reject if internal-looking, else allow
                if host.endswith(".internal") or host.endswith(".local"):
                    return False, f"Internal hostname '{host}' rejected by SSRF policy"

    for ip in ips:
        if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            return False, f"Disallowed webhook target IP: {ip}"
        if ip.is_loopback:
            if not allow_loopback:
                return False, "Loopback webhook URLs are disabled by policy"
            continue
        if ip.is_private and not allow_loopback:
            return False, f"Private network webhook IP '{ip}' rejected by SSRF policy"

    return True, ""


class A2AGateway:
    """Bridges normative A2A v1.0 (+ v0.3) RPCs/REST/Webhooks directly onto AGPStore."""

    BROKER_CARD_URI = "agp://local/gateway/a2a-broker"

    def __init__(
        self,
        store: AGPStore,
        notify_fn: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        resolve_target_fn: Optional[Callable[[str], str]] = None,
        wakeup_agent_fn: Optional[Callable[[str], bool]] = None,
        allow_loopback_webhooks: bool = ALLOW_LOOPBACK_WEBHOOKS,
    ) -> None:
        self.store = store
        self.notify_fn = notify_fn or (lambda _ev, _data: None)
        self.resolve_target_fn = resolve_target_fn
        self.wakeup_agent_fn = wakeup_agent_fn
        self.allow_loopback_webhooks = allow_loopback_webhooks
        self._broker_kp = get_or_create_agent_keypair(self.BROKER_CARD_URI, keys_dir=self.store.keys_dir)
        # Register task state listener on store so AGP acks/replies auto-trigger A2A webhooks & SSE!
        self.store.register_task_listener(self._on_store_task_updated)

    # ------------------------------------------------------------------
    # Discovery: Signed AgentCard (`/.well-known/agent-card.json`) & JWKS
    # ------------------------------------------------------------------

    def build_jwks(self) -> Dict[str, Any]:
        """Return RFC 7517 JWKS containing the broker gateway key + active registered agent keys."""
        keys = [public_jwk_from_keypair(self._broker_kp)]
        seen_kids = {self._broker_kp["kid"]}
        for ag in self.store.list_agents():
            kid = ag.get("key_id") or ""
            pub = ag.get("public_key") or ""
            if kid and pub and kid not in seen_kids:
                seen_kids.add(kid)
                keys.append(
                    {
                        "kty": "OKP",
                        "crv": "Ed25519",
                        "alg": "EdDSA",
                        "use": "sig",
                        "kid": kid,
                        "x": pub,
                    }
                )
        return {"keys": keys}

    def build_agent_card(self, base_url: str, extended: bool = False) -> Dict[str, Any]:
        """Build and cryptographically sign an A2A v1.0 `AgentCard` (JWS over RFC 8785 JCS)."""
        base = base_url.rstrip("/")
        online_agents = self.store.list_agents(status="online")
        skills: List[Dict[str, Any]] = []
        for ag in online_agents[:32]:
            role = ag.get("role") or "specialist"
            uri = ag.get("agent_uri") or ""
            skill_id = uri.split("/")[-1][:32] if uri else role
            skill: Dict[str, Any] = {
                "id": f"{role}:{skill_id}",
                "name": ag.get("display_name") or role,
                "description": ag.get("description") or f"AGY {role} agent on AGP mesh",
                "tags": sorted(set([role, *(ag.get("capabilities") or [])])),
                "inputModes": ["text/plain", "application/json"],
                "outputModes": ["text/plain", "application/json"],
            }
            if extended:
                skill["metadata"] = {
                    "agent_uri": uri,
                    "owned_paths": ag.get("owned_paths") or [],
                    "trust_tier": ag.get("trust_tier", "local-same-user"),
                    "key_id": ag.get("key_id", ""),
                }
            skills.append(skill)

        if not skills:
            skills.append(
                {
                    "id": "agp-coordinator",
                    "name": "AGP Mesh Coordinator",
                    "description": "Routes multi-agent software engineering tasks across Antigravity sessions.",
                    "tags": ["agp", "mcp", "a2a", "coding"],
                    "inputModes": ["text/plain", "application/json"],
                    "outputModes": ["text/plain", "application/json"],
                }
            )

        card: Dict[str, Any] = {
            "name": "Antigravity AGP Mesh Gateway",
            "description": (
                "A2A v1.0 interoperable gateway bridging external autonomous agents to local/team "
                "Antigravity (AGY) specialists with workspace lease governance and SHA-256 artifacts."
            ),
            "version": __version__,
            "provider": {
                "organization": "Antigravity AGP",
                "url": base,
            },
            "documentationUrl": f"{base}/",
            "supportedInterfaces": [
                {
                    "url": f"{base}/a2a",
                    "protocolBinding": "JSONRPC",
                    "protocolVersion": "1.0",
                },
                {
                    "url": base,
                    "protocolBinding": "HTTP+JSON",
                    "protocolVersion": "1.0",
                },
            ],
            "defaultInputModes": ["text/plain", "application/json"],
            "defaultOutputModes": ["text/plain", "application/json"],
            "capabilities": {
                "streaming": True,
                "pushNotifications": True,
                "stateTransitionHistory": True,
                "extendedAgentCard": True,
                "extensions": [
                    {
                        "uri": AGP_WORKSPACE_EXTENSION_URI,
                        "description": (
                            "Supports workspace path ownership, monotonic fencing-token file leases, "
                            "RFC 8693 actor chains, and taint tracking."
                        ),
                        "required": False,
                    }
                ],
            },
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT",
                }
            },
            "skills": skills,
        }
        if extended:
            _, lease_info = self.store.manage_lease("list", owner_uri=self.BROKER_CARD_URI)
            card["extendedMetadata"] = {
                "jwksUri": f"{base}/.well-known/jwks.json",
                "mcpEndpoint": f"{base}/mcp",
                "activeLeaseCount": len(lease_info.get("leases") or []),
            }

        jws = sign_jws_detached(
            card,
            private_seed_b64=self._broker_kp["private_seed"],
            kid=self._broker_kp["kid"],
        )
        card["signatures"] = [jws]
        return card

    # ------------------------------------------------------------------
    # Extension Negotiation Check
    # ------------------------------------------------------------------

    def validate_extensions_header(
        self, header_val: Optional[str], required_uris: Optional[List[str]] = None
    ) -> Tuple[bool, str]:
        """Validate `A2A-Extensions` header when required extensions are configured."""
        req_set = set(required_uris or [])
        if not req_set:
            return True, ""
        provided = {
            u.strip() for u in (header_val or "").split(",") if u and u.strip()
        }
        missing = sorted(req_set - provided)
        if missing:
            return False, f"ExtensionSupportRequiredError: missing required A2A-Extensions {missing}"
        return True, ""

    # ------------------------------------------------------------------
    # Core A2A Operations (`SendMessage`, `GetTask`, `ListTasks`, `CancelTask`, PushConfigs)
    # ------------------------------------------------------------------

    def _select_target_uri(self, params: Dict[str, Any], tenant: str = "") -> str:
        msg = params.get("message") or {}
        meta = msg.get("metadata") or params.get("metadata") or {}
        explicit = (
            meta.get("target_uri")
            or meta.get("to_uri")
            or tenant
            or ""
        ).strip()
        if explicit:
            if self.resolve_target_fn:
                return self.resolve_target_fn(explicit)
            return explicit
        online = self.store.list_agents(status="online")
        if online:
            return online[0]["agent_uri"]
        return "agp://local/channel/all"

    @staticmethod
    def _extract_parts(parts: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
        """Extract summary text, structured data payload, and inline raw/url parts from A2A v1.0 & v0.3 `Part[]`."""
        texts: List[str] = []
        data_payload: Dict[str, Any] = {}
        extra_parts: List[Dict[str, Any]] = []

        for idx, p in enumerate(parts or []):
            if not isinstance(p, dict):
                continue
            # v1.0 member-presence (`text`, `data`, `url`, `raw`) + v0.3 `kind` fallback
            if "text" in p and isinstance(p["text"], str):
                texts.append(p["text"].strip())
            elif "data" in p:
                d = p["data"]
                if isinstance(d, dict):
                    data_payload.update(d)
                else:
                    data_payload[f"part_{idx}"] = d
            elif "raw" in p or "url" in p or p.get("kind") == "file":
                extra_parts.append(p)
        summary = " ".join(t for t in texts if t).strip()
        if not summary and data_payload:
            summary = str(data_payload.get("goal") or data_payload.get("summary") or "A2A structured task")
        return summary or "A2A delegated task", data_payload, extra_parts

    def send_message(
        self,
        params: Dict[str, Any],
        tenant: str = "",
        caller_uri: str = "agp://external/a2a/client",
    ) -> Tuple[bool, Dict[str, Any]]:
        """Execute A2A v1.0 `SendMessage` (`SendMessageResponse`: `{ "task": Task }`)."""
        msg = params.get("message")
        if not isinstance(msg, dict):
            return False, {"code": -32602, "message": "SendMessage requires a 'message' object"}

        parts = msg.get("parts") or []
        if not isinstance(parts, list) or not parts:
            return False, {"code": -32602, "message": "message.parts must be a non-empty array"}

        msg_id = msg.get("messageId") or f"a2a_msg_{uuid.uuid4().hex[:12]}"
        task_id = msg.get("taskId") or f"tsk_{uuid.uuid4().hex[:16]}"
        context_id = msg.get("contextId") or f"ctx_{uuid.uuid4().hex[:16]}"

        # Verify contextId consistency if task already exists
        existing_task = self.store.get_task(task_id)
        if existing_task and msg.get("contextId") and existing_task["contextId"] != msg["contextId"]:
            return False, {
                "code": -32602,
                "message": f"contextId mismatch for task '{task_id}': expected '{existing_task['contextId']}'",
            }
        if existing_task:
            context_id = existing_task["contextId"]

        summary, data_payload, extra_parts = self._extract_parts(parts)
        target_uri = self._select_target_uri(params, tenant=tenant)

        # Store raw binary parts in SHA-256 Artifact Store
        artifacts: List[Dict[str, Any]] = []
        for ep in extra_parts:
            if "raw" in ep:
                try:
                    raw_b = base64.b64decode(ep["raw"])
                    text_c = raw_b.decode("utf-8", errors="replace")
                except Exception:
                    text_c = str(ep["raw"])
                ok_art, art_res = self.store.put_artifact(
                    kind=ep.get("filename") or "a2a_raw_part",
                    owner_uri=caller_uri,
                    content=text_c,
                    mime_type=ep.get("mediaType") or "application/octet-stream",
                )
                if ok_art:
                    artifacts.append(art_res["artifact"])

        is_clarification = (
            existing_task is not None
            and existing_task.get("status", {}).get("state")
            in {"TASK_STATE_INPUT_REQUIRED", "TASK_STATE_AUTH_REQUIRED"}
        )
        agp_type = "chat.note" if is_clarification else "task.request"
        agp_payload = dict(data_payload)
        if agp_type == "task.request" and not agp_payload.get("goal"):
            agp_payload["goal"] = summary

        meta = msg.get("metadata") or {}
        ok_send, send_res = self.store.send_message(
            {
                "msg_id": msg_id,
                "trace_id": context_id,
                "correlation_id": task_id,
                "from": {"uri": meta.get("from_uri") or caller_uri, "role": "a2a-client"},
                "to": {"uri": target_uri},
                "type": agp_type,
                "priority": meta.get("priority", "high"),
                "summary": summary[:600],
                "verbatim_constraints": meta.get("verbatim_constraints") or [],
                "payload": agp_payload,
                "artifacts": artifacts,
                "visited_uris": meta.get("visited_uris") or [],
                "security": {
                    "trust_tier": meta.get("trust_tier", "external"),
                    "content_taint": meta.get("content_taint", "peer_generated"),
                },
            }
        )
        if not ok_send:
            return False, {
                "code": -32001,
                "message": send_res.get("error", "AGP validation failed"),
                "data": {"error_code": send_res.get("error_code")},
            }

        normalized_msg = {
            "messageId": msg_id,
            "contextId": context_id,
            "taskId": task_id,
            "role": msg.get("role") or "ROLE_USER",
            "parts": parts,
        }
        if msg.get("extensions"):
            normalized_msg["extensions"] = msg["extensions"]

        new_state = "TASK_STATE_WORKING" if is_clarification else "TASK_STATE_SUBMITTED"
        task_obj = self.store.upsert_task(
            task_id=task_id,
            context_id=context_id,
            protocol="a2a",
            from_uri=meta.get("from_uri") or caller_uri,
            to_uri=target_uri,
            state=new_state,
            title=summary[:120],
            append_message=normalized_msg,
            metadata=meta,
        )

        # Register inline PushNotificationConfig if supplied in configuration
        cfg = params.get("configuration") or {}
        push_cfg = cfg.get("pushNotificationConfig")
        if isinstance(push_cfg, dict) and push_cfg.get("url"):
            ok_p, p_err = self.create_push_config(task_id, push_cfg)
            if not ok_p:
                return False, p_err

        # Trigger unified auto-wakeup if target agent opted in
        if self.wakeup_agent_fn:
            self.wakeup_agent_fn(target_uri)

        return True, {"task": task_obj}

    def get_task(self, task_id: str, history_length: Optional[int] = None) -> Tuple[bool, Dict[str, Any]]:
        task = self.store.get_task(task_id)
        if not task:
            return False, {"code": -32001, "message": f"TaskNotFoundError: Task '{task_id}' not found"}
        if history_length is not None and history_length >= 0:
            task = dict(task)
            hist = task.get("history") or []
            task["history"] = hist[-int(history_length) :] if history_length > 0 else []
        return True, task

    def list_tasks(
        self, context_id: Optional[str] = None, state: Optional[str] = None, limit: int = 50
    ) -> Tuple[bool, Dict[str, Any]]:
        tasks = self.store.list_tasks(context_id=context_id, state=state, limit=limit)
        return True, {"tasks": tasks, "nextPageToken": ""}

    def cancel_task(self, task_id: str, reason: str = "Canceled via A2A CancelTask") -> Tuple[bool, Dict[str, Any]]:
        ok, res = self.store.cancel_task(task_id, reason=reason)
        if not ok:
            return False, {"code": -32001, "message": res.get("error", "Cannot cancel task")}
        return True, res["task"]

    # ------------------------------------------------------------------
    # PushNotificationConfig CRUD & Signed Webhook Delivery
    # ------------------------------------------------------------------

    def create_push_config(self, task_id: str, config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        url = str(config.get("url") or "").strip()
        valid_url, reason = validate_webhook_url(url, allow_loopback=self.allow_loopback_webhooks)
        if not valid_url:
            return False, {"code": -32602, "message": f"Invalid PushNotificationConfig URL: {reason}"}
        task = self.store.get_task(task_id)
        if not task:
            return False, {"code": -32001, "message": f"TaskNotFoundError: Task '{task_id}' not found"}
        saved = self.store.create_push_config(
            task_id=task_id,
            url=url,
            token=str(config.get("token") or ""),
            authentication=config.get("authentication") or {},
            config_id=config.get("id"),
        )
        return True, saved

    def get_push_config(self, task_id: str, config_id: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
        cfg = self.store.get_push_config(task_id=task_id, config_id=config_id)
        if not cfg:
            return False, {"code": -32001, "message": "PushNotificationConfig not found"}
        return True, cfg

    def list_push_configs(self, task_id: str) -> Tuple[bool, Dict[str, Any]]:
        return True, {"configs": self.store.list_push_configs(task_id)}

    def delete_push_config(self, task_id: str, config_id: str) -> Tuple[bool, Dict[str, Any]]:
        removed = self.store.delete_push_config(task_id=task_id, config_id=config_id)
        if not removed:
            return False, {"code": -32001, "message": "PushNotificationConfig not found"}
        return True, {}

    def _on_store_task_updated(self, task: Dict[str, Any]) -> None:
        """Broadcast live A2A task updates to SSEHub and registered PushNotificationConfig webhooks."""
        task_id = task.get("id", "")
        event_payload = {
            "statusUpdate": {
                "taskId": task_id,
                "contextId": task.get("contextId", ""),
                "status": task.get("status", {}),
                "final": (task.get("status") or {}).get("state") in TERMINAL_TASK_STATES,
            },
            "task": task,
        }
        self.notify_fn("a2a_task_update", event_payload)

        configs = self.store.list_push_configs(task_id)
        if not configs:
            return
        for cfg in configs:
            threading.Thread(
                target=self._deliver_webhook,
                args=(cfg, event_payload),
                name=f"a2a-webhook-{task_id[:8]}",
                daemon=True,
            ).start()

    def _deliver_webhook(self, cfg: Dict[str, Any], payload: Dict[str, Any]) -> None:
        url = cfg.get("url", "")
        valid, _ = validate_webhook_url(url, allow_loopback=self.allow_loopback_webhooks)
        if not valid:
            return
        now = int(time.time())
        jwt_token = sign_jwt(
            {
                "iss": self.BROKER_CARD_URI,
                "aud": url,
                "iat": now,
                "exp": now + 120,
                "jti": f"wh_{uuid.uuid4().hex[:12]}",
                "task_id": cfg.get("taskId"),
            },
            private_seed_b64=self._broker_kp["private_seed"],
            kid=self._broker_kp["kid"],
        )
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "A2A-Version": "1.0",
            "Authorization": f"Bearer {jwt_token}",
        }
        if cfg.get("token"):
            headers["X-A2A-Notification-Token"] = str(cfg["token"])

        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=4.0):
                pass
        except Exception:
            pass

    # ------------------------------------------------------------------
    # JSON-RPC 2.0 Dispatcher (`POST /a2a`)
    # ------------------------------------------------------------------

    def handle_jsonrpc(self, body: Dict[str, Any], base_url: str) -> Tuple[int, Dict[str, Any]]:
        if not isinstance(body, dict) or body.get("jsonrpc") != "2.0":
            return 400, {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "Invalid JSON-RPC 2.0 request"},
            }
        req_id = body.get("id")
        method = str(body.get("method") or "")
        params = body.get("params") or {}

        if method in {"SendMessage", "message/send"}:
            ok, res = self.send_message(params)
            if ok and method == "message/send":
                # v0.3 compatibility: flat Task return when legacy method name is invoked
                res = res["task"]
        elif method in {"GetTask", "tasks/get"}:
            tid = str(params.get("id") or params.get("taskId") or "")
            ok, res = self.get_task(tid, history_length=params.get("historyLength"))
        elif method in {"ListTasks", "tasks/list"}:
            ok, res = self.list_tasks(
                context_id=params.get("contextId"),
                state=params.get("status") or params.get("state"),
                limit=int(params.get("pageSize") or params.get("limit") or 50),
            )
        elif method in {"CancelTask", "tasks/cancel"}:
            tid = str(params.get("id") or params.get("taskId") or "")
            ok, res = self.cancel_task(tid)
        elif method in {"GetExtendedAgentCard", "agent/getAuthenticatedExtendedCard"}:
            ok, res = True, self.build_agent_card(base_url=base_url, extended=True)
        elif method in {"CreateTaskPushNotificationConfig", "tasks/pushNotificationConfig/set"}:
            tid = str(params.get("taskId") or params.get("id") or "")
            cfg = params.get("pushNotificationConfig") or params.get("config") or params
            ok, res = self.create_push_config(tid, cfg)
        elif method in {"GetTaskPushNotificationConfig", "tasks/pushNotificationConfig/get"}:
            tid = str(params.get("taskId") or params.get("id") or "")
            cid = params.get("configId") or params.get("pushNotificationConfigId")
            ok, res = self.get_push_config(tid, config_id=cid)
        elif method in {"ListTaskPushNotificationConfigs", "tasks/pushNotificationConfig/list"}:
            tid = str(params.get("taskId") or params.get("id") or "")
            ok, res = self.list_push_configs(tid)
        elif method in {"DeleteTaskPushNotificationConfig", "tasks/pushNotificationConfig/delete"}:
            tid = str(params.get("taskId") or params.get("id") or "")
            cid = str(params.get("configId") or params.get("pushNotificationConfigId") or "")
            ok, res = self.delete_push_config(tid, config_id=cid)
        else:
            return 200, {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            }

        if not ok:
            return 200, {"jsonrpc": "2.0", "id": req_id, "error": res}
        return 200, {"jsonrpc": "2.0", "id": req_id, "result": res}
