"""Zero-dependency cryptographic primitives for AGP & A2A v1.0 interoperability.

Implements:
  - RFC 8785 JSON Canonicalization Scheme (JCS)
  - RFC 8032 Ed25519 key generation, signing & verification (uses `cryptography` C-backend
    automatically if installed, with a byte-for-byte identical pure-Python stdlib fallback)
  - RFC 7515 JSON Web Signature (JWS, `alg: "EdDSA"`) & RFC 7517 JWKS export for A2A `AgentCardSignature`
  - RFC 8693-style OAuth Token Exchange nested `act` delegation tokens + RFC 8707 `aud` binding
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import secrets
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import KEYS_DIR, ensure_data_dirs

try:
    from cryptography.hazmat.primitives import serialization as _crypto_ser
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey as _FastEdPriv,
        Ed25519PublicKey as _FastEdPub,
    )

    _HAS_CRYPTOGRAPHY = True
except Exception:  # pragma: no cover - pure-Python RFC 8032 fallback handles everything
    _HAS_CRYPTOGRAPHY = False


# ----------------------------------------------------------------------
# Base64url helpers (RFC 4648 Section 5, unpadded)
# ----------------------------------------------------------------------


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(text: str) -> bytes:
    raw = (text or "").strip().encode("ascii")
    pad = (-len(raw)) % 4
    if pad:
        raw += b"=" * pad
    return base64.urlsafe_b64decode(raw)


# ----------------------------------------------------------------------
# RFC 8785 JSON Canonicalization Scheme (JCS)
# ----------------------------------------------------------------------


def _jcs_sort_key(key: str) -> List[int]:
    """RFC 8785 Section 3.2.3 sorts object property names by UTF-16 code units."""
    encoded = key.encode("utf-16-be")
    return [int.from_bytes(encoded[i : i + 2], "big") for i in range(0, len(encoded), 2)]


def _jcs_serialize(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError("RFC 8785 JCS forbids NaN and Infinity")
        if value == 0.0:
            return "0"
        return json.dumps(value, allow_nan=False)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, allow_nan=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_jcs_serialize(item) for item in value) + "]"
    if isinstance(value, dict):
        items = sorted(value.items(), key=lambda kv: _jcs_sort_key(str(kv[0])))
        parts = [f"{json.dumps(str(k), ensure_ascii=False)}:{_jcs_serialize(v)}" for k, v in items]
        return "{" + ",".join(parts) + "}"
    return _jcs_serialize(str(value))


def jcs_canonicalize(value: Any) -> bytes:
    """Serialize a JSON-compatible Python value into canonical RFC 8785 UTF-8 bytes."""
    return _jcs_serialize(value).encode("utf-8")


# ----------------------------------------------------------------------
# Pure-Python RFC 8032 Ed25519 (Extended Twisted Edwards Coordinates)
# ----------------------------------------------------------------------

_P = (1 << 255) - 19
_L = (1 << 252) + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _P - 2, _P)) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _x_recover(y: int) -> int:
    xx = ((y * y - 1) * pow(_D * y * y + 1, _P - 2, _P)) % _P
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = (x * _I) % _P
    if (x * x - xx) % _P != 0:
        raise ValueError("Invalid Ed25519 point")
    if x & 1:
        x = _P - x
    return x


_BY = (4 * pow(5, _P - 2, _P)) % _P
_BX = _x_recover(_BY)
_BASE_POINT = (_BX, _BY, 1, (_BX * _BY) % _P)
_NEUTRAL_POINT = (0, 1, 1, 0)


def _ed_add(
    p1: Tuple[int, int, int, int], p2: Tuple[int, int, int, int]
) -> Tuple[int, int, int, int]:
    x1, y1, z1, t1 = p1
    x2, y2, z2, t2 = p2
    a = ((y1 - x1) * (y2 - x2)) % _P
    b = ((y1 + x1) * (y2 + x2)) % _P
    c = (t1 * 2 * _D * t2) % _P
    d = (z1 * 2 * z2) % _P
    e = (b - a) % _P
    f = (d - c) % _P
    g = (d + c) % _P
    h = (b + a) % _P
    return (e * f) % _P, (g * h) % _P, (f * g) % _P, (e * h) % _P


def _ed_scalar_mult(scalar: int, pt: Tuple[int, int, int, int]) -> Tuple[int, int, int, int]:
    res = _NEUTRAL_POINT
    addend = pt
    k = scalar
    while k > 0:
        if k & 1:
            res = _ed_add(res, addend)
        addend = _ed_add(addend, addend)
        k >>= 1
    return res


def _ed_compress(pt: Tuple[int, int, int, int]) -> bytes:
    x, y, z, _ = pt
    zi = pow(z, _P - 2, _P)
    xn = (x * zi) % _P
    yn = (y * zi) % _P
    encoded = yn | ((xn & 1) << 255)
    return encoded.to_bytes(32, "little")


def _ed_decompress(raw: bytes) -> Tuple[int, int, int, int]:
    if len(raw) != 32:
        raise ValueError("Ed25519 public key / point must be 32 bytes")
    val = int.from_bytes(raw, "little")
    y = val & ((1 << 255) - 1)
    sign = (val >> 255) & 1
    if y >= _P:
        raise ValueError("Ed25519 point y out of range")
    x = _x_recover(y)
    if x == 0 and sign == 1:
        raise ValueError("Invalid negative zero x in Ed25519 point")
    if (x & 1) != sign:
        x = _P - x
    return (x, y, 1, (x * y) % _P)


def ed25519_public_from_seed(seed: bytes) -> bytes:
    """Derive 32-byte RFC 8032 Ed25519 public key from 32-byte private seed."""
    if len(seed) != 32:
        raise ValueError("Ed25519 seed must be 32 bytes")
    if _HAS_CRYPTOGRAPHY:
        priv = _FastEdPriv.from_private_bytes(seed)
        return priv.public_key().public_bytes(_crypto_ser.Encoding.Raw, _crypto_ser.PublicFormat.Raw)
    h = hashlib.sha512(seed).digest()
    a_bytes = bytearray(h[:32])
    a_bytes[0] &= 248
    a_bytes[31] &= 127
    a_bytes[31] |= 64
    a = int.from_bytes(a_bytes, "little")
    return _ed_compress(_ed_scalar_mult(a, _BASE_POINT))


def ed25519_sign(seed: bytes, message: bytes) -> bytes:
    """Compute 64-byte deterministic RFC 8032 Ed25519 signature."""
    if len(seed) != 32:
        raise ValueError("Ed25519 seed must be 32 bytes")
    if _HAS_CRYPTOGRAPHY:
        return _FastEdPriv.from_private_bytes(seed).sign(message)
    h = hashlib.sha512(seed).digest()
    a_bytes = bytearray(h[:32])
    a_bytes[0] &= 248
    a_bytes[31] &= 127
    a_bytes[31] |= 64
    a = int.from_bytes(a_bytes, "little")
    prefix = h[32:64]
    pub = _ed_compress(_ed_scalar_mult(a, _BASE_POINT))
    r = int.from_bytes(hashlib.sha512(prefix + message).digest(), "little") % _L
    r_bytes = _ed_compress(_ed_scalar_mult(r, _BASE_POINT))
    k = int.from_bytes(hashlib.sha512(r_bytes + pub + message).digest(), "little") % _L
    s = (r + k * a) % _L
    return r_bytes + s.to_bytes(32, "little")


def ed25519_verify(public_key: bytes, message: bytes, signature: bytes) -> bool:
    """Verify a 64-byte RFC 8032 Ed25519 signature."""
    if len(public_key) != 32 or len(signature) != 64:
        return False
    if _HAS_CRYPTOGRAPHY:
        try:
            _FastEdPub.from_public_bytes(public_key).verify(signature, message)
            return True
        except Exception:
            return False
    try:
        r_bytes = signature[:32]
        s = int.from_bytes(signature[32:], "little")
        if s >= _L:
            return False
        r_pt = _ed_decompress(r_bytes)
        a_pt = _ed_decompress(public_key)
        k = int.from_bytes(hashlib.sha512(r_bytes + public_key + message).digest(), "little") % _L
        lhs = _ed_compress(_ed_scalar_mult(8 * s, _BASE_POINT))
        rhs = _ed_compress(_ed_add(_ed_scalar_mult(8, r_pt), _ed_scalar_mult(8 * k, a_pt)))
        return lhs == rhs
    except Exception:
        return False


# ----------------------------------------------------------------------
# Keypair Management, JWKS & RFC 7515 JWS (`EdDSA`)
# ----------------------------------------------------------------------


def compute_key_id(public_key_b64: str) -> str:
    """Compute a deterministic RFC 7638 JWK thumbprint-based `kid`."""
    jwk_core = {"crv": "Ed25519", "kty": "OKP", "x": public_key_b64}
    digest = hashlib.sha256(jcs_canonicalize(jwk_core)).hexdigest()
    return f"ed25519:{digest[:16]}"


def generate_ed25519_keypair() -> Dict[str, str]:
    """Generate a fresh Ed25519 keypair with base64url encoding and thumbprint kid."""
    seed = secrets.token_bytes(32)
    pub = ed25519_public_from_seed(seed)
    seed_b64 = b64url_encode(seed)
    pub_b64 = b64url_encode(pub)
    return {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "kid": compute_key_id(pub_b64),
        "private_seed": seed_b64,
        "public_key": pub_b64,
    }


def get_or_create_agent_keypair(agent_uri: str, keys_dir: Path = KEYS_DIR) -> Dict[str, str]:
    """Load or create a dedicated asymmetric Ed25519 keypair for a specific agent URI."""
    ensure_data_dirs()
    keys_dir = Path(keys_dir)
    keys_dir.mkdir(parents=True, exist_ok=True)
    uri_hash = hashlib.sha256((agent_uri or "default").encode("utf-8")).hexdigest()[:24]
    key_path = keys_dir / f"{uri_hash}.json"
    if key_path.exists():
        try:
            data = json.loads(key_path.read_text(encoding="utf-8"))
            if data.get("private_seed") and data.get("public_key") and data.get("kid"):
                return data
        except Exception:
            pass
    kp = generate_ed25519_keypair()
    kp["agent_uri"] = agent_uri
    key_path.write_text(json.dumps(kp, indent=2) + "\n", encoding="utf-8")
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return kp


def public_jwk_from_keypair(kp: Dict[str, str]) -> Dict[str, str]:
    return {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "use": "sig",
        "kid": kp["kid"],
        "x": kp["public_key"],
    }


def sign_jws_detached(payload_obj: Any, private_seed_b64: str, kid: str) -> Dict[str, str]:
    """Create an RFC 7515 JWS signature object over RFC 8785 JCS canonicalized JSON.

    Matches A2A v1.0 `AgentCardSignature` (`{ "protected": ..., "signature": ... }`).
    """
    protected_hdr = {"alg": "EdDSA", "kid": kid, "typ": "JWS"}
    protected_b64 = b64url_encode(jcs_canonicalize(protected_hdr))
    payload_b64 = b64url_encode(jcs_canonicalize(payload_obj))
    signing_input = f"{protected_b64}.{payload_b64}".encode("ascii")
    sig = ed25519_sign(b64url_decode(private_seed_b64), signing_input)
    return {
        "protected": protected_b64,
        "signature": b64url_encode(sig),
    }


def verify_jws_detached(
    payload_obj: Any,
    jws_obj: Dict[str, Any],
    public_key_b64: str,
    expected_kid: Optional[str] = None,
) -> bool:
    """Verify an RFC 7515 detached/AgentCard JWS signature over RFC 8785 JCS JSON."""
    try:
        protected_b64 = str(jws_obj.get("protected") or "")
        sig_b64 = str(jws_obj.get("signature") or "")
        if not protected_b64 or not sig_b64:
            return False
        hdr = json.loads(b64url_decode(protected_b64).decode("utf-8"))
        if hdr.get("alg") != "EdDSA":
            return False
        if expected_kid and hdr.get("kid") != expected_kid:
            return False
        payload_b64 = b64url_encode(jcs_canonicalize(payload_obj))
        signing_input = f"{protected_b64}.{payload_b64}".encode("ascii")
        return ed25519_verify(b64url_decode(public_key_b64), signing_input, b64url_decode(sig_b64))
    except Exception:
        return False


# ----------------------------------------------------------------------
# Compact EdDSA JWTs (A2A Webhook Push Auth + RFC 8693 `act` Delegation)
# ----------------------------------------------------------------------


def sign_jwt(
    claims: Dict[str, Any],
    private_seed_b64: str,
    kid: str,
    typ: str = "JWT",
) -> str:
    """Sign a compact `header.payload.signature` EdDSA JWT."""
    header = {"alg": "EdDSA", "kid": kid, "typ": typ}
    h_b64 = b64url_encode(jcs_canonicalize(header))
    p_b64 = b64url_encode(jcs_canonicalize(claims))
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")
    sig_b64 = b64url_encode(ed25519_sign(b64url_decode(private_seed_b64), signing_input))
    return f"{h_b64}.{p_b64}.{sig_b64}"


def verify_jwt(
    token: str,
    public_key_b64: str,
    expected_audience: Optional[str] = None,
    now_epoch: Optional[float] = None,
) -> Tuple[bool, Dict[str, Any], str]:
    """Verify a compact EdDSA JWT including `exp` freshness and RFC 8707 `aud` binding."""
    try:
        parts = (token or "").strip().split(".")
        if len(parts) != 3:
            return False, {}, "Malformed JWT"
        h_b64, p_b64, s_b64 = parts
        hdr = json.loads(b64url_decode(h_b64).decode("utf-8"))
        if hdr.get("alg") != "EdDSA":
            return False, {}, f"Unsupported JWT alg: {hdr.get('alg')}"
        signing_input = f"{h_b64}.{p_b64}".encode("ascii")
        if not ed25519_verify(b64url_decode(public_key_b64), signing_input, b64url_decode(s_b64)):
            return False, {}, "Invalid Ed25519 signature"
        claims = json.loads(b64url_decode(p_b64).decode("utf-8"))
        now = time.time() if now_epoch is None else now_epoch
        if "exp" in claims and float(claims["exp"]) < (now - 5.0):
            return False, claims, "JWT expired"
        if expected_audience is not None:
            aud = claims.get("aud")
            aud_ok = (aud == expected_audience) or (isinstance(aud, list) and expected_audience in aud)
            if not aud_ok:
                return False, claims, f"RFC 8707 audience mismatch: expected '{expected_audience}', got '{aud}'"
        return True, claims, ""
    except Exception as exc:
        return False, {}, str(exc)


def build_act_chain(actor_uri: str, prior_act: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build an RFC 8693 Section 4.1 nested `act` (actor) delegation claim."""
    node: Dict[str, Any] = {"sub": actor_uri}
    if prior_act and isinstance(prior_act, dict) and prior_act.get("sub"):
        node["act"] = prior_act
    return node


def mint_delegation_token(
    subject: str,
    actor_uri: str,
    audience_uri: str,
    scopes: List[str],
    private_seed_b64: str,
    kid: str,
    prior_act: Optional[Dict[str, Any]] = None,
    ttl_sec: int = 300,
    max_hops: int = 4,
) -> str:
    """Mint a downscoped RFC 8693 + RFC 8707 audience-bound delegation JWT (zero token passthrough)."""
    now = int(time.time())
    claims: Dict[str, Any] = {
        "iss": actor_uri,
        "sub": subject,
        "aud": audience_uri,
        "iat": now,
        "exp": now + max(10, int(ttl_sec)),
        "jti": f"jti_{uuid.uuid4().hex[:16]}",
        "scope": " ".join(sorted(set(scopes))),
        "act": build_act_chain(actor_uri, prior_act=prior_act),
        "max_hops": max(0, int(max_hops)),
    }
    return sign_jwt(claims, private_seed_b64=private_seed_b64, kid=kid, typ="at+jwt")
