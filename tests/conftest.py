"""Top-level pytest configuration and shared helpers."""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

# Make sure the repo root is on sys.path so all imports resolve.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def make_jwt(claims: dict) -> str:
    """Build a minimal unsigned JWT suitable for ALLOW_UNVERIFIED_LOCAL_JWT tests."""
    header = (
        base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode())
        .decode()
        .rstrip("=")
    )
    payload = (
        base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    )
    return f"{header}.{payload}."
