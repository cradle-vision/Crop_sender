"""Env file helpers for remote apply_env."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from typing import Dict, Iterable, Optional, Set

# Remotely managed keys only (per-store). Infra/ROI/streaming delivery are code defaults.
ALLOWED_ENV_KEYS: Set[str] = {
    # Backend cameras / auth (per store)
    "BACKEND_CAMERAS_URL",
    "BACKEND_CAMERAS_TOKEN",
    "BACKEND_CAMERAS_USERNAME",
    "BACKEND_CAMERAS_PASSWORD",
    "BACKEND_TOKEN_URL",
    # Agent identity (unique per store)
    "STREAMING_AGENT_ID",
    "STREAMING_AGENT_TOKEN",
    # Per-store capture rate
    "DEFAULT_FPS",
    # OTA pin
    "AGENT_VERSION",
    "AGENT_IMAGE",
}

SECRET_ENV_KEYS: Set[str] = {
    "BACKEND_CAMERAS_PASSWORD",
    "BACKEND_CAMERAS_TOKEN",
    "STREAMING_AGENT_TOKEN",
}

_LINE_RE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def filter_allowed_env(raw: Dict[str, object]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in (raw or {}).items():
        if key not in ALLOWED_ENV_KEYS or value is None:
            continue
        out[key] = str(value)
    return out


def compute_env_hash(env: Optional[Dict[str, object]]) -> str:
    normalized = filter_allowed_env(env or {})
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def read_env_file(path: str) -> Dict[str, str]:
    if not path or not os.path.isfile(path):
        return {}
    result: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            match = _LINE_RE.match(line)
            if not match:
                continue
            key, value = match.group(1), match.group(2)
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            result[key] = value
    return result


def write_env_file(path: str, env: Dict[str, str], *, preserve_unknown: bool = True) -> None:
    existing_lines: list[str] = []
    existing_keys: Set[str] = set()
    if preserve_unknown and os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as fh:
            existing_lines = fh.readlines()
        for line in existing_lines:
            match = _LINE_RE.match(line.strip())
            if match:
                existing_keys.add(match.group(1))

    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".env.", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:
            written: Set[str] = set()
            if preserve_unknown and existing_lines:
                for line in existing_lines:
                    stripped = line.strip()
                    match = _LINE_RE.match(stripped)
                    if not match:
                        out.write(line if line.endswith("\n") else line + "\n")
                        continue
                    key = match.group(1)
                    if key in env:
                        out.write(f"{key}={_quote(env[key])}\n")
                        written.add(key)
                    else:
                        out.write(line if line.endswith("\n") else line + "\n")
            for key, value in env.items():
                if key in written:
                    continue
                out.write(f"{key}={_quote(value)}\n")
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def merge_env_file(path: str, patch: Dict[str, object]) -> Dict[str, str]:
    current = read_env_file(path)
    filtered = filter_allowed_env(patch)
    current.update(filtered)
    # Also keep only allowed keys for hashing report, but write full merged file content keys.
    write_env_file(path, current, preserve_unknown=True)
    return filter_allowed_env(current)


def _quote(value: str) -> str:
    if re.search(r'[\s#"\'\\]', value):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def reported_env_from_os(keys: Optional[Iterable[str]] = None) -> Dict[str, str]:
    use_keys = list(keys) if keys is not None else sorted(ALLOWED_ENV_KEYS)
    out: Dict[str, str] = {}
    for key in use_keys:
        if key not in ALLOWED_ENV_KEYS:
            continue
        val = os.getenv(key)
        if val is not None and val != "":
            out[key] = val
    return out


def redact_reported_env(env: Optional[Dict[str, object]]) -> Dict[str, str]:
    """Return allowed env for WS reports with secret values masked."""
    filtered = filter_allowed_env(env or {})
    out: Dict[str, str] = {}
    for key, value in filtered.items():
        if key in SECRET_ENV_KEYS:
            out[key] = "***" if value else ""
        else:
            out[key] = value
    return out
