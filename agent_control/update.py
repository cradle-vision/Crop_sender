"""Apply software_update via docker compose (host docker.sock)."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

_DEFAULT_IMAGE_REPO = "ghcr.io/cradle-vision/crop-sender"


def _ghcr_login() -> Tuple[bool, str]:
    """Login to GHCR when the package is private. No-op if token missing."""
    token = (os.getenv("GHCR_TOKEN") or os.getenv("GITHUB_TOKEN") or "").strip()
    if not token:
        return True, "skip"
    user = (os.getenv("GHCR_USERNAME") or os.getenv("GITHUB_USERNAME") or "token").strip()
    if not shutil.which("docker"):
        return False, "docker CLI not found (needed for ghcr login)"
    try:
        completed = subprocess.run(
            ["docker", "login", "ghcr.io", "-u", user, "--password-stdin"],
            input=token,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except Exception as exc:
        return False, f"ghcr login failed: {exc}"
    if completed.returncode != 0:
        err = (completed.stderr or completed.stdout or "ghcr login failed").strip()
        return False, err[:500]
    return True, "ok"


def resolve_agent_image(version: str, image: Optional[str] = None) -> str:
    """
    Prefer explicit image; else retag AGENT_IMAGE / AGENT_IMAGE_REPO with version.
    """
    explicit = (image or "").strip()
    if explicit:
        return explicit
    existing = (os.getenv("AGENT_IMAGE") or "").strip()
    if existing:
        if ":" in existing.rsplit("/", 1)[-1]:
            base = existing.rsplit(":", 1)[0]
            return f"{base}:{version}"
        return f"{existing}:{version}"
    repo = (os.getenv("AGENT_IMAGE_REPO") or _DEFAULT_IMAGE_REPO).strip()
    return f"{repo}:{version}"


def run_software_update(
    *,
    version: str,
    image: Optional[str] = None,
    compose_file: Optional[str] = None,
    project_dir: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Update AGENT_VERSION / AGENT_IMAGE in .env and pull/up via docker compose.

    Requires Docker CLI in the container and /var/run/docker.sock mounted,
    or AGENT_UPDATE_CMD override (recommended: host scripts/update-agent.sh).
    """
    version = (version or "").strip()
    if not version:
        return False, "missing version"

    resolved_image = resolve_agent_image(version, image)
    login_ok, login_detail = _ghcr_login()
    if not login_ok:
        return False, f"ghcr login: {login_detail}"
    custom = (os.getenv("AGENT_UPDATE_CMD") or "").strip()
    if custom:
        env = os.environ.copy()
        env["AGENT_VERSION"] = version
        env["AGENT_IMAGE"] = resolved_image
        try:
            completed = subprocess.run(
                custom,
                shell=True,
                check=False,
                capture_output=True,
                text=True,
                env=env,
                timeout=float(os.getenv("AGENT_UPDATE_TIMEOUT_SEC", "600")),
            )
            if completed.returncode != 0:
                err = (completed.stderr or completed.stdout or "update failed").strip()
                return False, err[:2000]
            return True, "ok"
        except Exception as exc:
            return False, str(exc)

    root = project_dir or os.getenv("AGENT_COMPOSE_DIR") or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    compose = compose_file or os.getenv("AGENT_COMPOSE_FILE") or os.path.join(root, "docker-compose.yml")
    env_path = os.getenv("AGENT_ENV_PATH") or os.path.join(root, ".env")

    try:
        from agent_control.env_file import merge_env_file

        merge_env_file(
            env_path,
            {"AGENT_VERSION": version, "AGENT_IMAGE": resolved_image},
        )
    except Exception as exc:
        logger.warning("Failed to persist AGENT_VERSION/AGENT_IMAGE: %s", exc)

    if not os.path.exists("/var/run/docker.sock") and not os.getenv("DOCKER_HOST"):
        return (
            False,
            "docker.sock not available; mount /var/run/docker.sock and install "
            "docker CLI, or set AGENT_UPDATE_CMD to host scripts/update-agent.sh",
        )

    if not shutil.which("docker"):
        return (
            False,
            "docker CLI not found in container; set AGENT_UPDATE_CMD="
            "bash /opt/crop_sender/scripts/update-agent.sh (recommended) "
            "or install docker CLI + mount docker.sock",
        )

    cmd = [
        "docker",
        "compose",
        "-f",
        compose,
        "--env-file",
        env_path,
        "pull",
    ]
    up_cmd = [
        "docker",
        "compose",
        "-f",
        compose,
        "--env-file",
        env_path,
        "up",
        "-d",
        "--remove-orphans",
    ]
    try:
        for part in (cmd, up_cmd):
            completed = subprocess.run(
                part,
                check=False,
                capture_output=True,
                text=True,
                cwd=root,
                timeout=float(os.getenv("AGENT_UPDATE_TIMEOUT_SEC", "600")),
            )
            if completed.returncode != 0:
                err = (completed.stderr or completed.stdout or "compose failed").strip()
                return False, err[:2000]
        return True, "ok"
    except FileNotFoundError:
        return False, "docker CLI not found in container"
    except Exception as exc:
        return False, str(exc)
