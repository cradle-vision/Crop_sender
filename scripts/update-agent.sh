#!/usr/bin/env bash
# Host-side OTA helper. Prefer mounting this via AGENT_UPDATE_CMD from the agent container,
# or run on the store host after desired_version changes.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${AGENT_ENV_PATH:-$ROOT/.env}"
COMPOSE_FILE="${AGENT_COMPOSE_FILE:-$ROOT/docker-compose.yml}"
VERSION="${AGENT_VERSION:-${1:-}}"
IMAGE="${AGENT_IMAGE:-}"

if [[ -z "$VERSION" ]]; then
  echo "Usage: AGENT_VERSION=v1.2.3 $0   OR   $0 v1.2.3" >&2
  exit 1
fi

if [[ -z "$IMAGE" ]]; then
  REPO="${AGENT_IMAGE_REPO:-ghcr.io/cradle-vision/crop-sender}"
  if [[ -f "$ENV_FILE" ]] && grep -q '^AGENT_IMAGE=' "$ENV_FILE" 2>/dev/null; then
    CURRENT_IMAGE="$(grep '^AGENT_IMAGE=' "$ENV_FILE" | head -n1 | cut -d= -f2-)"
    CURRENT_IMAGE="${CURRENT_IMAGE%\"}"
    CURRENT_IMAGE="${CURRENT_IMAGE#\"}"
    if [[ -n "$CURRENT_IMAGE" ]]; then
      IMAGE="${CURRENT_IMAGE%:*}:${VERSION}"
    fi
  fi
  IMAGE="${IMAGE:-${REPO}:${VERSION}}"
fi

touch "$ENV_FILE"
if grep -q '^AGENT_VERSION=' "$ENV_FILE" 2>/dev/null; then
  sed -i.bak "s|^AGENT_VERSION=.*|AGENT_VERSION=${VERSION}|" "$ENV_FILE"
else
  echo "AGENT_VERSION=${VERSION}" >> "$ENV_FILE"
fi
if [[ -n "$IMAGE" ]]; then
  if grep -q '^AGENT_IMAGE=' "$ENV_FILE" 2>/dev/null; then
    sed -i.bak "s|^AGENT_IMAGE=.*|AGENT_IMAGE=${IMAGE}|" "$ENV_FILE"
  else
    echo "AGENT_IMAGE=${IMAGE}" >> "$ENV_FILE"
  fi
fi
rm -f "${ENV_FILE}.bak"

# Private GHCR: one org pull token (read:packages). Set via apply_env — not per-agent login.
_load_env_val() {
  local key="$1"
  if [[ -n "${!key:-}" ]]; then
    printf '%s' "${!key}"
    return
  fi
  [[ -f "$ENV_FILE" ]] || return 0
  local line
  line="$(grep -E "^${key}=" "$ENV_FILE" | head -n1 || true)"
  [[ -n "$line" ]] || return 0
  line="${line#*=}"
  line="${line%\"}"
  line="${line#\"}"
  printf '%s' "$line"
}

GHCR_TOKEN_VAL="$(_load_env_val GHCR_TOKEN)"
if [[ -z "$GHCR_TOKEN_VAL" ]]; then
  GHCR_TOKEN_VAL="$(_load_env_val GITHUB_TOKEN)"
fi
GHCR_USER_VAL="$(_load_env_val GHCR_USERNAME)"
if [[ -z "$GHCR_USER_VAL" ]]; then
  GHCR_USER_VAL="$(_load_env_val GITHUB_USERNAME)"
fi
GHCR_USER_VAL="${GHCR_USER_VAL:-token}"

if [[ -n "$GHCR_TOKEN_VAL" ]]; then
  echo "Logging in to ghcr.io as ${GHCR_USER_VAL}"
  printf '%s' "$GHCR_TOKEN_VAL" | docker login ghcr.io -u "$GHCR_USER_VAL" --password-stdin
fi

echo "Pulling and restarting store agent version=${VERSION}"
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" pull
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --remove-orphans
echo "Done."
