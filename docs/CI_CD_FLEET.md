# Store agent CI/CD and remote env

See backend doc: `Retail_Backend/docs/STORE_AGENT_FLEET_API.md`.

## Quick ops

1. **CI** (this repo): `.github/workflows/release-agent.yml` builds/pushes `ghcr.io/cradle-vision/crop-sender` and optionally registers the release via `POST /api/stream/releases/ci`.
2. **Secrets (GitHub)**: `AGENT_RELEASE_API_URL`, `AGENT_RELEASE_CI_TOKEN` (same value as backend `AGENT_RELEASE_CI_TOKEN`).
3. **Admin**: set env / rollout version → agent applies over `WS /ws/agents`.
4. **WS ownership**: crop-only → sender owns WS. With `--profile streaming`, streaming-agent writes `config/.agent_ws_owner` and sender skips its control client automatically.
5. **OTA on host** (recommended): set `AGENT_UPDATE_CMD=bash /opt/crop_sender/scripts/update-agent.sh` and mount the project directory. In-container `docker compose` needs docker CLI + `docker.sock` (optional; not in the default image).

```bash
# Pull pinned image
export AGENT_IMAGE=ghcr.io/cradle-vision/crop-sender:v1.2.3
export AGENT_VERSION=v1.2.3
docker compose pull && docker compose up -d
```

If admin sends only `version` (no `image`), the agent retags `AGENT_IMAGE` / default `ghcr.io/cradle-vision/crop-sender:<version>`.

## GHCR pull (100 agents, no per-PC login)

Package `crop-sender` is **private**; org policy disables Public/Internal visibility. Agents therefore cannot `docker compose pull` anonymously.

Do **not** log in on each mini-PC. Use one org PAT (`read:packages`) and push it once via admin `apply_env`:

```
GHCR_USERNAME=<github user that owns the PAT>
GHCR_TOKEN=<PAT with read:packages>
```

OTA (`software_update` / `scripts/update-agent.sh`) runs `docker login ghcr.io` before pull.

To allow anonymous pull instead: org admin → Settings → Packages → enable public packages, then Change package visibility → Public.
