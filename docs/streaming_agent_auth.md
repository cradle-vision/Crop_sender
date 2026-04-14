# Streaming Agent Authentication (WebSocket)

The agent connects to a listener over **WSS/WS** and sends the key in the header:

```http
Authorization: Bearer <api_key>
```

In code, this value comes from `auth_token` in YAML or from env var **`STREAMING_AGENT_TOKEN`** and is used **as-is** (plain `api_key`, without `Bearer ` prefix in env value).

---

## 1. Get an API key (once per agent / on rotation)

**Request:** by an authorized user with scope **`stream:write`**

```http
POST /api/stream/agents
Authorization: Bearer <user_jwt_or_session>
```

**Response:** contains **`api_key`**. Put this key into agent config.

- Use only the key issued for the current environment (dev/stage/prod).
- Do not reuse old tokens from other services (Kafka, snapshot upload, etc.). Listener validates a dedicated **stream agent key**.

---

## 2. Server-side requirements

- Agent record in DB has **`is_active == true`**.
- Agent config **`agent_id`** (`agent_id` in YAML / `STREAMING_AGENT_ID`) matches the backend record for this key.
- If `agent_id` mismatches or agent is inactive, listener rejects connection.

---

## 3. Key rotation

If key is reissued via the same `POST /api/stream/agents` (or your API's rotation endpoint):

- The old `api_key` stops working.
- Update `STREAMING_AGENT_TOKEN` / `auth_token` in `.env` or YAML and restart `streaming-agent`.

---

## 4. HTTP 403 on WebSocket connect

**HTTP 403** during WS handshake is usually **not** `ws` vs `wss` selection issue. Typical causes:

- invalid or expired Bearer token;
- inactive agent (`is_active != true`);
- wrong `agent_id` for the provided key.

Check TLS/port separately (SSL handshake, connection refused, etc.). If TLS is fine and you get 403, inspect the list above.

---

## 5. `.env` example (fragment)

```env
STREAMING_AGENT_ID=shop-12
STREAMING_BACKEND_URL=wss://your-api.example.com/ws/agents
STREAMING_AGENT_TOKEN=<api_key from POST /api/stream/agents>
```

---

## 6. Code reference

Header is formed in [`streaming_agent/signaling_client.py`](../streaming_agent/signaling_client.py): when `auth_token` is set, `Authorization: Bearer ...` is attached.

---

## 7. Immediate disconnect after `register`, log shows `code=1006`

**1006** means abnormal closure: socket was closed without a normal WS close frame (often server closes TCP after validation).

If logs show `WS connected, register sent: agent_id=... cameras=...` and reconnect happens in ~0.3-1s, backend usually rejected `register`:

- `agent_id` does not match the one bound to this `api_key` (wrong site/store, typo).
- `cameras` IDs must match backend DB/API IDs.
  Agent now loads cameras from `config/cameras.yaml` via `CAMERAS_CONFIG_PATH`, not manually from `streaming-agent.yaml`.
  Example: if backend camera id is `1`, then `camera_id` in `config/cameras.yaml` must be `"1"`, so register sends `"cameras": ["1"]` (strings).

Restart agent and check listener logs at register time (`agent_id`, `config/cameras.yaml` values).

---

## 8. Docker case: `websocat` works on host, container shows `no close frame`

If the same `ws://...` and same Bearer token (verified via `docker compose exec ... env`) work with host `websocat`, but agent inside container disconnects without close frame:

1. **Docker network (bridge):** try `network_mode: host` for `streaming-agent` (as for `sender-crop`), and keep `MEDIAMTX_API_URL=http://127.0.0.1:9997` while MediaMTX ports are published on host.
2. **Cameras:** agent reads camera list from `config/cameras.yaml` (`CAMERAS_CONFIG_PATH`). Verify `camera_id` values match backend expectations (`WS connected, register sent: ... cameras=[...]`).
3. Check listener logs on server for container source IP at connect time (see section 7 on `1006` right after `register`).
