# Авторизация Streaming Agent (WebSocket)

Агент подключается к listener по **WSS/WS** и передаёт ключ в заголовке:

```http
Authorization: Bearer <api_key>
```

В коде это поле `auth_token` в YAML или переменная окружения **`STREAMING_AGENT_TOKEN`** — подставляется **как есть** (plain `api_key`, без префиксов `Bearer ` в значении env).

---

## 1. Получить ключ через API (один раз на агента / при ротации)

**Запрос:** с авторизованным пользователем и scope **`stream:write`**

```http
POST /api/stream/agents
Authorization: Bearer <user_jwt_or_session>
```

**Ответ:** в теле будет **`api_key`** — его и кладём в конфиг агента.

- Используйте **только** ключ из этого ответа для данного окружения (dev/stage/prod).
- Не подставляйте старые токены от других сервисов (Kafka, snapshot upload и т.д.) — listener проверяет именно **stream agent key**.

---

## 2. Условия на стороне сервера

- В БД у записи агента **`is_active == true`**.
- **`agent_id`** в конфиге агента (`agent_id` в YAML / `STREAMING_AGENT_ID`) **совпадает** с тем, под которым агент зарегистрирован на backend.
- При несовпадении или неактивном агенте listener отклонит соединение.

---

## 3. Ротация ключа

Если ключ перевыпустили через **тот же** `POST /api/stream/agents` (или аналог ротации на вашем API):

- **Старый `api_key` перестаёт действовать.**
- Нужно **обновить** `STREAMING_AGENT_TOKEN` / `auth_token` в `.env` или YAML и **перезапустить** процесс/контейнер `streaming-agent`.

---

## 4. Ошибка 403

**HTTP 403** при установке WebSocket = **не проблема выбора `ws` vs `wss`**, а обычно:

- неверный или **устаревший** Bearer-токен;
- агент **неактивен** (`is_active != true`);
- неверный **`agent_id`** относительно записи ключа.

Проверьте TLS/порт отдельно (другие ошибки: SSL handshake, connection refused). После успешного TLS, **403** смотрите по списку выше.

---

## 5. Пример `.env` (фрагмент)

```env
STREAMING_AGENT_ID=shop-12
STREAMING_BACKEND_URL=wss://your-api.example.com/ws/agents
STREAMING_AGENT_TOKEN=<api_key из POST /api/stream/agents>
```

---

## 6. Связь с кодом агента

Заголовок формируется в [`streaming_agent/signaling_client.py`](../streaming_agent/signaling_client.py): при непустом `auth_token` добавляется `Authorization: Bearer …`.

---

## 7. После `register` сразу обрыв, в логе `code=1006`

**1006** — «abnormal closure»: соединение закрыто без нормального WebSocket close (часто **сервер рвёт TCP** после своей проверки).

Если в логах видно `WS connected, register sent: agent_id=… cameras=…` и через ~0.3–1 с снова коннект — почти всегда **отклонён `register`** на backend:

- **`agent_id`** не совпадает с тем, что привязан к выданному **`api_key`** (другой магазин / опечатка).
- **`cameras`** — id камер должны быть **как в вашей БД/API**.
  Теперь список камер берётся из вашего `config/cameras.yaml` (через env `CAMERAS_CONFIG_PATH`), а не вручную из `streaming-agent.yaml`.
  Например, если в backend камера с id **`1`**, то в `config/cameras.yaml` поле `camera_id` должно быть `"1"`, и в `register` уйдёт `"cameras": ["1"]` (строки).

Перезапустите агента и смотрите логи **listener** на сервере в момент `register` (проверьте `agent_id` и `config/cameras.yaml`).

---

## 8. Docker: с хоста `websocat` ок, в контейнере «no close frame»

Если **тот же** `ws://…` и **тот же** Bearer в `docker compose exec … env` совпадают с рабочим `websocat`, а в логах агента обрыв без close frame:

1. **Сеть Docker (bridge)** — попробуйте для сервиса `streaming-agent` **`network_mode: host`** (как у `sender-crop`), а `MEDIAMTX_API_URL=http://127.0.0.1:9997`, пока MediaMTX публикует порты на хост. Тогда исходящий путь к backend совпадёт с машиной, где `websocat` уже работает.
2. **Камеры** — агент берёт их из `config/cameras.yaml` (через env `CAMERAS_CONFIG_PATH`). Убедитесь, что `camera_id` совпадает с тем, что ожидает backend (в логах `WS connected, register sent: ... cameras=[...]`).
3. Логи **listener** на сервере в момент коннекта с IP контейнера (см. раздел **7** про **1006** после `register`).
