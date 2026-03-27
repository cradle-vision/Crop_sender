# План: Backend, listener и фронт (Streaming Agent)

Цель: центральный сервис принимает edge-агентов по WebSocket, отдаёт API фронту и по сессии включает/выключает поток (`start_stream` / `stop_stream`). Видео на фронт — с центрального ingest (HLS/WebRTC), не через backend.

---

## 1. Компоненты (логические сервисы)

| Компонент | Назначение |
|-----------|------------|
| **Agent Listener (WS)** | Долгоживущие соединения от **агентов** (200+ объектов). Приём `register`, `heartbeat`, `stream_status`; отправка команд `start_stream`, `stop_stream`. |
| **Public API (HTTP/HTTPS)** | Авторизация пользователей, справочники (объекты, камеры), **старт/стоп просмотра** сессии, выдача **playback URL** фронту. |
| **Session / Orchestration** | Связь «пользователь открыл камеру» → найти сокет агента → `start_stream` → сохранить `stream_key` / `playback_url` из `stream_status`. При закрытии → `stop_stream`. |
| **Хранилище** | Агенты, камеры, привязка к `agent_id`, активные сессии, последние `playback_url` / `stream_key`, опционально кэш heartbeat. |
| **Ingest (отдельно от кода API)** | RTMP/HLS/WebRTC на VPS/облаке — не часть этого репозитория; backend только знает **шаблон URL** для UI. |

---

## 2. Agent Listener (что должен уметь)

### Подключение

- **WSS** в проде (`wss://api.example.com/ws/agents`).
- Заголовок **`Authorization: Bearer <token>`** — выдаёте агенту при регистрации объекта (долгоживущий API key / JWT).

### Входящие сообщения от агента (уже есть в агенте)

| type | Действие на сервере |
|------|---------------------|
| `register` | Сохранить `agent_id`, список `cameras`, пометить соединение как online. |
| `heartbeat` | Обновить `last_seen`, сохранить `cpu`, `ram`, `streams_active` (метрики/алерты). |
| `stream_status` | Если `status: stream_ready` — сохранить `stream_key`, `playback_url`, `whep_url`, `delivery` для активной сессии; уведомить ждущий API/фронт. |
| `stream_status` / ошибка | Логировать, обновить статус сессии «ошибка». |
| `pong` | Опционально: ответ на `ping` с сервера. |

### Исходящие команды к агенту

| type | Когда слать |
|------|-------------|
| `start_stream` | Пользователь (или API) запросил просмотр камеры на этом объекте. |
| `stop_stream` | Сессия просмотра закрыта, таймаут, или явная остановка. |
| `ping` | Периодически для проверки живости (опционально). |

`viewer_join` / `viewer_leave` — **не обязательны**, если на агенте `viewer_idle_stop: false` и вы управляете только `start_stream` / `stop_stream`.

### Масштаб 200+ агентов

- Один процесс listener держит много WebSocket; при росте — **несколько инстансов** + **sticky** по `agent_id` (или Redis pub/sub: «команда для agent X» доставляется инстансу, у которого открыт сокет).
- Heartbeat: если нет N секунд — агент offline, не слать команды (или очередь до reconnect).

---

## 3. Public API (что должен быть у backend)

### Аутентификация

- JWT или session cookie для **пользователей** (не путать с токеном агента).

### Примеры эндпоинтов (контракт можно сузить под ваш стек)

| Метод | Путь | Назначение |
|-------|------|------------|
| GET | `/api/agents` | Список объектов (магазинов), статус online/offline. |
| GET | `/api/agents/{agent_id}/cameras` | Камеры (из БД или из последнего `register`). |
| POST | `/api/stream/sessions` | Тело: `{ "agent_id", "camera_id" }`. Создать сессию, отправить агенту `start_stream`, вернуть `{ "session_id", "status": "starting" }`. |
| GET | `/api/stream/sessions/{session_id}` | Статус + `playback_url` / `stream_key`, когда пришёл `stream_status`. |
| DELETE | `/api/stream/sessions/{session_id}` | Отправить агенту `stop_stream`, закрыть сессию. |

Альтернатива: один **POST /stream/open** и **POST /stream/close** без polling — проще для MVP.

### Связь Listener ↔ API

- После `start_stream` API **ждёт** `stream_status` с `playback_url` (короткий таймаут + polling GET session, или **внутренний pub/sub**).
- Храните в БД: `session_id`, `agent_id`, `camera_id`, `playback_url`, `state` (starting | live | stopped | error).

---

## 4. Фронт (что должен делать)

### Экраны / потоки

1. **Список объектов и камер** — данные из Public API.
2. **«Открыть трансляцию»** — вызов API (создать сессию / start). Показать loader, пока `status != live`.
3. **Плеер** — когда в ответе или в GET сессии есть **`playback_url`** (обычно **HLS** `.m3u8`):
   - `<video>` + **hls.js** (или нативный Safari).
   - Если у вас **`whep_url`** (локальный WebRTC на объекте) — отдельный сценарий; для центрального ingest достаточно HLS/HTTPS.
4. **«Закрыть»** — `beforeunload` / кнопка «Назад» → **DELETE** сессии или POST close → backend шлёт **`stop_stream`** агенту.

### Не делать на фронте

- Не подключаться к RTMP.
- Не хранить секреты агента; только пользовательский JWT к вашему API.

---

## 5. Порядок взаимодействия (сводка)

```text
[Агент] --WSS register/heartbeat/stream_status--> [Listener]
[Listener] <---- start_stream / stop_stream ---- [Session logic]

[Браузер] --HTTPS JWT--> [Public API]
[Public API] --> внутренняя шина --> [Listener] --> агент

[Браузер] --HTTPS--> [CDN / Ingest HLS]   (playback_url, без прокси через backend)
```

---

## 6. Рекомендуемый стек (ориентир)

| Слой | Варианты |
|------|----------|
| API + WS | Node (ws + Express/Fastify), Python (FastAPI + websockets), Go. |
| БД | PostgreSQL для сессий и агентов; Redis для pub/sub и sticky. |
| Фронт | React/Vue + hls.js; плеер по `playback_url`. |

---

## 7. MVP checklist

- [ ] Listener: приём `register` / `heartbeat`, маршрутизация по `agent_id`.
- [ ] Listener: отправка `start_stream` / `stop_stream` по запросу API.
- [ ] API: создание/закрытие сессии просмотра, выдача `playback_url` после `stream_status`.
- [ ] Фронт: открыть сессию → плеер HLS → при уходе закрыть сессию.
- [ ] Ingest: центральный RTMP + HLS на HTTPS (отдельно от этого репо).

Этот документ — план реализации на стороне вашего продукта; **edge-агент** из репозитория Sender_Crop уже реализует протокол к Listener и пуш `upstream_rtmp` при необходимости.
