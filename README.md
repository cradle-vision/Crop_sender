# Sender Crop

Камера → детекция людей (CPU person_detect) → обрезка (crop) → публикация в Kafka → backend → Triton.

## Схема

- **Камера(ы)** — захват кадров (RTSP/HTTP/USB), FPS из `cameras.yaml`
- **Детекция** — **person** (бинарник `person_detect` из `cpu-person-detection/`)
- **Обрезка** — crop по найденным областям
- **Kafka** — каждый crop публикуется в топик. Без MinIO: JSON с `image_base64`. С MinIO: crop загружается в бакет, в Kafka только `camera_id`, `timestamp`, `format`, `bucket`, `object_key`. Backend потребляет и отправляет в Triton.

## Установка

```bash
./setup.sh
```

## Конфигурация

- **.env** — единственный источник конфигурации (копия `.env.example` → `.env`). Не коммитить `.env` (секреты).
- **cameras.yaml** — список камер (путь в `CAMERAS_CONFIG_PATH`). Если задан **BACKEND_CAMERAS_URL**, при старте список запрашивается с backend и сохраняется в `cameras.yaml`.

Переменные окружения (см. `.env.example`): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_JPEG_QUALITY`, `MINIO_*`, `BACKEND_CAMERAS_URL`, `CAMERAS_CONFIG_PATH`, `RTSP_USE_FFMPEG_PIPE`, `PERSON_MODEL_PATH`, `PERSON_CONF`, `PERSON_IOU`, `DEFAULT_FPS`, `CAM1_FPS` и т.д.

Для **person** нужны: бинарник `cpu-person-detection/person_detection_linux_x64/person_detect` и модель `cpu-person-detection/models/person_detection_model.onnx` (собрать пакет: `cd cpu-person-detection && ./create_linux_package.sh`).

## Запуск

```bash
./run.sh
# или из папки sender:
cd sender && python3 main_agent.py
```

Docker (sender + person_detect в одном образе):

```bash
docker-compose build && docker-compose up -d
```

В образ копируются `cpu-person-detection/` (бинарник `person_detect`, `lib/`, модель). Конфиг — только из `.env`; камеры — `config/cameras.yaml` (монтируется в контейнер).

## Streaming Agent (RTSP → MediaMTX → WebRTC)

Отдельный edge-режим: **не** использует Kafka/person_detect. Агент подключается к центральному backend по **WebSocket** (команды, heartbeat). Два варианта доставки видео на фронт — см. `delivery` в конфиге:

| `delivery` | Смысл |
|------------|--------|
| `local_webrtc` | Только **MediaMTX** на объекте, браузер — **WHEP** (нужен доступ к порту 8889 с клиента). |
| `upstream_rtmp` | Пуш **RTSP → RTMP** на **ваш центральный сервер** через FFmpeg (исходящее соединение с объекта — **без белого IP** на магазине). Фронт смотрит уже **HLS/WebRTC с центра** (как настроите ingest). |
| `both` | И WHEP локально, и пуш на центр (две вытяжки с камеры). |

### Много объектов (например 200 агентов)

- На **каждом** объекте — свой `agent_id` и свой `upstream.rtmp_url_template` (часто один шаблон и общий ingest-хост; различие — в **`stream_key`** = `{agent_id}_{camera_id}`).
- **Backend** по WebSocket по-прежнему только управляет; **видео** идёт на центральный ingest по RTMP, дальше — ваша схема (nginx-rtmp, MediaMTX, SRS, облако): транскод, HLS, выдача фронту по HTTPS.
- В `stream_status` агент присылает `stream_key`, опционально **`playback_url`** (если задан `upstream.playback_url_template`) — фронт может открыть плеер по этому URL.
- Нагрузка: с каждого объекта вверх уходит ~2–4 Мбит/с на активную камеру; 200 объектов × N камер нужно закладывать в **пропускную способность ingest** и шардировать ingest по регионам/хостам при необходимости.

### Конфигурация

- Пример: [`streaming_agent/streaming-agent.yaml.example`](streaming_agent/streaming-agent.yaml.example) — скопируйте в `config/streaming-agent.yaml` или правьте example (в `docker-compose` по умолчанию смонтирован example).
- Переменные: `STREAMING_AGENT_CONFIG`, `STREAMING_AGENT_ID`, `STREAMING_BACKEND_URL`, `STREAMING_AGENT_TOKEN`, `STREAMING_DELIVERY`, `STREAMING_UPSTREAM_RTMP_URL_TEMPLATE`, `STREAMING_UPSTREAM_PLAYBACK_URL_TEMPLATE`, `MEDIAMTX_API_URL`, `MEDIAMTX_PUBLIC_WEBRTC_BASE`, `STREAMING_HEARTBEAT_INTERVAL_SEC`, `STREAMING_IDLE_GRACE_SEC` (см. `.env.example`).

### Запуск локально

```bash
# зависимости: websockets, psutil (уже в requirements.txt)
python3 -m streaming_agent.main --config streaming_agent/streaming-agent.yaml.example
```

### Запуск Docker (MediaMTX + агент)

```bash
docker compose up -d mediamtx streaming-agent
```

- API MediaMTX: `http://127.0.0.1:9997`
- WHEP/WebRTC HTTP: `http://127.0.0.1:8889` — в конфиге агента `mediamtx.public_webrtc_base` должен быть **доступен браузеру** (часто `http://<хост>:8889`).
- Агент ходит в MediaMTX по `http://mediamtx:9997` из контейнера.

### Протокол WebSocket (MVP)

Исходящие: `register`, `heartbeat`, `stream_status` (при старте/ошибке/остановке потока).

Входящие: `start_stream`, `stop_stream`, `viewer_join`, `viewer_leave`, `webrtc_signal` (зарезервировано), `ping` → `pong`.

После `start_stream` в `stream_status` приходят, в зависимости от режима: `whep_url` (локальный WebRTC), `stream_key`, `playback_url` (центральный просмотр), поле `delivery`.

### Smoke-проверка без backend

1. Поднять только MediaMTX: `docker compose up -d mediamtx`.
2. Добавить путь вручную:  
   `curl -s -X POST http://127.0.0.1:9997/v3/config/paths/add/cam1 -H 'Content-Type: application/json' -d '{"source":"rtsp://..."}'`  
   (или запустить агент с тестовым backend — см. ниже).
3. Проверить список путей: `curl -s http://127.0.0.1:9997/v3/paths/list`.

Для полного цикла нужен backend с WebSocket, принимающим `register` и шлющим `start_stream`.

## Типы камер

- **RTSP**: `rtsp://[user:pass@]ip:port/path`
- **HTTP/MJPEG**: `http://ip:port/path`

При "RTP: bad cseq" и "error while decoding MB": включите **захват через FFmpeg** — в `.env` задайте `RTSP_USE_FFMPEG_PIPE=true`. В коде также включены TCP, таймаут и авто-переподключение для OpenCV. Если ошибки остаются — переключитесь на **субпоток** камеры (Hikvision: `/Streaming/Channels/102` вместо `101`; у других — `/stream2`) — меньше битрейт, стабильнее по сети.

### Откуда берутся ошибки RTP / decoding MB

- Сообщения **идут из FFmpeg** при декодировании RTSP (OpenCV внутри использует FFmpeg). В коде RTSP открывается только в **snapshot_capture_agent** (основной захват) и в **camera_manager.test_camera** (разовый тест). Остальные модули (person_crop, kafka_sender_agent) камеру не открывают.
- При **OpenCV-захвате** (`RTSP_USE_FFMPEG_PIPE=false`) stderr FFmpeg попадает в консоль. При **захвате через FFmpeg pipe** (`RTSP_USE_FFMPEG_PIPE=true`) декодирование делает отдельный процесс `ffmpeg -rtsp_transport tcp`; его stderr не выводится.
- Другие причины: нестабильная сеть или Wi‑Fi, несколько клиентов на один поток (cseq путается), прошивка камеры. Решение: один клиент, по возможности провод, субпоток вместо основного.

## Структура проекта

```
Sender_Crop/
├── sender/
│   ├── main_agent.py           # Оркестратор: кадр → detect → crop → Kafka
│   ├── snapshot_capture_agent.py # Захват с камер
│   ├── person_crop.py          # Детекция людей (person_detect binary) + crop
│   ├── kafka_sender_agent.py   # Публикация crop в Kafka
│   ├── camera_manager.py       # Камеры из cameras.yaml или backend
│   └── cameras.yaml.example
├── streaming_agent/            # RTSP → MediaMTX, WS control
│   ├── main.py
│   ├── config.py
│   ├── signaling_client.py
│   ├── stream_manager.py
│   ├── mediamtx_client.py
│   ├── health_monitor.py
│   └── streaming-agent.yaml.example
├── mediamtx.yml                # конфиг MediaMTX для docker-compose
├── .env.example    # образец для .env (Kafka, MinIO, RTSP, FPS)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── setup.sh
└── run.sh
```

## Kafka / MinIO

- **Без MinIO:** сообщение в топике (JSON): `camera_id`, `timestamp` (ms), `format` ("jpeg"), `image_base64`. Backend декодирует base64 → JPEG и отправляет в Triton.
- **С MinIO** (`minio.enabled: true` или `MINIO_ENABLED=true`): crop загружается в бакет MinIO, в Kafka только метаданные: `camera_id`, `timestamp`, `format`, `bucket`, `object_key`. Backend по `object_key` скачивает объект из MinIO и отправляет в Triton (меньше трафика в Kafka).

## Отправка snapshot на SmartCamera

При старте для каждой камеры один раз отправляется **исходный кадр** на бэкенд; бэкенд сохраняет его в MinIO и обновляет поле `snapshot_url` у смарт-камеры. Далее, при наличии детекции, могут отправляться дополнительные кадры (кропы) той же камеры.

- **Эндпоинт:** `POST /company/{company_id}/smartcamera/{smartcamera_id}/snapshot`
- **Авторизация:** `Authorization: Bearer <token>` — используется тот же токен, что и для `BACKEND_CAMERAS_URL` (через `BACKEND_CAMERAS_TOKEN` или `BACKEND_CAMERAS_USERNAME`/`BACKEND_CAMERAS_PASSWORD` + `BACKEND_TOKEN_URL`).
- **Тело:** `multipart/form-data`, поле `file` — файл изображения (JPEG).

Отправка выполняется только если задан **BACKEND_CAMERAS_URL** и у камеры в конфиге есть **company_id** (из ответа backend или из `cameras.yaml`). При успешном ответе 200 в лог выводится возвращённый `snapshot_url`. При 401 выводится сообщение о неверном или отсутствующем bearer‑токене; в этом случае отправка будет повторена при следующем кадре.

Для фильтрации по зоне используйте **линию** в `cameras.yaml` (или через backend, если поля прокидываются в конфиг камеры):
`line_x1`, `line_y1`, `line_x2`, `line_y2`, `inside_x`, `inside_y`, `line_active`.
В этом режиме sender передаёт эти параметры в `person_detect` как `--line ... --inside_point ...`; при отсутствии линии детекция выполняется по всему кадру.
