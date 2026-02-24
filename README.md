# Sender Crop

Камера → детекция (лица или люди) → обрезка (crop) → публикация в Kafka → backend → Triton.

## Схема

- **Камера(ы)** — захват кадров (RTSP/HTTP/USB), FPS из `cameras.yaml`
- **Детекция** — **person** (бинарник `person_detect` из `cpu-person-detection/`) или **face** (OpenCV Haar)
- **Обрезка** — crop по найденным областям
- **Kafka** — каждый crop публикуется в топик. Без MinIO: JSON с `image_base64`. С MinIO: crop загружается в бакет, в Kafka только `camera_id`, `timestamp`, `format`, `bucket`, `object_key`. Backend потребляет и отправляет в Triton.

## Установка

```bash
./setup.sh
```

## Конфигурация

- **.env** — единственный источник конфигурации (копия `.env.example` → `.env`). Не коммитить `.env` (секреты).
- **cameras.yaml** — список камер (путь в `CAMERAS_CONFIG_PATH`). Если задан **BACKEND_CAMERAS_URL**, при старте список запрашивается с backend и сохраняется в `cameras.yaml`.

Переменные окружения (см. `.env.example`): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_JPEG_QUALITY`, `MINIO_*`, `BACKEND_CAMERAS_URL`, `CAMERAS_CONFIG_PATH`, `RTSP_USE_FFMPEG_PIPE`, `DETECTION_TYPE`, `PERSON_MODEL_PATH`, `PERSON_CONF`, `PERSON_IOU`, `DEFAULT_FPS`, `CAM1_FPS` и т.д.

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
│   ├── face_crop.py            # Детекция лиц (Haar) + crop
│   ├── person_crop.py          # Детекция людей (person_detect binary) + crop
│   ├── kafka_sender_agent.py   # Публикация crop в Kafka
│   ├── camera_manager.py       # Камеры из cameras.yaml
│   ├── scan_ip_cameras.py      # Утилита добавления камер
│   ├── cameras.yaml
│   └── cameras.yaml.example
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
