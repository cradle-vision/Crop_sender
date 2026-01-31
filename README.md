# Sender Crop

Камера наблюдает людей → детекция (лица или люди) → обрезка (crop) → отправка на gRPC сервер.

## Схема

- **Камера(ы)** — захват кадров (RTSP/HTTP/USB), FPS из `cameras.yaml`
- **Детекция** — **person** (бинарник `person_detect` из `cpu-person-detection/`) или **face** (OpenCV Haar)
- **Обрезка** — crop по найденным областям
- **gRPC** — каждый crop отправляется на сервер (`SendSnapshot`: camera_id, image_data, timestamp)

## Установка

```bash
./setup.sh
```

## Конфигурация

- **config.yaml** — `grpc.server_address`, `rtsp.use_ffmpeg_pipe` (true = захват RTSP через FFmpeg-подпроцесс, меньше ошибок RTP/декодирования), `detection.type` (person | face), `detection.person_conf`, `agent.timeout`, `agent.default_fps`
- **cameras.yaml** — список камер (создать из `cameras.yaml.example` или `python3 scan_ip_cameras.py --add`)

Переменные окружения: `GRPC_SERVER_ADDRESS`, `RTSP_USE_FFMPEG_PIPE` (1/true/yes переопределяет config), `DETECTION_TYPE` (person | face), `PERSON_MODEL_PATH`, `DEFAULT_FPS`, `CAM1_FPS` (и т.д.).

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

В образ копируются `cpu-person-detection/` (бинарник `person_detect`, `lib/`, модель). Нужны `config.yaml` и `cameras.yaml` в каталоге сборки (монтируются в контейнер).

## Типы камер

- **RTSP**: `rtsp://[user:pass@]ip:port/path`
- **HTTP/MJPEG**: `http://ip:port/path`

При "RTP: bad cseq" и "error while decoding MB": включите **захват через FFmpeg** — в `config.yaml` задайте `rtsp.use_ffmpeg_pipe: true` (или env `RTSP_USE_FFMPEG_PIPE=1`). В коде также включены TCP, таймаут и авто-переподключение для OpenCV. Если ошибки остаются — переключитесь на **субпоток** камеры (Hikvision: `/Streaming/Channels/102` вместо `101`; у других — `/stream2`) — меньше битрейт, стабильнее по сети.

### Откуда берутся ошибки RTP / decoding MB

- Сообщения **идут из FFmpeg** при декодировании RTSP (OpenCV внутри использует FFmpeg). В коде RTSP открывается только в **snapshot_capture_agent** (основной захват) и в **camera_manager.test_camera** (разовый тест). Остальные модули (person_crop, grpc_sender) камеру не открывают.
- При **OpenCV-захвате** (`rtsp.use_ffmpeg_pipe: false`) stderr FFmpeg попадает в консоль — в логах видны "RTP: PT=60: bad cseq" и "h264 ... error while decoding MB". При **захвате через FFmpeg pipe** (`rtsp.use_ffmpeg_pipe: true`) декодирование делает отдельный процесс `ffmpeg -rtsp_transport tcp`; его stderr не выводится — эти сообщения в логах не появятся.
- Другие причины: нестабильная сеть или Wi‑Fi, несколько клиентов на один поток (cseq путается), прошивка камеры. Решение: один клиент, по возможности провод, субпоток вместо основного.

## Структура проекта

```
Sender_Crop/
├── sender/
│   ├── main_agent.py           # Оркестратор: кадр → detect → crop → gRPC
│   ├── snapshot_capture_agent.py # Захват с камер
│   ├── face_crop.py            # Детекция лиц (Haar) + crop
│   ├── person_crop.py          # Детекция людей (person_detect binary) + crop
│   ├── grpc_sender_agent.py    # Отправка crop на gRPC
│   ├── camera_manager.py       # Камеры из cameras.yaml
│   ├── scan_ip_cameras.py      # Утилита добавления камер
│   ├── cameras.yaml
│   └── cameras.yaml.example
├── snapshot_service.proto      # gRPC: SnapshotService.SendSnapshot
├── config.yaml
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── setup.sh
└── run.sh
```

## gRPC

Сервер должен реализовать `snapshot.SnapshotService` / `SendSnapshot`. Запрос: `camera_id`, `image_data` (JPEG crop), `timestamp`, `format`.
