# Sender Crop

Camera -> person detection (CPU binary) -> crop -> MinIO -> Kafka metadata -> backend -> Triton.

## Current Sender Pipeline

1. Camera frame capture (`sender/snapshot_capture_agent.py`)
   - RTSP is captured via FFmpeg pipe (TCP), no RTSP OpenCV branch in sender.
2. Person detection (`sender/person_crop.py`)
   - Uses `cpu-person-detection/person_detection_linux_x64/bin/detect_main` via `subprocess.run`.
   - Temporary detector input file is created in `/dev/shm` and removed after each call.
3. Crop extraction (`sender/person_crop.py`)
4. JPEG encoding (`sender/jpeg_utils.py`)
   - TurboJPEG-only (single path).
5. Upload to MinIO + publish Kafka metadata (`sender/kafka_sender_agent.py`)

## Installation

```bash
./setup.sh
```

## Fleet CI/CD + remote env

Remote env updates and image rollout from the admin panel are documented in:

- [docs/CI_CD_FLEET.md](docs/CI_CD_FLEET.md)
- Backend: `Retail_Backend/docs/STORE_AGENT_FLEET_API.md`

## Configuration

- `.env` is the runtime config source (copy `.env.example` -> `.env`).
- `cameras.yaml` is read from `CAMERAS_CONFIG_PATH`.
- If `BACKEND_CAMERAS_URL` is set, cameras are fetched from backend and saved into `cameras.yaml`.

Common env vars (see `.env.example`):
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC`
- `MINIO_*`
- `BACKEND_CAMERAS_URL`
- `CAMERAS_CONFIG_PATH`
- `PERSON_MODEL_PATH`
- `PERSON_CONF`
- `PERSON_IOU`
- `DEFAULT_FPS`, `CAM1_FPS`, ...

Person detector assets required:
- `cpu-person-detection/person_detection_linux_x64/bin/detect_main`
- `cpu-person-detection/person_detection_linux_x64/lib/*`
- `cpu-person-detection/models/person_detection_model.onnx`

## Run

```bash
./run.sh
# or
cd sender && python3 main_agent.py
```

Docker:

```bash
docker compose build && docker compose up -d
```

## Streaming Agent (RTSP -> MediaMTX -> WebRTC/RTMP)

`streaming-agent` is a separate edge mode and does not use sender Kafka crop flow.

Delivery modes:

| delivery | Meaning |
|---|---|
| `local_webrtc` | Local MediaMTX + browser WHEP |
| `upstream_rtmp` | RTSP -> RTMP push to central ingest |
| `both` | Local WHEP + upstream RTMP push |

Config reference:
- `streaming_agent/streaming-agent.yaml.example`
- `docs/streaming_agent_auth.md`

## Camera Types

- RTSP: `rtsp://[user:pass@]ip:port/path`
- HTTP/MJPEG: `http://ip:port/path`
- USB: index source for local camera

## Project Structure

```text
Crop_sender/
├── sender/
│   ├── main_agent.py
│   ├── snapshot_capture_agent.py
│   ├── person_crop.py
│   ├── jpeg_utils.py
│   ├── kafka_sender_agent.py
│   ├── camera_manager.py
│   └── cameras.yaml.example
├── streaming_agent/
│   ├── main.py
│   ├── config.py
│   ├── signaling_client.py
│   ├── stream_manager.py
│   ├── mediamtx_client.py
│   ├── health_monitor.py
│   └── streaming-agent.yaml.example
├── docs/
│   ├── streaming_backend_frontend_plan.md
│   └── streaming_agent_auth.md
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── setup.sh
└── run.sh
```

## Kafka/MinIO Message Model

Sender uploads image bytes to MinIO and publishes metadata to Kafka:
- `camera_id`
- `timestamp` (unix ms)
- `format` (`jpeg`)
- `bucket`
- `object_key`
- optional: `company_id`, `building_id`, `company_name`, `building_name`

## Initial SmartCamera Snapshot Upload

On startup, sender uploads one initial full-frame snapshot per camera to backend endpoint:

`POST /company/{company_id}/smartcamera/{smartcamera_id}/snapshot`

- Uses Bearer token from backend auth settings.
- Multipart form field: `file` (JPEG).
- On HTTP 200, snapshot is marked sent for that camera.

## ROI / Tripwire / Exclude Zones

If camera line settings are present and active, sender passes them to detector:
- `line_x1`, `line_y1`, `line_x2`, `line_y2`
- `inside_x`, `inside_y`
- `line_active`

Detector receives:
- `--line x1 y1 x2 y2`
- `--inside_point ix iy`

**Exclude zones** (`exclude_zones`, `exclude_zones_active`) arrive on the same `roi_updated` /
`roi-sync` path as the tripwire line. Edge strips are pre-cropped before detect; mid-frame
rects drop detections whose bbox is **fully inside** a zone. Env fallback:
`PERSON_EXCLUDE_ZONES` / `PERSON_EXCLUDE_ZONES_<camera_id>` only when backend zones are unset.
