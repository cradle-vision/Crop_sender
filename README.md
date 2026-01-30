# Sender Crop - Snapshot Sending System

System for sending snapshots from IP cameras: **two senders** — (1) full snapshot → gRPC AI server, (2) face detect → crop → POST to user endpoint. Cameras list from **backend** (env) or `cameras.yaml`.

## Architecture

- **Snapshot Capture Agent** — Captures frames from IP cameras (FPS configurable)
- **Sender 1 (gRPC)** — Sends full snapshot to AI server via gRPC
- **Sender 2 (Crop)** — Detects face, crops, sends crop to user endpoint (HTTP POST)
- **Main Agent** — Coordinates capture + both senders
- **Camera Manager** — Loads cameras from backend (env `BACKEND_URL` + `CAMERAS_ENDPOINT`) or `cameras.yaml`
- **IP Camera Scanner** — Utility for scanning network and managing cameras (file mode)

## Installation

### Local

```bash
./setup.sh
```

### Docker

```bash
docker-compose build
docker-compose up -d
```

## Configuration

### Environment (recommended)

| Env | Description |
|-----|-------------|
| `BACKEND_URL` | Base URL for backend (e.g. `http://localhost:8000`) — cameras loaded from API |
| `CAMERAS_ENDPOINT` | Path for cameras list (default `/api/cameras`), GET returns `{"cameras": [...]}` |
| `GRPC_SERVER_ADDRESS` | gRPC server for full snapshots (Sender 1) |
| `CROP_ENABLED` | `true` to enable Sender 2 (face crop → user) |
| `CROP_DESTINATION_URL` | URL for POST cropped face (e.g. `http://backend/api/user/crop`) |
| `DEFAULT_FPS` | Default FPS for all cameras |

### Main config (`config.yaml`)

```yaml
grpc:
  server_address: "localhost:50051"
  max_message_size: 4194304

backend:
  url: null  # or set; env BACKEND_URL overrides
  cameras_endpoint: "/api/cameras"  # env CAMERAS_ENDPOINT
  timeout: 10

crop:
  enabled: false  # env CROP_ENABLED=true
  destination_url: null  # env CROP_DESTINATION_URL
  timeout: 5

agent:
  buffer_size: 30
  timeout: 5.0
```

### Cameras: backend or file

- **From backend:** set `BACKEND_URL` (and optionally `CAMERAS_ENDPOINT`). Backend GET returns JSON: `{"cameras": [...]}` with fields: `camera_id`, `name`, `type` (rtsp/http), `source` (full URL) or `ip_address`, `port`, `rtsp_path`, `fps`, `width`, `height`, `enabled`.
- **From file:** `cp cameras.yaml.example cameras.yaml` and edit, or `python3 scan_ip_cameras.py --add`

## Usage

### Add IP cameras

```bash
# Interactive
python3 scan_ip_cameras.py --add

# Scan network
python3 scan_ip_cameras.py --scan 192.168.1.0/24

# Auto-detect
python3 scan_ip_cameras.py --auto-detect 192.168.1.100:554
```

### Run

```bash
./run.sh
# or
python3 main_agent.py
```

### Docker

```bash
docker-compose up
```

## Camera Types

- **RTSP**: `rtsp://[user:pass@]ip:port/path`
- **HTTP/MJPEG**: `http://ip:port/path`

## FPS Configuration

FPS can be configured in multiple ways (priority order):

1. **Camera-specific environment variable** (highest priority)
   ```bash
   CAM1_FPS=20.0 docker-compose up
   # Or for any camera: CAMERA_ID_FPS=value
   ```

2. **Default FPS environment variable**
   ```bash
   DEFAULT_FPS=15.0 docker-compose up
   ```

3. **config.yaml default_fps**
   ```yaml
   agent:
     default_fps: 15.0  # Overrides cameras.yaml for all cameras
   ```

4. **cameras.yaml** (lowest priority)
   ```yaml
   cameras:
     - camera_id: "cam1"
       fps: 10.0
   ```

### Examples

```bash
# Set FPS for specific camera
CAM1_FPS=5.0 docker-compose up

# Set default FPS for all cameras
DEFAULT_FPS=15.0 docker-compose up

# Set multiple camera FPS
CAM1_FPS=10.0 IP_CAMERA_1_FPS=20.0 docker-compose up
```

## Troubleshooting

**Error: UNIMPLEMENTED - unknown service snapshot.SnapshotService**

Server doesn't recognize the service. Verify server uses same proto file:
- Package: `snapshot`
- Service: `SnapshotService`  
- Method: `SendSnapshot`

## Project Structure

```
Sender_Crop/
├── snapshot_service.proto      # gRPC proto definition
├── snapshot_capture_agent.py    # Capture agent
├── grpc_sender_agent.py         # gRPC sender agent
├── main_agent.py                # Main coordinator
├── camera_manager.py             # Camera manager
├── scan_ip_cameras.py            # Camera scanner utility
├── config.yaml                   # Main config
├── cameras.yaml                  # Camera config
├── cameras.yaml.example          # Camera config example
├── docker-compose.yml            # Docker compose
├── Dockerfile                    # Docker image
├── requirements.txt              # Dependencies
├── setup.sh                      # Setup script
└── run.sh                        # Run script
```
