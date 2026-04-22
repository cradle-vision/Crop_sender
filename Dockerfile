FROM python:3.11-slim

# Install system dependencies for OpenCV
RUN apt-get update && apt-get install -y \
    libturbojpeg0 \
    libopencv-dev \
    python3-opencv \
    ffmpeg \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Person detection: ensure binary is executable and model is present
RUN chmod +x /app/cpu-person-detection/person_detection_linux_x64/person_detect 2>/dev/null || true \
    && chmod +x /app/cpu-person-detection/person_detection_linux_x64/bin/detect_main 2>/dev/null || true \
    && test -f /app/cpu-person-detection/models/person_detection_model.onnx || echo "WARN: person_detection_model.onnx not found"

RUN mkdir -p /app/config

ENV PYTHONUNBUFFERED=1
ENV CAMERAS_CONFIG_PATH=/app/config/cameras.yaml

WORKDIR /app
CMD ["python3", "sender/main_agent.py"]
