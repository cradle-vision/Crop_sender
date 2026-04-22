FROM python:3.11-slim

# Install system dependencies for OpenCV
RUN apt-get update && apt-get install -y \
    libturbojpeg0 \
    libopencv-dev \
    python3-opencv \
    cmake \
    build-essential \
    wget \
    tar \
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

# Build fresh detector binary with raw-stdin support for runtime.
RUN set -e; \
    ORT_VER="1.23.2"; \
    ORT_DIR="/tmp/onnxruntime-linux-x64-${ORT_VER}"; \
    wget -q "https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VER}/onnxruntime-linux-x64-${ORT_VER}.tgz" -O /tmp/onnxruntime.tgz; \
    tar -xzf /tmp/onnxruntime.tgz -C /tmp; \
    cp "${ORT_DIR}/lib/"libonnxruntime.so* /usr/local/lib/; \
    ldconfig; \
    cmake -S /app/cpu-person-detection -B /app/cpu-person-detection/build -DONNXRUNTIME_ROOT="${ORT_DIR}"; \
    cmake --build /app/cpu-person-detection/build -j"$(nproc)"; \
    chmod +x /app/cpu-person-detection/build/detect_main; \
    if ! /app/cpu-person-detection/build/detect_main 2>&1 | grep -q -- "--stdin-bgr"; then \
      echo "ERROR: detect_main in image does not support --stdin-bgr"; \
      exit 1; \
    fi; \
    if [ ! -f /app/cpu-person-detection/models/person_detection_model.onnx ]; then \
      echo "WARN: person_detection_model.onnx not found"; \
    fi

RUN mkdir -p /app/config

ENV PYTHONUNBUFFERED=1
ENV CAMERAS_CONFIG_PATH=/app/config/cameras.yaml

WORKDIR /app
CMD ["python3", "sender/main_agent.py"]
