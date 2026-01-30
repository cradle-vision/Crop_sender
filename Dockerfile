FROM python:3.11-slim

# Install system dependencies for OpenCV and gRPC
RUN apt-get update && apt-get install -y \
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
RUN python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. snapshot_service.proto

RUN mkdir -p /app/config

ENV PYTHONUNBUFFERED=1
ENV CONFIG_PATH=/app/config.yaml
ENV CAMERAS_CONFIG_PATH=/app/cameras.yaml

CMD ["python3", "main_agent.py"]
