#!/bin/bash

# Quick start script

echo "=== Sender Crop - Snapshot Sending System ==="

# Cameras: from backend (BACKEND_URL + CAMERAS_ENDPOINT) or cameras.yaml
if [ ! -f "cameras.yaml" ] && [ -z "$BACKEND_URL" ]; then
    echo "Note: cameras.yaml not found and BACKEND_URL not set."
    echo "  Set BACKEND_URL to load cameras from API, or create cameras.yaml"
fi

# Check generated gRPC files
if [ ! -f "snapshot_service_pb2.py" ] || [ ! -f "snapshot_service_pb2_grpc.py" ]; then
    echo "Generating gRPC code..."
    python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. snapshot_service.proto
fi

# Start system
echo "Starting system..."
python3 main_agent.py
