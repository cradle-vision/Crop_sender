#!/bin/bash

# Quick start script

echo "=== Sender Crop - Snapshot Sending System ==="

if [ ! -f "sender/cameras.yaml" ] && [ ! -f "cameras.yaml" ]; then
    echo "Create cameras.yaml from sender/cameras.yaml.example or cameras.yaml.example"
fi

# Check generated gRPC files
if [ ! -f "snapshot_service_pb2.py" ] || [ ! -f "snapshot_service_pb2_grpc.py" ]; then
    echo "Generating gRPC code..."
    python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. snapshot_service.proto
fi

# Start from project root so config.yaml and cameras.yaml are found
echo "Starting system..."
python3 sender/main_agent.py
