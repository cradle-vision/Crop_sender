#!/bin/bash

# Setup script for Sender Crop project

echo "Installing dependencies..."
pip3 install -r requirements.txt

echo "Generating gRPC code from proto file..."
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. snapshot_service.proto

if [ $? -eq 0 ]; then
    echo "Setup completed successfully!"
    echo "Run with: python3 main_agent.py"
else
    echo "Error generating gRPC code"
    exit 1
fi
