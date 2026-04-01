#!/bin/bash

# Setup script for Sender Crop project

echo "Installing dependencies..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "Setup completed successfully!"
    echo "Copy .env.example to .env and set Kafka (and optionally MinIO)."
    echo "Run with: ./run.sh or python3 sender/main_agent.py"
    echo "Streaming Agent: python3 -m streaming_agent.main -c streaming_agent/streaming-agent.yaml.example"
else
    echo "Error installing dependencies"
    exit 1
fi
