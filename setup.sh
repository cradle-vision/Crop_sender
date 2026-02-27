#!/bin/bash

# Setup script for Sender Crop project

echo "Installing dependencies..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "Setup completed successfully!"
    echo "Copy .env.example to .env and set Kafka (and optionally MinIO)."
    echo "Run with: ./run.sh or python3 sender/main_agent.py"
else
    echo "Error installing dependencies"
    exit 1
fi
