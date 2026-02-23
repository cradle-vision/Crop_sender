#!/bin/bash

# Quick start script

echo "=== Sender Crop - Snapshot Sending System ==="

if [ ! -f "sender/cameras.yaml" ] && [ ! -f "cameras.yaml" ]; then
    echo "Create cameras.yaml from sender/cameras.yaml.example or cameras.yaml.example"
fi

# Start from project root so config.yaml and cameras.yaml are found
echo "Starting system..."
python3 sender/main_agent.py
