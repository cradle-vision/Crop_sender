#!/usr/bin/env bash
# Run FPS benchmark on all images in a directory. Run from build/.
# Usage: ./run_benchmark.sh [images_dir]
# Example: ./run_benchmark.sh /mnt/d/Cradle/Retail/DataSet/valid/valid
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
MODEL="${SCRIPT_DIR}/models/person_detection_model.onnx"
IMAGES_DIR="${1:-$SCRIPT_DIR/input}"

cd "$BUILD_DIR"
./detect_main "$MODEL" --benchmark "$IMAGES_DIR"
