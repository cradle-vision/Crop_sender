#!/usr/bin/env bash
# Run detect_main on all images in input/. Run from build/.
# Usage: ./run_all_test_images.sh [model_path] [input_dir] [output_dir]
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
MODEL="${1:-$SCRIPT_DIR/models/person_detection_model.onnx}"
INPUT_DIR="${2:-$SCRIPT_DIR/input}"
OUTPUT_DIR="${3:-$SCRIPT_DIR/output}"

cd "$BUILD_DIR"
mkdir -p "$OUTPUT_DIR"
count=0
for img in "$INPUT_DIR"/*.jpg "$INPUT_DIR"/*.jpeg "$INPUT_DIR"/*.png; do
  [ -f "$img" ] || continue
  name=$(basename "$img")
  echo "Processing $name..."
  ./detect_main "$MODEL" "$img" --draw "$OUTPUT_DIR/$name" && ((count++)) || true
done
echo "Done. Processed $count image(s). Outputs in $OUTPUT_DIR"
