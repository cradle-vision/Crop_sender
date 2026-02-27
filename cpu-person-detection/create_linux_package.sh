#!/usr/bin/env bash
# Create a relocatable Linux package: binary + shared libs + wrapper.
# Run from project root. Output: person_detection_linux_x64/ (and .tar.gz).
# On target: extract, then: ./person_detect model.onnx image.jpg  -> bbox and score to stdout.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
BUILD_DIR="$SCRIPT_DIR/build"
PKG_NAME="person_detection_linux_x64"
PKG_DIR="$SCRIPT_DIR/$PKG_NAME"
BIN_DIR="$PKG_DIR/bin"
LIB_DIR="$PKG_DIR/lib"

# Build if needed
if [ ! -f "$BUILD_DIR/detect_main" ]; then
  echo "[package] Building..."
  mkdir -p "$BUILD_DIR"
  (cd "$BUILD_DIR" && cmake .. && cmake --build .)
fi
if [ ! -f "$BUILD_DIR/detect_main" ]; then
  echo "[package] Build failed. Run: mkdir build && cd build && cmake .. && cmake --build ."
  exit 1
fi

rm -rf "$PKG_DIR"
mkdir -p "$BIN_DIR" "$LIB_DIR"
cp "$BUILD_DIR/detect_main" "$BIN_DIR/"

# Copy shared libs (skip system libs)
skip_system() {
  case "$1" in
    libc.so*|libm.so*|libdl.so*|libpthread.so*|libstdc++.so*|libgcc_s.so*|ld-linux*.so*) return 0 ;;
    *) return 1 ;;
  esac
}
echo "[package] Copying dependencies..."
ldd "$BIN_DIR/detect_main" 2>/dev/null | while IFS= read -r line; do
  lib=""
  case "$line" in
    *"=>"*) lib=$(echo "$line" | awk '{print $3}') ;;
    *) continue ;;
  esac
  if [ -n "$lib" ] && [ -f "$lib" ]; then
    base=$(basename "$lib")
    if skip_system "$base"; then
      true
    else
      cp -L "$lib" "$LIB_DIR/" 2>/dev/null || true
    fi
  fi
done

# Wrapper script (LF line endings).
# ORT emits "GPU device discovery failed" at library load (before our CPU-only session);
# filter that one line so output is clean on machines without GPU.
cat > "$PKG_DIR/person_detect" << 'WRAPPER'
#!/usr/bin/env bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export LD_LIBRARY_PATH="$DIR/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec "$DIR/bin/detect_main" "$@" 2> >(grep -v -E "device_discovery|GPU device discovery failed|ReadFileContents Failed to open file" >&2)
WRAPPER
chmod +x "$PKG_DIR/person_detect"

# Package README
cat > "$PKG_DIR/README.txt" << 'README'
Person detection (CPU, Linux): input = image, output = bbox and score.

Usage (default: print bbox and score to stdout):
  ./person_detect /path/to/model.onnx /path/to/image.jpg

Optional: conf and iou (no rebuild needed; pass as last arguments, default 0.4 0.5):
  ./person_detect /path/to/model.onnx /path/to/image.jpg 0.5 0.45

Draw boxes and save image:
  ./person_detect /path/to/model.onnx /path/to/image.jpg --draw /path/to/output.jpg 0.5 0.5

Benchmark (FPS on a folder of images):
  ./person_detect /path/to/model.onnx --benchmark /path/to/images/ 0.4 0.5

Requirements: Linux x86_64, no install needed. Put person_detection_model.onnx
(from this project's models/) next to the script or pass its path as first argument.
README

echo "[package] Created $PKG_DIR"
if command -v tar &>/dev/null; then
  tar -czf "${PKG_NAME}.tar.gz" -C "$SCRIPT_DIR" "$PKG_NAME"
  echo "[package] Archive: ${PKG_NAME}.tar.gz"
fi
echo "[package] On another Linux: extract and run ./person_detect model.onnx image.jpg"
