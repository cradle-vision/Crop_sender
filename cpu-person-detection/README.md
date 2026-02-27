# CPU Person Detection (C++)

Person-only detection on CPU using **YOLOv8n ONNX**. Main code is C++; Python scripts are in `python_for_reference/` (see .gitignore).

## Layout

- **models/** — Put `person_detection_model.onnx` here (export from Python once: `python_for_reference/scripts/export_onnx.py` if needed; then rename the output to this name).
- **src/** — C++ source (`main.cpp`, `detector.cpp`, `draw.cpp`).
- **include/** — Headers (`detector.hpp`, `draw.hpp`).
- **input/** — Input images (optional, for manual testing).
- **output/** — Output images (boxes drawn).

## Build

### Dependencies

- **OpenCV** (imgproc, imgcodecs, dnn)
- **ONNX Runtime** (CPU, C/C++)

**Linux (e.g. Ubuntu):**

```bash
sudo apt update
sudo apt install libopencv-dev cmake build-essential
```

ONNX Runtime: `libonnxruntime-dev` is often not in default repos. Download the Linux x64 CPU tarball from [ONNX Runtime releases](https://github.com/microsoft/onnxruntime/releases), extract it, then:

```bash
cmake -DONNXRUNTIME_ROOT=/tmp/onnxruntime-linux-x64-1.23.2 ..
```

### Build steps

From project root:

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

Executable: `build/detect_main`.

## Run

From `build/`:

**Default (bbox only)** — input image, output is bbox lines to stdout:

```bash
./detect_main ../models/person_detection_model.onnx ../input/image.jpg
```

Example output: `bbox (x1,y1,x2,y2)=(412,156,465,298) score=0.711719`

**With --draw** — input image, output is image with boxes drawn:

```bash
./detect_main ../models/person_detection_model.onnx ../input/image.jpg --draw ../output/out.jpg
```

Optional: confidence and IoU at the end (default 0.4, 0.5):

```bash
./detect_main ../models/person_detection_model.onnx ../input/in.jpg --draw ../output/out.jpg 0.5 0.5
```

**Benchmark (FPS)** — run on a folder of images (no drawing):

```bash
./detect_main ../models/person_detection_model.onnx --benchmark /path/to/images
```

Output: `[benchmark] images=... time=... s FPS=...`

## Packaging (run on any Linux CPU)

To get a **relocatable package** (binary + shared libs + wrapper) that runs on any Linux x86_64 with **input = image, output = bbox and score**:

From project root (after building once):

```bash
chmod +x create_linux_package.sh
./create_linux_package.sh
```

This creates **person_detection_linux_x64/** and **person_detection_linux_x64.tar.gz**. Copy the folder or tarball to another Linux machine, extract if needed, then:

```bash
./person_detect /path/to/person_detection_model.onnx /path/to/image.jpg
```

Output: bbox and score to stdout (e.g. `bbox (x1,y1,x2,y2)=(412,156,465,298) score=0.71`). No system install of OpenCV or ONNX Runtime needed; the package includes the required shared libs. See **person_detection_linux_x64/README.txt** for --draw and --benchmark usage.

## Model

Place **person_detection_model.onnx** in `models/`. To generate it from a `.pt` file, use the Python reference: `python_for_reference/scripts/export_onnx.py` (run from project root with a venv that has `ultralytics`), then rename the created `yolov8n.onnx` to `person_detection_model.onnx`.
