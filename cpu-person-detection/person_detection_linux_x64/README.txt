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
