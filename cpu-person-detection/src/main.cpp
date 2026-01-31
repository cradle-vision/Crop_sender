/**
 * Person detection (C++): image in -> bbox out (or image out with --draw).
 * Uses YOLOv8 ONNX (person only), CPU.
 * Default: input image -> print bbox (x1,y1,x2,y2) and score to stdout.
 * With --draw: input image -> output image with boxes drawn.
 * With --benchmark <input_dir>: run on all images in folder, report FPS.
 * Usage: detect_main <model.onnx> <input_image> [--draw <output_image>] [conf] [iou]
 *        detect_main <model.onnx> --benchmark <input_dir> [conf] [iou]
 */
#include "detector.hpp"
#include "draw.hpp"
#include <opencv2/imgcodecs.hpp>
#include <iostream>
#include <cstring>
#include <chrono>
#include <vector>
#include <string>
#include <algorithm>

#include <filesystem>
namespace fs = std::filesystem;

static bool hasImageExtension(const std::string& path) {
  std::string ext;
  size_t dot = path.rfind('.');
  if (dot != std::string::npos && dot + 1 < path.size())
    ext = path.substr(dot + 1);
  if (ext.empty()) return false;
  for (char& c : ext) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return ext == "jpg" || ext == "jpeg" || ext == "png";
}

static std::vector<std::string> listImagesInDir(const std::string& dir_path) {
  std::vector<std::string> paths;
  try {
    for (const auto& entry : fs::directory_iterator(dir_path)) {
      if (!entry.is_regular_file()) continue;
      std::string path = entry.path().string();
      if (hasImageExtension(path))
        paths.push_back(path);
    }
  } catch (const std::exception& e) {
    std::cerr << "[main] Failed to list directory: " << e.what() << std::endl;
    return {};
  }
  std::sort(paths.begin(), paths.end());
  return paths;
}

static int runBenchmark(const std::string& model_path, const std::string& input_dir,
                       float conf, float iou) {
  std::vector<std::string> image_paths = listImagesInDir(input_dir);
  if (image_paths.empty()) {
    std::cerr << "[main] No images found in " << input_dir << std::endl;
    return 1;
  }

  Detector detector;
  if (!detector.load(model_path)) {
    std::cerr << "[main] Failed to load model: " << model_path << std::endl;
    return 1;
  }
  detector.setConfidenceThreshold(conf);
  detector.setIouThreshold(iou);

  const int warmup = std::min(5, static_cast<int>(image_paths.size()));
  for (int i = 0; i < warmup; ++i) {
    cv::Mat img = cv::imread(image_paths[i]);
    if (!img.empty())
      detector.detect(img);
  }

  const int total = static_cast<int>(image_paths.size());
  std::cout << "[benchmark] Running on " << total << " images..." << std::endl;

  auto start = std::chrono::steady_clock::now();
  int processed = 0;
  for (const auto& path : image_paths) {
    cv::Mat img = cv::imread(path);
    if (img.empty()) continue;
    detector.detect(img);
    ++processed;
    // Progress every 50 images or at 10% steps
    if (processed % 50 == 0 || processed == total) {
      int pct = (total > 0) ? (100 * processed / total) : 0;
      std::cout << "\r[benchmark] " << processed << " / " << total << " (" << pct << "%)" << std::flush;
    }
  }
  std::cout << std::endl;
  auto end = std::chrono::steady_clock::now();
  double sec = std::chrono::duration<double>(end - start).count();
  double fps = (sec > 0 && processed > 0) ? (processed / sec) : 0;

  std::cout << "[benchmark] images=" << processed
            << " time=" << sec << " s"
            << " FPS=" << fps << std::endl;
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <model.onnx> <input_image> [--draw <output_image>] [conf] [iou]\n"
              << "       " << argv[0] << " <model.onnx> --benchmark <input_dir> [conf] [iou]\n"
              << "  Default: print bbox (x1,y1,x2,y2) and score to stdout.\n"
              << "  --draw: draw boxes and save to <output_image>.\n"
              << "  --benchmark: run on all images in <input_dir>, report FPS.\n"
              << "  Example (bbox):   " << argv[0] << " ../models/person_detection_model.onnx ../input/in.jpg\n"
              << "  Example (draw):   " << argv[0] << " ../models/person_detection_model.onnx ../input/in.jpg --draw ../output/out.jpg\n"
              << "  Example (FPS):    " << argv[0] << " ../models/person_detection_model.onnx --benchmark /path/to/images\n";
    return 1;
  }

  const std::string model_path = argv[1];
  const std::string arg2 = argv[2];
  bool benchmark_mode = (arg2 == "--benchmark");
  float conf = 0.4f;
  float iou = 0.5f;

  if (benchmark_mode) {
    if (argc < 4) {
      std::cerr << "[main] --benchmark requires <input_dir>\n";
      return 1;
    }
    std::string input_dir = argv[3];
    int idx = 4;
    if (idx < argc) conf = static_cast<float>(atof(argv[idx++]));
    if (idx < argc) iou = static_cast<float>(atof(argv[idx++]));
    return runBenchmark(model_path, input_dir, conf, iou);
  }

  const std::string input_path = arg2;
  bool draw_mode = false;
  std::string output_path;

  int idx = 3;
  if (idx < argc && std::strcmp(argv[idx], "--draw") == 0) {
    draw_mode = true;
    ++idx;
    if (idx >= argc) {
      std::cerr << "[main] --draw requires <output_image>\n";
      return 1;
    }
    output_path = argv[idx++];
  }
  if (idx < argc) conf = static_cast<float>(atof(argv[idx++]));
  if (idx < argc) iou = static_cast<float>(atof(argv[idx++]));

  cv::Mat image = cv::imread(input_path);
  if (image.empty()) {
    std::cerr << "[main] Failed to read image: " << input_path << std::endl;
    return 1;
  }

  Detector detector;
  if (!detector.load(model_path)) {
    std::cerr << "[main] Failed to load model: " << model_path << std::endl;
    return 1;
  }
  detector.setConfidenceThreshold(conf);
  detector.setIouThreshold(iou);

  std::vector<Detection> detections = detector.detect(image);

  for (size_t i = 0; i < detections.size(); ++i) {
    const auto& d = detections[i];
    std::cout << "bbox (x1,y1,x2,y2)=(" << static_cast<int>(d.x1) << ","
              << static_cast<int>(d.y1) << "," << static_cast<int>(d.x2) << ","
              << static_cast<int>(d.y2) << ") score=" << d.score << std::endl;
  }

  if (draw_mode) {
    drawDetections(image, detections);
    if (!cv::imwrite(output_path, image)) {
      std::cerr << "[main] Failed to write image: " << output_path << std::endl;
      return 1;
    }
    std::cout << "[main] Detected " << detections.size() << " person(s). Saved to " << output_path << std::endl;
  }

  return 0;
}
