/**
 * Person detection (C++): image in -> bbox out (or image out with --draw).
 * Uses YOLOv8 ONNX (person only), CPU.
 * Default: input image -> print bbox (x1,y1,x2,y2) and score to stdout.
 * With --draw: input image -> output image with boxes drawn.
 * With --stdin-bgr: raw BGR frames from stdin until EOF (--width/--height required).
 * Model is loaded once; each frame is followed by a __DETECT_END__ line on stdout.
 * Optional: --line x1 y1 x2 y2 --inside_point ix iy (store-front / tripwire filter).
 * With --benchmark <input_dir>: run on all images in folder, report FPS.
 * Usage: detect_main <model.onnx> <input_image> [--draw <out>] [--line ... --inside_point ...] [conf] [iou]
 *        detect_main <model.onnx> --stdin-bgr --width W --height H [--line ... --inside_point ...] [conf] [iou]
 *        detect_main <model.onnx> --benchmark <input_dir> [conf] [iou]
 */
#include "detector.hpp"
#include "draw.hpp"
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>
#include <iostream>
#include <cstring>
#include <chrono>
#include <vector>
#include <string>
#include <algorithm>
#include <cstdlib>
#include <cmath>
#include <limits>

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

// Store-front / tripwire: line a*x + b*y + c = 0; keep bbox bottom-center on same side as inside point.
struct LineEq {
  float a{0}, b{0}, c{0};
};

static LineEq lineFromTwoPoints(float x1, float y1, float x2, float y2) {
  LineEq L;
  L.a = y1 - y2;
  L.b = x2 - x1;
  L.c = x1 * y2 - x2 * y1;
  return L;
}

static float lineEval(const LineEq& L, float x, float y) {
  return L.a * x + L.b * y + L.c;
}

static constexpr float kLineEvalEps = 1e-6f;

static void applyStoreLineFilter(std::vector<Detection>& detections,
                                 bool has_store_line,
                                 bool has_inside_point,
                                 const LineEq& store_line,
                                 float inside_sign) {
  if (!has_store_line) return;
  if (!has_inside_point) {
    std::cerr << "[main] Warning: --line was provided but --inside_point is missing; skipping store-front filtering.\n";
    return;
  }
  if (std::fabs(inside_sign) < kLineEvalEps) {
    std::cerr << "[main] Warning: --inside_point is exactly on the line; inside test may be unstable.\n";
    return;
  }
  std::vector<Detection> filtered;
  filtered.reserve(detections.size());
  for (const auto& d : detections) {
    float cx = 0.5f * (d.x1 + d.x2);
    float cy = d.y2;
    float s = lineEval(store_line, cx, cy);
    if (inside_sign >= 0.0f) {
      if (s >= -kLineEvalEps) filtered.push_back(d);
    } else {
      if (s <= kLineEvalEps) filtered.push_back(d);
    }
  }
  detections.swap(filtered);
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

static bool parseFloatArg(const char* s, float& out) {
  if (s == nullptr) return false;
  char* end = nullptr;
  float v = std::strtof(s, &end);
  if (end == s || (end && *end != '\0')) return false;
  out = v;
  return true;
}

static bool parseIntArg(const char* s, int& out) {
  if (s == nullptr) return false;
  char* end = nullptr;
  long v = std::strtol(s, &end, 10);
  if (end == s || (end && *end != '\0')) return false;
  if (v < 0 || v > std::numeric_limits<int>::max()) return false;
  out = static_cast<int>(v);
  return true;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <model.onnx> <input_image> [--draw <output_image>] [conf] [iou]\n"
              << "       " << argv[0] << " <model.onnx> --stdin-bgr --width <W> --height <H> [--line x1 y1 x2 y2 --inside_point ix iy] [conf] [iou]\n"
              << "       " << argv[0] << " <model.onnx> --benchmark <input_dir> [conf] [iou]\n"
              << "  Default: print bbox (x1,y1,x2,y2) and score to stdout.\n"
              << "  --draw: draw boxes and save to <output_image>.\n"
              << "  --stdin-bgr: read raw BGR frames from stdin until EOF (model loaded once).\n"
              << "    After each frame, print detections then a __DETECT_END__ line.\n"
              << "  Optional store-front filter (image or stdin): --line x1 y1 x2 y2 --inside_point ix iy\n"
              << "    Keeps only persons whose bbox bottom-center is on the same side as --inside_point.\n"
              << "  --benchmark: run on all images in <input_dir>, report FPS.\n"
              << "  Example (bbox):   " << argv[0] << " ../models/person_detection_model.onnx ../input/in.jpg\n"
              << "  Example (stdin):  " << argv[0] << " ../models/person_detection_model.onnx --stdin-bgr --width 1280 --height 720\n"
              << "  Example (draw):   " << argv[0] << " ../models/person_detection_model.onnx ../input/in.jpg --draw ../output/out.jpg\n"
              << "  Example (FPS):    " << argv[0] << " ../models/person_detection_model.onnx --benchmark /path/to/images\n";
    return 1;
  }

  const std::string model_path = argv[1];
  const std::string arg2 = argv[2];
  bool benchmark_mode = (arg2 == "--benchmark");
  bool stdin_bgr_mode = (arg2 == "--stdin-bgr");
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

  if (stdin_bgr_mode) {
    int width = 0;
    int height = 0;
    bool has_store_line = false;
    bool has_inside_point = false;
    LineEq store_line;
    float inside_sign = 0.0f;
    int idx = 3;
    while (idx < argc) {
      const std::string flag = argv[idx];
      if (flag == "--width") {
        ++idx;
        if (idx >= argc || !parseIntArg(argv[idx], width) || width <= 0) {
          std::cerr << "[main] --width requires positive integer\n";
          return 1;
        }
        ++idx;
        continue;
      }
      if (flag == "--height") {
        ++idx;
        if (idx >= argc || !parseIntArg(argv[idx], height) || height <= 0) {
          std::cerr << "[main] --height requires positive integer\n";
          return 1;
        }
        ++idx;
        continue;
      }
      if (flag == "--line") {
        if (idx + 4 >= argc) {
          std::cerr << "[main] --line requires: --line x1 y1 x2 y2\n";
          return 1;
        }
        float x1 = static_cast<float>(atof(argv[idx + 1]));
        float y1 = static_cast<float>(atof(argv[idx + 2]));
        float x2 = static_cast<float>(atof(argv[idx + 3]));
        float y2 = static_cast<float>(atof(argv[idx + 4]));
        store_line = lineFromTwoPoints(x1, y1, x2, y2);
        has_store_line = true;
        idx += 5;
        continue;
      }
      if (flag == "--inside_point") {
        if (!has_store_line) {
          std::cerr << "[main] --inside_point must be provided after --line\n";
          return 1;
        }
        if (idx + 2 >= argc) {
          std::cerr << "[main] --inside_point requires: --inside_point ix iy\n";
          return 1;
        }
        float ix = static_cast<float>(atof(argv[idx + 1]));
        float iy = static_cast<float>(atof(argv[idx + 2]));
        inside_sign = lineEval(store_line, ix, iy);
        has_inside_point = true;
        idx += 3;
        continue;
      }
      break;
    }

    if (width <= 0 || height <= 0) {
      std::cerr << "[main] --stdin-bgr requires --width and --height\n";
      return 1;
    }
    if (idx < argc) {
      if (!parseFloatArg(argv[idx], conf)) {
        std::cerr << "[main] invalid conf value\n";
        return 1;
      }
      ++idx;
    }
    if (idx < argc) {
      if (!parseFloatArg(argv[idx], iou)) {
        std::cerr << "[main] invalid iou value\n";
        return 1;
      }
      ++idx;
    }

    const size_t frame_size = static_cast<size_t>(width) * static_cast<size_t>(height) * 3;
    std::vector<unsigned char> buf(frame_size);

    Detector detector;
    if (!detector.load(model_path)) {
      std::cerr << "[main] Failed to load model: " << model_path << std::endl;
      return 1;
    }
    detector.setConfidenceThreshold(conf);
    detector.setIouThreshold(iou);
    std::cerr << "[main] model loaded; reading stdin frames " << width << "x" << height << std::endl;

    auto read_exact = [&](size_t n) -> bool {
      size_t got = 0;
      while (got < n) {
        std::cin.read(reinterpret_cast<char*>(buf.data() + got),
                      static_cast<std::streamsize>(n - got));
        auto chunk = static_cast<size_t>(std::cin.gcount());
        if (chunk == 0) {
          return false;
        }
        got += chunk;
      }
      return true;
    };

    while (true) {
      if (!read_exact(frame_size)) {
        if (std::cin.eof() || std::cin.fail()) {
          return 0;
        }
        std::cerr << "[main] Failed to read raw frame from stdin\n";
        return 1;
      }

      cv::Mat image(height, width, CV_8UC3, buf.data());
      if (image.empty()) {
        std::cerr << "[main] Failed to construct BGR image from stdin buffer" << std::endl;
        return 1;
      }

      std::vector<Detection> detections = detector.detect(image);
      applyStoreLineFilter(detections, has_store_line, has_inside_point, store_line, inside_sign);
      for (size_t i = 0; i < detections.size(); ++i) {
        const auto& d = detections[i];
        std::cout << "bbox (x1,y1,x2,y2)=(" << static_cast<int>(d.x1) << ","
                  << static_cast<int>(d.y1) << "," << static_cast<int>(d.x2) << ","
                  << static_cast<int>(d.y2) << ") score=" << d.score << "\n";
      }
      std::cout << "__DETECT_END__\n" << std::flush;
    }
  }

  const std::string input_path = arg2;
  bool draw_mode = false;
  std::string output_path;

  bool has_store_line = false;
  bool has_inside_point = false;
  LineEq store_line;
  float inside_sign = 0.0f;

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

  int positional_count = 0;
  while (idx < argc) {
    const std::string tok = argv[idx];
    if (tok == "--line") {
      if (idx + 4 >= argc) {
        std::cerr << "[main] --line requires: --line x1 y1 x2 y2\n";
        return 1;
      }
      float x1 = static_cast<float>(atof(argv[idx + 1]));
      float y1 = static_cast<float>(atof(argv[idx + 2]));
      float x2 = static_cast<float>(atof(argv[idx + 3]));
      float y2 = static_cast<float>(atof(argv[idx + 4]));
      store_line = lineFromTwoPoints(x1, y1, x2, y2);
      has_store_line = true;
      idx += 5;
      continue;
    }
    if (tok == "--inside_point") {
      if (!has_store_line) {
        std::cerr << "[main] --inside_point must be provided after --line\n";
        return 1;
      }
      if (idx + 2 >= argc) {
        std::cerr << "[main] --inside_point requires: --inside_point ix iy\n";
        return 1;
      }
      float ix = static_cast<float>(atof(argv[idx + 1]));
      float iy = static_cast<float>(atof(argv[idx + 2]));
      inside_sign = lineEval(store_line, ix, iy);
      has_inside_point = true;
      idx += 3;
      continue;
    }
    if (positional_count == 0) {
      conf = static_cast<float>(atof(argv[idx++]));
      positional_count++;
    } else if (positional_count == 1) {
      iou = static_cast<float>(atof(argv[idx++]));
      positional_count++;
    } else {
      ++idx;
    }
  }

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
  applyStoreLineFilter(detections, has_store_line, has_inside_point, store_line, inside_sign);

  for (size_t i = 0; i < detections.size(); ++i) {
    const auto& d = detections[i];
    std::cout << "bbox (x1,y1,x2,y2)=(" << static_cast<int>(d.x1) << ","
              << static_cast<int>(d.y1) << "," << static_cast<int>(d.x2) << ","
              << static_cast<int>(d.y2) << ") score=" << d.score << std::endl;
  }

  if (draw_mode) {
    drawDetections(image, detections);
    if (has_store_line && has_inside_point) {
      int w = image.cols;
      cv::Point p1, p2;
      if (std::fabs(store_line.b) > kLineEvalEps) {
        float y0 = -(store_line.a * 0.0f + store_line.c) / store_line.b;
        float yw = -(store_line.a * static_cast<float>(w) + store_line.c) / store_line.b;
        p1 = cv::Point(0, static_cast<int>(y0));
        p2 = cv::Point(w, static_cast<int>(yw));
      } else {
        float x = (std::fabs(store_line.a) > kLineEvalEps) ? (-store_line.c / store_line.a) : 0.0f;
        p1 = cv::Point(static_cast<int>(x), 0);
        p2 = cv::Point(static_cast<int>(x), image.rows);
      }
      cv::line(image, p1, p2, cv::Scalar(0, 0, 255), 2);
    }
    if (!cv::imwrite(output_path, image)) {
      std::cerr << "[main] Failed to write image: " << output_path << std::endl;
      return 1;
    }
    std::cout << "[main] Detected " << detections.size() << " person(s). Saved to " << output_path << std::endl;
  }

  return 0;
}
