#include "detector.hpp"
#include <onnxruntime_cxx_api.h>
#include <opencv2/imgproc.hpp>
#include <opencv2/dnn.hpp>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <memory>

// Out-of-class definition so linker can find it
const int Detector::person_class_id_;

struct Detector::OrtSession {
  Ort::Env env{ORT_LOGGING_LEVEL_WARNING, "detect"};
  Ort::Session* session = nullptr;
  std::vector<const char*> input_names;
  std::vector<const char*> output_names;
  std::vector<std::string> input_names_storage;
  std::vector<std::string> output_names_storage;
  Ort::AllocatorWithDefaultOptions allocator;
};

Detector::~Detector() {
  if (session_) {
    delete static_cast<OrtSession*>(session_)->session;
    delete static_cast<OrtSession*>(session_);
    session_ = nullptr;
  }
}

bool Detector::load(const std::string& model_path) {
  try {
    auto* impl = new OrtSession();
    session_ = impl;

    Ort::SessionOptions options;
    options.SetIntraOpNumThreads(1);
    options.SetGraphOptimizationLevel(GraphOptimizationLevel::ORT_ENABLE_ALL);
    // CPU is the default when no provider is added (ORT 1.23 C++ API has no AppendExecutionProvider_CPU)

#ifdef _WIN32
    std::wstring wpath(model_path.begin(), model_path.end());
    impl->session = new Ort::Session(impl->env, wpath.c_str(), options);
#else
    impl->session = new Ort::Session(impl->env, model_path.c_str(), options);
#endif

    // Input name
    size_t num_inputs = impl->session->GetInputCount();
    if (num_inputs == 0) return false;
    auto input_name = impl->session->GetInputNameAllocated(0, impl->allocator);
    impl->input_names_storage.push_back(input_name.get());
    impl->input_names.push_back(impl->input_names_storage.back().c_str());

    // Output name
    size_t num_outputs = impl->session->GetOutputCount();
    if (num_outputs == 0) return false;
    auto output_name = impl->session->GetOutputNameAllocated(0, impl->allocator);
    impl->output_names_storage.push_back(output_name.get());
    impl->output_names.push_back(impl->output_names_storage.back().c_str());

    return true;
  } catch (const Ort::Exception& e) {
    std::cerr << "[Detector] Load failed: " << e.what() << std::endl;
    return false;
  }
}

void Detector::preprocess(const cv::Mat& image, cv::Mat& blob) {
  // Letterbox (match Python/Ultralytics): keep aspect ratio, pad to 640x640
  int orig_w = image.cols, orig_h = image.rows;
  float r = std::min(static_cast<float>(input_w_) / orig_w, static_cast<float>(input_h_) / orig_h);
  int new_w = static_cast<int>(orig_w * r);
  int new_h = static_cast<int>(orig_h * r);
  scale_ = 1.f / r;
  int pad_left_i = (input_w_ - new_w) / 2;
  int pad_top_i = (input_h_ - new_h) / 2;
  pad_left_ = static_cast<float>(pad_left_i);
  pad_top_ = static_cast<float>(pad_top_i);

  cv::Mat resized;
  cv::resize(image, resized, cv::Size(new_w, new_h));
  cv::Mat padded = cv::Mat::zeros(input_h_, input_w_, CV_8UC3);
  resized.copyTo(padded(cv::Rect(pad_left_i, pad_top_i, new_w, new_h)));
  if (padded.channels() == 3)
    cv::cvtColor(padded, padded, cv::COLOR_BGR2RGB);

  blob = cv::Mat(1, input_h_ * input_w_ * 3, CV_32F);
  float* ptr = blob.ptr<float>();
  for (int c = 0; c < 3; ++c)
    for (int y = 0; y < input_h_; ++y)
      for (int x = 0; x < input_w_; ++x)
        *ptr++ = padded.at<cv::Vec3b>(y, x)[c] / 255.f;
}

std::vector<Detection> Detector::postprocess(const float* output,
                                             int num_rows, int num_cols,
                                             int orig_w, int orig_h) {
  // YOLOv8 output: (1, 84, 8400) or (1, 8400, 84). Coords in 0..640; map back via letterbox scale/pad.
  const float sx = scale_;
  const float sy = scale_;
  const float px = pad_left_;
  const float py = pad_top_;

  std::vector<cv::Rect> boxes;
  std::vector<float> scores;
  std::vector<int> class_ids;

  // Support both layouts: (84, 8400) -> 84 rows, 8400 cols; or (8400, 84) -> 8400 rows, 84 cols
  const bool layout_84_8400 = (num_rows == 84 && num_cols == 8400);
  const int num_predictions = layout_84_8400 ? num_cols : num_rows;
  const int num_values = layout_84_8400 ? num_rows : num_cols;

  for (int j = 0; j < num_predictions; ++j) {
    float cx, cy, w, h, person_score;
    if (layout_84_8400) {
      cx = output[0 * num_cols + j];
      cy = output[1 * num_cols + j];
      w = output[2 * num_cols + j];
      h = output[3 * num_cols + j];
      person_score = output[(4 + person_class_id_) * num_cols + j];
    } else {
      const float* row = output + j * num_cols;
      cx = row[0];
      cy = row[1];
      w = row[2];
      h = row[3];
      person_score = row[4 + person_class_id_];
    }
    if (person_score < conf_threshold_) continue;

    float x1 = (cx - 0.5f * w - px) * sx;
    float y1 = (cy - 0.5f * h - py) * sy;
    float x2 = (cx + 0.5f * w - px) * sx;
    float y2 = (cy + 0.5f * h - py) * sy;
    x1 = std::max(0.f, std::min(x1, static_cast<float>(orig_w)));
    y1 = std::max(0.f, std::min(y1, static_cast<float>(orig_h)));
    x2 = std::max(0.f, std::min(x2, static_cast<float>(orig_w)));
    y2 = std::max(0.f, std::min(y2, static_cast<float>(orig_h)));
    if (x2 <= x1 || y2 <= y1) continue;

    boxes.push_back(cv::Rect(static_cast<int>(x1), static_cast<int>(y1),
                             static_cast<int>(x2 - x1), static_cast<int>(y2 - y1)));
    scores.push_back(person_score);
    class_ids.push_back(person_class_id_);
  }

  std::vector<int> indices;
  cv::dnn::NMSBoxes(boxes, scores, conf_threshold_, iou_threshold_, indices);

  std::vector<Detection> result;
  for (int i : indices) {
    Detection d;
    d.x1 = static_cast<float>(boxes[i].x);
    d.y1 = static_cast<float>(boxes[i].y);
    d.x2 = static_cast<float>(boxes[i].x + boxes[i].width);
    d.y2 = static_cast<float>(boxes[i].y + boxes[i].height);
    d.class_id = class_ids[i];
    d.score = scores[i];
    result.push_back(d);
  }
  return result;
}

std::vector<Detection> Detector::detect(const cv::Mat& image) {
  if (!session_ || image.empty()) return {};

  auto* impl = static_cast<OrtSession*>(session_);
  int orig_h = image.rows, orig_w = image.cols;

  cv::Mat blob;
  preprocess(image, blob);

  std::vector<int64_t> input_shape = {1, 3, input_h_, input_w_};
  Ort::Value input_tensor = Ort::Value::CreateTensor<float>(
      Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault),
      blob.ptr<float>(), blob.total(), input_shape.data(), input_shape.size());

  auto output_tensors = impl->session->Run(
      Ort::RunOptions{nullptr},
      impl->input_names.data(), &input_tensor, 1,
      impl->output_names.data(), 1);

  const float* output = output_tensors[0].GetTensorData<float>();
  auto shape = output_tensors[0].GetTensorTypeAndShapeInfo().GetShape();
  int num_rows = static_cast<int>(shape[1]);
  int num_cols = static_cast<int>(shape[2]);
  // Expect (1, 84, 8400) or (1, 8400, 84)
  if (shape.size() != 3u || (num_rows != 84 && num_rows != 8400)) {
    std::cerr << "[Detector] Unexpected output shape: " << shape[0] << "," << shape[1] << "," << shape[2] << std::endl;
    return {};
  }

  return postprocess(output, num_rows, num_cols, orig_w, orig_h);
}
