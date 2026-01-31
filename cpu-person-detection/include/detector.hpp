#ifndef DETECTOR_HPP
#define DETECTOR_HPP

#include <opencv2/core.hpp>
#include <string>
#include <vector>

/** One detection: box + class id + confidence */
struct Detection {
  float x1{0}, y1{0}, x2{0}, y2{0};
  int class_id{0};
  float score{0};
};

/**
 * YOLOv8 ONNX person detector (CPU).
 * Input: image. Output: list of detections (bbox, class_id, score).
 */
class Detector {
 public:
  Detector() = default;
  ~Detector();

  /** Load ONNX model from path. Returns false on error. */
  bool load(const std::string& model_path);

  /**
   * Run detection on image. Person only (class 0).
   * Returns detections in image coordinates (same size as input image).
   */
  std::vector<Detection> detect(const cv::Mat& image);

  void setConfidenceThreshold(float t) { conf_threshold_ = t; }
  void setIouThreshold(float t) { iou_threshold_ = t; }

 private:
  void preprocess(const cv::Mat& image, cv::Mat& blob);
  std::vector<Detection> postprocess(const float* output, int num_rows, int num_cols,
                                     int orig_w, int orig_h);

  float conf_threshold_{0.4f};  // 0.4 to align with Python (preprocessing/NMS can shift scores slightly)
  float iou_threshold_{0.5f};
  static const int input_h_ = 640;
  static const int input_w_ = 640;
  static const int person_class_id_ = 0;

  struct OrtSession;
  OrtSession* session_{nullptr};
  // Letterbox: scale and pad so we can map box coords from 640x640 back to original image
  float scale_{1.f};
  float pad_left_{0.f}, pad_top_{0.f};
};

#endif
