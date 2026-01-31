#include "draw.hpp"
#include <opencv2/imgproc.hpp>

void drawDetections(cv::Mat& image, const std::vector<Detection>& detections) {
  const cv::Scalar color(0, 255, 0);  // BGR green
  const int thickness = 2;
  const double font_scale = 0.5;
  const int font_thickness = 1;

  for (const auto& d : detections) {
    cv::Point pt1(static_cast<int>(d.x1), static_cast<int>(d.y1));
    cv::Point pt2(static_cast<int>(d.x2), static_cast<int>(d.y2));
    cv::rectangle(image, pt1, pt2, color, thickness);

    char label[64];
    snprintf(label, sizeof(label), "Person %.2f", d.score);
    int baseline = 0;
    cv::Size text_size = cv::getTextSize(label, cv::FONT_HERSHEY_SIMPLEX, font_scale, font_thickness, &baseline);
    cv::Point text_org(pt1.x, pt1.y - 5);
    if (text_org.y < text_size.height)
      text_org.y = pt1.y + text_size.height;
    cv::putText(image, label, text_org, cv::FONT_HERSHEY_SIMPLEX, font_scale, color, font_thickness);
  }
}
