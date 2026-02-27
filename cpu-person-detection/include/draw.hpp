#ifndef DRAW_HPP
#define DRAW_HPP

#include "detector.hpp"
#include <opencv2/core.hpp>

/**
 * Draw detections on image (boxes + label "Person score").
 * Modifies image in place.
 */
void drawDetections(cv::Mat& image, const std::vector<Detection>& detections);

#endif
