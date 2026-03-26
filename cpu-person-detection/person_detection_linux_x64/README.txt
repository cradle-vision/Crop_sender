Person detection (CPU, Linux): input = image, output = bbox and score.

Line filter (keep only detections inside a fixed line):
  ./person_detect /path/to/model.onnx /path/to/image.jpg --draw /path/to/output.jpg \
    --line x1 y1 x2 y2 --inside_point ix iy 0.4 0.5

--line: two points (x1,y1) and (x2,y2) defining the boundary line in image pixels
--inside_point: a point you know is inside; detections are kept if bbox bottom-center is on the same side

In Sender integration these values should come from cameras.yaml:
  line_x1, line_y1, line_x2, line_y2, inside_x, inside_y, line_active
