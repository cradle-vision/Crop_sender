cmake_minimum_required(VERSION 3.16)
project(cpu_person_detection LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

# ONNX Runtime: prefer system package or env
set(ONNXRUNTIME_ROOT "$ENV{ONNXRUNTIME_ROOT}" CACHE PATH "ONNX Runtime root")
if(NOT ONNXRUNTIME_ROOT)
  set(ONNXRUNTIME_ROOT "/usr" CACHE PATH "ONNX Runtime root")
endif()

# OpenCV
find_package(OpenCV REQUIRED)

# ONNX Runtime
find_path(ONNXRUNTIME_INCLUDE_DIR onnxruntime_cxx_api.h
  HINTS ${ONNXRUNTIME_ROOT}/include)
find_library(ONNXRUNTIME_LIB onnxruntime
  HINTS ${ONNXRUNTIME_ROOT}/lib ${ONNXRUNTIME_ROOT}/lib64)

if(NOT ONNXRUNTIME_INCLUDE_DIR OR NOT ONNXRUNTIME_LIB)
  message(FATAL_ERROR
    "ONNX Runtime not found. Exact steps:\n"
    "  1) cd /tmp\n"
    "  2) wget https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-linux-x64-1.23.2.tgz\n"
    "  3) tar xzf onnxruntime-linux-x64-1.23.2.tgz\n"
    "  4) cd /path/to/cpu_person_detection/build\n"
    "  5) cmake -DONNXRUNTIME_ROOT=/tmp/onnxruntime-linux-x64-1.23.2 ..\n"
    "  6) cmake --build .")
endif()

add_executable(detect_main
  src/main.cpp
  src/detector.cpp
  src/draw.cpp
)

target_include_directories(detect_main PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${OpenCV_INCLUDE_DIRS}
  ${ONNXRUNTIME_INCLUDE_DIR}
)

set(DETECT_MAIN_LIBS ${OpenCV_LIBS} ${ONNXRUNTIME_LIB})
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  list(APPEND DETECT_MAIN_LIBS stdc++fs)
endif()
target_link_libraries(detect_main PRIVATE ${DETECT_MAIN_LIBS})
