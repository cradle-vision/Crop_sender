#!/usr/bin/env python3
"""
Crop detected persons from image using person_detect.
Usage: python3 crop_persons.py <model.onnx> <input_image> <output_dir> [conf]
Example: python3 crop_persons.py models/person_detection_model.onnx input/image12.png output/crops 0.5
"""
import subprocess
import re
import sys
from pathlib import Path

try:
    from PIL import Image
except ImportError:
    print("Install: pip install Pillow")
    sys.exit(1)

def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    model = sys.argv[1]
    input_img = sys.argv[2]
    output_dir = Path(sys.argv[3])
    conf = float(sys.argv[4]) if len(sys.argv) > 4 else 0.5

    script_dir = Path(__file__).resolve().parent
    person_detect = script_dir / "person_detection_linux_x64" / "person_detect"
    if not person_detect.exists():
        person_detect = script_dir / "person_detect"

    model_path = (script_dir / model).resolve() if not Path(model).is_absolute() else Path(model)
    input_path = (script_dir / input_img).resolve() if not Path(input_img).is_absolute() else Path(input_img)
    if not input_path.exists():
        input_path = Path(input_img)
    if not model_path.exists():
        model_path = Path(model)

    out = subprocess.run(
        [str(person_detect), str(model_path), str(input_path)],
        capture_output=True, text=True, cwd=str(person_detect.parent)
    )
    if out.returncode != 0:
        print(out.stderr or out.stdout)
        sys.exit(1)

    pattern = re.compile(r'bbox \(x1,y1,x2,y2\)=\((\d+),(\d+),(\d+),(\d+)\) score=([\d.e+-]+)')
    img = Image.open(str(input_path)).convert("RGB")
    w, h = img.size
    output_dir.mkdir(parents=True, exist_ok=True)
    base = input_path.stem
    ext = input_path.suffix.lstrip(".") or "png"

    saved = 0
    for m in pattern.finditer(out.stdout):
        x1, y1, x2, y2 = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
        score = float(m.group(5))
        if score < conf:
            continue
        x1 = max(0, min(x1, w - 1))
        y1 = max(0, min(y1, h - 1))
        x2 = max(0, min(x2, w))
        y2 = max(0, min(y2, h))
        if x2 <= x1 or y2 <= y1:
            continue
        crop = img.crop((x1, y1, x2, y2))
        out_path = output_dir / f"{base}_person_{saved}.{ext}"
        crop.save(str(out_path))
        saved += 1

    print(f"[crop_persons] Cropped {saved} person(s) to {output_dir}")

if __name__ == "__main__":
    main()
