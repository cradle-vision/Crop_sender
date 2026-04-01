import sys
import cv2

# Click 2 points for the line, then 1 point for "inside".
# Prints: x1 y1 x2 y2 ix iy
def main():
    if len(sys.argv) < 2:
        print("Usage: python pick_line.py path/to/image.jpg")
        sys.exit(1)

    img_path = sys.argv[1]
    img = cv2.imread(img_path)
    if img is None:
        print(f"Could not read image: {img_path}")
        sys.exit(1)

    pts = []  # [ (x1,y1), (x2,y2), (ix,iy) ]

    def draw():
        vis = img.copy()
        # draw clicked points
        for i, (x, y) in enumerate(pts):
            color = (0, 0, 255) if i == 2 else (0, 255, 0)  # inside point red, line points green
            cv2.circle(vis, (x, y), 5, color, -1)
            cv2.putText(vis, f"p{i+1}", (x + 8, y - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)

        # draw line after 2 points
        if len(pts) >= 2:
            (x1, y1), (x2, y2) = pts[0], pts[1]
            cv2.line(vis, (x1, y1), (x2, y2), (255, 0, 0), 2)  # blue line

        # help text
        msg = "Click: p1 line, p2 line, p3 inside. Press ESC to exit."
        cv2.putText(vis, msg, (10, 25), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        cv2.imshow("pick-line", vis)

    def on_mouse(event, x, y, flags, param):
        if event != cv2.EVENT_LBUTTONDOWN:
            return
        if len(pts) >= 3:
            return

        pts.append((x, y))
        draw()

        if len(pts) == 3:
            (x1, y1), (x2, y2), (ix, iy) = pts
            print("\nValues to use in C++:")
            print(f"--line {x1} {y1} {x2} {y2} --inside_point {ix} {iy}\n")

    cv2.namedWindow("pick-line", cv2.WINDOW_NORMAL)
    cv2.setMouseCallback("pick-line", on_mouse)

    draw()
    while True:
        key = cv2.waitKey(10) & 0xFF
        if key == 27:  # ESC
            break

    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()