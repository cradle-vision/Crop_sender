#!/usr/bin/env python3
"""
Utility for scanning IP cameras in local network
Supports RTSP and HTTP/MJPEG cameras
"""

import socket
import argparse
import ipaddress
from concurrent.futures import ThreadPoolExecutor, as_completed
from camera_manager import CameraManager


def check_port(ip, port, timeout=1):
    """Check port availability"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((str(ip), port))
        sock.close()
        return result == 0
    except:
        return False


def scan_network_for_cameras(network="192.168.1.0/24", common_ports=[554, 8080, 8554, 80]):
    """
    Scan network for potential IP cameras
    
    Args:
        network: Network to scan (CIDR notation)
        common_ports: List of ports to check
        
    Returns:
        List of found IP addresses with open ports
    """
    print(f"\n=== Scanning network {network} for IP cameras ===")
    print(f"Checking ports: {common_ports}")
    
    network_obj = ipaddress.ip_network(network, strict=False)
    found_hosts = []
    
    # Multi-threaded scanning
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {}
        
        for host in network_obj.hosts():
            for port in common_ports:
                future = executor.submit(check_port, host, port)
                futures[future] = (host, port)
        
        for future in as_completed(futures):
            host, port = futures[future]
            try:
                if future.result():
                    found_hosts.append((str(host), port))
                    print(f"  ✓ Found host {host} on port {port}")
            except Exception as e:
                pass
    
    print(f"\nFound {len(found_hosts)} potential cameras")
    return found_hosts


def test_rtsp_camera(ip, port=554, path="/stream1", username=None, password=None):
    """Test RTSP camera"""
    import cv2
    
    # Build RTSP URL
    if username and password:
        url = f"rtsp://{username}:{password}@{ip}:{port}{path}"
    else:
        url = f"rtsp://{ip}:{port}{path}"
    
    print(f"  Testing RTSP: {url}...")
    
    try:
        cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
        if not cap.isOpened():
            return False, None
        
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        ret, frame = cap.read()
        cap.release()
        
        if ret and frame is not None:
            return True, frame.shape
        return False, None
    except Exception as e:
        return False, None


def test_http_camera(ip, port=80, path="/mjpeg"):
    """Test HTTP/MJPEG camera"""
    import cv2
    
    url = f"http://{ip}:{port}{path}"
    print(f"  Testing HTTP: {url}...")
    
    try:
        cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
        if not cap.isOpened():
            return False, None
        
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        ret, frame = cap.read()
        cap.release()
        
        if ret and frame is not None:
            return True, frame.shape
        return False, None
    except Exception as e:
        return False, None


def auto_detect_camera(ip, port):
    """Auto-detect camera type"""
    print(f"\nChecking {ip}:{port}...")
    
    # Common RTSP paths
    rtsp_paths = ["/stream1", "/stream", "/h264", "/live", "/cam/realmonitor"]
    # Common HTTP paths
    http_paths = ["/mjpeg", "/video", "/stream", "/cam.mjpg"]
    
    # Test RTSP
    if port == 554 or port == 8554:
        for path in rtsp_paths:
            success, resolution = test_rtsp_camera(ip, port, path)
            if success:
                return {
                    'type': 'rtsp',
                    'ip': ip,
                    'port': port,
                    'path': path,
                    'resolution': resolution
                }
    
    # Test HTTP
    if port == 80 or port == 8080:
        for path in http_paths:
            success, resolution = test_http_camera(ip, port, path)
            if success:
                return {
                    'type': 'http',
                    'ip': ip,
                    'port': port,
                    'path': path,
                    'resolution': resolution
                }
    
    return None


def list_cameras(manager: CameraManager):
    """List all cameras"""
    print("\n=== Camera List ===")
    cameras = manager.get_all_cameras()
    
    if not cameras:
        print("No cameras found")
        return
    
    for cam in cameras:
        status = "✓ Enabled" if cam.enabled else "✗ Disabled"
        print(f"\n{cam.camera_id} - {cam.name} ({status})")
        print(f"  Type: {cam.type}")
        if cam.type == 'usb':
            print(f"  Source: index {cam.source}")
        elif cam.type in ['rtsp', 'http']:
            print(f"  IP: {cam.ip_address}:{cam.port}")
            print(f"  Path: {cam.rtsp_path}")
        print(f"  FPS: {cam.fps}, Resolution: {cam.width}x{cam.height}")


def interactive_add_ip_camera(manager: CameraManager):
    """Interactive IP camera addition"""
    print("\n=== Add IP Camera ===")
    
    camera_id = input("Enter camera ID (e.g., ip_camera_1): ").strip()
    if not camera_id:
        print("Camera ID cannot be empty")
        return
    
    if camera_id in manager.cameras:
        print(f"Camera {camera_id} already exists")
        return
    
    name = input("Enter camera name: ").strip() or camera_id
    
    print("\nCamera type:")
    print("1. RTSP camera")
    print("2. HTTP/MJPEG camera")
    
    choice = input("Select type (1-2): ").strip()
    
    ip = input("Enter IP address: ").strip()
    if not ip:
        print("IP address is required")
        return
    
    if choice == '1':
        port = input("Enter port (default 554): ").strip() or "554"
        path = input("Enter RTSP path (e.g., /stream1): ").strip() or "/stream1"
        username = input("Username (optional, Enter to skip): ").strip() or None
        password = input("Password (optional, Enter to skip): ").strip() or None
        
        try:
            camera = manager.create_rtsp_camera(
                camera_id, name, ip, int(port), path, username, password
            )
        except ValueError:
            print("Invalid port")
            return
    
    elif choice == '2':
        port = input("Enter port (default 80): ").strip() or "80"
        path = input("Enter HTTP path (e.g., /mjpeg): ").strip() or "/mjpeg"
        username = input("Username (optional, Enter to skip): ").strip() or None
        password = input("Password (optional, Enter to skip): ").strip() or None
        
        try:
            camera = manager.create_http_camera(
                camera_id, name, ip, int(port), path, username, password
            )
        except ValueError:
            print("Invalid port")
            return
    
    else:
        print("Invalid choice")
        return
    
    # Test camera
    print("\nTesting camera...")
    if manager.test_camera(camera):
        manager.add_camera(camera)
        print(f"\n✓ Camera {camera_id} added successfully")
    else:
        print(f"\n✗ Failed to connect to camera. Check settings.")


def main():
    parser = argparse.ArgumentParser(description='Utility for scanning and managing IP cameras')
    parser.add_argument('--scan', type=str, metavar='NETWORK', 
                       help='Scan network (e.g., 192.168.1.0/24)')
    parser.add_argument('--add', action='store_true', help='Interactive IP camera addition')
    parser.add_argument('--list', action='store_true', help='Show camera list')
    parser.add_argument('--test', type=str, help='Test camera by ID')
    parser.add_argument('--config', type=str, default='cameras.yaml', help='Config file')
    parser.add_argument('--auto-detect', type=str, metavar='IP:PORT',
                       help='Auto-detect camera type (e.g., 192.168.1.100:554)')
    
    args = parser.parse_args()
    
    manager = CameraManager(config_file=args.config)
    
    if args.scan:
        found = scan_network_for_cameras(args.scan)
        if found:
            print("\nPotential cameras found. Use --auto-detect to verify.")
            for ip, port in found:
                print(f"  {ip}:{port}")
    
    if args.auto_detect:
        try:
            ip, port = args.auto_detect.split(':')
            port = int(port)
            result = auto_detect_camera(ip, port)
            if result:
                print(f"\n✓ Camera detected!")
                print(f"  Type: {result['type']}")
                print(f"  IP: {result['ip']}:{result['port']}")
                print(f"  Path: {result['path']}")
                if result['resolution']:
                    print(f"  Resolution: {result['resolution'][1]}x{result['resolution'][0]}")
            else:
                print(f"\n✗ Failed to detect camera")
        except ValueError:
            print("Invalid format. Use IP:PORT (e.g., 192.168.1.100:554)")
    
    if args.list:
        list_cameras(manager)
    
    if args.add:
        interactive_add_ip_camera(manager)
    
    if args.test:
        camera = manager.get_camera(args.test)
        if camera:
            manager.test_camera(camera)
        else:
            print(f"Camera {args.test} not found")
    
    if not any([args.scan, args.list, args.add, args.test, args.auto_detect]):
        # Interactive mode
        print("=== IP Camera Management Utility ===")
        print("1. Scan network for cameras")
        print("2. Auto-detect camera")
        print("3. Show camera list")
        print("4. Add IP camera")
        print("5. Test camera")
        print("6. Exit")
        
        choice = input("\nSelect action (1-6): ").strip()
        
        if choice == '1':
            network = input("Enter network to scan (e.g., 192.168.1.0/24): ").strip()
            if network:
                found = scan_network_for_cameras(network)
        elif choice == '2':
            ip_port = input("Enter IP:PORT (e.g., 192.168.1.100:554): ").strip()
            if ip_port:
                try:
                    ip, port = ip_port.split(':')
                    port = int(port)
                    result = auto_detect_camera(ip, port)
                    if result:
                        print(f"\n✓ Camera detected: {result}")
                except ValueError:
                    print("Invalid format")
        elif choice == '3':
            list_cameras(manager)
        elif choice == '4':
            interactive_add_ip_camera(manager)
        elif choice == '5':
            camera_id = input("Enter camera ID: ").strip()
            camera = manager.get_camera(camera_id)
            if camera:
                manager.test_camera(camera)
            else:
                print(f"Camera {camera_id} not found")


if __name__ == "__main__":
    main()
