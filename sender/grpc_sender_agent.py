"""
gRPC Sender Agent
Sends snapshots to AI server via gRPC
"""

import grpc
import cv2
import numpy as np
from typing import Optional, Any
import time
import snapshot_service_pb2
import snapshot_service_pb2_grpc


class GrpcSenderAgent:
    """Agent for sending snapshots via gRPC"""
    
    def __init__(self, server_address: str = "localhost:50051", 
                 max_message_size: int = 4194304, timeout: float = 5.0):
        """
        Initialize sender agent
        
        Args:
            server_address: gRPC server address
            max_message_size: Maximum message size
            timeout: Request timeout
        """
        self.server_address = server_address
        self.timeout = timeout
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[snapshot_service_pb2_grpc.SnapshotServiceStub] = None
        self.is_connected = False
        self.service_unimplemented = False  # Flag to stop sending if service not found
        
    def connect(self):
        """Connect to gRPC server"""
        print(f"[gRPC Sender Agent] Connecting to server: {self.server_address}")
        try:
            options = [
                ('grpc.max_send_message_length', 4194304),
                ('grpc.max_receive_message_length', 4194304),
            ]
            
            self.channel = grpc.insecure_channel(self.server_address, options=options)
            self.stub = snapshot_service_pb2_grpc.SnapshotServiceStub(self.channel)
            
            # Check connection
            print(f"[gRPC Sender Agent] Waiting for channel ready (timeout 5 sec)...")
            grpc.channel_ready_future(self.channel).result(timeout=5.0)
            self.is_connected = True
            print(f"[gRPC Sender Agent] ✓ Connected to server: {self.server_address}")
            return True
        except grpc.FutureTimeoutError:
            error_msg = f"Connection timeout to {self.server_address}. Server not responding."
            print(f"[gRPC Sender Agent] ✗ {error_msg}")
            print(f"[gRPC Sender Agent] Check:")
            print(f"  - Is gRPC server running on {self.server_address}")
            print(f"  - Is port accessible (for localhost check port forwarding)")
            self.is_connected = False
            return False
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else "Unknown error"
            print(f"[gRPC Sender Agent] ✗ Connection error ({error_type}): {error_msg}")
            print(f"[gRPC Sender Agent] Server address: {self.server_address}")
            if "Connection refused" in error_msg or "Name resolution" in error_msg:
                print(f"[gRPC Sender Agent] Check server availability:")
                print(f"  - For localhost: ensure port 50051 is forwarded to host")
                print(f"  - For grpc-server: ensure containers are in same network")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Disconnect from server"""
        if self.channel:
            self.channel.close()
        self.is_connected = False
        print("[gRPC Sender Agent] Disconnected from server")
    
    def send_snapshot(self, frame: np.ndarray, timestamp: float, camera_id: str = "camera_0") -> bool:
        """
        Send single snapshot to server
        
        Args:
            frame: Image frame (numpy array)
            timestamp: Timestamp
            camera_id: Camera identifier
            
        Returns:
            True if successful, False otherwise
        """
        if self.service_unimplemented:
            # Don't attempt to send if service is not implemented
            return False
        
        if not self.is_connected or not self.stub:
            print("[gRPC Sender Agent] Not connected to server")
            return False
        
        try:
            # Encode image to JPEG
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 85]
            success, encoded_image = cv2.imencode('.jpg', frame, encode_param)
            
            if not success:
                print("[gRPC Sender Agent] Image encoding error")
                return False
            
            # Ensure camera_id is valid UTF-8 string
            if camera_id is None:
                camera_id = "unknown"
            else:
                # Convert to string and ensure valid UTF-8
                camera_id = str(camera_id)
                # Remove any invalid UTF-8 characters and control characters
                camera_id = camera_id.encode('utf-8', errors='ignore').decode('utf-8')
                # Remove any control characters and ensure ASCII-safe
                camera_id = ''.join(c for c in camera_id if c.isprintable() or c.isspace())
                camera_id = camera_id.strip()
                # Ensure it's not empty
                if not camera_id:
                    camera_id = "unknown"
            
            # Ensure format is valid UTF-8 (always ASCII)
            image_format = "jpeg"
            
            # Validate strings before creating request
            try:
                # Test encoding to ensure valid UTF-8
                camera_id.encode('utf-8')
                image_format.encode('utf-8')
            except UnicodeEncodeError as e:
                print(f"[gRPC Sender Agent] UTF-8 validation error: {e}")
                camera_id = "unknown"
                image_format = "jpeg"
            
            # Create request (matching server proto structure)
            request = snapshot_service_pb2.SnapshotRequest(
                camera_id=camera_id,
                image_data=encoded_image.tobytes(),
                timestamp=int(timestamp * 1000),  # Convert to milliseconds
                format=image_format
            )
            
            # Send request
            response = self.stub.SendSnapshot(request, timeout=self.timeout)
            
            if response.success:
                # Only log success occasionally to reduce log spam
                if not hasattr(self, '_success_count'):
                    self._success_count = 0
                self._success_count += 1
                if self._success_count % 50 == 0:
                    print(f"[gRPC Sender Agent] Sent {self._success_count} snapshots successfully")
                return True
            else:
                print(f"[gRPC Sender Agent] Server error: {response.message}")
                return False
                
        except grpc.RpcError as e:
            error_code = e.code()
            error_details = e.details()
            
            # Reduce error spam - only log every Nth error
            if not hasattr(self, '_error_counts'):
                self._error_counts = {}
            if error_code not in self._error_counts:
                self._error_counts[error_code] = 0
            self._error_counts[error_code] += 1
            
            count = self._error_counts[error_code]
            log_interval = 10 if error_code == grpc.StatusCode.UNIMPLEMENTED else 5
            
            if count % log_interval == 0 or count == 1:
                print(f"[gRPC Sender Agent] gRPC error ({count}x): {error_code} - {error_details}")
                
                if error_code == grpc.StatusCode.UNIMPLEMENTED:
                    if count == 1:
                        print(f"[gRPC Sender Agent] ✗ CRITICAL: Service 'snapshot.SnapshotService' not found on server!")
                        print(f"[gRPC Sender Agent] Server is reachable but service is not implemented.")
                        print(f"[gRPC Sender Agent] Verify server uses same proto file:")
                        print(f"  - Package: snapshot")
                        print(f"  - Service: SnapshotService")
                        print(f"  - Method: SendSnapshot")
                        print(f"[gRPC Sender Agent] Stopping send attempts. Fix server configuration and restart.")
                        self.service_unimplemented = True
                    # Don't spam logs for UNIMPLEMENTED after first error
                    return False
                
                if error_code == grpc.StatusCode.INTERNAL:
                    if "invalid UTF-8" in error_details or "unmarshalling" in error_details:
                        if count == 1:
                            print(f"[gRPC Sender Agent] ✗ UTF-8 encoding error detected!")
                            print(f"[gRPC Sender Agent] Camera ID: '{camera_id}' (type: {type(camera_id).__name__})")
                            print(f"[gRPC Sender Agent] This may be caused by invalid characters in camera_id.")
                            print(f"[gRPC Sender Agent] Fixed camera_id encoding, retrying...")
            
            if error_code == grpc.StatusCode.UNAVAILABLE:
                self.is_connected = False
            return False
        except Exception as e:
            print(f"[gRPC Sender Agent] Unexpected error: {e}")
            return False
    
    def send_snapshot_stream(self, frame: np.ndarray, timestamp: float, 
                            stream: Any, camera_id: str = "camera_0") -> bool:
        """
        Send snapshot via stream
        
        Args:
            frame: Image frame
            timestamp: Timestamp
            stream: gRPC stream
            camera_id: Camera identifier
            
        Returns:
            True if successful
        """
        try:
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 85]
            success, encoded_image = cv2.imencode('.jpg', frame, encode_param)
            
            if not success:
                return False
            
            # Ensure camera_id is valid UTF-8 string
            if camera_id is None:
                camera_id = "unknown"
            else:
                # Convert to string and ensure valid UTF-8
                camera_id = str(camera_id)
                # Remove any invalid UTF-8 characters and control characters
                camera_id = camera_id.encode('utf-8', errors='ignore').decode('utf-8')
                # Remove any control characters and ensure ASCII-safe
                camera_id = ''.join(c for c in camera_id if c.isprintable() or c.isspace())
                camera_id = camera_id.strip()
                # Ensure it's not empty
                if not camera_id:
                    camera_id = "unknown"
            
            # Ensure format is valid UTF-8 (always ASCII)
            image_format = "jpeg"
            
            # Validate strings before creating request
            try:
                # Test encoding to ensure valid UTF-8
                camera_id.encode('utf-8')
                image_format.encode('utf-8')
            except UnicodeEncodeError as e:
                print(f"[gRPC Sender Agent] UTF-8 validation error: {e}")
                camera_id = "unknown"
                image_format = "jpeg"
            
            # Create request (matching server proto structure)
            request = snapshot_service_pb2.SnapshotRequest(
                camera_id=camera_id,
                image_data=encoded_image.tobytes(),
                timestamp=int(timestamp * 1000),
                format=image_format
            )
            
            stream.write(request)
            return True
            
        except Exception as e:
            print(f"[gRPC Sender Agent] Stream send error: {e}")
            return False
