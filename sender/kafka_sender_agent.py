"""
Kafka Sender Agent
Flow: Camera → Detection (crop) → JPEG encode → MinIO.put_object() → Kafka.send(metadata + object_key).
Uploads crop to MinIO, then publishes only metadata (bucket, object_key) to Kafka. Backend consumes and sends to Triton.
"""

import json
import uuid
import cv2
import numpy as np
from typing import Optional, Dict, Any

try:
    from confluent_kafka import Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

try:
    from minio import Minio
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False


class KafkaSenderAgent:
    """Agent: crop → JPEG encode → MinIO.put_object() → Kafka.send(metadata + object_key)."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "snapshots",
                 jpeg_quality: int = 85, minio_config: Optional[Dict[str, Any]] = None):
        """
        Args:
            bootstrap_servers: Kafka brokers (host:port; http:// is stripped automatically)
            topic: Topic name for crop messages
            jpeg_quality: JPEG encoding quality (1-100)
            minio_config: Required. Upload crop to MinIO, send only bucket/object_key in Kafka.
                Keys: enabled, endpoint, bucket, access_key, secret_key, secure (bool)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.jpeg_quality = jpeg_quality
        self._minio_config = minio_config or {}
        self._use_minio = bool(self._minio_config.get("enabled")) and HAS_MINIO
        self._minio_client: Optional[Any] = None
        self._minio_bucket = self._minio_config.get("bucket", "crops")
        self._producer: Optional[Producer] = None
        self._connected = False
        # Kafka expects "host:port", not "http://host:port"
        if self.bootstrap_servers:
            self.bootstrap_servers = str(self.bootstrap_servers).replace("http://", "").replace("https://", "").rstrip("/")

    def connect(self) -> bool:
        """Create Kafka producer and MinIO client. Flow requires MinIO (crop → MinIO → Kafka metadata)."""
        if not HAS_KAFKA:
            print("[Kafka Sender Agent] ✗ confluent_kafka not installed. pip install confluent-kafka")
            return False
        if not self._use_minio:
            print("[Kafka Sender Agent] ✗ MinIO is required. Set minio.enabled: true in config (flow: crop → MinIO → Kafka).")
            return False
        try:
            self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            endpoint = self._minio_config.get("endpoint", "localhost:9000").replace("http://", "").replace("https://", "").rstrip("/")
            secure = self._minio_config.get("secure", False)
            self._minio_client = Minio(
                endpoint,
                access_key=self._minio_config.get("access_key", "minioadmin"),
                secret_key=self._minio_config.get("secret_key", "minioadmin"),
                secure=secure,
            )
            if not self._minio_client.bucket_exists(self._minio_bucket):
                self._minio_client.make_bucket(self._minio_bucket)
            self._connected = True
            print(f"[Kafka Sender Agent] ✓ Flow: JPEG → MinIO → Kafka. bucket={self._minio_bucket}, topic={self.topic}")
            return True
        except Exception as e:
            print(f"[Kafka Sender Agent] ✗ Failed to connect: {e}")
            self._connected = False
            return False

    def disconnect(self) -> None:
        """Flush and close producer."""
        if self._producer:
            try:
                self._producer.flush(timeout=10)
            except Exception:
                pass
            self._producer = None
        self._minio_client = None
        self._connected = False
        print("[Kafka Sender Agent] Disconnected")

    def send_snapshot(self, frame: np.ndarray, timestamp: float, camera_id: str = "camera_0") -> bool:
        """
        Flow: JPEG encode → MinIO.put_object() → Kafka.send(metadata + object_key).
        """
        if not self._connected or not self._producer or not self._minio_client:
            return False
        camera_id = str(camera_id or "unknown").strip()
        camera_id = "".join(c for c in camera_id if c.isprintable() or c.isspace()).strip() or "unknown"
        try:
            # 1. JPEG encode
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), self.jpeg_quality]
            success, buf = cv2.imencode(".jpg", frame, encode_param)
            if not success:
                print("[Kafka Sender Agent] Image encode error")
                return False
            data = buf.tobytes()
            ts_ms = int(timestamp * 1000)

            # 2. MinIO.put_object()
            object_name = f"crops/{camera_id}/{ts_ms}_{uuid.uuid4().hex[:8]}.jpg"
            self._minio_client.put_object(
                self._minio_bucket,
                object_name,
                data=data,
                length=len(data),
                content_type="image/jpeg",
            )

            # 3. Kafka.send(metadata + object_key)
            payload = {
                "camera_id": camera_id,
                "timestamp": ts_ms,
                "format": "jpeg",
                "bucket": self._minio_bucket,
                "object_key": object_name,
            }
            value = json.dumps(payload).encode("utf-8")
            self._producer.produce(self.topic, value=value, key=camera_id.encode("utf-8"))
            self._producer.poll(0)
            if not hasattr(self, "_sent_count"):
                self._sent_count = 0
            self._sent_count += 1
            if self._sent_count % 50 == 0:
                print(f"[Kafka Sender Agent] Published {self._sent_count} messages")
            return True
        except Exception as e:
            print(f"[Kafka Sender Agent] Publish error: {e}")
            return False
