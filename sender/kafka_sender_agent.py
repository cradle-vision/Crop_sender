"""
Kafka Sender Agent
Publishes crop snapshots to Kafka: either JSON with image_base64, or (if MinIO enabled) upload to MinIO and send only metadata (bucket, object_key).
Backend consumes and sends to Triton.
"""

import base64
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
    """Agent for publishing snapshots to Kafka (optionally via MinIO)."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "snapshots",
                 jpeg_quality: int = 85, minio_config: Optional[Dict[str, Any]] = None):
        """
        Args:
            bootstrap_servers: Kafka brokers
            topic: Topic name for crop messages
            jpeg_quality: JPEG encoding quality (1-100)
            minio_config: If set and enabled, upload crop to MinIO and put only bucket/object_key in Kafka.
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

    def connect(self) -> bool:
        """Create Kafka producer and optionally MinIO client."""
        if not HAS_KAFKA:
            print("[Kafka Sender Agent] ✗ confluent_kafka not installed. pip install confluent-kafka")
            return False
        try:
            self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            self._connected = True
            if self._use_minio:
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
                print(f"[Kafka Sender Agent] ✓ Kafka + MinIO: bucket={self._minio_bucket}")
            else:
                print(f"[Kafka Sender Agent] ✓ Producer ready: {self.bootstrap_servers}, topic={self.topic}")
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
        Encode to JPEG; if MinIO enabled upload to bucket and publish metadata only, else publish base64 in Kafka.
        """
        if not self._connected or not self._producer:
            return False
        camera_id = str(camera_id or "unknown").strip()
        camera_id = "".join(c for c in camera_id if c.isprintable() or c.isspace()).strip() or "unknown"
        try:
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), self.jpeg_quality]
            success, buf = cv2.imencode(".jpg", frame, encode_param)
            if not success:
                print("[Kafka Sender Agent] Image encode error")
                return False
            data = buf.tobytes()
            ts_ms = int(timestamp * 1000)

            if self._use_minio and self._minio_client:
                object_name = f"crops/{camera_id}/{ts_ms}_{uuid.uuid4().hex[:8]}.jpg"
                self._minio_client.put_object(
                    self._minio_bucket,
                    object_name,
                    data=data,
                    length=len(data),
                    content_type="image/jpeg",
                )
                payload = {
                    "camera_id": camera_id,
                    "timestamp": ts_ms,
                    "format": "jpeg",
                    "bucket": self._minio_bucket,
                    "object_key": object_name,
                }
            else:
                image_base64 = base64.b64encode(data).decode("ascii")
                payload = {
                    "camera_id": camera_id,
                    "timestamp": ts_ms,
                    "format": "jpeg",
                    "image_base64": image_base64,
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
