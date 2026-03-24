import json
import uuid
import io
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

    def send_snapshot(
        self,
        frame: np.ndarray,
        timestamp: float,
        camera_id: str = "camera_0",
        company_id: Optional[str] = None,
        building_id: Optional[str] = None,
        company_name: Optional[str] = None,
        building_name: Optional[str] = None,
        camera_name: Optional[str] = None,
    ) -> bool:
        """
        Flow: JPEG encode → MinIO.put_object() → Kafka.send(metadata + object_key).
        company_id/building_id are optional and used only for MinIO path / metadata if provided.
        """
        if not self._connected or not self._producer or not self._minio_client:
            return False

        def _sanitize(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            value = str(value or "").strip()
            value = "".join(c for c in value if c.isprintable() or c.isspace()).strip()
            return value or None

        def _slug(value: Optional[str]) -> Optional[str]:
            """Create filesystem-friendly slug from name (lowercase, spaces->-, alnum/_/- only)."""
            value = _sanitize(value)
            if value is None:
                return None
            value = value.lower().replace(" ", "-")
            value = "".join(c for c in value if c.isalnum() or c in "-_")
            return value or None

        camera_id = _sanitize(camera_id) or "unknown"
        company_id = _sanitize(company_id)
        building_id = _sanitize(building_id)
        company_name = _sanitize(company_name)
        building_name = _sanitize(building_name)
        camera_name = _sanitize(camera_name)
        try:
            # 1. JPEG encode (фиксированное максимальное качество; не зависит от KAFKA_JPEG_QUALITY)
            encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 100]
            success, buf = cv2.imencode(".jpg", frame, encode_param)
            if not success:
                print("[Kafka Sender Agent] Image encode error")
                return False
            data = buf.tobytes()
            ts_ms = int(timestamp * 1000)

            # 2. MinIO.put_object()
            # Path pattern (по твоему запросу):
            #   crops/<camera_name>/<building_id>/<camera_id>/<timestamp_uuid>.jpg
            # company_* остаются только в метаданных Kafka.
            path_parts = ["crops"]

            # 1) camera_name (slug, если есть)
            cam_name_slug = _slug(camera_name)
            if cam_name_slug:
                path_parts.append(cam_name_slug)

            # 2) building_id (как строка), если есть
            if building_id:
                path_parts.append(str(building_id))

            # 3) camera_id (обязательный сегмент)
            path_parts.append(camera_id)
            prefix = "/".join(path_parts)
            object_name = f"{prefix}/{ts_ms}_{uuid.uuid4().hex[:8]}.jpg"
            data_stream = io.BytesIO(data)
            self._minio_client.put_object(
                self._minio_bucket,
                object_name,
                data=data_stream,
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
            if company_id:
                payload["company_id"] = company_id
            if building_id:
                payload["building_id"] = building_id
            if company_name:
                payload["company_name"] = company_name
            if building_name:
                payload["building_name"] = building_name
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
