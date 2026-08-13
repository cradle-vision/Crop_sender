import json
import os
import threading
import time
import uuid
import io
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, Tuple, List, Callable, Set

import yaml

from jpeg_utils import encode_jpeg_bgr

try:
    from confluent_kafka import Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

try:
    from minio import Minio
    import urllib3
    HAS_MINIO = True
except ImportError:
    HAS_MINIO = False
    urllib3 = None  # type: ignore[assignment,misc]


class KafkaSenderAgent:
    """Agent: crop → JPEG encode → MinIO.put_object() → Kafka.send(metadata + object_key).

    When Kafka is unreachable, metadata is queued on disk (FIFO) and replayed automatically
    when the broker recovers. Original detection timestamps are preserved on replay.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "snapshots",
        jpeg_quality: int = 100,
        minio_config: Optional[Dict[str, Any]] = None,
        resilience_config: Optional[Dict[str, Any]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.jpeg_quality = jpeg_quality
        self._minio_config = minio_config or {}
        self._use_minio = bool(self._minio_config.get("enabled")) and HAS_MINIO
        self._minio_client: Optional[Any] = None
        self._minio_bucket = self._minio_config.get("bucket", "crops")

        rc = resilience_config or {}
        self._reconnect_sec = float(rc.get("reconnect_sec", 15))
        self._offline_log_sec = float(rc.get("offline_log_sec", 60))
        self._broker_check_sec = float(rc.get("broker_check_sec", 10))
        self._delivery_timeout_sec = float(rc.get("delivery_timeout_sec", 10))
        self._minio_connect_timeout_sec = float(rc.get("minio_connect_timeout_sec", 10))
        self._minio_read_timeout_sec = float(rc.get("minio_read_timeout_sec", 60))
        self._buffer_enabled = bool(rc.get("buffer_enabled", True))
        self._buffer_max_items = int(rc.get("buffer_max_items", 340000))
        self._buffer_dir = str(rc.get("buffer_dir", "/app/config/kafka_buffer"))
        self._buffer_replay_batch = int(rc.get("buffer_replay_batch", 1000))
        self._buffer_replay_max_rounds = int(rc.get("buffer_replay_max_rounds", 30))
        self._drain_poll_sec = float(rc.get("drain_poll_sec", 2))
        self._buffer_max_bytes = rc.get("buffer_max_bytes")
        if self._buffer_max_bytes is not None:
            self._buffer_max_bytes = int(self._buffer_max_bytes)
        self._local_spill_enabled = bool(rc.get("local_spill_enabled", True))
        self._local_spill_dir = str(rc.get("local_spill_dir", "/app/config/kafka_spill"))
        self._drain_backlog_before_live = bool(rc.get("drain_backlog_before_live", True))
        self._replay_minio_workers = int(rc.get("replay_minio_workers", 8))
        self._replay_prefetch_ahead = int(rc.get("replay_prefetch_ahead", 32))
        self._replay_minio_retries = int(rc.get("replay_minio_retries", 3))

        if self.bootstrap_servers:
            self.bootstrap_servers = (
                str(self.bootstrap_servers).replace("http://", "").replace("https://", "").rstrip("/")
            )

        self._producer: Optional[Producer] = None
        self._minio_connected = False
        self._kafka_healthy = False
        self._kafka_lock = threading.Lock()
        self._buffer_lock = threading.Lock()
        self._buffer_seq = 0
        self._buffer_enqueue_count = 0
        self._sent_count = 0
        self._reconnect_thread: Optional[threading.Thread] = None
        self._reconnect_stop = threading.Event()
        self._last_offline_log = 0.0
        self._kafka_unhealthy_reason = ""
        self._was_draining_backlog = False
        self._camera_prefix_cache: Dict[str, str] = {}
        self._camera_prefix_cache_mtime: float = 0.0

    def _producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "socket.timeout.ms": 30000,
            "metadata.max.age.ms": 300000,
            "message.timeout.ms": 120000,
            "acks": "1",
            "retries": 5,
            "retry.backoff.ms": 500,
        }

    def connect(self) -> bool:
        """Connect MinIO (required unless local spill) and attempt Kafka (optional at boot)."""
        if not HAS_KAFKA:
            print("[Kafka Sender Agent] ✗ confluent_kafka not installed. pip install confluent-kafka")
            return False
        if not self._use_minio and not self._local_spill_enabled:
            print(
                "[Kafka Sender Agent] ✗ MinIO is required (or enable local spill). "
                "Set minio.enabled: true in config."
            )
            return False

        if self._buffer_enabled:
            self._ensure_buffer_dir()
            self._init_buffer_seq()
            pending = self._buffer_count()
            spill = self._spill_jpg_count()
            if pending or spill:
                print(
                    f"[Kafka Sender Agent] Found backlog from previous run "
                    f"({pending} buffered, {spill} spill file(s))"
                )
                self._was_draining_backlog = True

        minio_ok = self._connect_minio()
        if not minio_ok and not self._local_spill_enabled:
            print("[Kafka Sender Agent] ✗ MinIO unreachable and local spill disabled")
            return False

        if self._connect_kafka():
            self._mark_kafka_healthy()
            print(
                f"[Kafka Sender Agent] ✓ Flow: JPEG → MinIO → Kafka. "
                f"bucket={self._minio_bucket}, topic={self.topic}"
            )
        else:
            self._mark_kafka_unhealthy("broker unreachable at startup")
            print(
                f"[Kafka Sender Agent] Kafka offline at startup (capture continues). "
                f"MinIO={'ok' if minio_ok else 'offline/spill'}. topic={self.topic}"
            )
        return True

    def _minio_http_client(self, maxsize: int = 10) -> Any:
        if urllib3 is None:
            raise RuntimeError("urllib3 required for MinIO timeouts")
        return urllib3.PoolManager(
            timeout=urllib3.Timeout(
                connect=self._minio_connect_timeout_sec,
                read=self._minio_read_timeout_sec,
            ),
            maxsize=max(4, maxsize),
            cert_reqs="CERT_REQUIRED" if self._minio_config.get("secure") else "CERT_NONE",
        )

    def _create_minio_client(self, pool_size: Optional[int] = None) -> Any:
        endpoint = (
            self._minio_config.get("endpoint", "localhost:9000")
            .replace("http://", "")
            .replace("https://", "")
            .rstrip("/")
        )
        secure = self._minio_config.get("secure", False)
        http_max = pool_size if pool_size is not None else max(10, self._replay_minio_workers + 2)
        client = Minio(
            endpoint,
            access_key=self._minio_config.get("access_key", "minioadmin"),
            secret_key=self._minio_config.get("secret_key", "minioadmin"),
            secure=secure,
            http_client=self._minio_http_client(maxsize=http_max),
        )
        if not client.bucket_exists(self._minio_bucket):
            client.make_bucket(self._minio_bucket)
        return client

    def _connect_minio(self) -> bool:
        if not self._use_minio:
            return False
        try:
            self._minio_client = self._create_minio_client()
            self._minio_connected = True
            return True
        except Exception as e:
            print(f"[Kafka Sender Agent] MinIO connect failed: {e}")
            self._minio_client = None
            self._minio_connected = False
            return False

    def _connect_kafka(self) -> bool:
        if not HAS_KAFKA:
            return False
        try:
            with self._kafka_lock:
                if self._producer is None:
                    self._producer = Producer(self._producer_config())
                self._producer.list_topics(timeout=self._broker_check_sec)
            return True
        except Exception as e:
            with self._kafka_lock:
                self._producer = None
            print(f"[Kafka Sender Agent] Kafka connect/check failed: {e}")
            return False

    def _spill_jpg_count(self) -> int:
        if not self._local_spill_enabled or not os.path.isdir(self._local_spill_dir):
            return 0
        total = 0
        for root, _dirs, files in os.walk(self._local_spill_dir):
            total += sum(
                1 for name in files if name.endswith(".jpg") and not name.endswith(".tmp")
            )
        return total

    def _has_backlog(self) -> bool:
        return self._buffer_count() > 0 or self._spill_jpg_count() > 0

    def _should_store_locally(self) -> bool:
        """Keep new crops on disk while replay queue is non-empty (FIFO drain before live)."""
        return self._drain_backlog_before_live and self._has_backlog()

    def _check_backlog_drained(self) -> None:
        draining = self._should_store_locally()
        if self._was_draining_backlog and not draining:
            print("[Kafka Sender Agent] Backlog drained — live publishing resumed")
        self._was_draining_backlog = draining

    def _mark_kafka_healthy(self) -> None:
        with self._kafka_lock:
            was_unhealthy = not self._kafka_healthy
            self._kafka_healthy = True
            self._kafka_unhealthy_reason = ""
        if was_unhealthy:
            if self._should_store_locally():
                pending = self._buffer_count()
                spill = self._spill_jpg_count()
                print(
                    "[Kafka Sender Agent] Kafka recovered — draining backlog before live "
                    f"({pending} buffered, {spill} spill file(s))"
                )
                self._was_draining_backlog = True
            else:
                print("[Kafka Sender Agent] Kafka recovered — publishing resumed")

    def _mark_kafka_unhealthy(self, reason: str) -> None:
        with self._kafka_lock:
            was_healthy = self._kafka_healthy
            self._kafka_healthy = False
            self._kafka_unhealthy_reason = reason or "unknown"
            self._producer = None
        if was_healthy:
            print(f"[Kafka Sender Agent] Kafka offline: {reason} (capture continues)")

    def start_auto_reconnect(self) -> None:
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            return
        self._reconnect_stop.clear()
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop,
            name="kafka-reconnect",
            daemon=True,
        )
        self._reconnect_thread.start()
        print(
            f"[Kafka Sender Agent] Auto-reconnect started "
            f"(retry every {self._reconnect_sec:.0f}s)"
        )

    def stop_auto_reconnect(self) -> None:
        self._reconnect_stop.set()
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            self._reconnect_thread.join(timeout=5)

    def _drain_buffer_when_healthy(self) -> None:
        """Replay multiple batches per wake cycle while Kafka is up."""
        with self._kafka_lock:
            if self._producer:
                self._producer.list_topics(timeout=self._broker_check_sec)

        rounds = 0
        while self._kafka_healthy and self._has_backlog() and rounds < self._buffer_replay_max_rounds:
            before = self._buffer_count()
            self._replay_buffer()
            rounds += 1
            if self._buffer_count() >= before:
                break

    def _reconnect_loop(self) -> None:
        while not self._reconnect_stop.is_set():
            try:
                if self._use_minio and not self._minio_connected:
                    self._connect_minio()
                if self._use_minio and self._minio_connected:
                    self._replay_orphan_spill_batch()

                if self._kafka_healthy:
                    try:
                        self._drain_buffer_when_healthy()
                    except Exception as e:
                        self._mark_kafka_unhealthy(str(e))
                else:
                    now = time.time()
                    if now - self._last_offline_log >= self._offline_log_sec:
                        pending = self._buffer_count()
                        print(
                            f"[Kafka Sender Agent] Kafka still offline; retrying every "
                            f"{self._reconnect_sec:.0f}s "
                            f"({pending} buffered)"
                        )
                        self._last_offline_log = now
                    if self._connect_kafka():
                        self._mark_kafka_healthy()
                        try:
                            self._drain_buffer_when_healthy()
                        except Exception as e:
                            self._mark_kafka_unhealthy(str(e))
            except Exception as e:
                print(f"[Kafka Sender Agent] Reconnect loop error: {e}")

            sleep_sec = self._drain_poll_sec if self._has_backlog() else self._reconnect_sec
            self._reconnect_stop.wait(sleep_sec)

    def disconnect(self) -> None:
        self.stop_auto_reconnect()
        with self._kafka_lock:
            if self._producer:
                try:
                    self._producer.flush(timeout=10)
                except Exception:
                    pass
                self._producer = None
        self._minio_client = None
        self._minio_connected = False
        self._kafka_healthy = False
        print("[Kafka Sender Agent] Disconnected")

    def _ensure_buffer_dir(self) -> None:
        os.makedirs(self._buffer_dir, exist_ok=True)
        if self._local_spill_enabled:
            os.makedirs(self._local_spill_dir, exist_ok=True)

    def _list_buffer_files(self) -> List[str]:
        if not os.path.isdir(self._buffer_dir):
            return []
        names = [
            n for n in os.listdir(self._buffer_dir)
            if n.endswith(".json") and not n.endswith(".tmp")
        ]
        names.sort()
        return [os.path.join(self._buffer_dir, n) for n in names]

    def _buffer_count(self) -> int:
        return len(self._list_buffer_files())

    def _buffer_bytes(self) -> int:
        total = 0
        for path in self._list_buffer_files():
            try:
                total += os.path.getsize(path)
            except OSError:
                pass
        if self._local_spill_enabled and os.path.isdir(self._local_spill_dir):
            for root, _dirs, files in os.walk(self._local_spill_dir):
                for name in files:
                    try:
                        total += os.path.getsize(os.path.join(root, name))
                    except OSError:
                        pass
        return total

    def _init_buffer_seq(self) -> None:
        files = self._list_buffer_files()
        if not files:
            self._buffer_seq = 0
            return
        last_name = os.path.basename(files[-1])
        try:
            self._buffer_seq = int(last_name.split("_", 1)[0])
        except (ValueError, IndexError):
            self._buffer_seq = len(files)

    def _next_buffer_seq(self) -> int:
        self._buffer_seq += 1
        return self._buffer_seq

    def _discard_corrupt_buffer_file(self, path: str, reason: str) -> None:
        name = os.path.basename(path)
        try:
            os.remove(path)
            print(f"[Kafka Sender Agent] Dropped corrupt buffer file {name}: {reason}")
        except OSError as e:
            print(f"[Kafka Sender Agent] Failed to drop corrupt buffer file {name}: {e}")

    def _read_buffer_record(self, path: str) -> Optional[Tuple[dict, str]]:
        """Load one buffer JSON; discard and return None when file is corrupt."""
        try:
            if os.path.getsize(path) <= 0:
                self._discard_corrupt_buffer_file(path, "empty file")
                return None
            with open(path, encoding="utf-8") as f:
                record = json.load(f)
        except json.JSONDecodeError as e:
            self._discard_corrupt_buffer_file(path, f"invalid JSON ({e})")
            return None
        except OSError as e:
            print(f"[Kafka Sender Agent] Cannot read buffer file {os.path.basename(path)}: {e}")
            return None

        if not isinstance(record, dict):
            self._discard_corrupt_buffer_file(path, "root is not an object")
            return None
        payload = record.get("payload")
        if not isinstance(payload, dict):
            self._discard_corrupt_buffer_file(path, "missing payload object")
            return None
        camera_key = record.get("camera_key") or payload.get("camera_id", "unknown")
        return payload, str(camera_key)

    def _is_kafka_transport_error(self, exc: BaseException) -> bool:
        if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
            return True
        msg = str(exc).lower()
        needles = (
            "kafka",
            "broker",
            "transport",
            "timed out",
            "connection",
            "disconnect",
        )
        return any(n in msg for n in needles)

    def _drop_oldest_buffered(self) -> None:
        files = self._list_buffer_files()
        if not files:
            return
        oldest = files[0]
        visitor_ts = None
        try:
            with open(oldest, encoding="utf-8") as f:
                record = json.load(f)
            visitor_ts = record.get("payload", {}).get("timestamp")
        except Exception:
            pass
        try:
            os.remove(oldest)
        except OSError as e:
            print(f"[Kafka Sender Agent] Failed to drop oldest buffer file: {e}")
            return
        print(
            f"[Kafka Sender Agent] Buffer full ({self._buffer_max_items}); "
            f"dropped oldest queued crop (visitor_ts={visitor_ts}) — storing newest instead"
        )

    def _enqueue_buffer(self, payload: dict, camera_key: str) -> bool:
        if not self._buffer_enabled:
            return False
        with self._buffer_lock:
            while self._buffer_count() >= self._buffer_max_items:
                self._drop_oldest_buffered()
            if self._buffer_max_bytes:
                while self._buffer_bytes() >= self._buffer_max_bytes and self._buffer_count() > 0:
                    self._drop_oldest_buffered()

            seq = self._next_buffer_seq()
            filename = f"{seq:012d}_{uuid.uuid4().hex[:8]}.json"
            path = os.path.join(self._buffer_dir, filename)
            record = {
                "payload": payload,
                "camera_key": camera_key,
                "queued_at": time.time(),
            }
            tmp_path = path + ".tmp"
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False)
                os.replace(tmp_path, path)
            except Exception as e:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except OSError:
                    pass
                print(f"[Kafka Sender Agent] Buffer enqueue failed: {e}")
                return False

            self._buffer_enqueue_count += 1
            if self._buffer_enqueue_count == 1 or self._buffer_enqueue_count % 25 == 0:
                print(
                    f"[Kafka Sender Agent] Buffered crop #{self._buffer_enqueue_count} "
                    f"(visitor_ts={payload.get('timestamp')}, queue={self._buffer_count()})"
                )
            return True

    def _send_kafka_payload(self, payload: dict, camera_key: str) -> None:
        if not self._producer:
            raise RuntimeError("Kafka producer not connected")

        delivery_error: List[Optional[Exception]] = [None]
        delivery_done = threading.Event()

        def _on_delivery(err, _msg):
            if err is not None:
                delivery_error[0] = err
            delivery_done.set()

        value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        with self._kafka_lock:
            self._producer.produce(
                self.topic,
                value=value,
                key=camera_key.encode("utf-8"),
                callback=_on_delivery,
            )
            self._producer.poll(0)
            remaining = self._producer.flush(timeout=self._delivery_timeout_sec)

        if remaining > 0:
            raise TimeoutError(f"Kafka flush timed out ({remaining} message(s) still in queue)")
        if not delivery_done.wait(timeout=1.0):
            raise TimeoutError("Kafka delivery callback not received")
        if delivery_error[0] is not None:
            raise delivery_error[0]

        self._sent_count += 1
        if self._sent_count % 50 == 0:
            print(f"[Kafka Sender Agent] Published {self._sent_count} messages")

    @staticmethod
    def _slug(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value or "").strip()
        value = "".join(c for c in value if c.isprintable() or c.isspace()).strip()
        if not value:
            return None
        value = value.lower().replace(" ", "-")
        value = "".join(c for c in value if c.isalnum() or c in "-_")
        return value or None

    def _camera_crop_prefixes(self) -> Dict[str, str]:
        path = os.getenv("CAMERAS_CONFIG_PATH", "/app/config/cameras.yaml")
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return self._camera_prefix_cache

        if mtime == self._camera_prefix_cache_mtime and self._camera_prefix_cache:
            return self._camera_prefix_cache

        prefixes: Dict[str, str] = {}
        try:
            with open(path, encoding="utf-8") as f:
                doc = yaml.safe_load(f) or {}
            cameras = doc.get("cameras") if isinstance(doc, dict) else doc
            if not isinstance(cameras, list):
                cameras = []
            for cam in cameras:
                if not isinstance(cam, dict):
                    continue
                camera_id = str(cam.get("camera_id") or "").strip()
                if not camera_id:
                    continue
                path_parts = ["crops"]
                cam_slug = self._slug(cam.get("name"))
                if cam_slug:
                    path_parts.append(cam_slug)
                building_id = cam.get("building_id")
                if building_id:
                    path_parts.append(str(building_id))
                path_parts.append(camera_id)
                prefixes[camera_id] = "/".join(path_parts)
        except Exception as e:
            print(f"[Kafka Sender Agent] Camera prefix load failed: {e}")
            return self._camera_prefix_cache

        self._camera_prefix_cache = prefixes
        self._camera_prefix_cache_mtime = mtime
        return prefixes

    def _clear_spill_payload_refs(self, payload: dict, local_path: Optional[str] = None) -> None:
        payload.pop("local_spill", None)
        payload.pop("local_path", None)
        if local_path:
            meta_path = local_path + ".meta.json"
            try:
                if os.path.isfile(meta_path):
                    os.remove(meta_path)
            except OSError:
                pass

    def _object_exists(self, client: Any, bucket: str, object_key: str) -> bool:
        """True if object is in MinIO.

        Uses list_objects(prefix=object_key) rather than stat/get_object: some
        deployments grant PutObject + ListBucket but deny HeadObject/GetObject,
        so stat_object would raise AccessDenied even for existing objects.
        Transient errors propagate (caller retries/stops); a clean empty listing
        means the object is genuinely absent (so the caller may discard)."""
        for obj in client.list_objects(bucket, prefix=object_key, recursive=True):
            if obj.object_name == object_key:
                return True
        return False

    def _upload_spill_to_minio_with_client(self, payload: dict, client: Any) -> None:
        local_path = payload.get("local_path")
        object_key = payload.get("object_key")
        bucket = payload.get("bucket", self._minio_bucket)
        if not object_key:
            raise ValueError("payload missing object_key for spill upload")

        if local_path and os.path.isfile(local_path):
            with open(local_path, "rb") as f:
                data = f.read()
            client.put_object(
                bucket,
                object_key,
                data=io.BytesIO(data),
                length=len(data),
                content_type="image/jpeg",
            )
            try:
                os.remove(local_path)
            except OSError:
                pass
            self._clear_spill_payload_refs(payload, local_path)
            return

        if local_path and not os.path.isfile(local_path):
            # Local file gone. Either the upload already succeeded (and Kafka failed
            # afterwards, so this is a retry) or the spill file was truly lost.
            # Confirm against MinIO so we never drop metadata for an image that IS
            # stored (which would orphan it) and never send Kafka for one that isn't.
            if self._object_exists(client, bucket, object_key):
                print(
                    f"[Kafka Sender Agent] Spill file absent but {bucket}/{object_key} "
                    f"already in MinIO; sending Kafka"
                )
                self._clear_spill_payload_refs(payload, local_path)
                return
            raise FileNotFoundError(
                f"spill JPEG missing and not in MinIO: {local_path}"
            )

        # No local_path at all — orphan spill batch likely uploaded it already.
        if object_key and not self._object_exists(client, bucket, object_key):
            raise FileNotFoundError(
                f"spill payload has no local file and {bucket}/{object_key} not in MinIO"
            )
        self._clear_spill_payload_refs(payload, local_path)

    def _upload_spill_to_minio(self, payload: dict) -> None:
        if not self._minio_client:
            if not self._connect_minio():
                raise RuntimeError("MinIO unavailable for spill upload")
        self._upload_spill_to_minio_with_client(payload, self._minio_client)

    def _replay_minio_upload(self, payload: dict, client: Any) -> None:
        """Upload one spill file during replay; retries without blocking other workers."""
        if not payload.get("local_spill"):
            return
        last_err: Optional[BaseException] = None
        attempts = max(1, self._replay_minio_retries)
        for attempt in range(attempts):
            try:
                self._upload_spill_to_minio_with_client(payload, client)
                return
            except FileNotFoundError:
                # Definitive: file gone and object not in MinIO. Retrying won't help.
                raise
            except Exception as e:
                last_err = e
                if attempt + 1 < attempts:
                    time.sleep(0.5 * (attempt + 1))
        if last_err is not None:
            raise last_err

    def _ensure_payload_in_minio(self, payload: dict) -> None:
        """Upload spill files before Kafka; direct uploads are already in MinIO."""
        if not self._use_minio:
            return
        if payload.get("local_spill"):
            self._upload_spill_to_minio(payload)

    def _buffered_spill_paths(self) -> set:
        paths: set = set()
        for path in self._list_buffer_files():
            try:
                with open(path, encoding="utf-8") as f:
                    record = json.load(f)
                payload = record.get("payload") or {}
                local_path = payload.get("local_path")
                if payload.get("local_spill") and local_path:
                    paths.add(os.path.normpath(str(local_path)))
            except Exception:
                continue
        return paths

    def _replay_orphan_spill_batch(self) -> None:
        """Upload spilled JPEGs not referenced by pending kafka_buffer entries."""
        if not self._local_spill_enabled or not self._minio_client:
            return
        if not os.path.isdir(self._local_spill_dir):
            return

        buffered_spill_paths = self._buffered_spill_paths()
        prefixes = self._camera_crop_prefixes()
        if not prefixes:
            return

        uploaded = 0
        for camera_id in sorted(os.listdir(self._local_spill_dir)):
            spill_dir = os.path.join(self._local_spill_dir, camera_id)
            if not os.path.isdir(spill_dir):
                continue
            prefix = prefixes.get(camera_id)
            if not prefix:
                continue
            names = sorted(
                n for n in os.listdir(spill_dir)
                if n.endswith(".jpg") and not n.endswith(".tmp")
            )
            for name in names[: self._buffer_replay_batch]:
                local_path = os.path.join(spill_dir, name)
                if os.path.normpath(local_path) in buffered_spill_paths:
                    continue
                meta_path = local_path + ".meta.json"
                object_key = None
                bucket = self._minio_bucket
                if os.path.isfile(meta_path):
                    try:
                        with open(meta_path, encoding="utf-8") as f:
                            meta = json.load(f)
                        object_key = meta.get("object_key")
                        bucket = meta.get("bucket", bucket)
                    except Exception:
                        object_key = None
                if not object_key and prefix:
                    object_key = f"{prefix}/{name}"
                if not object_key:
                    continue
                try:
                    with open(local_path, "rb") as f:
                        data = f.read()
                    self._minio_client.put_object(
                        bucket,
                        object_key,
                        data=io.BytesIO(data),
                        length=len(data),
                        content_type="image/jpeg",
                    )
                    os.remove(local_path)
                    if os.path.isfile(meta_path):
                        os.remove(meta_path)
                    uploaded += 1
                except Exception as e:
                    print(
                        f"[Kafka Sender Agent] Orphan spill upload stopped at "
                        f"{object_key}: {e}"
                    )
                    self._minio_client = None
                    self._minio_connected = False
                    break
            if not self._minio_connected:
                break

        if uploaded:
            remaining = sum(
                len([
                    n for n in os.listdir(os.path.join(self._local_spill_dir, cid))
                    if n.endswith(".jpg") and not n.endswith(".tmp")
                ])
                for cid in os.listdir(self._local_spill_dir)
                if os.path.isdir(os.path.join(self._local_spill_dir, cid))
            )
            print(
                f"[Kafka Sender Agent] Uploaded {uploaded} orphan spill crop(s) to MinIO "
                f"({remaining} remaining locally)"
            )
        self._check_backlog_drained()

    def _replay_buffer(self) -> None:
        if not self._kafka_healthy:
            return
        files = self._list_buffer_files()
        if not files:
            return

        batch = files[: self._buffer_replay_batch]
        if self._replay_minio_workers > 1 and self._use_minio:
            self._replay_buffer_parallel(batch)
        else:
            self._replay_buffer_sequential(batch)

    def _log_replay_batch_result(self, replayed: int, discarded: int) -> None:
        if replayed or discarded:
            remaining = self._buffer_count()
            parts = []
            if replayed:
                parts.append(f"replayed {replayed}")
            if discarded:
                parts.append(f"discarded {discarded} corrupt")
            print(
                f"[Kafka Sender Agent] Buffer drain: {', '.join(parts)} "
                f"({remaining} remaining)"
            )
        self._check_backlog_drained()

    def _replay_buffer_sequential(self, files: List[str]) -> None:
        replayed = 0
        discarded = 0
        for path in files:
            loaded = self._read_buffer_record(path)
            if loaded is None:
                discarded += 1
                continue
            payload, camera_key = loaded
            try:
                if payload.get("local_spill"):
                    self._upload_spill_to_minio(payload)

                self._send_kafka_payload(payload, camera_key)
                os.remove(path)
                replayed += 1
            except FileNotFoundError as e:
                self._discard_corrupt_buffer_file(path, f"missing spill JPEG ({e})")
                discarded += 1
            except Exception as e:
                print(f"[Kafka Sender Agent] Replay stopped at {os.path.basename(path)}: {e}")
                if self._is_kafka_transport_error(e):
                    self._mark_kafka_unhealthy(str(e))
                break

        self._log_replay_batch_result(replayed, discarded)

    def _replay_buffer_parallel(self, files: List[str]) -> None:
        """Parallel MinIO uploads; Kafka sends strictly in FIFO order."""
        class _ReplayItem:
            __slots__ = ("path", "payload", "camera_key", "minio_ready", "minio_error", "needs_minio")

            def __init__(self, path: str, payload: dict, camera_key: str):
                self.path = path
                self.payload = payload
                self.camera_key = camera_key
                self.needs_minio = bool(payload.get("local_spill"))
                self.minio_ready = threading.Event()
                self.minio_error: Optional[BaseException] = None
                if not self.needs_minio:
                    self.minio_ready.set()

        items: List[_ReplayItem] = []
        discarded = 0
        for path in files:
            loaded = self._read_buffer_record(path)
            if loaded is None:
                discarded += 1
                continue
            payload, camera_key = loaded
            items.append(_ReplayItem(path, payload, camera_key))

        if not items:
            self._log_replay_batch_result(0, discarded)
            return

        minio_wait_sec = self._minio_connect_timeout_sec + self._minio_read_timeout_sec + 15.0
        minio_local = threading.local()
        submit_lock = threading.Lock()
        scheduled: Set[int] = set()
        next_submit_idx = 0

        def _worker_minio_client() -> Any:
            client = getattr(minio_local, "client", None)
            if client is None:
                client = self._create_minio_client(pool_size=4)
                minio_local.client = client
            return client

        def _minio_worker(item: _ReplayItem) -> None:
            try:
                self._replay_minio_upload(item.payload, _worker_minio_client())
            except Exception as e:
                item.minio_error = e
            finally:
                item.minio_ready.set()

        def _schedule_prefetch(kafka_idx: int, executor: ThreadPoolExecutor) -> None:
            nonlocal next_submit_idx
            limit = min(len(items), kafka_idx + self._replay_prefetch_ahead)
            with submit_lock:
                while next_submit_idx < limit:
                    idx = next_submit_idx
                    next_submit_idx += 1
                    if idx in scheduled:
                        continue
                    item = items[idx]
                    if not item.needs_minio or item.minio_ready.is_set():
                        continue
                    scheduled.add(idx)
                    executor.submit(_minio_worker, item)

        replayed = 0
        workers = max(2, self._replay_minio_workers)
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="replay-minio") as executor:
            for kafka_idx, item in enumerate(items):
                if not self._kafka_healthy:
                    break

                _schedule_prefetch(kafka_idx, executor)

                if not item.minio_ready.wait(timeout=minio_wait_sec):
                    print(
                        f"[Kafka Sender Agent] Replay stopped at {os.path.basename(item.path)}: "
                        f"MinIO upload timed out after {minio_wait_sec:.0f}s"
                    )
                    self._minio_connected = False
                    break

                if item.minio_error is not None:
                    err = item.minio_error
                    if isinstance(err, FileNotFoundError):
                        self._discard_corrupt_buffer_file(
                            item.path, f"missing spill JPEG ({err})"
                        )
                        discarded += 1
                        continue
                    print(
                        f"[Kafka Sender Agent] Replay stopped at {os.path.basename(item.path)}: "
                        f"{err}"
                    )
                    self._minio_client = None
                    self._minio_connected = False
                    break

                try:
                    self._send_kafka_payload(item.payload, item.camera_key)
                    os.remove(item.path)
                    replayed += 1
                except Exception as e:
                    print(f"[Kafka Sender Agent] Replay stopped at {os.path.basename(item.path)}: {e}")
                    if self._is_kafka_transport_error(e):
                        self._mark_kafka_unhealthy(str(e))
                    break

        self._log_replay_batch_result(replayed, discarded)

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
        """JPEG encode → MinIO (or local spill) → Kafka or disk buffer. Non-blocking for Kafka."""
        def _sanitize(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            value = str(value or "").strip()
            value = "".join(c for c in value if c.isprintable() or c.isspace()).strip()
            return value or None

        camera_id = _sanitize(camera_id) or "unknown"
        company_id = _sanitize(company_id)
        building_id = _sanitize(building_id)
        company_name = _sanitize(company_name)
        building_name = _sanitize(building_name)
        camera_name = _sanitize(camera_name)

        try:
            payload, camera_key = self._upload_crop(
                frame=frame,
                timestamp=timestamp,
                camera_id=camera_id,
                company_id=company_id,
                building_id=building_id,
                company_name=company_name,
                building_name=building_name,
                camera_name=camera_name,
                _slug=self._slug,
                force_disk=self._should_store_locally(),
            )
        except Exception as e:
            print(f"[Kafka Sender Agent] Crop upload failed: {e}")
            return False

        if payload is None:
            return False

        if self._should_store_locally():
            if self._enqueue_buffer(payload, camera_key):
                self._was_draining_backlog = True
                return True
            return False

        if self._use_minio:
            try:
                self._ensure_payload_in_minio(payload)
            except Exception as e:
                print(f"[Kafka Sender Agent] MinIO not ready for crop: {e}")
                if self._enqueue_buffer(payload, camera_key):
                    return True
                return False

        if self._kafka_healthy:
            try:
                self._send_kafka_payload(payload, camera_key)
                return True
            except Exception as e:
                self._mark_kafka_unhealthy(str(e))

        if self._enqueue_buffer(payload, camera_key):
            return True
        return False

    def _upload_crop(
        self,
        *,
        frame: np.ndarray,
        timestamp: float,
        camera_id: str,
        company_id: Optional[str],
        building_id: Optional[str],
        company_name: Optional[str],
        building_name: Optional[str],
        camera_name: Optional[str],
        _slug: Callable[[Optional[str]], Optional[str]],
        force_disk: bool = False,
    ) -> Tuple[Optional[dict], str]:
        data = encode_jpeg_bgr(frame, quality=self.jpeg_quality)
        if data is None:
            print("[Kafka Sender Agent] Image encode error")
            return None, camera_id

        ts_ms = int(timestamp * 1000)
        path_parts = ["crops"]
        cam_name_slug = _slug(camera_name)
        if cam_name_slug:
            path_parts.append(cam_name_slug)
        if building_id:
            path_parts.append(str(building_id))
        path_parts.append(camera_id)
        prefix = "/".join(path_parts)
        file_id = uuid.uuid4().hex[:8]
        object_name = f"{prefix}/{ts_ms}_{file_id}.jpg"

        payload: Dict[str, Any] = {
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

        uploaded = False
        if self._use_minio and not force_disk:
            if not self._minio_client:
                self._connect_minio()
            if self._minio_client:
                try:
                    self._minio_client.put_object(
                        self._minio_bucket,
                        object_name,
                        data=io.BytesIO(data),
                        length=len(data),
                        content_type="image/jpeg",
                    )
                    uploaded = True
                    self._minio_connected = True
                except Exception as e:
                    print(f"[Kafka Sender Agent] MinIO upload failed: {e}")
                    self._minio_client = None
                    self._minio_connected = False

        if not uploaded:
            if not self._local_spill_enabled:
                return None, camera_id
            spill_dir = os.path.join(self._local_spill_dir, camera_id)
            os.makedirs(spill_dir, exist_ok=True)
            local_path = os.path.join(spill_dir, f"{ts_ms}_{file_id}.jpg")
            tmp_path = local_path + ".tmp"
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, local_path)
            meta_path = local_path + ".meta.json"
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"object_key": object_name, "bucket": self._minio_bucket},
                    f,
                    ensure_ascii=False,
                )
            payload["local_spill"] = True
            payload["local_path"] = local_path

        return payload, camera_id
