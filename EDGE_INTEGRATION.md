# Edge Agent Integration — Indoor Store Tracking

Контракт между Edge-агентом (камеры) и backend для трекинга посетителей на 2D-плане магазина: heatmap, траектории, live-позиции.

Backend полностью готов принимать данные. От Edge требуются **два потока в Kafka** и **стабильный `track_id`**.

---

## 1. Трекинг людей на камере

На каждой камере Edge должен:

- детектировать людей и вести трекинг (bbox центра или точки ног);
- назначать каждому человеку **стабильный `track_id`** на время жизни трека, например `cam12_t47`;
- один и тот же человек на одной камере → один и тот же `track_id`, пока трек не потерян.

`track_id` — ключ связи между потоком позиций и распознаванием лица. Формат произвольная строка до 128 символов, уникальная в пределах камеры.

---

## 2. Топик позиций: `store_tracking_positions`

**Частота:** 1–2 Hz **на каждый активный track** (НЕ каждый кадр).

```json
{
  "camera_id": "12",
  "company_id": "1",
  "building_id": "3",
  "track_id": "cam12_t47",
  "local_x": 320.5,
  "local_y": 410.2,
  "timestamp": 1733400001200
}
```

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| `camera_id` | string | да | ID камеры (`smartcamera.id`) |
| `company_id` | string | да | ID компании |
| `building_id` | string | да | ID здания |
| `track_id` | string | да | Стабильный ID трека на этой камере |
| `local_x` | float | да | X в **пикселях кадра камеры** |
| `local_y` | float | да | Y в **пикселях кадра камеры** |
| `timestamp` | int64 | да | Unix time в **миллисекундах** |
| `visitor_id` | int | нет | Не отправлять — backend связывает сам через face |

**Координаты:**

- `local_x` / `local_y` — пиксели в той же системе (разрешении) кадра, в которой делалась калибровка камеры;
- преобразование в координаты плана (homography) выполняет `position-worker` на backend;
- Edge **не** отправляет `map_x` / `map_y`.

**Поведение backend:** если для камеры нет активной калибровки или точка выходит за границы плана — сообщение молча отбрасывается. Это нормально на этапе, пока камера не откалибрована.

---

## 3. Топик лиц: `triton_face_pipeline` (существующий, +1 поле)

Формат не меняется, добавляется поле `track_id`:

```json
{
  "entity_type": "visitor",
  "camera_id": "12",
  "company_id": "1",
  "building_id": "3",
  "track_id": "cam12_t47",
  "bucket": "visitorstorage",
  "object_key": "path/to/face.jpg",
  "timestamp": 1733400001200
}
```

**Зачем:** после распознавания лица `triton-face-worker` записывает связь `(camera_id, track_id) → visitor_id` в `track_visitor_link` и проставляет `visitor_id` на все уже сохранённые позиции с этим `track_id` (backfill).

**Правило:** face crop для человека отправляется с **тем же** `track_id`, что и его позиции в `store_tracking_positions`.

---

## 4. Рекомендуемая логика на Edge

```
Для каждого кадра:
  tracks = tracker.update(detections)

  Для каждого track:
    если прошло >= 500–1000 мс с последней отправки позиции этого track:
      publish → store_tracking_positions
        { camera_id, company_id, building_id, track_id, local_x, local_y, timestamp }

    если есть качественный face crop и для этого track ещё не отправляли (или пора обновить):
      upload face → MinIO
      publish → triton_face_pipeline
        { entity_type, camera_id, company_id, building_id, track_id, bucket, object_key, timestamp }
```

Точка позиции: центр нижней грани bbox (точка ног) даёт наиболее корректную проекцию на план.

### Реализация в Crop_sender

Включено по умолчанию (`TRACKING_ENABLED=true`):

| Компонент | Файл |
|-----------|------|
| Greedy IoU tracker | `sender/iou_tracker.py` |
| Throttle + face upper-third | `sender/store_tracking.py` |
| Kafka producers | `sender/kafka_sender_agent.py` (`send_tracking_position`, `send_face_pipeline`) |

**Env (совпадает с backend):**

```
TRACKING_ENABLED=true
TRACKING_POSITIONS_TOPIC=store_tracking_positions
FACE_PIPELINE_TOPIC=triton_face_pipeline
TRACKING_INTERVAL_MS=700
FACE_RESEND_SEC=0
```

- Трекер: greedy IoU matching, без Kalman/ReID — склейку личности делает backend через лицо.
- `local_x`/`local_y` = `(x1+x2)/2`, `y2` в пикселях **оригинального** кадра (детектор `stdin-bgr` без ресайза).
- Face path (старт): верхняя треть person-bbox → MinIO → `triton_face_pipeline`; worker сам найдёт лицо.
- Существующий crop → `snapshots` **не отключается**.
- Нужны `company_id` и `building_id` на камере (из backend cameras API).
- Если на камере активен tripwire — в трекинг попадают только детекции после фильтра линии; для heatmap по всему залу выключите `line_active`.

---

## 5. Пример producer (Python)

```python
import json
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def send_position(camera_id, company_id, building_id, track_id, x, y):
    producer.send(
        "store_tracking_positions",
        {
            "camera_id": str(camera_id),
            "company_id": str(company_id),
            "building_id": str(building_id),
            "track_id": track_id,
            "local_x": float(x),
            "local_y": float(y),
            "timestamp": int(time.time() * 1000),
        },
    )

send_position(12, 1, 3, "cam12_t47", 320.5, 410.2)
producer.flush()
```

---

## 6. Синхронизация калибровок (опционально)

Нужно только если Edge хочет локально проецировать точки на план (preview). Для отправки в Kafka не требуется.

```
GET /company/{company_id}/calibration-sync?since=2026-08-05T00:00:00Z
```

Ответ по каждой камере: `homography_matrix` (9 чисел, row-major 3x3), `frame_width`, `frame_height`, `store_map_id`, `building_id`, `is_active`, `updated_at`. Передавайте `since` из последнего ответа для инкрементальной синхронизации.

Применение homography к точке `(x, y)`:

```
w  = h6*x + h7*y + h8
mx = (h0*x + h1*y + h2) / w
my = (h3*x + h4*y + h5) / w
```

---

## 7. Предусловия на стороне backend (не Edge)

1. Загружен план магазина (`store_map`) для company + building.
2. Выполнена калибровка камеры (минимум 4 точки → homography).
3. Запущены воркеры `position-worker` и `triton-face-worker`.

---

## 8. Чего Edge делать НЕ нужно

- Не слать позиции 25–30 FPS — достаточно 1–2 Hz на track.
- Не считать homography на Edge (кроме локального preview, см. п. 6).
- Не слать `visitor_id` в топик позиций — связывание делает backend.
- Не менять существующий формат face-сообщений — только добавить `track_id`.

---

## 9. Проверка интеграции

1. Отправить 10–20 position-сообщений с известным `track_id`.
2. Отправить face crop с тем же `track_id`.
3. Проверить API backend:

| Endpoint | Что смотреть |
|----------|--------------|
| `GET /company/{cid}/building/{bid}/positions/current` | Точки на карте прямо сейчас |
| `GET /company/{cid}/building/{bid}/trajectories` | Путь по `visitor_id`/`track_id` |
| `GET /company/{cid}/building/{bid}/heatmap` | Агрегированная тепловая карта |
| `GET /company/{cid}/building/{bid}/positions/live/stream` | Live-позиции (SSE) |

4. После обработки face crop у позиций трека должен появиться `visitor_id`:

```sql
SELECT track_id, visitor_id, map_x, map_y, event_time
FROM visitor_position
ORDER BY id DESC LIMIT 20;
```

---

## Чеклист Edge-команды

- [x] Трекер со стабильным `track_id` per camera (`sender/iou_tracker.py`)
- [x] Producer в `store_tracking_positions` (1–2 Hz на track, `TRACKING_INTERVAL_MS`)
- [x] Поле `track_id` добавлено в сообщения `triton_face_pipeline`
- [x] `local_x`/`local_y` — пиксели кадра камеры (система калибровки)
- [x] `camera_id`, `company_id`, `building_id` присутствуют в обоих топиках
- [x] `timestamp` — unix milliseconds
