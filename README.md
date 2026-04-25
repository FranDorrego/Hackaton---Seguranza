# Video Processing API (Multi-Client + SQLite + Live Frames)

Servicio para subir videos, procesarlos frame a frame, enviar fotos en vivo al detector y consumir resultados por API REST y WebSocket.

## Funcionalidades

- Upload de video por cliente.
- Procesamiento configurable en FPS (`target_fps`) por endpoint.
- Procesamiento pausado cuando no hay viewers suscritos al video.
- Endpoint para enviar fotos en vivo directamente al detector (`/live/frame`).
- Persistencia en SQLite (`results.db`) para jobs y resultados historicos.
- Endpoints para listar videos procesados y activos.
- WebSocket por rooms para video por `job_id` y stream en vivo por `stream_id`.

## Arquitectura

### Flujo video (upload)

1. Frontend hace `POST /upload_video`.
2. Se crea `job_id` y estado inicial `queued`.
3. Thread de procesamiento pasa a `waiting_viewers`.
4. Solo cuando hay viewers suscritos al `job_id`, pasa a `processing`.
5. Cada frame procesado se guarda en SQLite y se emite por WebSocket (`new_detection`).
6. Al terminar: estado `done` + evento `finished`.

### Flujo live (fotos puras)

1. Frontend se suscribe por Socket.IO al `stream_id`.
2. Frontend envia fotos por `POST /live/frame` con `image`.
3. Si no hay viewers en ese `stream_id`, el backend responde `ignored` y no procesa.
4. Si hay viewers, el backend envia la foto al detector y emite `live_detection`.

## Base de datos (SQLite)

Base: `results.db`

Tabla `jobs`:
- `job_id` (PK)
- `status` (`queued` | `waiting_viewers` | `processing` | `done` | `error`)
- `video_path`
- `error_message`
- `created_at`
- `updated_at`

Tabla `results`:
- `id` (PK autoincrement)
- `job_id` (FK a `jobs.job_id`)
- `ms` (timestamp en ms dentro del video)
- `tracks_json` (JSON serializado)
- `frame`
- `ran_detector` (0/1)
- `created_at`

## Endpoints REST

### Salud / Config

#### `GET /settings/fps`

Devuelve los FPS actuales de procesamiento.

Response:

```json
{
  "target_fps": 2.0
}
```

#### `PUT /settings/fps`

Cambia los FPS de procesamiento para jobs en curso y futuros.

Body:

```json
{
  "target_fps": 4
}
```

Response:

```json
{
  "status": "ok",
  "target_fps": 4.0
}
```

Validacion: rango `(0, 120]`.

### Video upload

#### `POST /upload_video`

Request `multipart/form-data`:
- `video`: archivo de video

Response:

```json
{
  "status": "processing",
  "job_id": "a1b2c3d4..."
}
```

### Estado y resultados por job

#### `GET /jobs/<job_id>`

Response:

```json
{
  "job_id": "a1b2c3d4...",
  "status": "waiting_viewers",
  "error_message": null,
  "created_at": "2026-04-25 15:20:00",
  "updated_at": "2026-04-25 15:20:05"
}
```

#### `GET /jobs/<job_id>/results?limit=100`

Response:

```json
{
  "job_id": "a1b2c3d4...",
  "count": 2,
  "results": [
    {
      "ms": 0,
      "tracks": [],
      "frame": 1,
      "ran_detector": true,
      "created_at": "2026-04-25 15:20:06"
    }
  ]
}
```

### Catalogo de videos

#### `GET /videos/processed?limit=50`

Lista videos ya tratados (`status=done`) con resumen.

Response:

```json
{
  "count": 1,
  "videos": [
    {
      "job_id": "a1b2c3d4...",
      "status": "done",
      "error_message": null,
      "created_at": "2026-04-25 15:20:00",
      "updated_at": "2026-04-25 15:21:30",
      "result_count": 140,
      "last_ms": 69200
    }
  ]
}
```

#### `GET /videos/active`

Lista jobs activos y streams live activos.

Response:

```json
{
  "job_count": 1,
  "jobs": [
    {
      "job_id": "a1b2c3d4...",
      "status": "processing",
      "created_at": "2026-04-25 15:20:00",
      "updated_at": "2026-04-25 15:20:40",
      "viewer_count": 2
    }
  ],
  "live_stream_count": 1,
  "live_streams": [
    {
      "stream_id": "cam-entrada",
      "viewer_count": 1,
      "latest_result": {
        "stream_id": "cam-entrada",
        "timestamp_ms": 1714075260000,
        "tracks": [],
        "frame": 123,
        "ran_detector": true
      }
    }
  ]
}
```

### Live frames (fotos puras)

#### `POST /live/frame`

Envia una foto para procesar en vivo.

Request `multipart/form-data`:
- `image`: imagen requerida
- `stream_id`: opcional (default `default`)
- `force_detect`: opcional (`0` o `1`, default `0`)

Respuestas:

`200 OK` cuando procesa:

```json
{
  "status": "ok",
  "result": {
    "stream_id": "cam-entrada",
    "timestamp_ms": 1714075260000,
    "tracks": [],
    "frame": 123,
    "ran_detector": true
  }
}
```

`202 Accepted` cuando no hay viewers y no se procesa:

```json
{
  "status": "ignored",
  "reason": "no_viewers",
  "stream_id": "cam-entrada"
}
```

#### `GET /live/streams/<stream_id>`

Devuelve estado actual del stream live.

Response:

```json
{
  "stream_id": "cam-entrada",
  "viewer_count": 1,
  "latest_result": {
    "stream_id": "cam-entrada",
    "timestamp_ms": 1714075260000,
    "tracks": [],
    "frame": 123,
    "ran_detector": true
  }
}
```

### Vista simple para pruebas live

#### `GET /views/live-photo`

Devuelve un HTML simple para subir fotos manualmente a `/live/frame`.

## WebSocket (Socket.IO)

Conectar a:

`ws://HOST:6000`

### Suscribirse

Puedes suscribirte a un job de video, a un stream live, o ambos:

```javascript
socket.emit("subscribe", { job_id: "a1b2c3d4..." });
socket.emit("subscribe", { stream_id: "cam-entrada" });
```

Desuscribirse:

```javascript
socket.emit("unsubscribe", { job_id: "a1b2c3d4..." });
socket.emit("unsubscribe", { stream_id: "cam-entrada" });
```

### Eventos emitidos

#### `subscribed` (job)

```json
{
  "job_id": "a1b2c3d4...",
  "viewer_count": 2
}
```

#### `new_detection` (job)

```json
{
  "job_id": "a1b2c3d4...",
  "ms": 1500,
  "tracks": [],
  "frame": 3,
  "ran_detector": true
}
```

#### `finished` (job)

```json
{
  "job_id": "a1b2c3d4...",
  "status": "done"
}
```

#### `live_subscribed` (live)

```json
{
  "stream_id": "cam-entrada",
  "viewer_count": 1
}
```

#### `live_detection` (live)

```json
{
  "stream_id": "cam-entrada",
  "timestamp_ms": 1714075260000,
  "tracks": [],
  "frame": 123,
  "ran_detector": true
}
```

## Ejemplo frontend (video + live)

```javascript
import { io } from "socket.io-client";

const API = "https://TU_BACKEND";
const socket = io(API);

// 1) Upload de video
const formData = new FormData();
formData.append("video", file);
const upRes = await fetch(`${API}/upload_video`, { method: "POST", body: formData });
const { job_id } = await upRes.json();

// 2) Ver video en tiempo real
socket.emit("subscribe", { job_id });
socket.on("new_detection", (msg) => {
  console.log("Job detection", msg);
});

// 3) Ver stream live en tiempo real
const stream_id = "cam-entrada";
socket.emit("subscribe", { stream_id });
socket.on("live_detection", (msg) => {
  console.log("Live detection", msg);
});

// 4) Enviar foto live
const liveFd = new FormData();
liveFd.append("stream_id", stream_id);
liveFd.append("force_detect", "1");
liveFd.append("image", fileInput.files[0]);
await fetch(`${API}/live/frame`, { method: "POST", body: liveFd });

// 5) Cambiar FPS
await fetch(`${API}/settings/fps`, {
  method: "PUT",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ target_fps: 4 })
});
```

## Configuracion por variables de entorno

- `PROCESS_URL` (default `http://raspberrypi-1:5000/process`)
- `PROCESS_TIMEOUT` (default `30`)
- `TARGET_FPS` (default `2`)
- `WAIT_FOR_VIEWERS_SLEEP` (default `0.5` segundos)
- `ALLOWED_ORIGINS` (CSV)
  - default: `https://hack.arducloud.com,http://localhost:3000,http://127.0.0.1:3000`

Ejemplo Linux:

```bash
ALLOWED_ORIGINS="https://hack.arducloud.com,https://www.hack.arducloud.com" \
PROCESS_URL="http://100.100.100.100:5000/process" \
TARGET_FPS="2" \
python main.py
```

## Ejecucion

1. Instalar dependencias:

```bash
pip install -r requirements.txt
```

2. Ejecutar servidor:

```bash
python main.py
```

Servidor en `0.0.0.0:6000`.
