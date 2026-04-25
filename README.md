# Video Processing API (Multi-Client + SQLite)

Servicio para subir videos, procesarlos frame a frame y consumir resultados en tiempo real o por API REST, con persistencia en SQLite por job.

## Características

- Upload de video por cliente.
- Procesamiento a 2 FPS.
- Eventos en tiempo real por WebSocket usando rooms por `job_id`.
- Persistencia en SQLite (`results.db`) para estado del job y resultados.
- Endpoints REST para consultar estado y resultados históricos.

## Arquitectura

Cada upload crea un `job_id` único.

1. Cliente hace `POST /upload_video`.
2. Backend guarda job en SQLite con estado `queued`.
3. Un thread procesa el video y actualiza estado (`processing`, `done`, `error`).
4. Cada detección se guarda en SQLite y se emite por WebSocket al room del job.

## Estructura de datos (SQLite)

Base: `results.db`

Tabla `jobs`:
- `job_id` (PK)
- `status` (`queued` | `processing` | `done` | `error`)
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

### 1) Subir video

`POST /upload_video`

Request (`multipart/form-data`):
- `video`: archivo de video

Response:

```json
{
  "status": "processing",
  "job_id": "a1b2c3d4..."
}
```

### 2) Estado de job

`GET /jobs/<job_id>`

Response:

```json
{
  "job_id": "a1b2c3d4...",
  "status": "processing",
  "error_message": null,
  "created_at": "2026-04-25 15:20:00",
  "updated_at": "2026-04-25 15:20:05"
}
```

### 3) Resultados de job

`GET /jobs/<job_id>/results?limit=100`

- `limit` opcional, rango: 1 a 2000.

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
    },
    {
      "ms": 500,
      "tracks": [],
      "frame": 2,
      "ran_detector": false,
      "created_at": "2026-04-25 15:20:06"
    }
  ]
}
```

## WebSocket (Socket.IO)

Conectar a:

`ws://HOST:6000`

### Suscripción por job

El cliente debe suscribirse al room del `job_id`:

```javascript
socket.emit("subscribe", { job_id })
```

Para salir del room:

```javascript
socket.emit("unsubscribe", { job_id })
```

### Eventos emitidos por backend

`subscribed`

```json
{
  "job_id": "a1b2c3d4..."
}
```

`new_detection`

```json
{
  "job_id": "a1b2c3d4...",
  "ms": 1500,
  "tracks": [],
  "frame": 3,
  "ran_detector": true
}
```

`finished`

```json
{
  "job_id": "a1b2c3d4...",
  "status": "done"
}
```

En caso de error:

```json
{
  "job_id": "a1b2c3d4...",
  "status": "error",
  "error": "detalle..."
}
```

## Ejemplo de flujo frontend

```javascript
const formData = new FormData();
formData.append("video", file);

const uploadRes = await fetch("http://localhost:6000/upload_video", {
  method: "POST",
  body: formData,
});

const { job_id } = await uploadRes.json();

const socket = io("http://localhost:6000");
socket.emit("subscribe", { job_id });

socket.on("new_detection", (data) => {
  console.log("Deteccion", data);
});

socket.on("finished", (data) => {
  console.log("Fin job", data);
});

const statusRes = await fetch(`http://localhost:6000/jobs/${job_id}`);
const status = await statusRes.json();

const resultsRes = await fetch(`http://localhost:6000/jobs/${job_id}/results?limit=200`);
const results = await resultsRes.json();
```

## Ejecución

1. Instalar dependencias:

```bash
pip install -r requirements.txt
```

2. Ejecutar servidor:

```bash
python main.py
```

Opcional (recomendado): configurar endpoint de deteccion por variable de entorno para evitar dependencias de hostname fijo.

PowerShell:

```powershell
$env:PROCESS_URL="http://100.100.100.100:5000/process"
$env:PROCESS_TIMEOUT="30"
python main.py
```

Linux:

```bash
PROCESS_URL="http://100.100.100.100:5000/process" PROCESS_TIMEOUT="30" python main.py
```

El servidor inicia en `0.0.0.0:6000`.

## Notas

- El servicio de detección externo se configura en `PROCESS_URL` dentro de `main.py`.
- Los uploads se guardan en la carpeta `uploads/`.
- Para producción, considerar cola de trabajos y workers separados.
