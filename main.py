import cv2
import json
import os
import sqlite3
import threading
import time
import uuid
import requests
from requests.exceptions import RequestException

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from flask_socketio import SocketIO, join_room, leave_room

# -----------------------------------
# CONFIG
# -----------------------------------
PROCESS_URL = os.getenv("PROCESS_URL", "http://raspberrypi-1:5000/process")
DEFAULT_TARGET_FPS = float(os.getenv("TARGET_FPS", "2"))
REQUEST_TIMEOUT = float(os.getenv("PROCESS_TIMEOUT", "30"))
WAIT_FOR_VIEWERS_SLEEP = float(os.getenv("WAIT_FOR_VIEWERS_SLEEP", "0.5"))
MAX_STORED_JOBS = int(os.getenv("MAX_STORED_JOBS", "10"))
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "ALLOWED_ORIGINS",
        "https://hack.arducloud.com,http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "results.db")
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")

http = requests.Session()
state_lock = threading.Lock()
current_target_fps = DEFAULT_TARGET_FPS
job_viewers = {}
live_viewers = {}
sid_subscriptions = {}
latest_live_results = {}

# -----------------------------------
app = Flask(__name__)
CORS(
    app,
    resources={r"/*": {"origins": ALLOWED_ORIGINS}},
    methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)
socketio = SocketIO(app, cors_allowed_origins=ALLOWED_ORIGINS, async_mode="threading")


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def get_target_fps():
    with state_lock:
        return current_target_fps


def set_target_fps(value):
    global current_target_fps
    with state_lock:
        current_target_fps = value


def room_name_for_live(stream_id):
    return f"live:{stream_id}"


def add_subscription(sid, room_type, room_id):
    key = f"{room_type}:{room_id}"
    with state_lock:
        sid_subscriptions.setdefault(sid, set()).add(key)
        if room_type == "job":
            job_viewers.setdefault(room_id, set()).add(sid)
        else:
            live_viewers.setdefault(room_id, set()).add(sid)


def remove_subscription(sid, room_type, room_id):
    key = f"{room_type}:{room_id}"
    with state_lock:
        sid_rooms = sid_subscriptions.get(sid)
        if sid_rooms and key in sid_rooms:
            sid_rooms.remove(key)
            if not sid_rooms:
                sid_subscriptions.pop(sid, None)

        if room_type == "job":
            watchers = job_viewers.get(room_id)
            if watchers:
                watchers.discard(sid)
                if not watchers:
                    job_viewers.pop(room_id, None)
        else:
            watchers = live_viewers.get(room_id)
            if watchers:
                watchers.discard(sid)
                if not watchers:
                    live_viewers.pop(room_id, None)


def remove_sid_from_all_subscriptions(sid):
    with state_lock:
        sid_rooms = list(sid_subscriptions.pop(sid, set()))

    for key in sid_rooms:
        room_type, room_id = key.split(":", 1)
        remove_subscription(sid, room_type, room_id)


def get_job_viewer_count(job_id):
    with state_lock:
        return len(job_viewers.get(job_id, set()))


def get_live_viewer_count(stream_id):
    with state_lock:
        return len(live_viewers.get(stream_id, set()))


def cleanup_old_completed_jobs():
    if MAX_STORED_JOBS <= 0:
        return

    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT job_id, video_path
        FROM jobs
        WHERE status IN ('done', 'error')
        ORDER BY updated_at DESC, created_at DESC
        """
    ).fetchall()

    if len(rows) <= MAX_STORED_JOBS:
        conn.close()
        return

    rows_to_delete = rows[MAX_STORED_JOBS:]
    job_ids_to_delete = [row["job_id"] for row in rows_to_delete]

    conn.executemany(
        "DELETE FROM results WHERE job_id = ?",
        [(job_id,) for job_id in job_ids_to_delete],
    )
    conn.executemany(
        "DELETE FROM jobs WHERE job_id = ?",
        [(job_id,) for job_id in job_ids_to_delete],
    )
    conn.commit()
    conn.close()

    for row in rows_to_delete:
        video_path = row["video_path"]
        if video_path and os.path.exists(video_path):
            try:
                os.remove(video_path)
            except OSError:
                # Si el archivo esta bloqueado o ya no existe, no rompemos el flujo.
                pass


def set_job_status(job_id, status, error_message=None):
    conn = get_db_connection()
    if error_message is None:
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
            (status, job_id),
        )
    else:
        conn.execute(
            "UPDATE jobs SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
            (status, error_message, job_id),
        )
    conn.commit()
    conn.close()

    if status in ("done", "error"):
        cleanup_old_completed_jobs()


def send_frame_to_detector(image_bytes, filename="frame.jpg", data=None):
    files = {"image": (filename, image_bytes, "image/jpeg")}
    payload = data or {}
    res = http.post(
        PROCESS_URL,
        files=files,
        data=payload,
        timeout=REQUEST_TIMEOUT,
    )
    res.raise_for_status()
    return res.json()


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            video_path TEXT NOT NULL,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            ms INTEGER NOT NULL,
            tracks_json TEXT NOT NULL,
            frame INTEGER,
            ran_detector INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        )
        """
    )

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_results_job_ms ON results(job_id, ms)"
    )

    conn.commit()
    conn.close()


os.makedirs(UPLOADS_DIR, exist_ok=True)
init_db()
cleanup_old_completed_jobs()

# -----------------------------------
# 📥 Upload video
# -----------------------------------
@app.route("/upload_video", methods=["POST"])
def upload_video():
    if "video" not in request.files:
        return jsonify({"error": "no video"}), 400

    video = request.files["video"]
    job_id = uuid.uuid4().hex
    path = os.path.join(UPLOADS_DIR, f"{job_id}.mp4")
    video.save(path)

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO jobs (job_id, status, video_path) VALUES (?, ?, ?)",
        (job_id, "queued", path),
    )
    conn.commit()
    conn.close()

    threading.Thread(target=process_video, args=(job_id, path), daemon=True).start()

    return jsonify({"status": "processing", "job_id": job_id})


@app.route("/jobs/<job_id>", methods=["GET"])
def get_job(job_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT job_id, status, error_message, created_at, updated_at FROM jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    conn.close()

    if row is None:
        return jsonify({"error": "job not found"}), 404

    return jsonify(dict(row))


@app.route("/jobs/<job_id>/results", methods=["GET"])
def get_job_results(job_id):
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return jsonify({"error": "invalid limit"}), 400

    limit = max(1, min(limit, 2000))

    conn = get_db_connection()
    job_exists = conn.execute(
        "SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)
    ).fetchone()
    if job_exists is None:
        conn.close()
        return jsonify({"error": "job not found"}), 404

    rows = conn.execute(
        """
        SELECT ms, tracks_json, frame, ran_detector, created_at
        FROM results
        WHERE job_id = ?
        ORDER BY ms ASC
        LIMIT ?
        """,
        (job_id, limit),
    ).fetchall()
    conn.close()

    output = []
    for row in rows:
        output.append(
            {
                "ms": row["ms"],
                "tracks": json.loads(row["tracks_json"]),
                "frame": row["frame"],
                "ran_detector": bool(row["ran_detector"]),
                "created_at": row["created_at"],
            }
        )

    return jsonify({"job_id": job_id, "count": len(output), "results": output})


@app.route("/videos/processed", methods=["GET"])
def list_processed_videos():
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        return jsonify({"error": "invalid limit"}), 400

    limit = max(1, min(limit, 200))

    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT j.job_id, j.status, j.error_message, j.created_at, j.updated_at,
               COUNT(r.id) AS result_count,
               MAX(r.ms) AS last_ms
        FROM jobs j
        LEFT JOIN results r ON r.job_id = j.job_id
        WHERE j.status = 'done'
        GROUP BY j.job_id
        ORDER BY j.updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    videos = [dict(row) for row in rows]
    return jsonify({"count": len(videos), "videos": videos})


@app.route("/videos/active", methods=["GET"])
def list_active_videos():
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT job_id, status, created_at, updated_at
        FROM jobs
        WHERE status IN ('queued', 'waiting_viewers', 'processing')
        ORDER BY updated_at DESC
        """
    ).fetchall()
    conn.close()

    items = []
    for row in rows:
        item = dict(row)
        item["viewer_count"] = get_job_viewer_count(row["job_id"])
        items.append(item)

    with state_lock:
        live_streams = [
            {
                "stream_id": stream_id,
                "viewer_count": len(viewers),
                "latest_result": latest_live_results.get(stream_id),
            }
            for stream_id, viewers in live_viewers.items()
        ]

    return jsonify(
        {
            "job_count": len(items),
            "jobs": items,
            "live_stream_count": len(live_streams),
            "live_streams": live_streams,
        }
    )


@app.route("/live/frame", methods=["POST"])
def process_live_frame():
    if "image" not in request.files:
        return jsonify({"error": "no image"}), 400

    stream_id = request.form.get("stream_id", "default").strip() or "default"
    force_detect = request.form.get("force_detect", "0")
    viewer_count = get_live_viewer_count(stream_id)
    if viewer_count == 0:
        return jsonify(
            {
                "status": "ignored",
                "reason": "no_viewers",
                "stream_id": stream_id,
            }
        ), 202

    image = request.files["image"]
    image_bytes = image.read()
    if not image_bytes:
        return jsonify({"error": "empty image"}), 400

    try:
        detector_data = send_frame_to_detector(
            image_bytes,
            filename=image.filename or "live.jpg",
            data={"force_detect": force_detect},
        )
    except RequestException as e:
        return jsonify({"error": f"error enviando frame a detector: {e}"}), 502
    except ValueError as e:
        return jsonify({"error": f"respuesta JSON invalida del detector: {e}"}), 502

    result = {
        "stream_id": stream_id,
        "timestamp_ms": int(time.time() * 1000),
        "tracks": detector_data.get("tracks", []),
        "frame": detector_data.get("frame"),
        "ran_detector": detector_data.get("ran_detector", False),
    }

    with state_lock:
        latest_live_results[stream_id] = result

    socketio.emit("live_detection", result, room=room_name_for_live(stream_id))
    return jsonify({"status": "ok", "result": result})


@app.route("/live/streams/<stream_id>", methods=["GET"])
def get_live_stream(stream_id):
    with state_lock:
        latest = latest_live_results.get(stream_id)
    return jsonify(
        {
            "stream_id": stream_id,
            "viewer_count": get_live_viewer_count(stream_id),
            "latest_result": latest,
        }
    )


@app.route("/settings/fps", methods=["GET", "PUT", "PATCH"])
def manage_fps():
    if request.method == "GET":
        return jsonify({"target_fps": get_target_fps()})

    body = request.get_json(silent=True) or {}
    value = body.get("target_fps")

    try:
        value = float(value)
    except (TypeError, ValueError):
        return jsonify({"error": "target_fps debe ser numerico"}), 400

    if value <= 0 or value > 120:
        return jsonify({"error": "target_fps fuera de rango (0, 120]"}), 400

    set_target_fps(value)
    return jsonify({"status": "ok", "target_fps": get_target_fps()})


@app.route("/views/live-photo", methods=["GET"])
def live_photo_view():
    html = """
    <!doctype html>
    <html lang=\"es\"> 
    <head>
      <meta charset=\"utf-8\" />
      <title>Live Photo Sender</title>
    </head>
    <body style=\"font-family: sans-serif; max-width: 720px; margin: 2rem auto;\">
      <h1>Enviar foto en vivo</h1>
      <p>Esta vista envia imagenes al endpoint <code>/live/frame</code>.</p>
      <form action=\"/live/frame\" method=\"post\" enctype=\"multipart/form-data\">
        <label>stream_id <input type=\"text\" name=\"stream_id\" value=\"default\" /></label><br/><br/>
        <label>force_detect <input type=\"text\" name=\"force_detect\" value=\"1\" /></label><br/><br/>
        <input type=\"file\" name=\"image\" accept=\"image/*\" required /><br/><br/>
        <button type=\"submit\">Enviar foto</button>
      </form>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


# -----------------------------------
# 🎥 Procesamiento
# -----------------------------------
def process_video(job_id, path):
    set_job_status(job_id, "waiting_viewers")

    cap = cv2.VideoCapture(path)
    status_processing_set = False
    waiting_status_set = True

    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0:
            fps = 30

        frame_idx = 0

        while True:
            # No se procesa si nadie esta mirando este job.
            while get_job_viewer_count(job_id) == 0:
                status_processing_set = False
                if not waiting_status_set:
                    set_job_status(job_id, "waiting_viewers")
                    waiting_status_set = True
                time.sleep(WAIT_FOR_VIEWERS_SLEEP)

            if not status_processing_set:
                set_job_status(job_id, "processing")
                status_processing_set = True
                waiting_status_set = False

            ret, frame = cap.read()
            if not ret:
                break

            frame_interval = max(int(fps / get_target_fps()), 1)

            if frame_idx % frame_interval == 0:
                success, buffer = cv2.imencode(".jpg", frame)
                if not success:
                    frame_idx += 1
                    continue

                try:
                    data = send_frame_to_detector(buffer.tobytes(), filename="frame.jpg")
                except RequestException as e:
                    print(f"Error enviando frame a {PROCESS_URL}: {e}")
                    frame_idx += 1
                    continue
                except ValueError as e:
                    print(f"Respuesta JSON invalida desde {PROCESS_URL}: {e}")
                    frame_idx += 1
                    continue

                timestamp = int((frame_idx / fps) * 1000)

                result = {
                    "job_id": job_id,
                    "ms": timestamp,
                    "tracks": data.get("tracks", []),
                    "frame": data.get("frame"),
                    "ran_detector": data.get("ran_detector", False)
                }

                conn = get_db_connection()
                conn.execute(
                    """
                    INSERT INTO results (job_id, ms, tracks_json, frame, ran_detector)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        timestamp,
                        json.dumps(result["tracks"]),
                        result["frame"],
                        1 if result["ran_detector"] else 0,
                    ),
                )
                conn.commit()
                conn.close()

                # emitir por websocket solo al room del job
                socketio.emit("new_detection", result, room=job_id)

            frame_idx += 1

        set_job_status(job_id, "done")

        socketio.emit("finished", {"job_id": job_id, "status": "done"}, room=job_id)

    except Exception as e:
        set_job_status(job_id, "error", str(e))
        socketio.emit("finished", {"job_id": job_id, "status": "error", "error": str(e)}, room=job_id)
    finally:
        cap.release()


# -----------------------------------
# 🔌 WebSocket
# -----------------------------------
@socketio.on("connect")
def handle_connect():
    print("Cliente conectado")


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    remove_sid_from_all_subscriptions(sid)


@socketio.on("subscribe")
def handle_subscribe(data):
    if not isinstance(data, dict):
        return

    job_id = data.get("job_id")
    stream_id = data.get("stream_id")

    if not job_id and not stream_id:
        return

    sid = request.sid

    if job_id:
        join_room(job_id)
        add_subscription(sid, "job", job_id)
        socketio.emit(
            "subscribed",
            {"job_id": job_id, "viewer_count": get_job_viewer_count(job_id)},
            room=job_id,
        )

    if stream_id:
        live_room = room_name_for_live(stream_id)
        join_room(live_room)
        add_subscription(sid, "live", stream_id)
        socketio.emit(
            "live_subscribed",
            {"stream_id": stream_id, "viewer_count": get_live_viewer_count(stream_id)},
            room=live_room,
        )


@socketio.on("unsubscribe")
def handle_unsubscribe(data):
    if not isinstance(data, dict):
        return

    job_id = data.get("job_id")
    stream_id = data.get("stream_id")

    if not job_id and not stream_id:
        return

    sid = request.sid

    if job_id:
        leave_room(job_id)
        remove_subscription(sid, "job", job_id)

    if stream_id:
        leave_room(room_name_for_live(stream_id))
        remove_subscription(sid, "live", stream_id)


# -----------------------------------
if __name__ == "__main__":
    print("[stream-server] iniciado en 0.0.0.0:6000")
    socketio.run(app, host="0.0.0.0", port=6000)