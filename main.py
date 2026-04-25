import cv2
import json
import os
import sqlite3
import threading
import uuid
import requests

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, join_room, leave_room

# -----------------------------------
# CONFIG
# -----------------------------------
PROCESS_URL = "http://raspberrypi-1:5000/process"
TARGET_FPS = 2  # frames por segundo
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "results.db")
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")

# -----------------------------------
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


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


# -----------------------------------
# 🎥 Procesamiento
# -----------------------------------
def process_video(job_id, path):
    conn = get_db_connection()
    conn.execute(
        "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
        ("processing", job_id),
    )
    conn.commit()
    conn.close()

    cap = cv2.VideoCapture(path)

    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0:
            fps = 30

        frame_interval = max(int(fps / TARGET_FPS), 1)

        frame_idx = 0

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_idx % frame_interval == 0:
                success, buffer = cv2.imencode(".jpg", frame)
                if not success:
                    frame_idx += 1
                    continue

                files = {
                    "image": ("frame.jpg", buffer.tobytes(), "image/jpeg")
                }

                try:
                    res = requests.post(PROCESS_URL, files=files, timeout=30)
                    res.raise_for_status()
                    data = res.json()
                except Exception as e:
                    print("Error enviando frame:", e)
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

        conn = get_db_connection()
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
            ("done", job_id),
        )
        conn.commit()
        conn.close()

        socketio.emit("finished", {"job_id": job_id, "status": "done"}, room=job_id)

    except Exception as e:
        conn = get_db_connection()
        conn.execute(
            "UPDATE jobs SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
            ("error", str(e), job_id),
        )
        conn.commit()
        conn.close()
        socketio.emit("finished", {"job_id": job_id, "status": "error", "error": str(e)}, room=job_id)
    finally:
        cap.release()


# -----------------------------------
# 🔌 WebSocket
# -----------------------------------
@socketio.on("connect")
def handle_connect():
    print("Cliente conectado")


@socketio.on("subscribe")
def handle_subscribe(data):
    if not isinstance(data, dict):
        return

    job_id = data.get("job_id")
    if not job_id:
        return

    join_room(job_id)
    socketio.emit("subscribed", {"job_id": job_id}, room=job_id)


@socketio.on("unsubscribe")
def handle_unsubscribe(data):
    if not isinstance(data, dict):
        return

    job_id = data.get("job_id")
    if not job_id:
        return

    leave_room(job_id)


# -----------------------------------
if __name__ == "__main__":
    print("[stream-server] iniciado en 0.0.0.0:6000")
    socketio.run(app, host="0.0.0.0", port=6000)