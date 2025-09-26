# app.py
import os
import json
import threading
import time
from io import BytesIO
import mimetypes
from mimetypes import guess_type
from urllib.parse import unquote
from flask import Flask, render_template, send_file, jsonify, request, url_for, abort, Response,request
from dotenv import load_dotenv
from PIL import Image
import pillow_heif
import re

# register HEIF opener
pillow_heif.register_heif_opener()

from scanner import scan_folders_with_progress  # scanner will upsert to DB
import db as dbmod

load_dotenv()
FOLDERS = os.getenv('FOLDERS', '')
FOLDERS = [p.strip() for p in FOLDERS.split(',') if p.strip()]

THUMB_DIR = os.getenv('THUMB_DIR', './static/thumbnails')
THUMB_SIZE = int(os.getenv('THUMB_SIZE', '256'))
CACHE_FILE = os.getenv('CACHE_FILE', './file_index.json')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '8'))
ENABLE_FACE_DETECT = os.getenv('ENABLE_FACE_DETECT', '0') in ('1', 'true', 'True')
ENABLE_VIDEO_PROBE = os.getenv('ENABLE_VIDEO_PROBE', '0') in ('1','true','True')  # optional ffprobe for videos

app = Flask(__name__, static_folder='static', template_folder='templates')

# initialize DB
dbmod.init_db()

# near top of app.py (after imports)
IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'}
VIDEO_EXTS = {'.mp4', '.mov', '.avi', '.mkv', '.webm'}



# scan state shared between threads for SSE
SCAN_STATE = {
    'running': False,
    'done': 0,
    'total': 0,
    'stage': None,
    'last_update': None,
    'message': ''
}
SCAN_LOCK = threading.Lock()

def _progress_cb(payload):
    with SCAN_LOCK:
        SCAN_STATE.update(payload)
        SCAN_STATE['last_update'] = time.time()

def _background_scan():
    with SCAN_LOCK:
        SCAN_STATE.update({'running': True, 'stage': 'start', 'done': 0, 'total': 0, 'message': 'Scan started'})
    try:
        scan_folders_with_progress(
            FOLDERS, THUMB_DIR, thumb_size=(THUMB_SIZE, THUMB_SIZE),
            cache_file=CACHE_FILE, max_workers=MAX_WORKERS,
            enable_face_detect=ENABLE_FACE_DETECT, enable_video_probe=ENABLE_VIDEO_PROBE,
            progress_callback=_progress_cb
        )
        with SCAN_LOCK:
            SCAN_STATE.update({'running': False, 'stage': 'finished', 'message': 'Scan finished'})
    except Exception as e:
        with SCAN_LOCK:
            SCAN_STATE.update({'running': False, 'stage': 'error', 'message': str(e)})

@app.route('/')
def index():
    # fetch recent media from DB (images + video)
    all_files = dbmod.get_recent_media(limit=1000)

    out = []
    for f in all_files:
        ext = (f.get('ext') or '').lower()
        # fallback to DB category if ext missing
        category = f.get('category') or ''

        # canonicalize type using extension first (most reliable)
        if ext in IMAGE_EXTS:
            typ = 'images'
        elif ext in VIDEO_EXTS:
            typ = 'video'
        else:
            # fall back to DB category names if ext ambiguous
            cat_lower = (category or '').lower()
            if cat_lower.startswith('image') or cat_lower == 'images' or cat_lower == 'photo' or cat_lower == 'photos':
                typ = 'images'
            elif cat_lower.startswith('video') or cat_lower == 'video' or cat_lower == 'videos':
                typ = 'video'
            else:
                typ = 'other'

        out.append({
            'path': f['path'],
            'thumbnail': f.get('thumbnail'),
            'ext': ext or f.get('ext'),
            'type': typ
        })

    return render_template('index.html', all_files=out)


@app.route('/category/<cat>')
def view_category(cat):
    sort = request.args.get('sort', 'orig_time')
    desc = request.args.get('order', 'desc') != 'asc'
    order_by = 'COALESCE(orig_time, mtime, ctime)' if sort == 'orig_time' else sort
    rows = dbmod.get_files_by_category(cat, limit=None, order_by=order_by, desc=desc)

    # Normalization - ensure category/value exists and ext present
    files = []
    for r in rows:
        ext = (r.get('ext') or '').lower()
        files.append({
            'path': r.get('path'),
            'thumbnail': r.get('thumbnail'),
            'ext': ext,
            'name': r.get('name'),
            'rel_path': r.get('rel_path'),
            'category': r.get('category') or ('images' if ext in IMAGE_EXTS else ('video' if ext in VIDEO_EXTS else 'other'))
        })
    return render_template('category.html', cat=cat, files=files, subcats={})

# helper: make absolute-safe path check
def _is_path_allowed(path):
    # allow if path is inside any configured folder (resolve symlinks)
    try:
        path = os.path.abspath(path)
        for root in FOLDERS:
            if not root:
                continue
            root_abs = os.path.abspath(root)
            if path.startswith(root_abs + os.sep) or path == root_abs:
                return True
        return False
    except Exception:
        return False

# helper generator: yield whole file in chunks (no range)
def _read_file_stream(path, chunk_size=64*1024):
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk

# helper: generator that yields file chunks
def _read_file_range(path, start, length=None, chunk_size=64*1024):
    with open(path, 'rb') as f:
        f.seek(start)
        remaining = length
        while True:
            if remaining is None:
                chunk = f.read(chunk_size)
            else:
                to_read = min(chunk_size, remaining)
                if to_read <= 0:
                    break
                chunk = f.read(to_read)
                remaining -= len(chunk)
            if not chunk:
                break
            yield chunk



# Replace view_file route so video files open viewer page (not direct file)
@app.route('/file/view')
def view_file():
    """
    Show a viewer page for images/videos, or stream/download other files.
    Query params:
      - path: filesystem path to the file (required)
      - download=1 : force download (Content-Disposition: attachment)
    """
    path = request.args.get('path')
    if not path:
        return "File not specified", 400

    # unquote in case the path contains spaces or special chars encoded in URLs
    path = unquote(path)

    # Security: ensure path exists
    if not os.path.exists(path):
        return "File not found", 404

    # Security: ensure file is under allowed folders
    if not _is_path_allowed(path):
        return "Access denied", 403

    # If download requested, force attachment download
    if request.args.get('download') in ('1', 'true', 'True'):
        try:
            return send_file(path, as_attachment=True)
        except Exception as e:
            # fallback: return error
            return f"Download error: {e}", 500

    # determine extension and mime type
    ext = os.path.splitext(path)[1].lower()
    mime_type, _ = guess_type(path)
    if not mime_type:
        if ext == '.mov':
            # common mapping for QuickTime MOV
            mime_type = 'video/quicktime'
        else:
            mime_type = 'application/octet-stream'

    # attempt to read DB metadata (orig_time, etc.) if available
    orig_time = None
    try:
        meta = dbmod.get_file(path)
        if meta:
            # meta['orig_time'] may be None; pass through
            orig_time = meta.get('orig_time')
    except Exception:
        orig_time = None

    

    if ext in IMAGE_EXTS:
        # render viewer that embeds an <img> or uses /media to convert HEIC->JPEG
        return render_template('viewer.html', file_path=path, is_video=False, mime_type=mime_type, orig_time=orig_time)

    if ext in VIDEO_EXTS:
        # render viewer with HTML5 video that will stream from /media (supports Range or no_range)
        return render_template('viewer.html', file_path=path, is_video=True, mime_type=mime_type, orig_time=orig_time)

    # For other files (documents, audio, etc.) stream directly (inline if browser supports)
    try:
        return send_file(path, as_attachment=False)
    except Exception as e:
        return f"Error serving file: {e}", 500

# Replace /media route with range-supporting streaming
@app.route('/media')
def media():
    """
    Serve media files. By default supports Range requests (206 Partial Content).
    If client passes ?no_range=1 it will stream the entire file (200 OK) and ignore Range headers.
    """
    path = request.args.get('path')
    if not path:
        abort(404)
    path = unquote(path)
    if not os.path.exists(path):
        abort(404)
    if not _is_path_allowed(path):
        abort(403)

    # HEIC image on-the-fly convert (unchanged)
    _, ext = os.path.splitext(path)
    ext = ext.lower()
    if ext in ('.heic', '.heif'):
        try:
            with Image.open(path) as im:
                if im.mode in ("RGBA", "P"):
                    im = im.convert("RGB")
                buf = BytesIO()
                im.save(buf, format='JPEG', quality=90)
                buf.seek(0)
                return send_file(buf, mimetype='image/jpeg')
        except Exception:
            pass

    # Determine mime type (fallback for .mov -> video/quicktime)
    mime_type, _ = mimetypes.guess_type(path)
    if not mime_type:
        if ext == '.mov':
            mime_type = 'video/quicktime'
        else:
            mime_type = 'application/octet-stream'

    file_size = os.path.getsize(path)

    # If client explicitly requests no_range, always return full file (200)
    no_range_flag = request.args.get('no_range', '0') in ('1', 'true', 'True')

    # If no_range requested: stream full file as 200 with Content-Length
    if no_range_flag:
        headers = {
            'Content-Length': str(file_size),
            'Content-Type': mime_type,
            'Accept-Ranges': 'none',   # indicate we are not supporting range for this response
            'Cache-Control': 'public, max-age=86400'
        }
        return Response(_read_file_stream(path), headers=headers)

    # Otherwise, honor Range headers (normal behavior)
    range_header = request.headers.get('Range', None)
    headers_common = {
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'public, max-age=86400'
    }

    if range_header:
        # parse Range header: bytes=start-end
        m = re.match(r'bytes=(\d*)-(\d*)', range_header)
        if not m:
            return Response(status=416)
        gstart, gend = m.groups()
        try:
            if gstart == '':
                # suffix range: last N bytes
                suffix_len = int(gend)
                start = max(0, file_size - suffix_len)
                end = file_size - 1
            else:
                start = int(gstart)
                end = int(gend) if gend else file_size - 1
        except Exception:
            return Response(status=416)

        if start >= file_size:
            return Response(status=416)

        end = min(end, file_size - 1)
        length = end - start + 1
        headers = dict(headers_common)
        headers.update({
            'Content-Range': f'bytes {start}-{end}/{file_size}',
            'Content-Length': str(length),
            'Content-Type': mime_type
        })
        return Response(_read_file_range(path, start, length), status=206, headers=headers)

    # No Range header and no_range_flag False: stream full file with Content-Length (200)
    headers = dict(headers_common)
    headers.update({
        'Content-Length': str(file_size),
        'Content-Type': mime_type
    })
    return Response(_read_file_stream(path), headers=headers)

@app.route('/refresh', methods=['POST'])
def refresh():
    # start background scan if not already running
    with SCAN_LOCK:
        if SCAN_STATE.get('running'):
            return jsonify(success=False, message='Scan already running'), 409
        SCAN_STATE.update({'running': True, 'stage': 'queued', 'message': 'Queued to start', 'done': 0, 'total': 0})
    t = threading.Thread(target=_background_scan, daemon=True)
    t.start()
    return jsonify(success=True, message='Scan started')

def sse_format(event_name, data):
    return f"event: {event_name}\ndata: {json.dumps(data)}\n\n"

@app.route('/scan/stream')
def scan_stream():
    def gen():
        last_sent = {}
        while True:
            with SCAN_LOCK:
                state = dict(SCAN_STATE)
            if state != last_sent:
                yield sse_format("progress", state)
                last_sent = state
            if not state.get('running') and state.get('stage') in ('finished', 'error'):
                break
            time.sleep(0.5)
        with SCAN_LOCK:
            yield sse_format("progress", dict(SCAN_STATE))
    return Response(gen(), mimetype='text/event-stream')

@app.route('/thumbnail/<path:thumb_rel>')
def thumbnail(thumb_rel):
    tpath = os.path.join(THUMB_DIR, thumb_rel)
    if not os.path.exists(tpath):
        return "No Thumb", 404
    return send_file(tpath, mimetype='image/jpeg')

if __name__ == '__main__':
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '5005'))
    app.run(debug=True, host=host, port=port)
