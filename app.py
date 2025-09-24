import os
import json
import threading
import time
from flask import Flask, render_template, send_file, jsonify, request, url_for, abort, Response
from dotenv import load_dotenv
from scanner import scan_folders_with_progress

load_dotenv()
FOLDERS = os.getenv('FOLDERS', '')
FOLDERS = [p.strip() for p in FOLDERS.split(',') if p.strip()]

THUMB_DIR = os.getenv('THUMB_DIR', './static/thumbnails')
THUMB_SIZE = int(os.getenv('THUMB_SIZE', '256'))
CACHE_FILE = os.getenv('CACHE_FILE', './file_index.json')
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '8'))
ENABLE_FACE_DETECT = os.getenv('ENABLE_FACE_DETECT', '0') in ('1', 'true', 'True')

app = Flask(__name__, static_folder='static', template_folder='templates')

# Load cache if exists
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None

INDEX = load_cache() or {'scanned_at': 0, 'folders': FOLDERS, 'categories': {}}

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
    global INDEX
    with SCAN_LOCK:
        SCAN_STATE.update({'running': True, 'stage': 'start', 'done': 0, 'total': 0, 'message': 'Scan started'})
    try:
        idx = scan_folders_with_progress(
            FOLDERS, THUMB_DIR, thumb_size=(THUMB_SIZE, THUMB_SIZE),
            cache_file=CACHE_FILE, max_workers=MAX_WORKERS,
            enable_face_detect=ENABLE_FACE_DETECT, progress_callback=_progress_cb
        )
        INDEX = idx
        with SCAN_LOCK:
            SCAN_STATE.update({'running': False, 'stage': 'finished', 'message': 'Scan finished'})
    except Exception as e:
        with SCAN_LOCK:
            SCAN_STATE.update({'running': False, 'stage': 'error', 'message': str(e)})

@app.route('/')
def index():
    global INDEX
    INDEX = load_cache() or INDEX
    # Collect all images and videos for Pinterest-style grid
    all_files = []
    for k, v in INDEX.get('categories', {}).items():
        if k in ('images', 'video'):
            for f in v:
                all_files.append({
                    'path': f['path'],
                    'thumbnail': f.get('thumbnail'),
                    'ext': f.get('ext'),
                    'type': k
                })
    # Sort by mtime descending (most recent first)
    all_files.sort(key=lambda x: os.path.getmtime(x['path']) if os.path.exists(x['path']) else 0, reverse=True)
    return render_template('index.html', all_files=all_files)

@app.route('/category/<cat>')
def view_category(cat):
    global INDEX
    INDEX = load_cache() or INDEX
    files = INDEX.get('categories', {}).get(cat, [])
    subcats = {}
    if cat == 'images':
        for f in files:
            sc = f.get('image_subcat', 'other')
            subcats.setdefault(sc, []).append(f)
    return render_template('category.html', cat=cat, files=files, subcats=subcats)

@app.route('/file/view')
def view_file():
    path = request.args.get('path')
    if not path or not os.path.exists(path):
        return "File not found", 404
    ext = os.path.splitext(path)[1].lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff']:
        return render_template('viewer.html', file_path=path)
    return send_file(path, as_attachment=False)

@app.route('/media')
def media():
    path = request.args.get('path')
    if not path or not os.path.exists(path):
        abort(404)

    _, ext = os.path.splitext(path)
    ext = ext.lower()

    # If HEIC/HEIF, convert to JPEG in-memory and serve
    if ext in ('.heic', '.heif'):
        try:
            # Pillow will open HEIC because pillow_heif.register_heif_opener() was called
            with Image.open(path) as im:
                # Optionally convert to RGB if needed
                if im.mode in ("RGBA", "P"):
                    im = im.convert("RGB")
                buf = BytesIO()
                im.save(buf, format='JPEG', quality=90)
                buf.seek(0)
                return send_file(buf, mimetype='image/jpeg')
        except Exception as e:
            # fallback: try to send raw file (may not render)
            print("HEIC->JPEG conversion error:", e)
            return send_file(path)

    # For normal images and other files
    return send_file(path)

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
                state = dict(SCAN_STATE)  # snapshot
            # send only if changed (simple)
            if state != last_sent:
                yield sse_format("progress", state)
                last_sent = state
            if not state.get('running') and state.get('stage') in ('finished', 'error'):
                break
            time.sleep(0.5)
        # final state
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
    app.run(debug=True, port=5000)


THUMB_DIR = os.getenv('THUMB_DIR', './static/thumbnails')
THUMB_SIZE = int(os.getenv('THUMB_SIZE', '256'))
CACHE_FILE = os.getenv('CACHE_FILE', './file_index.json')

app = Flask(__name__, static_folder='static', template_folder='templates')

# load existing cache if present
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None

INDEX = load_cache()
if not INDEX:
    INDEX = scan_folders(FOLDERS, THUMB_DIR, thumb_size=(THUMB_SIZE, THUMB_SIZE), cache_file=CACHE_FILE)

@app.route('/')
def index():
    cats = {k: len(v) for k, v in INDEX.get('categories', {}).items()}
    return render_template('index.html', categories=cats)

@app.route('/category/<cat>')
def view_category(cat):
    files = INDEX.get('categories', {}).get(cat, [])
    subcats = {}
    if cat == 'images':
        for f in files:
            sc = f.get('image_subcat', 'other')
            subcats.setdefault(sc, []).append(f)
    return render_template('category.html', cat=cat, files=files, subcats=subcats)

@app.route('/file/view')
def view_file():
    # query args: path (absolute or relative as stored)
    path = request.args.get('path')
    if not path or not os.path.exists(path):
        return "File not found", 404
    # always stream the file via Flask send_file (avoids browser file:// issues)
    # for images show a simple viewer template that uses our /media route to stream
    ext = os.path.splitext(path)[1].lower()
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff']:
        # show viewer page that embeds /media?path=<...>
        return render_template('viewer.html', file_path=path)
    return send_file(path, as_attachment=False)

@app.route('/media')
def media():
    # serve arbitrary media/file path safely (path param must exist on disk)
    path = request.args.get('path')
    if not path or not os.path.exists(path):
        abort(404)
    # Note: In a production server you should validate that the path is within allowed folders
    return send_file(path)

@app.route('/refresh', methods=['POST'])
def refresh():
    global INDEX
    INDEX = scan_folders(FOLDERS, THUMB_DIR, thumb_size=(THUMB_SIZE, THUMB_SIZE), cache_file=CACHE_FILE)
    return jsonify(success=True, scanned_at=INDEX.get('scanned_at'))

@app.route('/thumbnail/<path:thumb_rel>')
def thumbnail(thumb_rel):
    tpath = os.path.join(THUMB_DIR, thumb_rel)
    if not os.path.exists(tpath):
        return "No Thumb", 404
    return send_file(tpath, mimetype='image/jpeg')

if __name__ == '__main__':
    app.run(debug=True,host="0.0.0.0", port=5005)
