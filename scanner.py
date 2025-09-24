# scanner.py
import os
import json
import time
import re
from pathlib import Path
from PIL import Image, ImageOps
import piexif
import cv2
from concurrent.futures import ThreadPoolExecutor, as_completed

import pillow_heif

# register pillow-heif plugin so PIL.Image.open can read HEIC/HEIF files
pillow_heif.register_heif_opener()

# Extensions mapping
EXT_MAP = {
    'images': {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.tiff'},
    'documents': {'.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xlsx', '.xls'},
    'text': {'.txt', '.md', '.csv', '.log', '.json', '.xml'},
    'audio': {'.mp3', '.wav', '.flac', '.m4a', '.aac'},
    'video': {'.mp4', '.mov', '.avi', '.mkv', '.webm'},
}

_face_cascade = None
def get_face_cascade():
    global _face_cascade
    if _face_cascade is None:
        cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        _face_cascade = cv2.CascadeClassifier(cascade_path)
    return _face_cascade

def classify_by_ext(ext):
    ext = ext.lower()
    for k, s in EXT_MAP.items():
        if ext in s:
            return k
    return 'others'

def sanitize_folder_key(folder_path):
    key = folder_path.replace(os.sep, '_')
    key = re.sub(r'[:\s\\/]+', '_', key)
    key = re.sub(r'[^A-Za-z0-9_\-\.]', '', key)
    if not key:
        key = "folder"
    return key

def read_exif(path):
    try:
        exif = piexif.load(path)
        return exif
    except Exception:
        return None

def make_thumbnail(src_path, dest_path, size=(256,256)):
    try:
        with Image.open(src_path) as img:
            img.thumbnail(size)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            thumb = ImageOps.fit(img, size, Image.LANCZOS)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            thumb.save(dest_path, format='JPEG', quality=85)
            return True
    except Exception:
        return False

# --- Video thumbnail generation ---
def make_video_thumbnail(src_path, dest_path, size=(256,256)):
    try:
        import cv2
        cap = cv2.VideoCapture(src_path)
        success, frame = cap.read()
        cap.release()
        if not success or frame is None:
            return False
        # Resize and save as JPEG
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        from PIL import Image
        img = Image.fromarray(frame)
        img.thumbnail(size)
        thumb = ImageOps.fit(img, size, Image.LANCZOS)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        thumb.save(dest_path, format='JPEG', quality=85)
        return True
    except Exception:
        return False

def _np_from_file(path):
    import numpy as np
    with open(path, 'rb') as f:
        arr = np.asarray(bytearray(f.read()), dtype=np.uint8)
    return arr

def detect_faces(path):
    try:
        import numpy as np
        arr = _np_from_file(path)
        img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return 0
        casc = get_face_cascade()
        faces = casc.detectMultiScale(img, scaleFactor=1.1, minNeighbors=4, minSize=(30,30))
        return len(faces)
    except Exception:
        return 0

def image_subcategory(path, enable_face_detect=False):
    exif = read_exif(path)
    filename = os.path.basename(path).lower()
    if 'screenshot' in filename or 'screen_shot' in filename or 'screen-shot' in filename:
        return 'screenshots'
    if not exif or len(exif.get('0th', {})) == 0:
        try:
            with Image.open(path) as img:
                w, h = img.size
                ratio = max(w, h) / (min(w, h) + 1e-9)
                if max(w, h) >= 1000 and ratio > 1.4:
                    return 'screenshots'
        except Exception:
            pass
    if enable_face_detect:
        try:
            faces = detect_faces(path)
            if faces > 0:
                return 'selfies'
        except Exception:
            pass
    if exif and len(exif.get('0th', {})) > 0:
        return 'camera'
    return 'other'

def _collect_all_files(folders):
    """
    Returns list of (folder_root, full_path, filename)
    """
    all_files = []
    for folder in folders:
        if not os.path.exists(folder):
            # will be reported upstream
            continue
        for root, dirs, files in os.walk(folder):
            for fn in files:
                full = os.path.join(root, fn)
                all_files.append((folder, full, fn))
    return all_files

def scan_folders_with_progress(folders, thumb_dir, thumb_size=(256,256), cache_file=None,
                               max_workers=8, enable_face_detect=False, progress_callback=None):
    """
    Scans given folders using a thread pool and reports progress.
    - progress_callback is a callable receiving a dict payload, e.g. {'stage':'processing','done':5,'total':100}
    """
    start = time.time()
    index = {
        'scanned_at': time.time(),
        'folders': folders,
        'categories': {}
    }

    # 1) collect all files first
    all_files = _collect_all_files(folders)
    total = len(all_files)
    if progress_callback:
        progress_callback({'stage':'collected','total':total})
    else:
        # terminal feedback: simple print
        print(f"[scanner] collected {total} files to process")

    # if zero return quickly
    if total == 0:
        if cache_file:
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(index, f, indent=2)
            except Exception:
                pass
        return index

    # worker function processes one file and returns category entry
    def process_one(args):
        folder, full, fn = args
        ext = os.path.splitext(fn)[1].lower()
        cat = classify_by_ext(ext)
        try:
            rel = os.path.relpath(full, folder)
        except Exception:
            rel = fn
        entry = {
            'path': full,
            'name': fn,
            'ext': ext,
            'folder_root': folder,
            'rel_path': rel,
            'size': os.path.getsize(full) if os.path.exists(full) else 0,
            'mtime': os.path.getmtime(full) if os.path.exists(full) else 0,
        }

        if cat == 'images':
            folder_key = sanitize_folder_key(folder)
            safe_rel = rel.replace(os.sep, '_').replace('..', '_')
            thumb_rel = os.path.join(folder_key, safe_rel + '.jpg')
            thumb_path = os.path.join(thumb_dir, thumb_rel)
            # create thumbnail (idempotent)
            if not os.path.exists(thumb_path):
                make_thumbnail(full, thumb_path, size=thumb_size)
            entry['thumbnail'] = thumb_rel
            entry['image_subcat'] = image_subcategory(full, enable_face_detect=enable_face_detect)
        elif cat == 'video':
            folder_key = sanitize_folder_key(folder)
            safe_rel = rel.replace(os.sep, '_').replace('..', '_')
            thumb_rel = os.path.join(folder_key, safe_rel + '.jpg')
            thumb_path = os.path.join(thumb_dir, thumb_rel)
            # create video thumbnail (idempotent)
            if not os.path.exists(thumb_path):
                make_video_thumbnail(full, thumb_path, size=thumb_size)
            entry['thumbnail'] = thumb_rel

        return (cat, entry)

    # 2) process files in parallel
    # Use ThreadPoolExecutor since operations are I/O bound (disk + PIL)
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as exc:
        future_to_item = {exc.submit(process_one, args): args for args in all_files}
        for fut in as_completed(future_to_item):
            try:
                cat, entry = fut.result()
                index['categories'].setdefault(cat, []).append(entry)
            except Exception:
                # ignore per-file errors
                pass
            done += 1
            if progress_callback:
                progress_callback({'stage':'processing','done':done,'total':total})
            else:
                if done % 50 == 0 or done == total:
                    print(f"[scanner] processed {done}/{total}")

    # 3) write cache
    if cache_file:
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(index, f, indent=2)
        except Exception:
            pass

    elapsed = time.time() - start
    if progress_callback:
        progress_callback({'stage':'done','total':total,'elapsed':elapsed})
    else:
        print(f"[scanner] done: {total} files in {elapsed:.1f}s")

    return index
