# scanner.py
import os
import json
import time
import re
import subprocess
from pathlib import Path
from datetime import datetime
from PIL import Image, ImageOps
import piexif
import cv2
from concurrent.futures import ThreadPoolExecutor, as_completed

import pillow_heif
pillow_heif.register_heif_opener()

# local DB module (unchanged)
import db as dbmod

# Extensions mapping
EXT_MAP = {
    'images': {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.heif', '.tiff'},
    'documents': {'.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xlsx', '.xls'},
    'text': {'.txt', '.md', '.csv', '.log', '.json', '.xml'},
    'audio': {'.mp3', '.wav', '.flac', '.m4a', '.aac'},
    'video': {'.mp4', '.mov', '.avi', '.mkv', '.webm'},
}

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
    """Always attempts to create thumbnail from src_path -> dest_path"""
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

def make_video_thumbnail(src_path, dest_path, size=(256,256)):
    """Always attempts to create a video thumbnail by reading first frame"""
    try:
        cap = cv2.VideoCapture(src_path)
        success, frame = cap.read()
        cap.release()
        if not success or frame is None:
            return False
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        img = Image.fromarray(frame)
        img.thumbnail(size)
        thumb = ImageOps.fit(img, size, Image.LANCZOS)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        thumb.save(dest_path, format='JPEG', quality=85)
        return True
    except Exception:
        return False

def thumbnail_needs_update(src_path, thumb_path):
    """
    Return True if thumbnail should be (re)generated:
      - thumbnail does not exist
      - OR source mtime > thumbnail mtime
    """
    if not os.path.exists(thumb_path):
        return True
    try:
        src_m = os.path.getmtime(src_path)
        thumb_m = os.path.getmtime(thumb_path)
        return src_m > thumb_m + 0.1  # small tolerance
    except Exception:
        return True

# ---------- Original time extraction helpers (unchanged) ----------
EXIF_DATETIME_FORMAT = "%Y:%m:%d %H:%M:%S"

def parse_exif_datetime(s: str):
    if not s:
        return None
    s = s.strip()
    try:
        dt = datetime.strptime(s, EXIF_DATETIME_FORMAT)
        return dt.timestamp()
    except Exception:
        try:
            dt = datetime.fromisoformat(s)
            return dt.timestamp()
        except Exception:
            try:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                return dt.timestamp()
            except Exception:
                return None

def get_exif_dates(path):
    try:
        im = Image.open(path)
        try:
            exif = piexif.load(path)
        except Exception:
            exif = {}
        dto = None
        dtd = None
        if exif:
            exif_ifd = exif.get('Exif', {})
            if exif_ifd:
                raw = exif_ifd.get(piexif.ExifIFD.DateTimeOriginal) or exif_ifd.get(piexif.ExifIFD.DateTimeDigitized)
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode('utf-8', errors='ignore')
                    dto = parse_exif_datetime(raw)
                d_raw = exif_ifd.get(piexif.ExifIFD.DateTimeDigitized)
                if d_raw:
                    if isinstance(d_raw, bytes):
                        d_raw = d_raw.decode('utf-8', errors='ignore')
                    dtd = parse_exif_datetime(d_raw)
        if not dto:
            try:
                exif_dict = getattr(im, '_getexif', lambda: {})() or {}
                raw = exif_dict.get(36867) or exif_dict.get(306)
                if raw:
                    dto = parse_exif_datetime(raw)
            except Exception:
                pass
        return (dto, dtd)
    except Exception:
        return (None, None)

def get_video_creation_time_ffprobe(path):
    try:
        cmd = [
            "ffprobe", "-v", "error", "-print_format", "json",
            "-show_entries", "format_tags=creation_time:format_tags=com.apple.quicktime.creation_date",
            path
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=6)
        if proc.returncode != 0:
            return None
        import json
        data = json.loads(proc.stdout)
        tags = (data.get('format') or {}).get('tags', {}) or {}
        for key in ('creation_time', 'com.apple.quicktime.creation_date'):
            if key in tags:
                ts = tags[key]
                s = ts.rstrip('Z')
                try:
                    dt = datetime.fromisoformat(s)
                    return dt.timestamp()
                except Exception:
                    try:
                        dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                        return dt.timestamp()
                    except Exception:
                        pass
        return None
    except Exception:
        return None

def get_original_time(path, ext, enable_video_probe=False):
    ext = ext.lower()
    img_exts = {'.jpg', '.jpeg', '.heic', '.heif', '.tiff'}
    video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.webm'}
    if ext in img_exts:
        dto, dtd = get_exif_dates(path)
        return dto or dtd
    if ext in video_exts and enable_video_probe:
        vt = get_video_creation_time_ffprobe(path)
        return vt
    return None
# ---------- end orig time helpers ----------

def _collect_all_files(folders):
    all_files = []
    for folder in folders:
        if not os.path.exists(folder):
            continue
        for root, dirs, files in os.walk(folder):
            for fn in files:
                full = os.path.join(root, fn)
                all_files.append((folder, full, fn))
    return all_files

def scan_folders_with_progress(folders, thumb_dir, thumb_size=(256,256), cache_file=None,
                               max_workers=8, enable_face_detect=False, enable_video_probe=False, progress_callback=None):
    """
    Scans folders using a thread pool and reports progress.
    Thumbnails are (re)generated only when missing or source is newer than thumbnail.
    Screenshot/selfie categorization is currently disabled.
    """
    start = time.time()
    index = {
        'scanned_at': time.time(),
        'folders': folders,
        'categories': {}
    }

    all_files = _collect_all_files(folders)
    total = len(all_files)
    if progress_callback:
        progress_callback({'stage':'collected','total':total})
    else:
        print(f"[scanner] collected {total} files to process")

    if total == 0:
        if cache_file:
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(index, f, indent=2)
            except Exception:
                pass
        return index

    def process_one(args):
        folder, full, fn = args
        ext = os.path.splitext(fn)[1].lower()
        cat = classify_by_ext(ext)
        try:
            rel = os.path.relpath(full, folder)
        except Exception:
            rel = fn
        size = os.path.getsize(full) if os.path.exists(full) else 0
        mtime = os.path.getmtime(full) if os.path.exists(full) else 0
        ctime = os.path.getctime(full) if os.path.exists(full) else mtime

        entry = {
            'path': full,
            'name': fn,
            'ext': ext,
            'folder_root': folder,
            'rel_path': rel,
            'size': size,
            'mtime': mtime,
            'ctime': ctime
        }

        # original capture time (EXIF or optional ffprobe for videos)
        try:
            orig = get_original_time(full, ext, enable_video_probe=enable_video_probe)
        except Exception:
            orig = None
        entry['orig_time'] = orig

        # Thumbnail logic: only create thumbnail if missing or source newer than thumb
        if cat == 'images':
            folder_key = sanitize_folder_key(folder)
            safe_rel = rel.replace(os.sep, '_').replace('..', '_')
            thumb_rel = os.path.join(folder_key, safe_rel + '.jpg')
            thumb_path = os.path.join(thumb_dir, thumb_rel)
            if thumbnail_needs_update(full, thumb_path):
                make_thumbnail(full, thumb_path, size=thumb_size)
            entry['thumbnail'] = thumb_rel
            # image_subcat removed for now
            entry['image_subcat'] = None
        elif cat == 'video':
            folder_key = sanitize_folder_key(folder)
            safe_rel = rel.replace(os.sep, '_').replace('..', '_')
            thumb_rel = os.path.join(folder_key, safe_rel + '.jpg')
            thumb_path = os.path.join(thumb_dir, thumb_rel)
            if thumbnail_needs_update(full, thumb_path):
                make_video_thumbnail(full, thumb_path, size=thumb_size)
            entry['thumbnail'] = thumb_rel
            entry['image_subcat'] = None
        else:
            entry['thumbnail'] = None
            entry['image_subcat'] = None

        return (cat, entry)

    # process files and upsert to DB in batches
    done = 0
    BATCH_SIZE = 500
    batch = []
    seen_paths = []

    with ThreadPoolExecutor(max_workers=max_workers) as exc:
        future_to_item = {exc.submit(process_one, args): args for args in all_files}
        for fut in as_completed(future_to_item):
            try:
                cat, entry = fut.result()
                index['categories'].setdefault(cat, []).append(entry)
                row = {
                    'path': entry['path'],
                    'folder_root': entry['folder_root'],
                    'rel_path': entry['rel_path'],
                    'name': entry['name'],
                    'ext': entry['ext'],
                    'category': cat,
                    'image_subcat': None,
                    'size': entry.get('size'),
                    'mtime': entry.get('mtime'),
                    'ctime': entry.get('ctime'),
                    'orig_time': entry.get('orig_time'),
                    'thumbnail': entry.get('thumbnail'),
                    'scanned_at': time.time()
                }
                batch.append(row)
                seen_paths.append(entry['path'])
            except Exception:
                pass

            done += 1

            if len(batch) >= BATCH_SIZE:
                try:
                    dbmod.upsert_files(batch)
                except Exception:
                    pass
                batch = []

            if progress_callback:
                progress_callback({'stage':'processing','done':done,'total':total})
            else:
                if done % 50 == 0 or done == total:
                    print(f"[scanner] processed {done}/{total}")

    # flush last batch
    if batch:
        try:
            dbmod.upsert_files(batch)
        except Exception:
            pass

    # cleanup DB entries for files removed from disk
    try:
        dbmod.delete_missing_files(seen_paths)
    except Exception:
        pass

    # optionally write JSON cache as small summary
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
