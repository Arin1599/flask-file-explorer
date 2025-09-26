# db.py
import sqlite3
import os
from typing import List, Dict, Any, Optional

DB_PATH = os.getenv('INDEX_DB', './file_index.db')

SCHEMA = """
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    folder_root TEXT,
    rel_path TEXT,
    name TEXT,
    ext TEXT,
    category TEXT,
    image_subcat TEXT,
    size INTEGER,
    mtime REAL,
    ctime REAL,
    orig_time REAL,
    thumbnail TEXT,
    scanned_at REAL
);
CREATE INDEX IF NOT EXISTS idx_category ON files(category);
CREATE INDEX IF NOT EXISTS idx_mtime ON files(mtime);
CREATE INDEX IF NOT EXISTS idx_ctime ON files(ctime);
CREATE INDEX IF NOT EXISTS idx_orig ON files(orig_time);
CREATE INDEX IF NOT EXISTS idx_scanned ON files(scanned_at);
"""

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.execute('PRAGMA foreign_keys = ON;')
    conn.execute('PRAGMA journal_mode = WAL;')
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(SCHEMA)
    conn.commit()
    conn.close()

def upsert_files(entries: List[Dict[str, Any]]):
    if not entries:
        return
    conn = get_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO files (path, folder_root, rel_path, name, ext, category, image_subcat, size, mtime, ctime, orig_time, thumbnail, scanned_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
        folder_root=excluded.folder_root,
        rel_path=excluded.rel_path,
        name=excluded.name,
        ext=excluded.ext,
        category=excluded.category,
        image_subcat=excluded.image_subcat,
        size=excluded.size,
        mtime=excluded.mtime,
        ctime=excluded.ctime,
        orig_time=excluded.orig_time,
        thumbnail=excluded.thumbnail,
        scanned_at=excluded.scanned_at
    ;
    """
    params = []
    for e in entries:
        params.append((
            e.get('path'),
            e.get('folder_root'),
            e.get('rel_path'),
            e.get('name'),
            e.get('ext'),
            e.get('category'),
            e.get('image_subcat'),
            e.get('size'),
            e.get('mtime'),
            e.get('ctime'),
            e.get('orig_time'),
            e.get('thumbnail'),
            e.get('scanned_at'),
        ))
    try:
        cur.executemany(sql, params)
        conn.commit()
    finally:
        conn.close()

def delete_missing_files(seen_paths: List[str]):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_seen(path TEXT PRIMARY KEY);")
    cur.execute("DELETE FROM tmp_seen;")
    if seen_paths:
        cur.executemany("INSERT OR IGNORE INTO tmp_seen(path) VALUES (?)", [(p,) for p in seen_paths])
        cur.execute("DELETE FROM files WHERE path NOT IN (SELECT path FROM tmp_seen);")
    conn.commit()
    conn.close()

def get_category_counts() -> Dict[str,int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT category, COUNT(*) FROM files GROUP BY category;")
    rows = cur.fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}

def get_files_by_category(category: str, limit:int=None, order_by:str='COALESCE(orig_time, mtime, ctime)', desc:bool=True) -> List[Dict[str,Any]]:
    conn = get_conn()
    cur = conn.cursor()
    q = f"SELECT path, folder_root, rel_path, name, ext, category, image_subcat, size, mtime, ctime, orig_time, thumbnail FROM files WHERE category=?"
    if order_by:
        q += f" ORDER BY {order_by} {'DESC' if desc else 'ASC'}"
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q, (category,))
    rows = cur.fetchall()
    conn.close()
    keys = ['path','folder_root','rel_path','name','ext','category','image_subcat','size','mtime','ctime','orig_time','thumbnail']
    return [dict(zip(keys, r)) for r in rows]

def get_recent_media(limit:int=500) -> List[Dict[str,Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT path, folder_root, rel_path, name, ext, category, image_subcat, size, mtime, ctime, orig_time, thumbnail FROM files WHERE category IN ('images','video') ORDER BY COALESCE(orig_time, mtime, ctime) DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    keys = ['path','folder_root','rel_path','name','ext','category','image_subcat','size','mtime','ctime','orig_time','thumbnail']
    return [dict(zip(keys, r)) for r in rows]

def get_file(path: str) -> Optional[Dict[str,Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, path, folder_root, rel_path, name, ext, category, image_subcat, size, mtime, ctime, orig_time, thumbnail, scanned_at FROM files WHERE path=?", (path,))
    row = cur.fetchone()
    cols = ['id','path','folder_root','rel_path','name','ext','category','image_subcat','size','mtime','ctime','orig_time','thumbnail','scanned_at']
    conn.close()
    if not row:
        return None
    return dict(zip(cols, row))
