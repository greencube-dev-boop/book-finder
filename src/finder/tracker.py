"""SQLite tracker: remembers every saved file (by SHA-256) and every failed URL.

Lives on Google Drive at  OCR Engine/config/download_tracker.db  so dedup
survives across Colab sessions and runs. Google Drive's FUSE layer can
occasionally return "database is locked", so every statement runs through a
retry wrapper.
"""

import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    sha256        TEXT PRIMARY KEY,
    url           TEXT NOT NULL UNIQUE,
    filepath      TEXT,
    source        TEXT,
    media         TEXT,
    tag           TEXT,
    title         TEXT,
    ext           TEXT,
    size_bytes    INTEGER,
    downloaded_at TEXT
);
CREATE TABLE IF NOT EXISTS failures (
    url          TEXT PRIMARY KEY,
    source       TEXT,
    error        TEXT,
    attempts     INTEGER DEFAULT 1,
    last_attempt TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_tag ON files(tag);
CREATE INDEX IF NOT EXISTS idx_files_source ON files(source);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DownloadTracker:
    """Thread-safe tracker with an in-memory cache ("the array").

    Per the pipeline flow: the whole DB is loaded into memory once per run
    (all SHA-256 hashes, URLs and failure counters), so every dedup check is
    a RAM lookup — the Drive DB is only touched when a NEW file is recorded.
    All DB operations run under a re-entrant lock (SQLite connection + Drive
    FUSE are not thread-safe) with locked-retry on 'database is locked'."""

    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.db_path), timeout=60.0,
                                    check_same_thread=False)
        self.conn.execute("PRAGMA busy_timeout = 60000")
        self._retry(lambda: self.conn.executescript(SCHEMA))
        self.conn.commit()
        # in-memory arrays, filled by load_cache()
        self._shas = None          # set[str]
        self._urls = None          # set[str]
        self._fail_attempts = None # dict[str, int]

    # ------------------------------------------------------------------ core

    def _retry(self, fn, attempts: int = 6):
        """Run a DB operation under the lock, retrying on 'database is
        locked' (Drive FUSE)."""
        delay = 1.0
        for i in range(attempts):
            try:
                with self._lock:
                    return fn()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) and "busy" not in str(exc):
                    raise
                if i == attempts - 1:
                    raise
                time.sleep(delay)
                delay *= 1.7

    # ------------------------------------------------- in-memory array cache

    def load_cache(self):
        """Load the DB into memory: (n_hashes, n_urls, n_failed_urls)."""
        cur = self._retry(lambda: self.conn.execute("SELECT sha256 FROM files"))
        self._shas = {r[0] for r in cur.fetchall()}
        cur = self._retry(lambda: self.conn.execute("SELECT url FROM files"))
        self._urls = {r[0] for r in cur.fetchall()}
        cur = self._retry(
            lambda: self.conn.execute("SELECT url, attempts FROM failures"))
        self._fail_attempts = dict(cur.fetchall())
        return len(self._shas), len(self._urls), len(self._fail_attempts)

    def knows_sha(self, sha256: str) -> bool:
        """Is this content hash already saved? RAM lookup once cache loaded."""
        if self._shas is not None:
            return sha256 in self._shas
        return self.has_sha_db(sha256)

    def knows_url(self, url: str) -> bool:
        if self._urls is not None:
            return url in self._urls
        return self.has_url_db(url)

    def remember_sha(self, sha256: str):
        """Add a hash to the in-memory array (before the DB sync + move)."""
        if self._shas is not None:
            self._shas.add(sha256)

    def forget(self, sha256: str, url: str):
        """Roll back a record (the file move to Drive failed): drop the hash
        and URL from the array and delete the DB row so content can be
        retried on a later run."""
        if self._shas is not None:
            self._shas.discard(sha256)
        if self._urls is not None:
            self._urls.discard(url)

        def _del():
            self.conn.execute("DELETE FROM files WHERE sha256 = ?", (sha256,))
            self.conn.commit()

        self._retry(_del)

    # ---------------------------------------------------------------- queries

    def has_sha_db(self, sha256: str) -> bool:
        cur = self._retry(
            lambda: self.conn.execute("SELECT 1 FROM files WHERE sha256 = ?", (sha256,))
        )
        return cur.fetchone() is not None

    def has_url_db(self, url: str) -> bool:
        cur = self._retry(
            lambda: self.conn.execute("SELECT 1 FROM files WHERE url = ?", (url,))
        )
        return cur.fetchone() is not None

    def failure_attempts(self, url: str) -> int:
        """Cached once load_cache() ran; DB lookup before that."""
        if self._fail_attempts is not None:
            return self._fail_attempts.get(url, 0)
        cur = self._retry(
            lambda: self.conn.execute(
                "SELECT attempts FROM failures WHERE url = ?", (url,)
            )
        )
        row = cur.fetchone()
        return row[0] if row else 0

    def stats(self) -> dict:
        cur = self._retry(
            lambda: self.conn.execute("SELECT COUNT(*) FROM files")
        )
        total = cur.fetchone()[0]
        cur = self._retry(
            lambda: self.conn.execute(
                "SELECT source, COUNT(*) FROM files GROUP BY source ORDER BY 2 DESC"
            )
        )
        return {"total_files": total, "by_source": dict(cur.fetchall())}

    def recent(self, limit: int = 10):
        cur = self._retry(
            lambda: self.conn.execute(
                "SELECT downloaded_at, source, title, size_bytes FROM files "
                "ORDER BY downloaded_at DESC LIMIT ?",
                (limit,),
            )
        )
        return cur.fetchall()

    # ---------------------------------------------------------------- writes

    def record_file(self, *, sha256, url, filepath, source, media, tag, title,
                    ext, size_bytes) -> bool:
        """Sync a new file to the DB. Returns False if sha/url was already
        known (another worker saved the same content first). Also updates
        the in-memory arrays."""

        def _insert():
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO files "
                "(sha256, url, filepath, source, media, tag, title, ext, size_bytes, downloaded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (sha256, url, str(filepath), source, media, tag, title, ext,
                 size_bytes, utcnow()),
            )
            self.conn.commit()
            return cur.rowcount > 0

        inserted = self._retry(_insert)
        if self._shas is not None:
            self._shas.add(sha256)
        if inserted and self._urls is not None:
            self._urls.add(url)
        return inserted

    def record_failure(self, *, url, source, error):
        """Register (or bump the attempt counter of) a failed URL."""

        def _upsert():
            self.conn.execute(
                "INSERT INTO failures (url, source, error, attempts, last_attempt) "
                "VALUES (?, ?, ?, 1, ?) "
                "ON CONFLICT(url) DO UPDATE SET "
                "    attempts = attempts + 1, error = excluded.error, "
                "    source = excluded.source, last_attempt = excluded.last_attempt",
                (url, source, str(error)[:500], utcnow()),
            )
            self.conn.commit()

        self._retry(_upsert)
        if self._fail_attempts is not None:
            self._fail_attempts[url] = self._fail_attempts.get(url, 0) + 1

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
