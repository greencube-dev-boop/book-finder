"""SQLite tracker: remembers every saved file (by SHA-256) and every failed URL.

Lives on Google Drive at  OCR Engine/config/download_tracker.db  so dedup
survives across Colab sessions and runs. Google Drive's FUSE layer can
occasionally return "database is locked", so every statement runs through a
retry wrapper.
"""

import sqlite3
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
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=60.0)
        self.conn.execute("PRAGMA busy_timeout = 60000")
        self._retry(lambda: self.conn.executescript(SCHEMA))
        self.conn.commit()

    # ------------------------------------------------------------------ core

    def _retry(self, fn, attempts: int = 6):
        """Run a DB operation, retrying on 'database is locked' (Drive FUSE)."""
        delay = 1.0
        for i in range(attempts):
            try:
                return fn()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) and "busy" not in str(exc):
                    raise
                if i == attempts - 1:
                    raise
                time.sleep(delay)
                delay *= 1.7

    # ---------------------------------------------------------------- queries

    def has_sha(self, sha256: str) -> bool:
        cur = self._retry(
            lambda: self.conn.execute("SELECT 1 FROM files WHERE sha256 = ?", (sha256,))
        )
        return cur.fetchone() is not None

    def has_url(self, url: str) -> bool:
        cur = self._retry(
            lambda: self.conn.execute("SELECT 1 FROM files WHERE url = ?", (url,))
        )
        return cur.fetchone() is not None

    def failure_attempts(self, url: str) -> int:
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
        """Insert a downloaded file. Returns False if sha/url was already known."""
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

        return self._retry(_insert)

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

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
