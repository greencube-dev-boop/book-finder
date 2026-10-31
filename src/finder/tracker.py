"""SQLite tracker: remembers every saved file (by SHA-256) and every failed URL.

Lives on Google Drive at  OCR Engine/config/download_tracker.db  so dedup
survives across Colab sessions and runs. Google Drive's FUSE layer can
occasionally return "database is locked", so every statement runs through a
retry wrapper.
"""

import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
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
CREATE TABLE IF NOT EXISTS works (
    work_key TEXT PRIMARY KEY,
    sha256   TEXT
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
        self._works = None         # set[str] — work-level dedup keys
        self._failures = None      # dict[str, (attempts, last_attempt_iso)]

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
        """Load the DB into memory: (n_hashes, n_urls, n_works, n_failed_urls)."""
        cur = self._retry(lambda: self.conn.execute("SELECT sha256 FROM files"))
        self._shas = {r[0] for r in cur.fetchall()}
        cur = self._retry(lambda: self.conn.execute("SELECT url FROM files"))
        self._urls = {r[0] for r in cur.fetchall()}
        cur = self._retry(lambda: self.conn.execute("SELECT work_key FROM works"))
        self._works = {r[0] for r in cur.fetchall()}
        cur = self._retry(lambda: self.conn.execute(
            "SELECT url, attempts, last_attempt FROM failures"))
        self._failures = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        return len(self._shas), len(self._urls), len(self._works), len(self._failures)

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

    def forget(self, sha256: str, url: str, work_key: str = ""):
        """Roll back a record (the file move to Drive failed): drop the hash,
        URL and work key from the arrays and delete the DB rows so the
        content can be retried on a later run."""
        if self._shas is not None:
            self._shas.discard(sha256)
        if self._urls is not None:
            self._urls.discard(url)
        if work_key and self._works is not None:
            self._works.discard(work_key)

        def _del():
            self.conn.execute("DELETE FROM files WHERE sha256 = ?", (sha256,))
            if work_key:
                self.conn.execute("DELETE FROM works WHERE work_key = ?", (work_key,))
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
        entry = self._failure_entry(url)
        return entry[0] if entry else 0

    def _failure_entry(self, url: str):
        if self._failures is not None:
            return self._failures.get(url)
        cur = self._retry(
            lambda: self.conn.execute(
                "SELECT attempts, last_attempt FROM failures WHERE url = ?",
                (url,),
            )
        )
        return cur.fetchone()

    def should_skip_failure(self, url: str, max_attempts: int = 3,
                            within_days: int = 30) -> bool:
        """True only if the URL failed >= max_attempts times AND the last
        failure is recent — older failures get another chance (self-healing
        after provider outages)."""
        entry = self._failure_entry(url)
        if not entry:
            return False
        attempts, last = entry
        if attempts < max_attempts:
            return False
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(str(last))
        except ValueError:
            return True
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - last_dt) < timedelta(days=within_days)

    # ------------------------------------------------------- work-level dedup

    def knows_work(self, work_key: str) -> bool:
        if self._works is not None:
            return work_key in self._works
        cur = self._retry(
            lambda: self.conn.execute(
                "SELECT 1 FROM works WHERE work_key = ?", (work_key,))
        )
        return cur.fetchone() is not None

    def record_work(self, work_key: str, sha256: str) -> bool:
        """Sync a work key to the DB. False if the work was already known."""

        def _ins():
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO works (work_key, sha256) VALUES (?, ?)",
                (work_key, sha256),
            )
            self.conn.commit()
            return cur.rowcount > 0

        inserted = self._retry(_ins)
        if self._works is not None:
            self._works.add(work_key)
        return inserted

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

    def record_failure(self, *, url, source, error, bump_to=None):
        """Register (or bump the attempt counter of) a failed URL.

        bump_to: floor the attempt counter at this value — used for
        deterministic content problems (corrupt files) that must not be
        retried until the retry window expires."""

        new_attempts = max(self.failure_attempts(url) + 1, int(bump_to or 0))
        now = utcnow()

        def _upsert():
            self.conn.execute(
                "INSERT OR REPLACE INTO failures "
                "(url, source, error, attempts, last_attempt) VALUES (?, ?, ?, ?, ?)",
                (url, source, str(error)[:500], new_attempts, now),
            )
            self.conn.commit()

        self._retry(_upsert)
        if self._failures is not None:
            self._failures[url] = (new_attempts, now)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
