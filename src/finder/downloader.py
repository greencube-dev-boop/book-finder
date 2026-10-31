"""Streaming downloader: computes the file's SHA-256 while saving it, then
checks the tracker so identical content/URLs are never stored twice."""

import hashlib
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from . import config as C
from .utils import sanitize_filename


class PermanentError(Exception):
    """Content problem that retrying will not fix (HTML page, too large, ...)."""


@dataclass
class DownloadResult:
    status: str  # "downloaded" | "duplicate" | "failed"
    source: str
    title: str
    url: str
    tag: str = ""
    filepath: str = ""
    sha256: str = ""
    size_bytes: int = 0
    error: str = ""


# Magic bytes so we never store an HTML error page renamed to .pdf.
MAGIC = {"pdf": b"%PDF-", "epub": b"PK\x03\x04"}


def _cleanup(path: Path):
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _fetch(url: str, tmp_path: Path, max_bytes: int, session):
    """Stream `url` to `tmp_path`; return (sha256_hex, size).

    Raises PermanentError for content problems that a retry cannot fix.
    """
    sha = hashlib.sha256()
    size = 0
    with session.get(url, stream=True, timeout=C.REQUEST_TIMEOUT,
                     allow_redirects=True) as resp:
        resp.raise_for_status()
        ctype = (resp.headers.get("content-type") or "").lower()
        if "text/html" in ctype:
            raise PermanentError(f"got an HTML page, not a document (content-type: {ctype})")
        clen = resp.headers.get("content-length")
        if clen and clen.isdigit() and int(clen) > max_bytes:
            raise PermanentError(f"file too large ({int(clen)} B > {max_bytes} B cap)")
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=C.DOWNLOAD_CHUNK):
                if not chunk:
                    continue
                size += len(chunk)
                if size > max_bytes:
                    raise PermanentError(f"download exceeded size cap ({max_bytes} B)")
                sha.update(chunk)
                fh.write(chunk)
    return sha.hexdigest(), size


def _valid_magic(path: Path, ext: str) -> bool:
    sig = MAGIC.get(ext)
    if not sig:
        return True
    try:
        with open(path, "rb") as fh:
            return fh.read(len(sig)) == sig
    except OSError:
        return False


def download(record, dest_dir: Path, tracker, session,
             min_bytes: int, max_bytes: int) -> DownloadResult:
    """Download one record into dest_dir with dedup + tracking.

    Returns a DownloadResult; never raises for per-file problems.
    """
    res = DownloadResult(status="failed", source=record.source, title=record.title,
                         url=record.url, tag=record.tag)
    dest_dir.mkdir(parents=True, exist_ok=True)

    stem = sanitize_filename(f"{record.source}-{record.id}",
                             fallback=sanitize_filename(record.title, "download"))
    tmp_path = dest_dir / f".tmp_{stem}.{record.ext}"
    final_path = dest_dir / f"{stem}.{record.ext}"
    res.filepath = str(final_path)

    # --- fetch with retries (network errors only) -------------------------
    sha = size = None
    for attempt in range(1, C.DOWNLOAD_RETRIES + 1):
        try:
            sha, size = _fetch(record.url, tmp_path, max_bytes, session)
            break
        except PermanentError as exc:
            res.error = str(exc)
            _cleanup(tmp_path)
            return res
        except requests.RequestException as exc:
            res.error = f"{type(exc).__name__}: {exc}"[:300]
            _cleanup(tmp_path)
            if attempt < C.DOWNLOAD_RETRIES:
                time.sleep(2 * attempt)
    if sha is None:
        return res  # retries exhausted

    # --- sanity checks -----------------------------------------------------
    if size < min_bytes:
        res.error = f"file too small ({size} B < {min_bytes} B min)"
        _cleanup(tmp_path)
        return res
    if not _valid_magic(tmp_path, record.ext):
        res.error = f"content is not a valid {record.ext.upper()}"
        _cleanup(tmp_path)
        return res

    # --- dedup by content hash ----------------------------------------------
    if tracker.has_sha(sha):
        res.status = "duplicate"
        res.sha256 = sha
        res.size_bytes = size
        _cleanup(tmp_path)
        return res

    # --- finalize -----------------------------------------------------------
    try:
        shutil.move(str(tmp_path), str(final_path))
    except OSError as exc:
        res.error = f"could not save file: {exc}"
        _cleanup(tmp_path)
        return res

    inserted = tracker.record_file(
        sha256=sha, url=record.url, filepath=final_path, source=record.source,
        media=record.media, tag=record.tag, title=record.title, ext=record.ext,
        size_bytes=size,
    )
    res.status = "downloaded" if inserted else "duplicate"
    res.sha256 = sha
    res.size_bytes = size
    return res
