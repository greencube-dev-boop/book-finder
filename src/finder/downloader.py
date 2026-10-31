"""Downloader implementing the pipeline flow:

  1. download into the STAGING folder (Colab env / local disk), hashing the
     content (SHA-256) while it streams in;
  2. check the hash against the in-memory array loaded from the tracker DB;
     if already known -> delete the staged file, done (never saved twice);
  3. if new -> remember the hash in the array, sync the DB on Drive, then
     move the finished file to its Drive (raw) location. If that move fails
     the record is rolled back so the content can be retried later.
"""

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
    invalid: bool = False  # content downloaded but failed validation (corrupt)


# Magic bytes so we never store an HTML error page renamed to .pdf.
MAGIC = {"pdf": b"%PDF-", "epub": b"PK\x03\x04"}


def _validate_document(path: Path, ext: str):
    """Content-integrity check: (ok, reason).

    PDFs must parse and have >= 1 page (pypdf); EPUBs must be intact zips.
    Returns (True, "") when no validator is available (dependency missing) so
    downloads are never blocked by an optional import."""
    if ext == "pdf":
        try:
            from pypdf import PdfReader
        except ImportError:
            return True, "pypdf not installed - PDF validation skipped"
        try:
            if len(PdfReader(str(path)).pages) < 1:
                return False, "PDF is unparseable or has 0 pages"
            return True, ""
        except Exception as exc:
            return False, f"PDF is corrupt ({type(exc).__name__})"
    if ext == "epub":
        try:
            import zipfile
            with zipfile.ZipFile(str(path)) as zf:
                if zf.testzip() is not None:
                    return False, "EPUB zip contains a corrupt member"
            return True, ""
        except Exception as exc:
            return False, f"EPUB is corrupt ({type(exc).__name__})"
    return True, ""


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


def download(record, dest_dir: Path, staging_dir: Path, tracker, session,
             min_bytes: int, max_bytes: int, work_key: str = "") -> DownloadResult:
    """Download one record: stage -> hash -> dedup -> validate -> sync -> move.

    Returns a DownloadResult; never raises for per-file problems.
    """
    res = DownloadResult(status="failed", source=record.source, title=record.title,
                         url=record.url, tag=record.tag)
    dest_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    stem = sanitize_filename(f"{record.source}-{record.id}",
                             fallback=sanitize_filename(record.title, "download"))
    tmp_path = staging_dir / f".tmp_{stem}.{record.ext}"    # Colab env (fast disk)
    final_path = dest_dir / f"{stem}.{record.ext}"          # Drive raw folder
    res.filepath = str(final_path)

    # --- 1. download to staging, hashing while streaming -------------------
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

    # --- sanity checks ------------------------------------------------------
    if size < min_bytes:
        res.error = f"file too small ({size} B < {min_bytes} B min)"
        _cleanup(tmp_path)
        return res
    if not _valid_magic(tmp_path, record.ext):
        res.error = f"content is not a valid {record.ext.upper()}"
        res.invalid = True
        _cleanup(tmp_path)
        return res
    ok, why = _validate_document(tmp_path, record.ext)
    if not ok:
        res.error = why
        res.invalid = True  # deterministic corruption: caller marks the URL fully failed
        _cleanup(tmp_path)
        return res

    # --- 2. check the hash in the in-memory array ---------------------------
    res.sha256 = sha
    res.size_bytes = size
    if tracker.knows_sha(sha):
        res.status = "duplicate"
        _cleanup(tmp_path)  # delete the staged file, proceed to the next
        return res

    # --- 3. new content: remember -> sync DB -> move to Drive ---------------
    tracker.remember_sha(sha)  # add to the in-memory array first
    inserted = tracker.record_file(
        sha256=sha, url=record.url, filepath=final_path, source=record.source,
        media=record.media, tag=record.tag, title=record.title, ext=record.ext,
        size_bytes=size,
    )
    if work_key:
        tracker.record_work(work_key, sha)
    try:
        shutil.move(str(tmp_path), str(final_path))  # Colab env -> Drive
    except OSError as exc:
        tracker.forget(sha, record.url, work_key)  # roll back arrays + DB rows
        res.error = f"could not move file to Drive: {exc}"
        _cleanup(tmp_path)
        return res

    if not inserted:
        # a parallel worker saved identical content first: drop our copy
        res.status = "duplicate"
        _cleanup(final_path)
        return res

    res.status = "downloaded"
    return res
