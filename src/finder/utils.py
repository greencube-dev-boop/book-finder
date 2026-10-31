"""Small shared helpers: logging, rate limiting, filename/tag sanitizing."""

import logging
import re
import sys
import threading
import time
import unicodedata

log = logging.getLogger("bookfinder")


def setup_logging(level=logging.INFO):
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S")
        )
        log.addHandler(handler)
    log.setLevel(level)
    return log


class RateLimiter:
    """Thread-safe per-key spacing: waits so calls to the same key are at
    least `delay` apart (holding the key's lock while sleeping, so parallel
    workers stay polite to each provider)."""

    def __init__(self):
        self._last = {}
        self._locks = {}
        self._guard = threading.Lock()

    def wait(self, key: str, delay: float):
        with self._guard:
            key_lock = self._locks.setdefault(key, threading.Lock())
        with key_lock:
            gap = time.monotonic() - self._last.get(key, 0.0)
            if gap < delay:
                time.sleep(delay - gap)
            self._last[key] = time.monotonic()


RATE = RateLimiter()


def slugify(text, max_len: int = 60) -> str:
    """ASCII, lowercase, dash-separated — safe for Drive folder names."""
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return text[:max_len].rstrip("-") or "untagged"


def sanitize_filename(text, fallback: str = "file", max_len: int = 100) -> str:
    """ASCII word characters only, safe on Windows + Drive."""
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    text = re.sub(r"[^\w\-. ]+", "_", text)
    text = re.sub(r"[\s_]+", "_", text).strip("._")
    text = text[:max_len].strip("._")
    return text or fallback


# Non-dotted arXiv categories that are still valid query targets.
_ARXIV_PLAIN = {
    "quant-ph", "hep-th", "hep-ph", "hep-ex", "hep-lat", "gr-qc", "cond-mat",
    "astro-ph", "nucl-th", "nucl-ex", "nlin", "physics", "math-ph",
}
_ARXIV_PREFIXES = {
    "cs", "math", "physics", "astro-ph", "cond-mat", "hep-ex", "hep-lat",
    "hep-ph", "hep-th", "gr-qc", "nucl-ex", "nucl-th", "quant-ph", "q-bio",
    "q-fin", "stat", "eess", "econ", "nlin", "math-ph",
}


def is_arxiv_category(tag: str) -> bool:
    """True for tags like `cs.AI`, `quant-ph`, `math.CO` — searched as categories."""
    t = tag.strip().lower()
    if "." in t:
        return t.split(".", 1)[0] in _ARXIV_PREFIXES
    return t in _ARXIV_PLAIN


_DOI_URL = re.compile(r"^https?://(dx\.)?doi\.org/", re.IGNORECASE)


def normalize_doi(doi) -> str:
    """'https://doi.org/10.1234/X.Y' -> '10.1234/x.y'."""
    return _DOI_URL.sub("", str(doi or "").strip()).lower()


def work_key(doi, title) -> str:
    """Work-level identity: DOI when available, else the normalized title.

    Same paper fetched from several sources (different bytes, different URLs)
    shares one key, so it is downloaded only once. Empty when neither is
    usable (no dedup possible)."""
    d = normalize_doi(doi)
    if d:
        return "doi:" + d
    t = re.sub(r"[^a-z0-9]+", "", str(title or "").lower())[:150]
    return "title:" + t if t else ""
