"""Small shared helpers: logging, rate limiting, filename/tag sanitizing."""

import logging
import re
import sys
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
    """Remembers the last call time per key and enforces a minimum spacing."""

    def __init__(self):
        self._last = {}

    def wait(self, key: str, delay: float):
        now = time.monotonic()
        gap = now - self._last.get(key, 0.0)
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
