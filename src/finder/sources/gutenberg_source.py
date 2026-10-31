"""Project Gutenberg — public-domain books via the Gutendex API.

PDF is preferred; EPUB is the fallback (Gutenberg's PDFs are sparse).
Gutendex is a free community service that can be slow, so requests retry
once on timeout.
"""

import requests

from .. import config as C
from ..utils import log
from .base import Record, Source

API = "https://gutendex.com/books"
MAX_PAGES = 5


class GutenbergSource(Source):
    name = "gutenberg"
    media = "books"

    def _get(self, session, params):
        for attempt in (1, 2):
            self._polite()
            try:
                return session.get(API, params=params,
                                   timeout=C.REQUEST_TIMEOUT)
            except requests.Timeout:
                if attempt == 2:
                    raise
                log.info("Gutendex timed out; retrying once")
        raise RuntimeError("unreachable")

    def search(self, tag, limit, session):
        records = []
        page = 1
        while len(records) < limit and page <= MAX_PAGES:
            resp = self._get(session, {"search": tag, "page": page})
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results") or []
            if not results:
                break

            for book in results:
                gid = book.get("id")
                title = (book.get("title") or "").strip()
                url, ext = self._pick_format(book.get("formats") or {})
                if not (gid and title and url):
                    continue
                records.append(Record(
                    source=self.name, media=self.media, id=str(gid), title=title,
                    url=url, ext=ext,
                ))
                if len(records) >= limit:
                    break

            if not data.get("next"):  # no further pages
                break
            page += 1
        return records

    @staticmethod
    def _pick_format(formats: dict):
        """Prefer PDF, fall back to EPUB. Keys may carry charset suffixes."""
        for prefix, ext in (("application/pdf", "pdf"), ("application/epub+zip", "epub")):
            for key, url in formats.items():
                if key.startswith(prefix) and url:
                    return url, ext
        return "", None
