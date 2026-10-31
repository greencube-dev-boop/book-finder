"""DOAJ — Directory of Open Access Journals.

Only direct .pdf URLs are taken. DOAJ's fulltext links mostly point at
publisher landing pages via doi.org (which block programmatic clients), so
content_type hints alone are not trustworthy.
"""

from urllib.parse import quote

from .. import config as C
from .base import Record, Source

API = "https://doaj.org/api/search/articles/"
MAX_PAGES = 10  # fewer direct PDFs per page, so allow more pages


class DoajSource(Source):
    name = "doaj"
    media = "papers"

    def search(self, tag, limit, session):
        records = []
        page = 1
        while len(records) < limit and page <= MAX_PAGES:
            self._polite()
            resp = session.get(
                API + quote(tag),
                params={"pageSize": 100, "page": page},
                timeout=C.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                break

            for art in results:
                bj = art.get("bibjson") or {}
                title = (bj.get("title") or "").strip()
                aid = art.get("id") or bj.get("id") or ""
                url = self._pdf_url(bj)
                if not (title and aid and url):
                    continue
                records.append(Record(
                    source=self.name, media=self.media, id=aid, title=title,
                    url=url, ext="pdf",
                ))
                if len(records) >= limit:
                    break
            page += 1
        return records

    @staticmethod
    def _pdf_url(bibjson) -> str:
        """Return a fulltext link that is a direct .pdf URL, else ''."""
        for link in bibjson.get("link", []):
            url = link.get("url") or ""
            if url.lower().split("?")[0].endswith(".pdf"):
                return url
        return ""
