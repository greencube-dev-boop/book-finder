"""Internet Archive — freely downloadable books (public domain / no lending).

Search finds text items with a "Text PDF" file; per-item metadata is then
checked so lending-restricted (inlibrary) items are skipped. Metadata fetches
run in a small thread pool (the global rate limiter keeps the aggregate rate
polite) because they dominate the books search time.
"""

import concurrent.futures
from urllib.parse import quote

from .. import config as C
from .base import Record, Source

SEARCH = "https://archive.org/advancedsearch.php"
METADATA = "https://archive.org/metadata/"
MAX_PAGES = 10
META_WORKERS = 3


class ArchiveOrgSource(Source):
    name = "archive"
    media = "books"

    def search(self, tag, limit, session):
        records = []
        page = 1
        while len(records) < limit and page <= MAX_PAGES:
            self._polite()
            resp = session.get(SEARCH, params={
                "q": f'("{tag}") AND mediatype:texts AND format:"Text PDF"',
                "fl[]": ["identifier", "title"],
                "rows": 100,
                "page": page,
                "sort[]": "downloads desc",
                "output": "json",
            }, timeout=C.REQUEST_TIMEOUT)
            resp.raise_for_status()
            docs = ((resp.json().get("response") or {}).get("docs")) or []
            if not docs:
                break

            jobs = []
            for doc in docs[: limit - len(records)]:
                ident = doc.get("identifier")
                if not ident:
                    continue
                title = doc.get("title") or ident
                if isinstance(title, list):
                    title = title[0] if title else ident
                jobs.append((str(ident), str(title)))
            if not jobs:
                break

            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=META_WORKERS, thread_name_prefix="ia-meta") as pool:
                futs = [pool.submit(self._pdf_record, ident, title, session)
                        for ident, title in jobs]
                for fut in futs:
                    try:
                        rec = fut.result()
                    except Exception:
                        rec = None
                    if rec:
                        records.append(rec)
            page += 1
        return records

    def _pdf_record(self, ident: str, title: str, session):
        """Fetch item metadata; return a Record if a free Text PDF exists."""
        self._polite()
        try:
            resp = session.get(METADATA + quote(ident), timeout=C.REQUEST_TIMEOUT)
            resp.raise_for_status()
            meta = resp.json()
        except Exception:
            return None

        md = meta.get("metadata") or {}
        if str(md.get("access-restricted-item", "")).lower() in ("true", "1"):
            return None
        colls = md.get("collection")
        colls = [colls] if isinstance(colls, str) else (colls or [])
        if "inlibrary" in colls:  # lending library — not freely downloadable
            return None

        for f in meta.get("files", []):
            if f.get("format") == "Text PDF" and f.get("name"):
                url = f"https://archive.org/download/{quote(ident)}/{quote(f['name'])}"
                return Record(source=self.name, media=self.media, id=ident,
                              title=title, url=url, ext="pdf")
        return None
