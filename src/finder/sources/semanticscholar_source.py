"""Semantic Scholar — Graph API search; uses openAccessPdf links (mostly arXiv
and publisher OA PDFs). Unauthenticated rate limit is 1 req/s, occasionally 429."""

import time

from .. import config as C
from ..utils import log
from .base import Record, Source

API = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,externalIds,openAccessPdf,isOpenAccess"


class SemanticScholarSource(Source):
    name = "semanticscholar"
    media = "papers"

    def search(self, tag, limit, session):
        limit = min(limit, 100)  # one page is usually enough
        records = []
        offset = 0
        throttled = 0

        while len(records) < limit and offset < 1000:
            self._polite()
            resp = session.get(API, params={
                "query": tag,
                "limit": min(100, limit),
                "offset": offset,
                "fields": FIELDS,
            }, timeout=C.REQUEST_TIMEOUT)

            if resp.status_code == 429:
                throttled += 1
                if throttled > 3:
                    log.warning("Semantic Scholar kept throttling; giving up for this tag")
                    break
                log.info("Semantic Scholar rate limited; waiting 10 s")
                time.sleep(10)
                continue
            throttled = 0
            resp.raise_for_status()
            data = resp.json()
            papers = data.get("data") or []
            if not papers:
                break

            for paper in papers:
                oa = paper.get("openAccessPdf") or {}
                url = oa.get("url") or ""
                title = (paper.get("title") or "").strip()
                ids = paper.get("externalIds") or {}
                pid = ids.get("ArXiv") or paper.get("paperId") or ""
                if not (url and title and pid):
                    continue
                records.append(Record(
                    source=self.name, media=self.media, id=str(pid), title=title,
                    url=url, ext="pdf", doi=ids.get("DOI") or "",
                ))
                if len(records) >= limit:
                    break

            offset += len(papers)
            if offset >= (data.get("total") or 0):
                break
        return records
