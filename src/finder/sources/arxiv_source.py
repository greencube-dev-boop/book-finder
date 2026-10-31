"""arXiv — scientific preprints. Atom API, direct PDF downloads."""

import re
import xml.etree.ElementTree as ET

from .. import config as C
from ..utils import is_arxiv_category
from .base import Record, Source

API = "https://export.arxiv.org/api/query"
_ATOM = "{http://www.w3.org/2005/Atom}"
_WS = re.compile(r"\s+")


class ArxivSource(Source):
    name = "arxiv"
    media = "papers"

    def search(self, tag, limit, session):
        limit = min(limit, 200)  # one request, well within API limits
        if is_arxiv_category(tag):
            query = f"cat:{tag}"
        else:
            query = f'all:"{tag}"'

        params = {
            "search_query": query,
            "start": 0,
            "max_results": limit,
            "sortBy": "relevance",
        }
        self._polite()
        resp = session.get(API, params=params, timeout=C.REQUEST_TIMEOUT)
        resp.raise_for_status()

        records = []
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError:
            return records

        for entry in root.findall(f"{_ATOM}entry"):
            abs_url = (entry.findtext(f"{_ATOM}id") or "").strip()
            arxiv_id = abs_url.rsplit("/abs/", 1)[-1]
            title = _WS.sub(" ", entry.findtext(f"{_ATOM}title") or "").strip()
            if not arxiv_id or not title:
                continue
            records.append(Record(
                source=self.name, media=self.media, id=arxiv_id, title=title,
                url=f"https://arxiv.org/pdf/{arxiv_id}", ext="pdf",
            ))
        return records
