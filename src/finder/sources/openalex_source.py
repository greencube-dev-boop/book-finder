"""OpenAlex — index of ~250M works; we take only works with open-access PDFs."""

from .. import config as C
from .base import Record, Source

API = "https://api.openalex.org/works"
SELECT = "id,display_name,best_oa_location,primary_location"
MAX_PAGES = 5


class OpenAlexSource(Source):
    name = "openalex"
    media = "papers"

    def search(self, tag, limit, session):
        records = []
        per_page = min(max(limit, 50), 200)
        page = 1
        while len(records) < limit and page <= MAX_PAGES:
            self._polite()
            resp = session.get(API, params={
                "search": tag,
                "filter": "open_access.is_oa:true",
                "per-page": per_page,
                "page": page,
                "select": SELECT,
                "mailto": C.MAILTO,
            }, timeout=C.REQUEST_TIMEOUT)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                break

            for work in results:
                pdf = self._pdf_url(work)
                oa_id = (work.get("id") or "").rsplit("/", 1)[-1]
                title = (work.get("display_name") or "").strip()
                if not (pdf and oa_id and title):
                    continue
                records.append(Record(
                    source=self.name, media=self.media, id=oa_id, title=title,
                    url=pdf, ext="pdf",
                ))
                if len(records) >= limit:
                    break
            page += 1
        return records

    @staticmethod
    def _pdf_url(work) -> str:
        for key in ("best_oa_location", "primary_location"):
            loc = work.get(key) or {}
            url = loc.get("pdf_url")
            if url:
                return url
        return ""
