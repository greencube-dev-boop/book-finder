"""Source interface + Record dataclass shared by all providers."""

from dataclasses import dataclass, field

from .. import config as C
from ..utils import RATE


@dataclass
class Record:
    """One downloadable item found by a source."""

    source: str   # key in the registry, e.g. "arxiv"
    media: str    # "papers" | "books"
    id: str       # provider-specific id (also used for filenames)
    title: str
    url: str      # direct download URL
    ext: str = "pdf"
    tag: str = field(default="")  # which search tag produced this record


class Source:
    """A search provider. Subclasses implement search() and must be polite
    (call _polite() before every HTTP request)."""

    name = "base"
    media = "papers"

    def search(self, tag: str, limit: int, session) -> list:
        """Return up to `limit` Records for `tag` using the shared `session`."""
        raise NotImplementedError

    def _polite(self):
        RATE.wait(self.name, C.SEARCH_DELAY.get(self.name, 1.0))
