"""Registry of download sources (all legal / open-access APIs)."""

from .arxiv_source import ArxivSource
from .archive_org_source import ArchiveOrgSource
from .doaj_source import DoajSource
from .gutenberg_source import GutenbergSource
from .openalex_source import OpenAlexSource
from .semanticscholar_source import SemanticScholarSource

REGISTRY = {
    "arxiv": ArxivSource,
    "openalex": OpenAlexSource,
    "semanticscholar": SemanticScholarSource,
    "doaj": DoajSource,
    "archive": ArchiveOrgSource,
    "gutenberg": GutenbergSource,
}
