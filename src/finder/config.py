"""Central configuration: paths, limits and per-source politeness settings."""

import os
from pathlib import Path

# Root data directory. In Colab this lives on Google Drive; override with the
# BOOK_FINDER_DATA_DIR env var (or RunConfig.data_dir) for local testing.
DEFAULT_COLAB_ROOT = "/content/drive/MyDrive/OCR Engine"
ENV_DATA_DIR = "BOOK_FINDER_DATA_DIR"

MAILTO = "greencube-dev-boop@users.noreply.github.com"  # polite pool for OpenAlex
USER_AGENT = f"BookFinder/1.0 (+https://github.com/greencube-dev-boop/book-finder; mailto:{MAILTO})"

REQUEST_TIMEOUT = 60  # seconds per HTTP request

MIN_FILE_BYTES = 10 * 1024          # 10 KB — smaller is a stub/error page
MAX_FILE_BYTES = 100 * 1024 * 1024  # 100 MB — safety cap per file

DOWNLOAD_RETRIES = 3        # attempts per file (network errors only)
DOWNLOAD_CHUNK = 256 * 1024
DOWNLOAD_WORKERS = 3        # parallel download threads (searches always run in parallel)

# URLs that failed this many times are skipped on reruns (stored in `failures`)
# — but only while the last failure is younger than FAILURE_RETRY_DAYS; older
# failures get another chance (self-healing after provider outages).
MAX_FAILURE_ATTEMPTS = 3
FAILURE_RETRY_DAYS = 30

# Politeness delays (seconds) between requests to each provider.
SEARCH_DELAY = {
    "arxiv": 3.1,           # arXiv asks for >= 3 s between API calls
    "openalex": 0.15,       # allows 10 req/s in the polite pool
    "semanticscholar": 1.1, # unauthenticated: 1 req/s
    "doaj": 0.6,
    "archive": 0.4,
    "gutenberg": 0.5,
}
DOWNLOAD_DELAY = {
    "arxiv": 3.0,
    "openalex": 0.5,
    "semanticscholar": 1.1,
    "doaj": 0.6,
    "archive": 1.0,
    "gutenberg": 0.5,
}

PAPERS_SOURCES = ("arxiv", "openalex", "semanticscholar", "doaj")
BOOKS_SOURCES = ("archive", "gutenberg")
ALL_SOURCES = PAPERS_SOURCES + BOOKS_SOURCES

SOURCE_MEDIA = {
    "arxiv": "papers",
    "openalex": "papers",
    "semanticscholar": "papers",
    "doaj": "papers",
    "archive": "books",
    "gutenberg": "books",
}

SOURCE_LABEL = {
    "arxiv": "arXiv",
    "openalex": "OpenAlex",
    "semanticscholar": "Semantic Scholar",
    "doaj": "DOAJ",
    "archive": "Internet Archive",
    "gutenberg": "Project Gutenberg",
}


def default_data_root() -> str:
    return os.environ.get(ENV_DATA_DIR, DEFAULT_COLAB_ROOT)


def paths_for(data_root) -> dict:
    """Standard directory layout under the OCR Engine root."""
    root = Path(data_root)
    return {
        "root": root,
        "raw": root / "raw",
        "config": root / "config",
        "reports": root / "config" / "reports",
        "db": root / "config" / "download_tracker.db",
    }


def staging_dir_for(data_root) -> Path:
    """Where downloads are staged (fast local disk) before being moved to
    Drive: /content in Colab, a hidden folder next to the data root locally."""
    root_str = str(data_root).replace("\\", "/")
    if root_str.startswith("/content/"):
        return Path("/content/book_finder_staging")
    return Path(data_root) / ".staging"
