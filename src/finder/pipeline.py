"""Pipeline: parallel search + live download workers -> track -> report.

A search/download CYCLE instead of two sequential phases:

  * every (tag, source) pair is searched in its own thread;
  * the moment a source returns records they are pushed into a shared queue;
  * a pool of download workers pulls from that queue and starts saving
    immediately — downloading overlaps the remaining searching.

This is the single entry point the Colab notebook calls.
"""

import concurrent.futures
import json
import queue
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from tqdm import tqdm

from . import config as C
from .downloader import download
from .sources import REGISTRY
from .tracker import DownloadTracker
from .utils import RATE, log, setup_logging, slugify

_STOP = object()  # queue sentinel: download workers exit when they see this

_sessions = threading.local()


def _get_session() -> requests.Session:
    """One requests.Session per thread (Session is not thread-safe)."""
    session = getattr(_sessions, "session", None)
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = C.USER_AGENT
        _sessions.session = session
    return session


@dataclass
class RunConfig:
    tags: list
    media: str = "papers"          # "papers" | "books" | "both"
    max_downloads: int = 100       # how many files to try downloading this run
    sources: str = "all"           # "all" | "papers" | "books" | comma-separated names
    workers: int = C.DOWNLOAD_WORKERS  # parallel download threads (1-8)
    min_size_bytes: int = C.MIN_FILE_BYTES
    max_size_bytes: int = C.MAX_FILE_BYTES
    data_dir: str = ""             # override the OCR Engine root (for local tests)

    def __post_init__(self):
        self.tags = [t for t in (self.tags or []) if t and str(t).strip()]
        self.workers = max(1, min(int(self.workers or 1), 8))


def resolve_sources(sources: str, media: str) -> list:
    wanted = (sources or "all").strip().lower()
    media_list = ["papers", "books"] if media == "both" else [media]
    if wanted in ("", "all"):
        return [s for s in C.ALL_SOURCES if C.SOURCE_MEDIA[s] in media_list]
    if wanted in ("papers", "books"):
        return [s for s in C.ALL_SOURCES if C.SOURCE_MEDIA[s] == wanted]
    names = [p.strip() for p in wanted.split(",") if p.strip()]
    unknown = [n for n in names if n not in C.ALL_SOURCES]
    if unknown:
        raise ValueError(f"unknown source(s) {unknown}; choose from {list(C.ALL_SOURCES)}")
    return names


def run(cfg: RunConfig) -> dict:
    setup_logging()
    t0 = time.time()
    if not cfg.tags:
        raise ValueError("RunConfig.tags is empty — nothing to search for.")

    paths = C.paths_for(cfg.data_dir or C.default_data_root())
    for key in ("raw", "config", "reports"):
        paths[key].mkdir(parents=True, exist_ok=True)

    # staging folder (fast Colab disk) — downloads land here first, get
    # hashed, then move to Drive only if their SHA-256 is new
    staging = C.staging_dir_for(paths["root"])
    staging.mkdir(parents=True, exist_ok=True)
    stale = 0
    for f in staging.iterdir():
        if f.is_file() and f.name.startswith(".tmp_"):
            try:
                f.unlink()
                stale += 1
            except OSError:
                pass
    if stale:
        log.info("Cleared %d stale staged file(s) from %s", stale, staging)

    tracker = DownloadTracker(paths["db"])
    n_shas, n_urls, n_failed = tracker.load_cache()  # the in-memory array
    log.info("Tracker array loaded from DB: %d hashes, %d URLs, %d failed URLs",
             n_shas, n_urls, n_failed)
    src_names = resolve_sources(cfg.sources, cfg.media)
    per_source_limit = max(50, min(cfg.max_downloads * 2, 300))

    # ---------------------------------------------------------- shared state
    record_q = queue.Queue(maxsize=max(cfg.max_downloads * 2, 100))
    lock = threading.Lock()
    seen_urls = set()
    counts = Counter()
    by_source = defaultdict(Counter)
    failures = []
    source_errors = []
    stats = {"found": 0, "enqueued": 0, "already_tracked": 0,
             "blocked": 0, "attempted": 0, "not_attempted": 0}
    budget_left = cfg.max_downloads

    bar = tqdm(total=0, unit="file", desc="Downloading")

    # ------------------------------------------------------------- consumers
    def consume(rec):
        """Dedup-check, reserve budget, download and track one record."""
        nonlocal budget_left
        with lock:
            if budget_left <= 0:
                stats["not_attempted"] += 1
                return
        if tracker.knows_url(rec.url):
            with lock:
                stats["already_tracked"] += 1
            return
        if tracker.failure_attempts(rec.url) >= C.MAX_FAILURE_ATTEMPTS:
            with lock:
                stats["blocked"] += 1
            return
        with lock:  # reserve one download slot
            if budget_left <= 0:
                stats["not_attempted"] += 1
                return
            budget_left -= 1
            stats["attempted"] += 1

        subdir = paths["raw"] / rec.media / slugify(rec.tag)
        RATE.wait(rec.source, C.DOWNLOAD_DELAY.get(rec.source, 1.0))
        result = download(rec, subdir, staging, tracker, _get_session(),
                          cfg.min_size_bytes, cfg.max_size_bytes)
        with lock:
            counts[result.status] += 1
            by_source[rec.source][result.status] += 1
            if result.status == "failed":
                failures.append({"source": rec.source, "title": rec.title[:80],
                                 "url": rec.url, "error": result.error})
        if result.status == "failed":
            tracker.record_failure(url=rec.url, source=rec.source,
                                   error=result.error)
        bar.set_postfix(downloaded=counts["downloaded"],
                        duplicate=counts["duplicate"],
                        failed=counts["failed"])

    def worker():
        while True:
            rec = record_q.get()
            try:
                if rec is _STOP:
                    return
                try:
                    consume(rec)
                except Exception as exc:  # one bad record must not kill a worker
                    log.warning("worker error on %r: %s",
                                getattr(rec, "url", "?"), exc)
                bar.update(1)
            finally:
                record_q.task_done()

    # ------------------------------------------------------------- producers
    def produce(tag, name):
        source = REGISTRY[name]()
        try:
            recs = source.search(tag, per_source_limit, _get_session())
        except Exception as exc:  # one failing source must not kill the run
            with lock:
                source_errors.append(f"{name}/{tag}: {type(exc).__name__}: {exc}"[:200])
            log.warning("source %s failed on tag %r: %s", name, tag, exc)
            return
        log.info("  %-16s %-30r -> %d records",
                 C.SOURCE_LABEL.get(name, name), tag, len(recs))
        fresh = []
        with lock:
            stats["found"] += len(recs)
            for r in recs:
                r.tag = tag
                if r.url not in seen_urls:
                    seen_urls.add(r.url)
                    fresh.append(r)
            stats["enqueued"] += len(fresh)
        for r in fresh:  # blocks if workers fall behind (backpressure)
            record_q.put(r)
        with bar.get_lock():  # downloads can now start on these records
            bar.total += len(fresh)
            bar.refresh()

    workers = [threading.Thread(target=worker, daemon=True, name=f"dl-{i}")
               for i in range(cfg.workers)]
    for w in workers:
        w.start()

    jobs = [(tag, name) for tag in cfg.tags for name in src_names]
    log.info("Searching %d source(s) x %d tag(s) in parallel with %d download "
             "worker(s): %s", len(src_names), len(cfg.tags), cfg.workers,
             ", ".join(cfg.tags))
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(jobs), 12), thread_name_prefix="search") as pool:
        futures = [pool.submit(produce, tag, name) for tag, name in jobs]
        concurrent.futures.wait(futures)  # exceptions handled inside produce

    for _ in workers:  # all searches done: tell workers to drain and exit
        record_q.put(_STOP)
    for w in workers:
        w.join()
    bar.close()

    # ---------------------------------------------------------------- report
    elapsed = round(time.time() - t0, 1)
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tags": cfg.tags,
        "media": cfg.media,
        "sources": src_names,
        "workers": cfg.workers,
        "max_downloads": cfg.max_downloads,
        "found": stats["found"],
        "unique_urls": stats["enqueued"],
        "already_tracked": stats["already_tracked"],
        "skipped_permanently_failed": stats["blocked"],
        "attempted": stats["attempted"],
        "not_attempted": stats["not_attempted"],
        "downloaded": counts["downloaded"],
        "duplicates": counts["duplicate"],
        "failed": counts["failed"],
        "by_source": {src: dict(cnt) for src, cnt in sorted(by_source.items())},
        "source_errors": source_errors,
        "failures_sample": failures[:15],
        "db_totals": tracker.stats(),
        "raw_dir": str(paths["raw"]),
        "staging_dir": str(staging),
        "tracker_db": str(paths["db"]),
        "elapsed_sec": elapsed,
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = paths["reports"] / f"run_{stamp}.json"
    report_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                           encoding="utf-8")

    tracker.close()
    _print_report(summary, report_path)
    return summary


def _print_report(summary: dict, report_path):
    p = print
    p("")
    p("=" * 64)
    p(" BOOK FINDER - RUN SUMMARY")
    p("=" * 64)
    p(f" Tags searched      : {', '.join(summary['tags'])}")
    p(f" Media / sources    : {summary['media']} / {', '.join(summary['sources'])}")
    p(f" Parallelism        : search x{len(summary['sources']) * max(len(summary['tags']), 1)}"
      f" threads, downloads x{summary['workers']} workers")
    p(f" Records found      : {summary['found']}  (unique URLs: {summary['unique_urls']})")
    p(f" Already tracked    : {summary['already_tracked']} (downloaded in earlier runs)")
    p("-" * 64)
    p(f" DOWNLOADED         : {summary['downloaded']}")
    p(f" DUPLICATES skipped : {summary['duplicates']} (same SHA-256 already saved)")
    p(f" FAILED             : {summary['failed']}")
    if summary.get("not_attempted"):
        p(f" Not attempted      : {summary['not_attempted']} (download budget reached)")
    p("-" * 64)
    for src, cnt in summary["by_source"].items():
        label = C.SOURCE_LABEL.get(src, src)
        p(f"   {label:<18} downloaded={cnt.get('downloaded', 0):<5}"
          f" duplicate={cnt.get('duplicate', 0):<4} failed={cnt.get('failed', 0)}")
    p("-" * 64)
    p(f" Saved to           : {summary['raw_dir']}")
    p(f" Staged in          : {summary['staging_dir']} (local, moved after SHA-256 check)")
    p(f" Tracker DB         : {summary['tracker_db']}")
    p(f"   unique files in DB : {summary['db_totals']['total_files']}")
    p(f" Full report        : {report_path}")
    p(f" Elapsed            : {summary['elapsed_sec']} s")
    p("=" * 64)
