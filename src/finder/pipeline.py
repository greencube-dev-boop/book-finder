"""Pipeline: search sources -> dedupe -> download -> track -> report.

This is the single entry point the Colab notebook calls.
"""

import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests
from tqdm import tqdm

from . import config as C
from .downloader import download
from .sources import REGISTRY
from .tracker import DownloadTracker
from .utils import RATE, log, setup_logging, slugify


@dataclass
class RunConfig:
    tags: list
    media: str = "papers"          # "papers" | "books" | "both"
    max_downloads: int = 100       # how many files to try downloading this run
    sources: str = "all"           # "all" | "papers" | "books" | comma-separated names
    min_size_bytes: int = C.MIN_FILE_BYTES
    max_size_bytes: int = C.MAX_FILE_BYTES
    data_dir: str = ""             # override the OCR Engine root (for local tests)


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


def _interleave(groups: dict) -> list:
    """Round-robin merge {(tag, source): [records]} so every source/tag gets
    a fair share of the download budget."""
    queues = [list(v) for v in groups.values() if v]
    out = []
    while queues:
        remaining = []
        for q in queues:
            if q:
                out.append(q.pop(0))
                if q:
                    remaining.append(q)
        queues = remaining
    return out


def run(cfg: RunConfig) -> dict:
    setup_logging()
    t0 = time.time()
    cfg.tags = [t for t in (cfg.tags or []) if t and str(t).strip()]
    if not cfg.tags:
        raise ValueError("RunConfig.tags is empty — nothing to search for.")

    paths = C.paths_for(cfg.data_dir or C.default_data_root())
    for key in ("raw", "config", "reports"):
        paths[key].mkdir(parents=True, exist_ok=True)

    tracker = DownloadTracker(paths["db"])
    session = requests.Session()
    session.headers["User-Agent"] = C.USER_AGENT

    src_names = resolve_sources(cfg.sources, cfg.media)
    per_source_limit = max(50, min(cfg.max_downloads * 2, 300))

    # 1) search every source for every tag ---------------------------------
    groups = {}
    source_errors = []
    found_raw = 0
    log.info("Searching %d source(s) for tags: %s", len(src_names), ", ".join(cfg.tags))
    for tag in cfg.tags:
        for name in src_names:
            source = REGISTRY[name]()
            try:
                recs = source.search(tag, per_source_limit, session)
            except Exception as exc:  # one failing source must not kill the run
                source_errors.append(f"{name}/{tag}: {type(exc).__name__}: {exc}"[:200])
                log.warning("source %s failed on tag %r: %s", name, tag, exc)
                continue
            log.info("  %-16s %-30r -> %d records",
                     C.SOURCE_LABEL.get(name, name), tag, len(recs))
            found_raw += len(recs)
            for r in recs:
                r.tag = tag
            groups.setdefault((tag, name), []).extend(recs)

    # 2) dedupe by URL -------------------------------------------------------
    seen_urls, unique = set(), []
    for rec in _interleave(groups):
        if rec.url in seen_urls:
            continue
        seen_urls.add(rec.url)
        unique.append(rec)

    # 3) skip URLs already saved / known to fail permanently ------------------
    already_tracked = sum(1 for r in unique if tracker.has_url(r.url))
    candidates = [r for r in unique if not tracker.has_url(r.url)]
    kept = []
    for rec in candidates:
        if tracker.failure_attempts(rec.url) >= C.MAX_FAILURE_ATTEMPTS:
            continue
        kept.append(rec)
    batch = kept[: cfg.max_downloads]

    log.info("Found %d records (%d unique, %d already tracked, %d blocked as "
             "previously failed) -> downloading %d",
             found_raw, len(unique), already_tracked, len(candidates) - len(kept),
             len(batch))

    # 4) download --------------------------------------------------------------
    counts = Counter()
    by_source = defaultdict(Counter)
    failures = []

    with tqdm(total=len(batch), unit="file", desc="Downloading") as bar:
        for rec in batch:
            subdir = paths["raw"] / rec.media / slugify(rec.tag)
            RATE.wait(rec.source, C.DOWNLOAD_DELAY.get(rec.source, 1.0))
            result = download(rec, subdir, tracker, session,
                              cfg.min_size_bytes, cfg.max_size_bytes)
            counts[result.status] += 1
            by_source[rec.source][result.status] += 1
            if result.status == "failed":
                failures.append({"source": rec.source, "title": rec.title[:80],
                                 "url": rec.url, "error": result.error})
                tracker.record_failure(url=rec.url, source=rec.source,
                                       error=result.error)
            bar.update(1)
            bar.set_postfix(downloaded=counts["downloaded"],
                            duplicate=counts["duplicate"],
                            failed=counts["failed"])

    # 5) summary + report -------------------------------------------------------
    elapsed = round(time.time() - t0, 1)
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tags": cfg.tags,
        "media": cfg.media,
        "sources": src_names,
        "max_downloads": cfg.max_downloads,
        "found": found_raw,
        "unique_urls": len(unique),
        "already_tracked": already_tracked,
        "attempted": len(batch),
        "downloaded": counts["downloaded"],
        "duplicates": counts["duplicate"],
        "failed": counts["failed"],
        "by_source": {src: dict(cnt) for src, cnt in sorted(by_source.items())},
        "source_errors": source_errors,
        "failures_sample": failures[:15],
        "db_totals": tracker.stats(),
        "raw_dir": str(paths["raw"]),
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
    p(f" Records found      : {summary['found']}  (unique URLs: {summary['unique_urls']})")
    p(f" Already tracked    : {summary['already_tracked']} (downloaded in earlier runs)")
    p("-" * 64)
    p(f" DOWNLOADED         : {summary['downloaded']}")
    p(f" DUPLICATES skipped : {summary['duplicates']} (same SHA-256 already saved)")
    p(f" FAILED             : {summary['failed']}")
    p("-" * 64)
    for src, cnt in summary["by_source"].items():
        label = C.SOURCE_LABEL.get(src, src)
        p(f"   {label:<18} downloaded={cnt.get('downloaded', 0):<5}"
          f" duplicate={cnt.get('duplicate', 0):<4} failed={cnt.get('failed', 0)}")
    p("-" * 64)
    p(f" Saved to           : {summary['raw_dir']}")
    p(f" Tracker DB         : {summary['tracker_db']}")
    p(f"   unique files in DB : {summary['db_totals']['total_files']}")
    p(f" Full report        : {report_path}")
    p(f" Elapsed            : {summary['elapsed_sec']} s")
    p("=" * 64)
