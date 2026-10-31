"""Regression test for the staged download flow (run after smoke_test.py):

  Part A: tracker array semantics (load, remember, record, forget rollback).
  Part B: real duplicate-content download -> knows_sha -> staged file deleted,
          nothing new on the raw drive (needs scripts/smoke_test.py to have
          run at least once; skipped otherwise).

Usage: python scripts/test_flow.py
"""

import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import requests  # noqa: E402

from finder.downloader import download  # noqa: E402
from finder.sources.base import Record  # noqa: E402
from finder.tracker import DownloadTracker  # noqa: E402

failures = []


def check(name, cond):
    print(("PASS  " if cond else "FAIL  ") + name)
    if not cond:
        failures.append(name)


# ---------------- Part A: tracker array semantics ----------------
tmp = Path(tempfile.mkdtemp(prefix="bf_track_test_"))
t = DownloadTracker(tmp / "t.db")
check("A1 load_cache on empty db", t.load_cache() == (0, 0, 0))
check("A2 unknown sha -> not known", t.knows_sha("deadbeef") is False)
t.remember_sha("deadbeef")
check("A3 remember_sha -> known", t.knows_sha("deadbeef") is True)

ins1 = t.record_file(sha256="deadbeef", url="http://x/u1", filepath="p1.pdf",
                     source="arxiv", media="papers", tag="t", title="T",
                     ext="pdf", size_bytes=123)
check("A4 first record inserted", ins1 is True)
check("A5 url now known", t.knows_url("http://x/u1") is True)
ins2 = t.record_file(sha256="deadbeef", url="http://x/u2", filepath="p2.pdf",
                     source="arxiv", media="papers", tag="t", title="T",
                     ext="pdf", size_bytes=123)
check("A6 same sha again -> not inserted", ins2 is False)
check("A7 failed url recorded+cached", (t.record_failure(url="http://x/bad",
      source="s", error="boom"), t.failure_attempts("http://x/bad") == 1)[1])

t.forget("deadbeef", "http://x/u1")
check("A8 forget -> sha unknown again", t.knows_sha("deadbeef") is False)
check("A9 forget -> url unknown again", t.knows_url("http://x/u1") is False)
check("A10 db row really deleted", t.has_sha_db("deadbeef") is False)
t.close()
shutil.rmtree(tmp, ignore_errors=True)

# ---------------- Part B: real duplicate-content download ----------------
db_path = ROOT / ".smoke_data" / "config" / "download_tracker.db"
if not db_path.exists():
    print("SKIP Part B (no smoke db) — run scripts/smoke_test.py first")
else:
    tr = DownloadTracker(db_path)
    tr.load_cache()
    row = tr.conn.execute(
        "SELECT sha256, url, tag, filepath FROM files WHERE source='arxiv' LIMIT 1"
    ).fetchone()
    if row is None:
        print("SKIP Part B (no arxiv rows)")
    else:
        sha, url, tag, filepath = row
        raw_dir = Path(filepath).parent
        staging = ROOT / ".smoke_data" / ".staging"
        before = sorted(p.name for p in raw_dir.glob("*.pdf"))
        rec = Record(source="arxiv", media="papers", id="duptest-0001",
                     title="dup test file", url=url, ext="pdf", tag=tag)
        res = download(rec, raw_dir, staging, tr, requests.Session(),
                       min_bytes=1024, max_bytes=100 * 1024 * 1024)
        check("B1 same content -> status duplicate", res.status == "duplicate")
        after = sorted(p.name for p in raw_dir.glob("*.pdf"))
        check("B2 raw dir unchanged (not saved twice)", before == after)
        leftovers = [p.name for p in staging.iterdir()] if staging.exists() else []
        check("B3 staging empty (staged file deleted)", not leftovers)
        tr.close()

print()
print("RESULT:", "ALL OK" if not failures else f"{len(failures)} FAILURES: {failures}")
sys.exit(1 if failures else 0)
