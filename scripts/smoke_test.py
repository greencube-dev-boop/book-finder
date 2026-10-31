"""Local smoke test — downloads a few files into .smoke_data/ (no Drive needed).

Run it TWICE to verify dedup: the second run must report 0 downloaded and the
files as already tracked / duplicates.

Usage:
  python scripts/smoke_test.py                        # 5 papers, "quantum computing"
  python scripts/smoke_test.py --tag astronomy --media books --max 3
  python scripts/smoke_test.py --max 2 --sources arxiv,openalex
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from finder.pipeline import RunConfig, run  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Book Finder local smoke test")
    parser.add_argument("--tag", default="quantum computing")
    parser.add_argument("--media", default="papers", choices=["papers", "books", "both"])
    parser.add_argument("--max", type=int, default=5)
    parser.add_argument("--sources", default="all")
    parser.add_argument("--data-dir", default=str(ROOT / ".smoke_data"))
    args = parser.parse_args()

    summary = run(RunConfig(
        tags=[args.tag],
        media=args.media,
        max_downloads=args.max,
        sources=args.sources,
        data_dir=args.data_dir,
    ))

    if summary["found"] == 0:
        print("SMOKE TEST FAILED: no records found from any source")
        sys.exit(1)
    if summary["attempted"] == 0 and summary["already_tracked"] == 0:
        print("SMOKE TEST FAILED: candidates were found but none survived dedup filters")
        sys.exit(1)
    print()
    print("SMOKE TEST OK "
          f"(found={summary['found']}, downloaded={summary['downloaded']}, "
          f"duplicates={summary['duplicates']}, failed={summary['failed']}, "
          f"already_tracked={summary['already_tracked']})")


if __name__ == "__main__":
    main()
