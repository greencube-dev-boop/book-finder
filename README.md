# Book Finder — scientific papers & books downloader for Google Colab

For a given tag (or list of tags), searches **legal open-access APIs**, downloads large
batches of research **papers** or **books** into Google Drive, and tracks every saved
file by **SHA-256** so nothing is ever stored twice.

- Papers: **arXiv**, **OpenAlex**, **Semantic Scholar**, **DOAJ**
- Books: **Internet Archive**, **Project Gutenberg**

## Quickstart (Colab)

1. Open `notebook/Book_Finder.ipynb` in Colab once
   (File → Upload notebook, or `colab.research.google.com/github/greencube-dev-boop/book-finder`).
2. **Save a copy in Drive** — you will never need to re-upload it.
3. Run **Cell 1** (setup): mounts Drive and pulls the latest code from this repo.
4. Set your options in **Cell 2** (tags, papers/books, how many to download).
5. Run **Cell 3** — it prints *found / downloaded / duplicates / failed* and saves
   everything to `MyDrive/OCR Engine/raw/`.

### Fast by default: search + download cycle

Searches run **in parallel** (one thread per tag x source) and **downloads start
the moment a search returns results** — a pool of worker threads (Cell 2's
`WORKERS`, default 3) pulls from a shared queue while the remaining sources are
still being searched. Per-source politeness delays and API rate limits are
still respected (thread-safe rate limiter).

### The download flow (staging + SHA-256 array)

1. The tracker DB is loaded into memory once per run (all hashes, URLs and
   failure counters) — every dedup check is a RAM lookup, the Drive DB is
   only written for genuinely new files.
2. Each file downloads into a **staging folder in the Colab environment**
   (`/content/book_finder_staging`) and is hashed (SHA-256) while streaming.
3. Hash already in the array? → the staged file is **deleted**, next file.
4. New? → hash is added to the array, **synced to `download_tracker.db`** on
   Drive, then the file is **moved** to `OCR Engine/raw/`. (If the move fails
   the record is rolled back so it can be retried later.) Drive never sees
   partial or duplicate files.

### Why the notebook never needs re-uploading

All logic lives in this repo. Cell 1 runs `git clone` (or `git pull`) **every run**, so
after changing any script here you only need to `git push` — the next Colab run
automatically uses the new code.

## Drive layout

```
MyDrive/OCR Engine/
├── raw/
│   ├── papers/<tag-slug>/arxiv-2401.12345v1.pdf
│   └── books/<tag-slug>/gutenberg-1342.epub
└── config/
    ├── download_tracker.db          ← SQLite, SHA-256 dedup + failure tracking
    └── reports/run_YYYYmmdd_HHMMSS.json
```

## Tracker database (`download_tracker.db`)

| table | columns | purpose |
|---|---|---|
| `files` | sha256 (PK), url (UNIQUE), filepath, source, media, tag, title, ext, size_bytes, downloaded_at | one row per saved file; the SHA-256 PK means identical content is never saved twice, the UNIQUE url means a URL is never downloaded twice |
| `failures` | url (PK), source, error, attempts, last_attempt | URLs that failed ≥ 3 times are skipped on reruns |

## Local development

```bash
pip install -r requirements.txt
python scripts/smoke_test.py --max 5                 # 5 papers into .smoke_data/
python scripts/smoke_test.py --max 5                 # run again -> 0 downloaded (dedup works)
python scripts/smoke_test.py --media books --tag astronomy --max 3
```

## Updating the tool

Edit scripts locally → `git add -A && git commit -m "..." && git push` →
re-run the notebook in Colab (Cell 1 pulls the change automatically).

## Legal note

Only open-access / public-domain sources are used. No Sci-Hub, LibGen or similar.
Respect each provider's terms — the code rate-limits itself politely.
