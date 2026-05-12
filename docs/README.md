# Big Brother — Reddit Intelligence Pipeline

A pipeline that continuously discovers, scrapes, and LLM-analyses Reddit posts
across a configurable set of subreddits, writing enriched JSONL records to disk.

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Architecture overview](#architecture-overview)
3. [Directory layout](#directory-layout)
4. [Data flow](#data-flow)
5. [Component reference](#component-reference)
6. [Configuration](#configuration)
7. [Running the pipeline](#running-the-pipeline)
8. [Output format](#output-format)
9. [Resilience & safety features](#resilience--safety-features)
10. [Logging](#logging)
11. [Common failure modes & fixes](#common-failure-modes--fixes)
12. [Known limits](#known-limits)

---

## What it does

Big Brother monitors a list of subreddits (default: `shopify`, `AmazonSeller`,
`ecommerce`, `Accounting`, `Entrepreneur`, `Contractor`, `smallbusiness`,
`dropship`, `ecommercemarketing`, `reviewmyshopify`) and, for each post:

1. **Discovers** the latest N posts from each subreddit via the public
   Reddit JSON API (`/r/<sub>/new.json`).
2. **Scrapes** the full post body and the entire comment tree for each post.
3. **Analyses** the content with a local Ollama LLM (`llama3.1:8b`), producing
   a structured JSON business-insight signal.
4. **Saves** the enriched record to a JSONL file — **immediately after each
   post is analysed**, so no work is lost if the process crashes.

---

## Architecture overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Orchestrator                                  │
│                                                                      │
│  1. Load seen IDs from vault (resume support)                        │
│  2. RedditCrawler.scan()   → list of {id, title, url, …}            │
│  3. Filter out already-seen IDs                                      │
│  4. RedditScraper.scrape() → for each new post:                     │
│       a. Fetch full post JSON  (aiohttp, 6–12 s random delay)       │
│       b. Parse post + comment tree                                   │
│       c. asyncio.create_task( LLM analysis )                        │
│            – GPU semaphore(1): one inference at a time              │
│            – 300 s timeout, up to 3 retries with back-off           │
│       d. on_post_complete() callback → append JSONL line to disk    │
│  5. Wait for all tasks (return_exceptions=True)                      │
│  6. Print summary                                                    │
└──────────────────────────────────────────────────────────────────────┘
```

The fetch loop and the LLM analysis are **pipelined**: while the GPU is
running analysis on post N, the network fetcher is already sleeping before
fetching post N+1.  This makes full use of both the GPU and the network
connection simultaneously.

---

## Directory layout

```
big-brother/
│
├── orchestrator/
│   └── orchestrator.py       Main pipeline coordinator + CLI entry point
│
├── crawler/
│   └── crawler.py            Discovers post IDs from subreddit listing pages
│
├── scraper/
│   └── scraper.py            Fetches full post+comments; fires off LLM tasks
│
├── ingestion/
│   ├── analysis.py           Ollama wrapper (timeout, retry, JSON parsing)
│   └── base_prompt.py        System prompt for the LLM
│
├── utils/
│   ├── config.py             .env loading, proxy config, base paths
│   ├── logging_setup.py      Centralised logging configuration
│   └── network.py            Async HTTP fetch wrapper (rate-limit detection)
│
├── data/
│   └── <sub>_<ts>.jsonl      Single-sub mode output files (one per run)
│
├── logs/
│   └── pipeline.log          Combined log, appended across all runs
│
├── docs/
│   └── README.md             This file
│
├── ingestion_vault.jsonl     Default multi-sub output (append + dedup)
├── schema.json               Example LLM output schema
├── requirements.txt          Python dependencies
└── .env.example              Environment variable template
```

---

## Data flow

### Phase 1 — Discovery (RedditCrawler)

`RedditCrawler.scan(targets, limit)` fetches
`https://www.reddit.com/r/<sub>/new.json?limit=100` for each subreddit,
paginating with the `after=` cursor until `limit` posts are collected or
Reddit's listing is exhausted.

Each discovered post is a lightweight dict:

```json
{
  "id":          "1t9urcg",
  "title":       "Help with Shopify checkout",
  "subreddit":   "shopify",
  "url":         "https://www.reddit.com/…",
  "created_utc": 1747027523.0
}
```

### Phase 2 — Scraping (RedditScraper)

For each discovered post, the scraper fetches
`https://www.reddit.com/comments/<id>.json` and parses:

- Full post body (`selftext`)
- Author, score, upvote ratio, comment count
- The **complete nested comment tree** (recursive, preserving reply depth)

A random `6–12 second` delay between requests mimics human browsing and
reduces the risk of a 429 rate-limit.

### Phase 3 — LLM Analysis (ingestion/analysis.py)

Each enriched post is formatted as a flat text blob:

```
TITLE: <title>
OP BODY: <selftext>

--- COMMENTS ---
[author]: comment body
  [author]: reply
    [author]: nested reply
…
```

This is sent to `llama3.1:8b` running on local Ollama with `format="json"`.
The model returns a JSON object matching the schema in `schema.json`:

```json
{
  "is_problem":          true,
  "problem_summary":     "Can't connect Stripe to Shopify",
  "problem_category":    "payments",
  "sentiment":           -0.7,
  "agreement_signal":    0.85,
  "business_potential":  7.4,
  "urgency":             8.1,
  "advice":              false
}
```

**Timeout**: each inference has a hard 60-second timeout. If Ollama doesn't
respond within 60 s (or returns bad JSON / a connection error), the post is
skipped — a fallback dict with `"error": "skipped"` is stored and the pipeline
immediately moves on. No retries.

### Phase 4 — Incremental Save

As soon as a post's LLM analysis is attached, `on_post_complete()` appends a
single JSONL line to the output file.  **This happens inside the LLM task,
outside the GPU semaphore**, so the disk write and the next LLM inference
overlap.  A crash at any point after this write cannot lose that post.

---

## Component reference

### `orchestrator/orchestrator.py`

| Symbol | Description |
|--------|-------------|
| `Orchestrator` | Main class; holds crawler, scraper, state |
| `Orchestrator.run_pipeline()` | Async coroutine that runs all phases |
| `Orchestrator._load_seen_ids(path)` | Reads vault to build resume set |
| `Orchestrator._make_save_callback(path)` | Returns the per-post save fn |
| `Orchestrator._summarise()` | Logs throughput stats at end |
| `DEFAULT_MULTI_SUBS` | The 10 default subreddits |
| `main()` | CLI entry point; sets up logging + signal handlers |

### `crawler/crawler.py`

| Symbol | Description |
|--------|-------------|
| `RedditCrawler` | Discovers posts from listing pages |
| `RedditCrawler.scan(targets, limit)` | Main entry; sequential per-sub |
| `RedditCrawler._scan_one(session, sub, limit)` | Paginates one subreddit |
| `REDDIT_PAGE_SIZE` | 100 (Reddit's max per page) |
| `REDDIT_LISTING_HARD_CAP` | 1 000 (Reddit's overall listing cap) |

### `scraper/scraper.py`

| Symbol | Description |
|--------|-------------|
| `RedditScraper` | Fetches post detail + runs LLM |
| `RedditScraper.scrape(posts, on_post_complete, skip_ids)` | Main entry |
| `RedditScraper._analyze_with_semaphore(post, on_post_complete, label)` | GPU-gated LLM call + save callback |
| `RedditScraper.extract_comments_recursive(data)` | Parses nested comment tree |
| `RedditScraper.partial_on_abort` | Tasks completed before a RateLimitError |

### `ingestion/analysis.py`

| Symbol | Description |
|--------|-------------|
| `run_analysis(content, post_id)` | Async; never raises; returns dict |
| `OLLAMA_TIMEOUT` | 60.0 s — skip if no response |
| `MODEL_NAME` | `"llama3.1:8b"` |

### `utils/logging_setup.py`

| Symbol | Description |
|--------|-------------|
| `setup_logging(log_dir, level)` | Called once from `main()`; attaches file + stream handlers to root logger; suppresses httpx noise |

### `utils/network.py`

| Symbol | Description |
|--------|-------------|
| `AsyncFetcher` | aiohttp wrapper with semaphore, proxy, random delay |
| `AsyncFetcher.fetch_json(session, url, delay_range)` | Returns parsed JSON or `None` |
| `RateLimitError` | Raised on HTTP 429; propagates to abort pipeline |
| `HTTP_TIMEOUT_SECONDS` | 30 s per HTTP request |

---

## Configuration

### `.env` file

```dotenv
PROXY_URL=http://user:pass@host:port    # leave empty to go direct
PROXY_ENABLED=false                     # true to route all Reddit requests via proxy
```

### Tuneable constants

| File | Constant | Default | Effect |
|------|----------|---------|--------|
| `ingestion/analysis.py` | `OLLAMA_TIMEOUT` | 60 s | Skip post if Ollama doesn't respond |
| `ingestion/analysis.py` | `MODEL_NAME` | `llama3.1:8b` | Ollama model |
| `utils/network.py` | `HTTP_TIMEOUT_SECONDS` | 30 s | Per-Reddit-request timeout |
| `scraper/scraper.py` | `gpu_limit` | `Semaphore(1)` | Concurrent LLM calls |
| `network.py` `fetch_json` | `delay_range` | `(6, 12)` s | Random inter-request delay |

---

## Running the pipeline

### Prerequisites

```bash
# 1. Activate the venv
source venv/bin/activate

# 2. Make sure Ollama is running with llama3.1:8b pulled
ollama serve &
ollama pull llama3.1:8b

# 3. Copy and edit .env
cp .env.example .env
```

### Commands

```bash
# Multi-sub sweep — 100 posts per sub (default), append to ingestion_vault.jsonl
python3 -m orchestrator.orchestrator

# Multi-sub, 500 posts per sub (~5 000 total)
python3 -m orchestrator.orchestrator -n 500

# Single subreddit, 1 000 posts, fresh file
python3 -m orchestrator.orchestrator -s shopify -n 1000

# Single sub, skip LLM (fast — just scrape)
python3 -m orchestrator.orchestrator -s shopify -n 200 --no-llm

# Override output path
python3 -m orchestrator.orchestrator -s shopify -n 100 -o /data/shopify.jsonl

# Force re-process everything (ignore resume)
python3 -m orchestrator.orchestrator --no-resume

# Full help
python3 -m orchestrator.orchestrator --help
```

### Resume / restart

If the pipeline is interrupted (crash, OOM, SIGTERM), simply **re-run the
exact same command**.  The orchestrator reads the existing output file,
builds the set of already-processed post IDs, and skips them.  Only new or
not-yet-processed posts are fetched and analysed.

This means you can safely restart a 1 000-post run that failed at post 750
and it will pick up from post 751 without re-doing any LLM work.

---

## Output format

Each line of the `.jsonl` output file is a self-contained JSON object:

```json
{
  "post": {
    "id":           "1t9urcg",
    "title":        "Help with Shopify checkout — payment keeps failing",
    "body":         "I've been trying to set up Stripe…",
    "author":       "u/some_merchant",
    "score":        42,
    "upvote_ratio": 0.95,
    "num_comments": 17,
    "subreddit":    "shopify",
    "created_utc":  1747027523.0
  },
  "comments": [
    {
      "id":        "kc1a2b3",
      "author":    "u/another_user",
      "body":      "Have you checked your Stripe webhook settings?",
      "score":     12,
      "parent_id": "t3_1t9urcg",
      "replies": [
        {
          "id":        "kc9x8y7",
          "author":    "u/some_merchant",
          "body":      "Yes, the webhooks look fine…",
          "score":     3,
          "parent_id": "t1_kc1a2b3",
          "replies":   []
        }
      ]
    }
  ],
  "analysis": {
    "is_problem":         true,
    "problem_summary":    "Payment gateway integration failure on checkout",
    "problem_category":   "payments",
    "sentiment":          -0.65,
    "agreement_signal":   0.8,
    "business_potential": 7.2,
    "urgency":            8.5,
    "advice":             false
  }
}
```

### Analysis fields

| Field | Type | Description |
|-------|------|-------------|
| `is_problem` | bool | Post describes a concrete business/technical problem |
| `problem_summary` | string | One-sentence summary of the problem |
| `problem_category` | string | Category tag (e.g. `payments`, `inventory`, `marketing`) |
| `sentiment` | float −1…1 | Negative = frustrated, positive = satisfied |
| `agreement_signal` | float 0…1 | Fraction of commenters who agree with / validate the OP |
| `business_potential` | float 0…10 | How strong a commercial opportunity the problem represents |
| `urgency` | float 0…10 | How urgently the OP needs a solution |
| `advice` | bool | Post is advice-seeking rather than problem-reporting |

---

## Resilience & safety features

### Incremental saves (most important)

Each post is written to disk **as soon as its LLM analysis completes**.
A 1 000-post run that crashes at post 999 loses exactly one post, not all 999.
The callback fires *outside* the GPU semaphore so disk I/O never delays the
next inference.

### Resume support

On startup the orchestrator reads the output file to collect already-processed
post IDs.  Re-running after a crash automatically skips them.

Disable with `--no-resume` if you want to re-analyse everything.

### Ollama timeout + retry

Every inference is wrapped with `asyncio.wait_for(timeout=300)`.  If Ollama
hangs (common after OOM or model swap), the request is aborted within 5 minutes
rather than blocking forever.

On failure the request is retried up to 3 times with exponential back-off
(5 s → 10 s → 20 s).  After all retries a fallback struct with `"error"` is
stored so the post is still persisted and the pipeline moves on.

### Rate-limit abort

HTTP 429 from Reddit raises `RateLimitError`, which immediately cancels all
in-flight LLM tasks and triggers a clean exit.  Posts completed before the
abort are on disk (incremental saves); the run can be resumed later.

### Signal handling (SIGTERM / SIGINT)

On `Ctrl-C` or `kill`, all asyncio tasks are cancelled gracefully.
Because saves happen per-post, all completed work is safe.

### `return_exceptions=True` in gather

`asyncio.gather(*tasks, return_exceptions=True)` ensures a single failing LLM
task does not cancel the remaining 999.  Exceptions are logged individually.

### `CancelledError` propagation

`asyncio.CancelledError` is explicitly **not** caught by the retry loop in
`analysis.py`, so task cancellation propagates correctly through the pipeline.

---

## Logging

All components use named loggers (`Orchestrator`, `RedditCrawler`,
`RedditScraper`, `NetworkEngine`, `Analysis`) that propagate to the root
logger configured in `utils/logging_setup.py`.

Output goes to:
- **stdout** — live terminal view
- **`logs/pipeline.log`** — persistent combined log, appended across runs

`httpx` and `httpcore` (used internally by the Ollama client) are silenced at
`WARNING` level to avoid cluttering the log with HTTP Request lines.

### Log event dictionary

| Event | Level | Source | Meaning |
|-------|-------|--------|---------|
| `PIPELINE_INIT` | INFO | Orchestrator | Pipeline starting, key params |
| `PHASE=discovery` | INFO | Orchestrator | Crawl phase beginning |
| `DISCOVERY_DONE` | INFO | Orchestrator | Crawl complete, post count |
| `RESUME` | INFO | Orchestrator | Loaded N seen IDs from vault |
| `RESUME_SKIP` | INFO | Orchestrator | Skipping N already-done posts |
| `PHASE=scrape+analyse` | INFO | Orchestrator | Scrape+LLM phase beginning |
| `SAVED` | INFO | Orchestrator | One post written to disk (with #count, rate) |
| `PIPELINE_COMPLETE` | INFO | Orchestrator | Summary stats |
| `CRAWL_START` | INFO | RedditCrawler | Crawl loop starting |
| `CRAWL_SUB` | INFO | RedditCrawler | Starting one subreddit |
| `FETCH_PAGE` | INFO | RedditCrawler | Fetching one listing page |
| `PAGE_DONE` | INFO | RedditCrawler | Page result with counts |
| `CRAWL_COMPLETE` | INFO | RedditCrawler | All subs crawled |
| `SCRAPE_START` | INFO | RedditScraper | Scraper loop starting |
| `QUEUED` | INFO | RedditScraper | Post fetched, LLM task created |
| `FETCH_PHASE_DONE` | INFO | RedditScraper | All posts fetched, LLM tasks running |
| `SCRAPE_DONE` | INFO | RedditScraper | All LLM tasks complete |
| `LLM_START` | INFO | Analysis | Sending post to Ollama |
| `LLM_DONE` | INFO | Analysis | Inference complete (with timing) |
| `LLM_TIMEOUT` | WARN | Analysis | 300 s limit exceeded — will retry |
| `LLM_RETRY` | INFO | Analysis | Back-off before next attempt |
| `LLM_FAILED` | ERROR | Analysis | All retries exhausted; fallback stored |
| `FETCH_OK` | INFO | NetworkEngine | HTTP 200 from Reddit |
| `RATE_LIMITED` | CRIT | NetworkEngine | HTTP 429 received — aborting |
| `FETCH_TIMEOUT` | ERROR | NetworkEngine | 30 s HTTP timeout |
| `SIGNAL_RECEIVED` | WARN | Orchestrator | SIGTERM/SIGINT, cancelling tasks |
| `PIPELINE_ABORT` | CRIT | Orchestrator | Rate-limit abort path |

---

## Common failure modes & fixes

### Process dies with no error in log

**Cause**: OOM kill (kernel kills the process; Python has no chance to log).
**Fix**: Incremental saves mean all completed posts are on disk. Just restart.

### Only `http://127.0.0.1:11434/api/chat` in logs

**Not a bug.** This happens after all Reddit fetches are done — the fetch
phase completes first, then the LLM analysis phase runs through the remaining
queued tasks. The log event `FETCH_PHASE_DONE | all posts fetched, LLM tasks
running…` marks this transition explicitly.

### Ollama timeout errors

Ollama can stall under memory pressure, especially when VRAM is tight.
The 300-second timeout ensures the pipeline detects this within 5 minutes
and retries. If timeouts are frequent:
- Reduce `OLLAMA_TIMEOUT` to `120` to fail faster
- Free VRAM by stopping other GPU workloads
- Reduce `num_ctx` in `OLLAMA_OPTIONS`

### HTTP 429 (rate limited by Reddit)

Reddit throttles IPs that make too many requests too quickly.
The random 6–12 second delay between requests is already conservative.
If you get 429s:
- Set `PROXY_ENABLED=true` in `.env` and provide a residential proxy
- Increase the `delay_range` in `network.py fetch_json`

### Vault has a mix of raw and enriched records

The `ingestion_vault.jsonl` may contain legacy raw-format records
(`{"id": "…", "title": "…", …}`) from older runs.  The dedup logic handles
both formats — raw records are recognised by their top-level `id` key.

---

## Known limits

| Limit | Value | Source |
|-------|-------|--------|
| Max posts per subreddit | 1 000 | Reddit API hard cap on listing endpoints |
| Concurrent Reddit requests | 1 | `AsyncFetcher(max_concurrent=1)` |
| Concurrent LLM inferences | 1 | `gpu_limit = Semaphore(1)` |
| LLM inference timeout | 300 s | `OLLAMA_TIMEOUT` in `analysis.py` |
| HTTP request timeout | 30 s | `HTTP_TIMEOUT_SECONDS` in `network.py` |
| Comment depth | unlimited | Recursive extraction with no depth cap |
