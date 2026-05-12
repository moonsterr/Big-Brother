"""
Big Brother — Reddit ingestion orchestrator.

Coordinates the full pipeline:
  1. RedditCrawler  – discovers post IDs from target subreddits
  2. RedditScraper  – fetches full post + comment trees
  3. LLM analysis   – classifies each post with Ollama (llama3.1:8b)
  4. Incremental save – each finished post is written to disk immediately
                        so a crash / OOM never loses completed work

Resume support: if the output file already exists, posts whose IDs are
already stored are skipped automatically.

Usage
-----
  # Multi-subreddit sweep (default 100 posts per sub, ~1 000 total):
  python3 -m orchestrator.orchestrator

  # Single sub, 1 000 posts:
  python3 -m orchestrator.orchestrator -s shopify -n 1000

  # Multi-sub, 500 posts per sub, skip LLM:
  python3 -m orchestrator.orchestrator -n 500 --no-llm
"""

import argparse
import asyncio
import json
import logging
import re
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from crawler.crawler import REDDIT_LISTING_HARD_CAP, REDDIT_PAGE_SIZE, RedditCrawler
from scraper.scraper import RedditScraper
from utils.config import BASE_DIR
from utils.logging_setup import setup_logging
from utils.network import RateLimitError

logger = logging.getLogger("Orchestrator")

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MULTI_SUBS = [
    "shopify", "AmazonSeller", "ecommerce", "Accounting", "Entrepreneur",
    "Contractor", "smallbusiness", "dropship", "ecommercemarketing",
    "reviewmyshopify",
]
LOG_DIR  = BASE_DIR / "logs"
SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")
# ─────────────────────────────────────────────────────────────────────────────


class Orchestrator:
    def __init__(
        self,
        subs:        List[str],
        limit:       int = REDDIT_PAGE_SIZE,
        run_llm:     bool = True,
        output_path: Optional[Path] = None,
        dedup:       bool = True,
    ):
        self.subs        = subs
        self.limit       = limit
        self.run_llm     = run_llm
        self.output_path = output_path   # absolute path or None → default vault
        self.dedup       = dedup

        self.crawler = RedditCrawler()
        self.scraper = RedditScraper(run_llm=run_llm)

        # Runtime stats
        self._saved_count   = 0
        self._skipped_count = 0
        self._start_time    = 0.0

        # Write lock – ensures only one coroutine writes to disk at a time.
        # (With gpu_limit=Semaphore(1) this is redundant, but kept for safety
        # if concurrency is ever raised.)
        self._write_lock = asyncio.Lock()

        # Graceful shutdown flag (set by SIGTERM / SIGINT).
        self._abort = False

    # ── Output path ───────────────────────────────────────────────────────────

    def _resolve_output_path(self) -> Path:
        if self.output_path is not None:
            return self.output_path
        return BASE_DIR / "ingestion_vault.jsonl"

    # ── Resume helpers ────────────────────────────────────────────────────────

    def _load_seen_ids(self, path: Path) -> Set[str]:
        """
        Read all post IDs already stored in `path`.

        Handles both formats:
          • Enriched format: {"post": {"id": "abc123", …}, "comments": […], …}
          • Legacy crawler format: {"id": "abc123", "title": "…", …}
        """
        seen: Set[str] = set()
        if not path.exists():
            return seen

        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # Enriched format (current)
                    post_id = record.get("post", {}).get("id")
                    # Legacy / raw crawler format
                    if not post_id:
                        post_id = record.get("id")
                    if post_id:
                        seen.add(post_id)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        f"VAULT_PARSE_ERROR | {path} line {lineno}: {exc} — skipping line"
                    )

        if seen:
            logger.info(
                f"RESUME | loaded {len(seen)} already-processed IDs from {path}"
            )
        return seen

    # ── Incremental save ──────────────────────────────────────────────────────

    def _make_save_callback(self, save_path: Path) -> Callable:
        """
        Return a synchronous callback that appends one enriched post to disk.

        This is called from within an asyncio task (in _analyze_with_semaphore)
        after the LLM result is attached to the post dict.  The write happens
        outside the GPU semaphore so the next LLM task can start immediately.
        """
        save_path.parent.mkdir(parents=True, exist_ok=True)

        def _save_one(enriched_post: Dict[str, Any]) -> None:
            post_id  = enriched_post.get("post", {}).get("id", "?")
            sub      = enriched_post.get("post", {}).get("subreddit", "?")
            elapsed  = time.monotonic() - self._start_time

            # Compute rate and ETA.
            rate_str = eta_str = ""
            if elapsed > 0 and self._saved_count > 0:
                rate  = self._saved_count / elapsed          # posts/sec
                rate_str = f" | rate={rate * 60:.1f} posts/min"
                # We don't know the exact total here; logged in run_pipeline.

            try:
                with open(save_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(enriched_post, ensure_ascii=False) + "\n")

                self._saved_count += 1
                logger.info(
                    f"SAVED | #{self._saved_count} | ID: {post_id} | "
                    f"sub=r/{sub} | elapsed={elapsed:.0f}s{rate_str}"
                )
            except OSError as exc:
                logger.error(
                    f"SAVE_FAILED | ID: {post_id} | path={save_path} | {exc}"
                )

        return _save_one

    # ── Pipeline ──────────────────────────────────────────────────────────────

    async def run_pipeline(self) -> None:
        self._start_time = time.monotonic()
        save_path = self._resolve_output_path()

        logger.info(
            f"PIPELINE_INIT | subs={self.subs} | limit_per_sub={self.limit} | "
            f"llm={'on' if self.run_llm else 'off'} | output={save_path}"
        )

        # ── Discovery phase ───────────────────────────────────────────────────
        logger.info("PHASE=discovery | crawling subreddits …")
        try:
            raw_discovery = await self.crawler.scan(self.subs, limit=self.limit)
        except RateLimitError as exc:
            logger.critical(
                f"PIPELINE_ABORT | rate-limited during discovery ({exc.url}) | "
                "no posts collected — nothing to save."
            )
            return

        if not raw_discovery:
            logger.warning("DISCOVERY_EMPTY | no posts returned from crawler — aborting.")
            return

        logger.info(
            f"DISCOVERY_DONE | found {len(raw_discovery)} posts across "
            f"{len(self.subs)} subreddit(s)"
        )

        # ── Resume: skip already-processed posts ──────────────────────────────
        seen_ids = self._load_seen_ids(save_path) if self.dedup else set()
        new_posts = [p for p in raw_discovery if p["id"] not in seen_ids]
        self._skipped_count = len(raw_discovery) - len(new_posts)

        if self._skipped_count:
            logger.info(
                f"RESUME_SKIP | {self._skipped_count} post(s) already in vault — "
                f"will process {len(new_posts)} new post(s) only."
            )

        if not new_posts:
            logger.info("NOTHING_NEW | all discovered posts already exist in vault.")
            return

        # ── Scrape + analysis phase ───────────────────────────────────────────
        logger.info(
            f"PHASE=scrape+analyse | {len(new_posts)} posts to process | "
            f"saving each post to disk as it completes → {save_path}"
        )

        save_callback = self._make_save_callback(save_path)

        try:
            final_results = await self.scraper.scrape(
                new_posts,
                on_post_complete=save_callback,
                skip_ids=seen_ids,   # belt-and-suspenders: also checked inside scraper
            )
        except RateLimitError as exc:
            partial = self.scraper.partial_on_abort
            logger.critical(
                f"PIPELINE_ABORT | rate-limited during scrape ({exc.url}) | "
                f"{len(partial)} LLM task(s) had already completed and were saved."
            )
            # Incremental saves already wrote the completed ones; just summarise.
            self._summarise(extra=f"(PARTIAL — rate-limited at {exc.url})")
            return

        # ── Final summary ─────────────────────────────────────────────────────
        self._summarise(total_returned=len(final_results))

    def _summarise(
        self,
        total_returned: Optional[int] = None,
        extra: str = "",
    ) -> None:
        elapsed = time.monotonic() - self._start_time
        rate    = self._saved_count / elapsed * 60 if elapsed > 0 else 0
        logger.info("─" * 60)
        logger.info(f"PIPELINE_COMPLETE {extra}")
        logger.info(f"  Posts saved to disk : {self._saved_count}")
        logger.info(f"  Posts skipped (dup) : {self._skipped_count}")
        if total_returned is not None:
            logger.info(f"  Tasks returned      : {total_returned}")
        logger.info(f"  Elapsed             : {elapsed / 60:.1f} min")
        logger.info(f"  Avg throughput      : {rate:.1f} posts/min")
        logger.info(f"  Output file         : {self._resolve_output_path()}")
        logger.info("─" * 60)


# ── CLI argument parsing ──────────────────────────────────────────────────────

def _positive_int(value: str) -> int:
    try:
        n = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"must be an integer, got '{value}'") from exc
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
    if n > REDDIT_LISTING_HARD_CAP:
        print(
            f"[WARN] --limit {n} exceeds Reddit hard cap "
            f"{REDDIT_LISTING_HARD_CAP}; the crawler will clamp it."
        )
    return n


def _normalize_subreddit(name: str) -> str:
    cleaned = name.strip()
    if cleaned.lower().startswith("r/"):
        cleaned = cleaned[2:]
    cleaned = cleaned.lstrip("/")
    if not SUBREDDIT_RE.match(cleaned):
        raise argparse.ArgumentTypeError(
            f"invalid subreddit name '{name}' "
            "(use only letters / digits / underscores, 2–21 chars)"
        )
    return cleaned


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="big-brother",
        description=(
            "Big Brother Reddit ingestion pipeline.\n\n"
            "With no arguments: runs the default 10-subreddit sweep, "
            "100 posts per sub (1 000 total), appending to ingestion_vault.jsonl.\n\n"
            "With --subreddit: single-sub mode, writes a fresh "
            "data/<sub>_<ts>.jsonl file.\n\n"
            "-n / --limit applies to BOTH modes (posts per subreddit)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-s", "--subreddit",
        type=_normalize_subreddit,
        default=None,
        help=(
            "Single subreddit to scrape (e.g. 'shopify' or 'r/shopify'). "
            "Enables single-sub mode with a fresh output file."
        ),
    )
    parser.add_argument(
        "-n", "--limit",
        type=_positive_int,
        default=None,
        help=(
            "Posts per subreddit to fetch (default: 100 in multi-sub mode, "
            "25 in single-sub mode). Reddit hard cap: 1 000."
        ),
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip Ollama analysis — output records will not have an 'analysis' field.",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help=(
            "Override output file path. "
            "Default: data/<sub>_<ts>.jsonl (single-sub) "
            "or ingestion_vault.jsonl (multi-sub)."
        ),
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help=(
            "Disable resume mode — process ALL discovered posts even if they "
            "already exist in the output file."
        ),
    )
    return parser


def _default_single_sub_output(subreddit: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return BASE_DIR / "data" / f"{subreddit}_{ts}.jsonl"


# ── Signal handling ───────────────────────────────────────────────────────────

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """On SIGTERM / SIGINT log a warning and cancel all running tasks."""
    def _handler(sig_name: str) -> None:
        logger.warning(
            f"SIGNAL_RECEIVED | {sig_name} | cancelling all pending tasks — "
            "posts already saved to disk are safe."
        )
        for task in asyncio.all_tasks(loop):
            task.cancel()

    try:
        loop.add_signal_handler(signal.SIGTERM, lambda: _handler("SIGTERM"))
        loop.add_signal_handler(signal.SIGINT,  lambda: _handler("SIGINT"))
    except NotImplementedError:
        # Windows does not support add_signal_handler.
        pass


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    # ── Logging must be set up before any other import side-effects log ───────
    setup_logging(LOG_DIR)

    args = _build_arg_parser().parse_args()

    if args.subreddit:
        # ── Single-sub mode ───────────────────────────────────────────────────
        limit = args.limit if args.limit is not None else 25
        output_path = args.output or _default_single_sub_output(args.subreddit)
        if not output_path.is_absolute():
            output_path = (BASE_DIR / output_path).resolve()

        orchestrator = Orchestrator(
            subs=[args.subreddit],
            limit=limit,
            run_llm=not args.no_llm,
            output_path=output_path,
            dedup=not args.no_resume,
        )
        logger.info(
            f"MODE=single-sub | sub=r/{args.subreddit} | limit={limit} | "
            f"llm={'off' if args.no_llm else 'on'} | output={output_path} | "
            f"resume={'off' if args.no_resume else 'on'}"
        )

    else:
        # ── Multi-sub mode ────────────────────────────────────────────────────
        limit = args.limit if args.limit is not None else REDDIT_PAGE_SIZE
        output_path = args.output
        if output_path is not None and not output_path.is_absolute():
            output_path = (BASE_DIR / output_path).resolve()

        orchestrator = Orchestrator(
            subs=DEFAULT_MULTI_SUBS,
            limit=limit,
            run_llm=not args.no_llm,
            output_path=output_path,
            dedup=not args.no_resume,
        )
        logger.info(
            f"MODE=multi-sub | subs={DEFAULT_MULTI_SUBS} | "
            f"limit_per_sub={limit} | max_total={limit * len(DEFAULT_MULTI_SUBS)} | "
            f"llm={'off' if args.no_llm else 'on'} | "
            f"resume={'off' if args.no_resume else 'on'}"
        )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)

    try:
        loop.run_until_complete(orchestrator.run_pipeline())
    except asyncio.CancelledError:
        logger.warning(
            "PIPELINE_CANCELLED | tasks cancelled by signal — "
            "all posts saved before cancellation are safe on disk."
        )
    except KeyboardInterrupt:
        logger.warning(
            "USER_ABORT | KeyboardInterrupt received — "
            "all posts saved so far are on disk."
        )
    finally:
        # Give pending tasks a moment to flush.
        try:
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(
                    asyncio.wait(pending, timeout=5)
                )
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    main()
