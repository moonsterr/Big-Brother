import argparse
import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from crawler.crawler import REDDIT_LISTING_HARD_CAP, REDDIT_PAGE_SIZE, RedditCrawler
from scraper.scraper import RedditScraper
from utils.config import BASE_DIR
from utils.network import RateLimitError

logger = logging.getLogger("Orchestrator")

DEFAULT_MULTI_SUBS = [
    "shopify", "AmazonSeller", "ecommerce", "Accounting", "Entrepreneur",
    "Contractor", "smallbusiness", "dropship", "ecommercemarketing",
    "reviewmyshopify",
]

# Allowed subreddit chars per Reddit's rules: letters, digits, underscore, 3-21 chars.
SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")


class Orchestrator:
    def __init__(
        self,
        subs: List[str],
        limit: int = REDDIT_PAGE_SIZE,
        run_llm: bool = True,
        output_path: Optional[Path] = None,
        dedup: bool = True,
    ):
        self.subs = subs
        self.limit = limit
        self.run_llm = run_llm
        self.output_path = output_path  # absolute path or None (=> default vault)
        self.dedup = dedup
        self.data: List[Dict[str, Any]] = []
        self.crawler = RedditCrawler()
        self.scraper = RedditScraper(run_llm=run_llm)

    def print_state_sample(self, label: str, data_sample: Any):
        print(f"\n{'='*20} STATE: {label} {'='*20}")
        if isinstance(data_sample, list) and len(data_sample) > 0:
            item = data_sample[0]
            print(f"Data Type: List | Count: {len(data_sample)}")
            print(f"First Item Structure: {json.dumps(item, indent=2)[:500]}...")
        else:
            print(f"Data: {data_sample}")
        print(f"{'='*50}\n")

    async def run_pipeline(self):
        logger.info(
            f"MISSION_START | Targets: {self.subs} | limit={self.limit} | "
            f"llm={'on' if self.run_llm else 'off'}"
        )

        # --- Discovery phase ---
        try:
            raw_discovery = await self.crawler.scan(self.subs, limit=self.limit)
        except RateLimitError as e:
            logger.critical(
                f"PIPELINE_ABORTED | Rate limited during discovery phase ({e.url}). "
                "No posts collected — nothing to save. Exiting."
            )
            return

        if not raw_discovery:
            logger.warning("DISCOVERY_FAILED")
            return

        self.data = raw_discovery
        self.print_state_sample("AFTER_CRAWLER_DISCOVERY", self.data)

        # --- Scrape + analysis phase ---
        try:
            self.data = await self.scraper.scrape(raw_discovery)
        except RateLimitError as e:
            # Scraper already cancelled LLM tasks and stored whatever finished.
            self.data = self.scraper.partial_on_abort
            logger.critical(
                f"PIPELINE_ABORTED | Rate limited during scrape phase ({e.url}). "
                f"Saving {len(self.data)} partial result(s) and exiting."
            )
            if self.data:
                self.summarize_session()
                self.save_to_disk()
            else:
                logger.warning("STORAGE_SKIPPED | No completed posts to save.")
            return

        if len(self.data) > 0 and "comments" in self.data[0]:
            self.print_state_sample("AFTER_SCRAPER_ENRICHMENT", self.data)

        self.summarize_session()
        self.save_to_disk()

    def summarize_session(self):
        total_posts = len(self.data)

        def count_all(comments):
            return len(comments) + sum(count_all(c.get("replies", [])) for c in comments)

        total_comments = sum(count_all(post.get("comments", [])) for post in self.data)

        logger.info("-" * 40)
        logger.info("MISSION_COMPLETE")
        logger.info(f"Total Unique Posts Scraped: {total_posts}")
        logger.info(f"Total Nested Comments Captured: {total_comments}")
        logger.info("-" * 40)

    def save_to_disk(self, filename: str = "ingestion_vault.jsonl"):
        if self.output_path is not None:
            save_path = self.output_path
        else:
            save_path = BASE_DIR / filename

        save_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.data:
            logger.warning("No data to save.")
            return

        existing_ids: set = set()
        if self.dedup and save_path.exists():
            with open(save_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        post_id = record.get("post", {}).get("id")
                        if post_id:
                            existing_ids.add(post_id)
                    except json.JSONDecodeError:
                        continue

        new_entries = [
            e for e in self.data
            if e.get("post", {}).get("id") not in existing_ids
        ]

        if not new_entries:
            logger.info("STORAGE_SKIPPED | All posts already exist in vault.")
            return

        mode = "a" if self.dedup else "w"
        with open(save_path, mode, encoding="utf-8") as f:
            for entry in new_entries:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        skipped = len(self.data) - len(new_entries)
        logger.info(
            f"STORAGE_COMPLETE | {len(new_entries)} new posts saved "
            f"(skipped {skipped} duplicates) | File: {save_path}"
        )


def _positive_int(value: str) -> int:
    try:
        n = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"must be an integer, got '{value}'") from exc
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {n}")
    if n > REDDIT_LISTING_HARD_CAP:
        # Don't reject — the crawler will clamp + warn — but signal intent here too.
        print(f"[WARN] --limit {n} exceeds Reddit cap {REDDIT_LISTING_HARD_CAP}; will clamp.")
    return n


def _normalize_subreddit(name: str) -> str:
    cleaned = name.strip()
    if cleaned.lower().startswith("r/"):
        cleaned = cleaned[2:]
    if cleaned.startswith("/"):
        cleaned = cleaned.lstrip("/")
    if not SUBREDDIT_RE.match(cleaned):
        raise argparse.ArgumentTypeError(
            f"invalid subreddit name '{name}' (use only letters/digits/underscore, 2-21 chars)"
        )
    return cleaned


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="big-brother",
        description=(
            "Big Brother Reddit ingestion pipeline. "
            "With no arguments, runs the default multi-subreddit sweep into ingestion_vault.jsonl. "
            "With --subreddit, scrapes a single subreddit's last N posts into data/<sub>_<ts>.jsonl."
        ),
    )
    parser.add_argument(
        "-s", "--subreddit",
        type=_normalize_subreddit,
        default=None,
        help="Single subreddit to scrape (e.g. 'shopify'). Enables single-sub mode.",
    )
    parser.add_argument(
        "-n", "--limit",
        type=_positive_int,
        default=25,
        help="Number of latest posts to scrape (default: 25; Reddit hard cap: 1000). "
             "Only meaningful with --subreddit.",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip Ollama analysis. Output records will not contain an 'analysis' field.",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=None,
        help="Override output file path. Default: data/<subreddit>_<UTC-timestamp>.jsonl "
             "in single-sub mode, or ingestion_vault.jsonl in multi-sub mode.",
    )
    return parser


def _default_single_sub_output(subreddit: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return BASE_DIR / "data" / f"{subreddit}_{ts}.jsonl"


def main() -> None:
    args = _build_arg_parser().parse_args()

    if args.subreddit:
        # Single-sub mode: write to data/<sub>_<ts>.jsonl, fresh file, no dedup.
        output_path = args.output if args.output else _default_single_sub_output(args.subreddit)
        if not output_path.is_absolute():
            output_path = (BASE_DIR / output_path).resolve()

        orchestrator = Orchestrator(
            subs=[args.subreddit],
            limit=args.limit,
            run_llm=not args.no_llm,
            output_path=output_path,
            dedup=False,
        )
        logger.info(
            f"MODE=single-sub | sub=r/{args.subreddit} | limit={args.limit} | "
            f"llm={'off' if args.no_llm else 'on'} | output={output_path}"
        )
    else:
        # Multi-sub mode: existing behavior. Append + dedup against vault.
        output_path = args.output  # may be None (=> default vault)
        if output_path is not None and not output_path.is_absolute():
            output_path = (BASE_DIR / output_path).resolve()

        orchestrator = Orchestrator(
            subs=DEFAULT_MULTI_SUBS,
            limit=REDDIT_PAGE_SIZE,
            run_llm=not args.no_llm,
            output_path=output_path,
            dedup=True,
        )
        logger.info(
            f"MODE=multi-sub | subs={DEFAULT_MULTI_SUBS} | "
            f"llm={'off' if args.no_llm else 'on'}"
        )

    try:
        asyncio.run(orchestrator.run_pipeline())
    except KeyboardInterrupt:
        logger.warning("USER_ABORT")


if __name__ == "__main__":
    main()
