import asyncio
import aiohttp
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

from utils.network import AsyncFetcher, RateLimitError

logger = logging.getLogger("RedditCrawler")

# Reddit caps listing endpoints at ~1000 items regardless of pagination.
REDDIT_LISTING_HARD_CAP = 1000
REDDIT_PAGE_SIZE = 100


class RedditCrawler:
    def __init__(self):
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
        )
        self.engine = AsyncFetcher(user_agent=ua)

    async def process_post(
        self, post_data: Dict[str, Any], sub_name: str
    ) -> Optional[Dict[str, Any]]:
        try:
            d = post_data.get("data", {})
            if not d.get("url") or d.get("removed_by_category"):
                return None

            title_preview = (d.get("title") or "")[:60]
            logger.debug(f"CAPTURED | [r/{sub_name}] {title_preview!r}")

            return {
                "id":          d.get("id"),
                "title":       d.get("title"),
                "subreddit":   sub_name,
                "url":         d.get("url"),
                "created_utc": d.get("created_utc"),
            }
        except Exception:
            return None

    async def _scan_one(
        self,
        session: aiohttp.ClientSession,
        sub: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Fetch up to `limit` posts from /r/<sub>/new, paginating via `after=`."""
        if limit > REDDIT_LISTING_HARD_CAP:
            logger.warning(
                f"LIMIT_CLAMPED | r/{sub} | requested {limit} > "
                f"Reddit cap {REDDIT_LISTING_HARD_CAP}; clamping."
            )
            limit = REDDIT_LISTING_HARD_CAP

        collected: List[Dict[str, Any]] = []
        after: Optional[str] = None
        page_num = 0

        while len(collected) < limit:
            remaining = limit - len(collected)
            page_size = min(REDDIT_PAGE_SIZE, remaining)
            page_num += 1

            url = f"https://www.reddit.com/r/{sub}/new.json?limit={page_size}"
            if after:
                url += f"&after={after}"

            logger.info(
                f"FETCH_PAGE | r/{sub} | page={page_num} | "
                f"page_size={page_size} | collected_so_far={len(collected)}"
            )

            raw = await self.engine.fetch_json(session, url)
            if not raw:
                logger.warning(f"PAGE_FETCH_FAILED | r/{sub} | page={page_num} | after={after}")
                break

            children = raw.get("data", {}).get("children", []) or []
            if not children:
                logger.info(f"PAGE_EMPTY | r/{sub} | page={page_num} | end of listing reached.")
                break

            page_results = await asyncio.gather(
                *(self.process_post(child, sub) for child in children)
            )
            valid = [p for p in page_results if p]
            collected.extend(valid)

            logger.info(
                f"PAGE_DONE | r/{sub} | page={page_num} | "
                f"got={len(valid)}/{len(children)} valid | "
                f"total_collected={len(collected)}/{limit}"
            )

            after = raw.get("data", {}).get("after")
            if not after:
                logger.info(f"LISTING_EXHAUSTED | r/{sub} | no more pages from Reddit.")
                break

        return collected[:limit]

    async def scan(self, targets: List[str], limit: int = REDDIT_PAGE_SIZE):
        logger.info(
            f"CRAWL_START | targets={targets} | limit_per_sub={limit} | "
            f"max_total={limit * len(targets)}"
        )

        final_data: List[Dict[str, Any]] = []

        # Sequential — one sub at a time to avoid burst-request rate limits.
        async with aiohttp.ClientSession(headers=self.engine.headers) as session:
            for idx, sub in enumerate(targets, 1):
                logger.info(
                    f"CRAWL_SUB | [{idx}/{len(targets)}] r/{sub} | "
                    f"requesting up to {limit} posts"
                )
                # RateLimitError propagates up; Orchestrator handles abort.
                posts = await self._scan_one(session, sub, limit)
                final_data.extend(posts)
                logger.info(
                    f"CRAWL_SUB_DONE | [{idx}/{len(targets)}] r/{sub} | "
                    f"collected={len(posts)} | running_total={len(final_data)}"
                )

        logger.info(f"CRAWL_COMPLETE | total_posts_discovered={len(final_data)}")
        return final_data


# ── Stand-alone quick test ────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Minimal logging so the standalone test prints something readable.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    target_subs = ["shopify", "AmazonSeller", "Entrepreneur", "smallbusiness"]
    crawler = RedditCrawler()

    try:
        data = asyncio.run(crawler.scan(target_subs))
        logger.info(f"MISSION_COMPLETE | total={len(data)}")
    except KeyboardInterrupt:
        logger.warning("USER_ABORT | Shutdown initiated.")
