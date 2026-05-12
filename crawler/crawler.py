import asyncio
import aiohttp
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

from utils.network import AsyncFetcher, RateLimitError

ROOT_DIR = Path.cwd()
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RedditCrawler")

# Reddit caps listing endpoints at ~1000 items regardless of pagination.
REDDIT_LISTING_HARD_CAP = 1000
REDDIT_PAGE_SIZE = 100


class RedditCrawler:
    def __init__(self):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0"
        self.engine = AsyncFetcher(user_agent=ua)

    async def process_post(self, post_data: Dict[str, Any], sub_name: str) -> Optional[Dict[str, Any]]:
        try:
            d = post_data.get("data", {})
            if not d.get("url") or d.get("removed_by_category"):
                return None

            logger.info(f"  Captured: [r/{sub_name}] {d.get('title')[:60]}...")

            return {
                "id": d.get("id"),
                "title": d.get("title"),
                "subreddit": sub_name,
                "url": d.get("url"),
                "created_utc": d.get("created_utc")
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
                f"LIMIT_CLAMPED | Requested {limit} > Reddit cap {REDDIT_LISTING_HARD_CAP}; clamping."
            )
            limit = REDDIT_LISTING_HARD_CAP

        collected: List[Dict[str, Any]] = []
        after: Optional[str] = None

        while len(collected) < limit:
            remaining = limit - len(collected)
            page_size = min(REDDIT_PAGE_SIZE, remaining)

            url = f"https://www.reddit.com/r/{sub}/new.json?limit={page_size}"
            if after:
                url += f"&after={after}"

            raw = await self.engine.fetch_json(session, url)
            if not raw:
                logger.warning(f"PAGE_FETCH_FAILED | r/{sub} | after={after}")
                break

            children = raw.get("data", {}).get("children", []) or []
            if not children:
                logger.info(f"PAGE_EMPTY | r/{sub} | end of listing reached.")
                break

            page_results = await asyncio.gather(
                *(self.process_post(child, sub) for child in children)
            )
            collected.extend([p for p in page_results if p])

            after = raw.get("data", {}).get("after")
            if not after:
                # Reddit returned no continuation cursor — listing exhausted.
                break

        # We may have over-collected by up to a page if process_post filtered nothing.
        return collected[:limit]

    async def scan(self, targets: List[str], limit: int = REDDIT_PAGE_SIZE):
        logger.info(f"Starting crawl on targets: {targets} | limit_per_sub={limit}")

        final_data: List[Dict[str, Any]] = []

        # Sequential — one subreddit at a time so Reddit sees a human browsing pattern,
        # not a burst of concurrent requests from the same IP.
        async with aiohttp.ClientSession(headers=self.engine.headers) as session:
            for sub in targets:
                # RateLimitError propagates up untouched; caller (Orchestrator) handles abort.
                posts = await self._scan_one(session, sub, limit)
                final_data.extend(posts)

        return final_data


if __name__ == "__main__":
    target_subs = ["shopify", "AmazonSeller", "Entrepreneur", "smallbusiness"]

    crawler = RedditCrawler()

    try:
        data = asyncio.run(crawler.scan(target_subs))
        logger.info(f"MISSION_COMPLETE | Total Unique Posts Found: {len(data)}")
    except KeyboardInterrupt:
        logger.warning("USER_ABORT | Shutdown initiated.")