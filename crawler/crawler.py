import asyncio
import aiohttp
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

from utils.network import AsyncFetcher

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

    async def scan(self, targets: List[str]):
        logger.info(f"Starting crawl on targets: {targets}")
        
        async with aiohttp.ClientSession(headers=self.engine.headers) as session:
            tasks = []
            for sub in targets:
                url = f"https://www.reddit.com/r/{sub}/new.json?limit=100"
                tasks.append(self.engine.fetch_json(session, url))
            
            results = await asyncio.gather(*tasks)
            
            post_tasks = []
            for sub_raw in results:
                if sub_raw:
                    children = sub_raw.get("data", {}).get("children", [])
                    for child in children:
                        # We DON'T await here. We just create the coroutine objects.
                        post_tasks.append(self.process_post(child, targets[i]))

            final_data = await asyncio.gather(*post_tasks)
            final_data = [p for p in final_data if p]
            
            return final_data

if __name__ == "__main__":
    target_subs = ["shopify", "AmazonSeller", "Entrepreneur", "smallbusiness"]
    
    crawler = RedditCrawler()
    
    try:
        data = asyncio.run(crawler.scan(target_subs))
        logger.info(f"MISSION_COMPLETE | Total Unique Posts Found: {len(data)}")
    except KeyboardInterrupt:
        logger.warning("USER_ABORT | Shutdown initiated.")