import asyncio
import logging
import json
from typing import List, Dict, Any
from crawler.crawler import RedditCrawler
from scraper.scraper import RedditScraper
from utils.config import BASE_DIR

logger = logging.getLogger("Orchestrator")

class Orchestrator:
    def __init__(self, subs: List[str]):
        self.subs = subs
        self.data: List[Dict[str, Any]] = []
        self.crawler = RedditCrawler()
        self.scraper = RedditScraper()

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
        logger.info(f"MISSION_START | Targets: {self.subs}")

        raw_discovery = await self.crawler.scan(self.subs)
        if not raw_discovery:
            logger.warning("DISCOVERY_FAILED")
            return
        
        self.data = raw_discovery 
        self.print_state_sample("AFTER_CRAWLER_DISCOVERY", self.data)

        self.data = await self.scraper.scrape(raw_discovery) 
        
        
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
        logger.info(f"MISSION_COMPLETE")
        logger.info(f"Total Unique Posts Scraped: {total_posts}")
        logger.info(f"Total Nested Comments Captured: {total_comments}")
        logger.info("-" * 40)
    from utils.config import BASE_DIR

    def save_to_disk(self, filename="ingestion_vault.jsonl"):
        save_path = BASE_DIR / filename
        
        if not self.data:
            logger.warning("No data to save.")
            return
    
        with open(save_path, "a", encoding="utf-8") as f:
            for entry in self.data:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    
        logger.info(f"STORAGE_COMPLETE | File is at: {save_path}")
        

if __name__ == "__main__":
    target_subs = ["shopify", "AmazonSeller"]
    orchestrator = Orchestrator(target_subs)
    try:
        asyncio.run(orchestrator.run_pipeline())
    except KeyboardInterrupt:
        logger.warning("USER_ABORT")