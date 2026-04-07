import asyncio
import asyncpraw
from typing import List, Dict, Optional, Any
from utils.config import REDDIT_CONFIG

async def process_post(submission: asyncpraw.models.Submission, sub_name: str) -> Optional[Dict[str, Any]]:
    try:
        if not submission.url or submission.removed_by_category:
            return None

        return {
            "id": submission.id,
            "url": submission.url,
            "title": submission.title,
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio,
            "created_utc": submission.created_utc,
            "subreddit": sub_name,
            "is_external": not submission.is_self
        }
    except Exception as e:
        print(f"[-] Logic Error in processor: {e}")
        return None

async def crawl_subreddit(reddit: asyncpraw.Reddit, sub_name: str, limit: int = 25) -> List[Dict[str, Any]]:
    posts_found = {} 
    
    try:
        subreddit = await reddit.subreddit(sub_name)
        
        feeds = [subreddit.hot(limit=limit), subreddit.new(limit=limit)]
        
        for feed in feeds:
            async for submission in feed:
                data = await process_post(submission, sub_name)
                if data:
                    posts_found[data["id"]] = data
                    
        return list(posts_found.values())

    except Exception as e:
        print(f"[!] Network Error in r/{sub_name}: {e}")
        return []

async def master_crawler(targets: List[str], limit_per_feed: int = 25) -> List[Dict[str, Any]]:

    async with asyncpraw.Reddit(**REDDIT_CONFIG) as reddit:
        tasks = [crawl_subreddit(reddit, sub, limit_per_feed) for sub in targets]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        master_list = []
        for res in results:
            if isinstance(res, list):
                master_list.extend(res)
            elif isinstance(res, Exception):
                print(f"[!!] Critical Task Failure: {res}")
                
        return master_list

if __name__ == "__main__":
    target_subreddits = ["technology", "programming", "dataisbeautiful"]
    
    try:
        print(f"[*] Initializing Crawler for: {target_subreddits}")
        final_results = asyncio.run(master_crawler(target_subreddits))
        
        print(f"\n[+] Extraction Complete.")
        print(f"[+] Total Unique Posts Found: {len(final_results)}")
        
        # Print first 3 results as verification
        for post in final_results[:3]:
            print(f" - [{post['subreddit']}] {post['title'][:50]}... -> {post['url']}")
            
    except KeyboardInterrupt:
        print("\n[!] Shutdown initiated by user.")