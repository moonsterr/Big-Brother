import asyncio
import aiohttp
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

from utils.network import AsyncFetcher
from ingestion.analysis import run_analysis

logger = logging.getLogger("RedditScraper")

class RedditScraper:
    def __init__(self):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BigBrother/1.0"
        self.engine = AsyncFetcher(user_agent=ua, max_concurrent=1)
        self.gpu_limit = asyncio.Semaphore(1)
    
    def _prepare_content_for_llm(self, enriched_post: Dict[str, Any]) -> str:
        post = enriched_post['post']
        content = f"TITLE: {post['title']}\n"
        content += f"OP BODY: {post['body']}\n\n"
        content += "--- COMMENTS ---\n"
        
        def flatten(comments, depth=0):
            text = ""
            for c in comments:
                text += f"{'  ' * depth}[{c.get('author')}]: {c.get('body')}\n"
                if c.get('replies'):
                    text += flatten(c['replies'], depth + 1)
            return text
            
        content += flatten(enriched_post['comments'])
        return content

    def extract_comments_recursive(self, comment_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        
        comments = []
        children = comment_data.get("data", {}).get("children", [])
        
        for child in children:
            data = child.get("data", {})
            if child.get("kind") == "t1":
                comment_node = {
                    "id": data.get("id"),
                    "author": data.get("author"),
                    "body": data.get("body"),
                    "score": data.get("score"),
                    "parent_id": data.get("parent_id"),
                    "replies": []
                }
                
                replies_raw = data.get("replies")
                if isinstance(replies_raw, dict):
                    comment_node["replies"] = self.extract_comments_recursive(replies_raw)
                
                comments.append(comment_node)
        
        return comments

    async def process_post_detail(self, raw_json: List[Any]) -> Optional[Dict[str, Any]]:
        try:
            post_listing = raw_json[0].get("data", {}).get("children", [{}])[0].get("data", {})
            
            comment_listing = raw_json[1]
            all_comments = self.extract_comments_recursive(comment_listing)
            
            return {
                "post": {
                    "id": post_listing.get("id"),
                    "title": post_listing.get("title"),
                    "body": post_listing.get("selftext"),
                    "author": post_listing.get("author"),
                    "score": post_listing.get("score"),
                    "upvote_ratio": post_listing.get("upvote_ratio"),
                    "num_comments": post_listing.get("num_comments"),
                    "created_utc": post_listing.get("created_utc")
                },
                "comments": all_comments
            }
        except Exception as e:
            logger.error(f"PARSE_ERROR | Extraction failed: {e}")
            return None
    
    async def _analyze_with_semaphore(self, enriched_post: Dict[str, Any]):
        async with self.gpu_limit:
            llm_input = self._prepare_content_for_llm(enriched_post)
            
            analysis_result = await run_analysis(llm_input)
            
            enriched_post['analysis'] = analysis_result
            
            score = analysis_result.get('business_potential', 'N/A') if isinstance(analysis_result, dict) else "Done"
            logger.info(f"SUCCESS   | ID: {enriched_post['post']['id']} | Signal: {score}")
            return enriched_post

    async def scrape(self, discovered_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.info(f"PIPELINE_START | Target: {len(discovered_posts)} nodes")

        tasks = []
        async with aiohttp.ClientSession(headers=self.engine.headers) as session:
            for post in discovered_posts:
                url = f"https://www.reddit.com/comments/{post['id']}.json"

                raw_data = await self.engine.fetch_json(session, url)
                if not raw_data:
                    continue

                enriched_post = await self.process_post_detail(raw_data)
                if not enriched_post:
                    continue

                if not enriched_post['post']['body'].strip() and not enriched_post['comments']:
                    continue

                logger.info(f"QUEUED    | ID: {post['id']} | Moving to next fetch...")
                
                task = asyncio.create_task(self._analyze_with_semaphore(enriched_post))
                tasks.append(task)

        logger.info(f"FETCH_COMPLETE | Waiting for {len(tasks)} analyses to finalize...")
        
        final_results = await asyncio.gather(*tasks)
        
        final_results = [r for r in final_results if r]

        logger.info(f"PIPELINE_COMPLETE | Final Count: {len(final_results)}")
        return final_results