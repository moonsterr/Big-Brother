import asyncio
import aiohttp
import random
from typing import List, Dict, Optional, Any

request_semaphore = asyncio.Semaphore(1)

async def process_raw_post(post_data: Dict[str, Any], sub_name: str) -> Optional[Dict[str, Any]]:
    try:
        data = post_data.get("data", {})
        if not data.get("url") or data.get("removed_by_category"):
            return None

        return {
            "id": data.get("id"),
            "title": data.get("title"),
            "subreddit": sub_name,
            "url": data.get("url")
        }
    except Exception:
        return None

async def fetch_with_throttle(session: aiohttp.ClientSession, sub_name: str):
    async with request_semaphore:
        delay = random.uniform(2.0, 4.0) 
        await asyncio.sleep(delay)
        
        url = f"https://www.reddit.com/r/{sub_name}/new.json?limit=100"
        
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    raw = await response.json()
                    children = raw.get("data", {}).get("children", [])
                    print(f"[+] Successfully fetched r/{sub_name} (Paused {delay:.2f}s)")
                    return [await process_raw_post(c, sub_name) for c in children]
                
                elif response.status == 429:
                    print(f"[!!!] 429 Too Many Requests on r/{sub_name}. REDDIT BLOCKED YOU.")
                    return []
                else:
                    print(f"[!] HTTP {response.status} on r/{sub_name}")
                    return []
        except Exception as e:
            print(f"[!] Connection failed for r/{sub_name}: {e}")
            return []

async def master_defensive_crawler(targets: List[str]):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_with_throttle(session, sub) for sub in targets]
        results = await asyncio.gather(*tasks)
        
        final_list = [post for sublist in results if sublist for post in sublist if post]
        return final_list

if __name__ == "__main__":
    subs = ["ecommerce", "shopify", "Entrepreneur", "smallbusiness"]
    
    print("[*] Launching Defensive Scraper...")
    data = asyncio.run(master_defensive_crawler(subs))
    print(f"\n[+] Collected {len(data)} posts safely.")