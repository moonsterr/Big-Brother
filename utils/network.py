import asyncio
import aiohttp
import random
import logging
from typing import Optional, Dict, Any
from utils.config import PROXY_CONFIG
from utils.config import BASE_DIR

logger = logging.getLogger("NetworkEngine")

class AsyncFetcher:
    def __init__(self, user_agent: str, max_concurrent: int = 1):
        self.headers = {"User-Agent": user_agent}
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.proxy = PROXY_CONFIG["url"] if PROXY_CONFIG.get("enabled") else None

    async def fetch_json(self, session: aiohttp.ClientSession, url: str, delay_range: tuple = (6, 12)) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            wait = random.uniform(*delay_range)
            await asyncio.sleep(wait)
            
            try:
                async with session.get(url, proxy=self.proxy, timeout=15) as response:
                    if response.status == 200:
                        logger.info(f"FETCH_SUCCESS | {url}")
                        return await response.json()
                    
                    elif response.status == 429:
                        logger.error(f"RATE_LIMITED | {url} | Switching IP/Stopping is advised.")
                    else:
                        logger.warning(f"HTTP_{response.status} | {url}")
                        
            except Exception as e:
                logger.error(f"CONNECTION_FAILED | {url} | Error: {e}")
            
            return None