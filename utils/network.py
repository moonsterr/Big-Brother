import asyncio
import aiohttp
import random
import logging
from typing import Optional, Dict, Any
from utils.config import PROXY_CONFIG


logger = logging.getLogger("NetworkEngine")


class RateLimitError(Exception):
    """
    Raised immediately when Reddit returns HTTP 429.
    Propagates up through the entire pipeline to trigger a clean abort,
    stopping all further requests to protect the IP.
    """
    def __init__(self, url: str):
        super().__init__(f"Rate limited by Reddit at: {url}")
        self.url = url


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
                        logger.critical(
                            f"RATE_LIMITED | {url} | "
                            "Reddit is throttling this IP — raising abort signal immediately."
                        )
                        raise RateLimitError(url)

                    else:
                        logger.warning(f"HTTP_{response.status} | {url}")

            except RateLimitError:
                raise  # never suppress — must propagate to abort the pipeline
            except Exception as e:
                logger.error(f"CONNECTION_FAILED | {url} | Error: {e}")

            return None