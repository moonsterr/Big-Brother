import asyncio
import aiohttp
import random
import logging
import time
from typing import Optional, Dict, Any

from utils.config import PROXY_CONFIG

logger = logging.getLogger("NetworkEngine")

# Default per-request HTTP timeout in seconds.
HTTP_TIMEOUT_SECONDS = 30


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

        if self.proxy:
            logger.info(f"PROXY_ENABLED | routing requests via {self.proxy}")
        else:
            logger.debug("PROXY_DISABLED | sending requests directly")

    async def fetch_json(
        self,
        session: aiohttp.ClientSession,
        url: str,
        delay_range: tuple = (6, 12),
    ) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            wait = random.uniform(*delay_range)
            logger.debug(f"FETCH_DELAY | {wait:.1f}s before {url}")
            await asyncio.sleep(wait)

            t0 = time.monotonic()
            try:
                timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
                async with session.get(url, proxy=self.proxy, timeout=timeout) as response:
                    elapsed = time.monotonic() - t0

                    if response.status == 200:
                        data = await response.json(content_type=None)
                        logger.info(
                            f"FETCH_OK | {response.status} | {elapsed:.2f}s | {url}"
                        )
                        return data

                    elif response.status == 429:
                        retry_after = response.headers.get("Retry-After", "?")
                        logger.critical(
                            f"RATE_LIMITED | 429 | {url} | Retry-After: {retry_after}s | "
                            "raising abort signal — no more requests will be made."
                        )
                        raise RateLimitError(url)

                    elif response.status in (403, 404):
                        logger.warning(
                            f"FETCH_SKIP | {response.status} | {elapsed:.2f}s | {url}"
                        )

                    else:
                        body_preview = ""
                        try:
                            body_preview = (await response.text())[:200]
                        except Exception:
                            pass
                        logger.warning(
                            f"FETCH_ERROR | HTTP {response.status} | {elapsed:.2f}s | {url} | "
                            f"body_preview={body_preview!r}"
                        )

            except RateLimitError:
                raise  # never suppress — must propagate to abort the pipeline

            except asyncio.TimeoutError:
                elapsed = time.monotonic() - t0
                logger.error(
                    f"FETCH_TIMEOUT | {elapsed:.2f}s (limit={HTTP_TIMEOUT_SECONDS}s) | {url}"
                )

            except aiohttp.ClientConnectionError as exc:
                elapsed = time.monotonic() - t0
                logger.error(f"FETCH_CONN_ERROR | {elapsed:.2f}s | {url} | {exc}")

            except Exception as exc:
                elapsed = time.monotonic() - t0
                logger.error(
                    f"FETCH_UNEXPECTED | {elapsed:.2f}s | {url} | "
                    f"{type(exc).__name__}: {exc}"
                )

            return None
