import asyncio
import aiohttp
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set

from ingestion.analysis import run_analysis
from utils.network import AsyncFetcher, RateLimitError

logger = logging.getLogger("RedditScraper")


class RedditScraper:
    def __init__(self, run_llm: bool = True):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BigBrother/1.0"
        self.engine = AsyncFetcher(user_agent=ua, max_concurrent=1)
        self.gpu_limit = asyncio.Semaphore(1)   # one LLM inference at a time
        self.run_llm  = run_llm
        # Populated with whatever finished before a RateLimitError abort.
        self.partial_on_abort: List[Dict[str, Any]] = []

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _prepare_content_for_llm(self, enriched_post: Dict[str, Any]) -> str:
        post    = enriched_post["post"]
        content = f"TITLE: {post['title']}\n"
        content += f"OP BODY: {post['body']}\n\n"
        content += "--- COMMENTS ---\n"

        def flatten(comments, depth=0):
            text = ""
            for c in comments:
                indent = "  " * depth
                text += f"{indent}[{c.get('author')}]: {c.get('body')}\n"
                if c.get("replies"):
                    text += flatten(c["replies"], depth + 1)
            return text

        content += flatten(enriched_post["comments"])
        return content

    def extract_comments_recursive(
        self, comment_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        comments = []
        children = comment_data.get("data", {}).get("children", [])

        for child in children:
            data = child.get("data", {})
            if child.get("kind") != "t1":
                continue

            comment_node: Dict[str, Any] = {
                "id":        data.get("id"),
                "author":    data.get("author"),
                "body":      data.get("body"),
                "score":     data.get("score"),
                "parent_id": data.get("parent_id"),
                "replies":   [],
            }

            replies_raw = data.get("replies")
            if isinstance(replies_raw, dict):
                comment_node["replies"] = self.extract_comments_recursive(replies_raw)

            comments.append(comment_node)

        return comments

    async def process_post_detail(
        self, raw_json: List[Any]
    ) -> Optional[Dict[str, Any]]:
        try:
            post_listing = (
                raw_json[0]
                .get("data", {})
                .get("children", [{}])[0]
                .get("data", {})
            )
            comment_listing = raw_json[1]
            all_comments     = self.extract_comments_recursive(comment_listing)

            return {
                "post": {
                    "id":           post_listing.get("id"),
                    "title":        post_listing.get("title"),
                    "body":         post_listing.get("selftext"),
                    "author":       post_listing.get("author"),
                    "score":        post_listing.get("score"),
                    "upvote_ratio": post_listing.get("upvote_ratio"),
                    "num_comments": post_listing.get("num_comments"),
                    "subreddit":    post_listing.get("subreddit"),
                    "created_utc":  post_listing.get("created_utc"),
                },
                "comments": all_comments,
            }
        except Exception as exc:
            logger.error(f"PARSE_ERROR | extraction failed: {exc}")
            return None

    # ── LLM analysis (runs under GPU semaphore) ───────────────────────────────

    async def _analyze_with_semaphore(
        self,
        enriched_post: Dict[str, Any],
        on_post_complete: Optional[Callable] = None,
        progress_label: str = "",
    ) -> Dict[str, Any]:
        """
        Run LLM analysis for one post.  Calls `on_post_complete(post)` as soon
        as the result is attached — this is the hook the Orchestrator uses to
        save the post to disk immediately without waiting for the whole batch.
        """
        post_id = enriched_post["post"]["id"]

        async with self.gpu_limit:
            t0          = time.monotonic()
            llm_input   = self._prepare_content_for_llm(enriched_post)
            token_est   = len(llm_input) // 4  # rough token estimate
            logger.info(
                f"LLM_START {progress_label}| ID: {post_id} | "
                f"~{token_est} tokens | sub={enriched_post['post'].get('subreddit', '?')}"
            )

            analysis_result = await run_analysis(llm_input, post_id=post_id)

            enriched_post["analysis"] = analysis_result
            elapsed = time.monotonic() - t0

            # Surface a human-readable score from whatever the LLM returned.
            if isinstance(analysis_result, dict):
                bp    = analysis_result.get("business_potential", "?")
                score = f"bp={bp}"
                if analysis_result.get("error"):
                    score = f"ANALYSIS_ERROR({analysis_result['error'][:40]})"
            else:
                score = "UNEXPECTED_TYPE"

            logger.info(
                f"LLM_DONE  {progress_label}| ID: {post_id} | "
                f"{elapsed:.1f}s | {score}"
            )

        # ── Immediate disk persistence ────────────────────────────────────────
        # Called OUTSIDE the semaphore so the next LLM task can start while
        # the orchestrator writes to disk.
        if on_post_complete is not None:
            try:
                on_post_complete(enriched_post)
            except Exception as cb_exc:
                logger.error(
                    f"SAVE_CALLBACK_ERROR | ID: {post_id} | {cb_exc}"
                )

        return enriched_post

    # ── Main entry point ──────────────────────────────────────────────────────

    async def scrape(
        self,
        discovered_posts: List[Dict[str, Any]],
        on_post_complete: Optional[Callable] = None,
        skip_ids: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Scrape and (optionally) LLM-analyse each post in `discovered_posts`.

        Parameters
        ----------
        discovered_posts  : raw list from RedditCrawler.scan()
        on_post_complete  : called immediately after each post's LLM analysis
                            with the fully enriched post dict.  Use this to
                            save posts to disk one-by-one so no work is lost
                            if the process dies mid-run.
        skip_ids          : set of post IDs to skip (already processed in a
                            prior run — resume support).
        """
        skip_ids = skip_ids or set()

        # Filter out already-processed posts.
        posts_to_run = [p for p in discovered_posts if p["id"] not in skip_ids]
        skipped_count = len(discovered_posts) - len(posts_to_run)

        logger.info(
            f"SCRAPE_START | total_discovered={len(discovered_posts)} | "
            f"skip_already_done={skipped_count} | to_process={len(posts_to_run)} | "
            f"llm={'on' if self.run_llm else 'off'}"
        )

        self.partial_on_abort = []
        tasks: List[asyncio.Task]        = []
        raw_results: List[Dict[str, Any]] = []

        total   = len(posts_to_run)
        fetched = 0

        try:
            async with aiohttp.ClientSession(headers=self.engine.headers) as session:
                for post in posts_to_run:
                    url = f"https://www.reddit.com/comments/{post['id']}.json"

                    raw_data = await self.engine.fetch_json(session, url)
                    if not raw_data:
                        logger.warning(f"FETCH_EMPTY | ID: {post['id']} | skipping")
                        continue

                    enriched_post = await self.process_post_detail(raw_data)
                    if not enriched_post:
                        continue

                    # Skip posts with no content worth analysing.
                    body = enriched_post["post"].get("body") or ""
                    if not body.strip() and not enriched_post["comments"]:
                        logger.debug(
                            f"CONTENT_EMPTY | ID: {post['id']} | "
                            "no body and no comments — skipping"
                        )
                        continue

                    fetched += 1

                    if self.run_llm:
                        progress = f"[{fetched}/{total}] "
                        logger.info(
                            f"QUEUED {progress}| ID: {post['id']} | "
                            f"sub={post.get('subreddit', '?')} | "
                            "fetch done, LLM task created"
                        )
                        task = asyncio.create_task(
                            self._analyze_with_semaphore(
                                enriched_post,
                                on_post_complete=on_post_complete,
                                progress_label=progress,
                            )
                        )
                        tasks.append(task)
                    else:
                        logger.info(
                            f"CAPTURED [{fetched}/{total}] | ID: {post['id']} | LLM skipped"
                        )
                        if on_post_complete:
                            try:
                                on_post_complete(enriched_post)
                            except Exception as cb_exc:
                                logger.error(
                                    f"SAVE_CALLBACK_ERROR | ID: {post['id']} | {cb_exc}"
                                )
                        raw_results.append(enriched_post)

        except RateLimitError:
            # Cancel every pending LLM task immediately.
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # Collect tasks that finished before the abort.
            completed = [
                t.result()
                for t in tasks
                if t.done() and not t.cancelled() and t.exception() is None
            ]
            self.partial_on_abort = [r for r in completed if r]

            logger.error(
                f"RATE_LIMIT_ABORT | fetch loop terminated | "
                f"fetched={fetched} | llm_tasks_cancelled={len(tasks)} | "
                f"completed_before_abort={len(self.partial_on_abort)}"
            )
            raise  # let Orchestrator handle final persistence + exit

        # ── Normal completion path ────────────────────────────────────────────
        if self.run_llm:
            logger.info(
                f"FETCH_PHASE_DONE | fetched={fetched}/{total} posts | "
                f"waiting for {len(tasks)} LLM tasks to complete …"
            )
            # return_exceptions=True so one failed task doesn't cancel the rest.
            raw_final = await asyncio.gather(*tasks, return_exceptions=True)

            final_results = []
            for i, res in enumerate(raw_final):
                if isinstance(res, Exception):
                    logger.error(f"TASK_EXCEPTION | task[{i}]: {type(res).__name__}: {res}")
                elif res is not None:
                    final_results.append(res)
        else:
            final_results = raw_results

        logger.info(
            f"SCRAPE_DONE | total_processed={len(final_results)} | "
            f"skipped_empty={fetched - len(final_results) if self.run_llm else 0}"
        )
        return final_results
