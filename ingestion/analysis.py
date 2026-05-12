import asyncio
import json
import logging

from ollama import AsyncClient

from ingestion.base_prompt import build_prompt

logger = logging.getLogger("Analysis")

OLLAMA_TIMEOUT = 60.0   # if it hasn't responded in 60s, skip it
MODEL_NAME     = "llama3.1:8b"
OLLAMA_OPTIONS = {
    "num_ctx": 16384,
    "temperature": 0.1,
    "num_gpu": 1,
    "format": "json",
}

_FALLBACK: dict = {
    "error":              "skipped",
    "is_problem":         False,
    "problem_summary":    "",
    "problem_category":   "",
    "sentiment":          0.0,
    "agreement_signal":   0.0,
    "business_potential": 0.0,
    "urgency":            0.0,
    "advice":             False,
}


async def run_analysis(post_content: str, post_id: str = "?") -> dict:
    """
    Call Ollama once.  If it doesn't respond within 60 s, or returns bad JSON,
    log it and return a fallback dict — never retries, never raises.
    """
    system_prompt = build_prompt()

    try:
        logger.debug(f"LLM_REQUEST | ID: {post_id} | timeout={OLLAMA_TIMEOUT}s")

        response = await asyncio.wait_for(
            AsyncClient().chat(
                model=MODEL_NAME,
                format="json",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": post_content},
                ],
                options=OLLAMA_OPTIONS,
            ),
            timeout=OLLAMA_TIMEOUT,
        )

        raw_text = response["message"]["content"]
        result   = json.loads(raw_text)
        logger.debug(f"LLM_OK | ID: {post_id}")
        return result

    except asyncio.CancelledError:
        raise  # let the pipeline cancel cleanly

    except asyncio.TimeoutError:
        logger.warning(
            f"LLM_TIMEOUT | ID: {post_id} | no response in {OLLAMA_TIMEOUT}s — skipping"
        )
        return {**_FALLBACK, "error": f"timeout_{OLLAMA_TIMEOUT}s"}

    except json.JSONDecodeError as exc:
        logger.warning(f"LLM_JSON_ERROR | ID: {post_id} | {exc} — skipping")
        return {**_FALLBACK, "error": f"json_decode: {exc}"}

    except Exception as exc:
        logger.warning(
            f"LLM_ERROR | ID: {post_id} | {type(exc).__name__}: {exc} — skipping"
        )
        return {**_FALLBACK, "error": f"{type(exc).__name__}: {exc}"}
