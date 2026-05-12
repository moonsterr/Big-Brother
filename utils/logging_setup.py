"""
Centralised logging configuration for the big-brother pipeline.

Call `setup_logging(log_dir)` exactly ONCE from orchestrator main() before
any component loggers are used.  All named loggers (RedditCrawler,
RedditScraper, Orchestrator, NetworkEngine, Analysis …) propagate to the
root logger, which fans out to:

  logs/pipeline.log    — full combined log, appended across runs
  stdout               — same output mirrored to the terminal
"""

import logging
import sys
from pathlib import Path


def setup_logging(log_dir: Path, level: int = logging.INFO) -> None:
    """Configure the root logger with a file handler and a stream handler."""

    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()

    # Avoid double-adding handlers if called more than once (e.g. during tests).
    if root.handlers:
        return

    root.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── File handler — combined pipeline log (append mode) ───────────────────
    fh = logging.FileHandler(log_dir / "pipeline.log", mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(level)

    # ── Stream handler — stdout ───────────────────────────────────────────────
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(level)

    root.addHandler(fh)
    root.addHandler(sh)

    # Suppress httpx / httpcore INFO spam (Ollama client uses httpx internally).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
