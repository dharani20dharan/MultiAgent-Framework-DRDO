"""
pipeline.py
Connects the async crawler to the sync extractor.
Handles output routing, deduplication, and progress reporting.
"""

import asyncio
import json
import logging
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Optional

from crawler import CrawlResult, CrawlerConfig, crawl
from extractor import Article, ExtractionConfig, extract

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

class JsonLinesWriter:
    """Appends one JSON object per line to a .jsonl file. Thread-safe via lock."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._file = open(self.path, "a", encoding="utf-8")
        self._count = 0

    async def write(self, article: Article) -> None:
        async with self._lock:
            self._file.write(json.dumps(asdict(article), ensure_ascii=False) + "\n")
            self._file.flush()
            self._count += 1

    def close(self) -> None:
        self._file.close()

    @property
    def count(self) -> int:
        return self._count


# ---------------------------------------------------------------------------
# Pipeline config
# ---------------------------------------------------------------------------

class PipelineConfig:
    def __init__(
        self,
        output_path: str = "output/articles.jsonl",
        crawler: Optional[CrawlerConfig] = None,
        extractor: Optional[ExtractionConfig] = None,
        skip_failed_crawls: bool = True,
        skip_failed_extractions: bool = False,
        deduplicate: bool = True,
        on_article: Optional[Callable[[Article], None]] = None,
    ):
        self.output_path = output_path
        self.crawler = crawler or CrawlerConfig()
        self.extractor = extractor or ExtractionConfig()
        self.skip_failed_crawls = skip_failed_crawls
        self.skip_failed_extractions = skip_failed_extractions
        self.deduplicate = deduplicate
        self.on_article = on_article  # optional callback per article


# ---------------------------------------------------------------------------
# Main pipeline runner
# ---------------------------------------------------------------------------

async def run(
    urls: list[str],
    config: Optional[PipelineConfig] = None,
) -> list[Article]:
    """
    Run the full pipeline: crawl → extract → write.
    Returns list of all Article objects processed.
    """
    config = config or PipelineConfig()
    writer = JsonLinesWriter(config.output_path)
    seen_ids: set[str] = set()
    results: list[Article] = []

    total = len(urls)
    crawled = 0
    extracted = 0
    skipped = 0

    logger.info("Starting pipeline: %d URLs → %s", total, config.output_path)

    loop = asyncio.get_event_loop()

    async for crawl_result in crawl(urls, config.crawler):
        crawled += 1
        _log_progress(crawled, total, crawl_result.url, crawl_result.elapsed_ms)

        # Skip failed crawls if configured
        if crawl_result.error and config.skip_failed_crawls:
            logger.warning("Skipping %s (crawl error: %s)", crawl_result.url, crawl_result.error)
            skipped += 1
            continue

        if not crawl_result.html:
            skipped += 1
            continue

        # Extraction runs in a thread pool to avoid blocking the event loop
        # (trafilatura is CPU-bound)
        article: Article = await loop.run_in_executor(
            None,
            extract,
            crawl_result.html,
            crawl_result.final_url,
            config.extractor,
        )

        # Deduplication
        if config.deduplicate:
            if article.id in seen_ids:
                logger.debug("Duplicate skipped: %s", article.url)
                skipped += 1
                continue
            seen_ids.add(article.id)

        # Skip poor extractions if configured
        if not article.extraction_ok and config.skip_failed_extractions:
            logger.warning("Extraction failed for %s — skipping", article.url)
            skipped += 1
            continue

        extracted += 1
        await writer.write(article)
        results.append(article)

        if config.on_article:
            config.on_article(article)

    writer.close()

    logger.info(
        "Pipeline complete — crawled: %d, extracted: %d, skipped: %d → %s",
        crawled, extracted, skipped, config.output_path,
    )

    return results


def _log_progress(done: int, total: int, url: str, elapsed_ms: float) -> None:
    pct = (done / total) * 100
    short_url = url[:60] + "…" if len(url) > 60 else url
    logger.info("[%3.0f%%] %d/%d  %.0fms  %s", pct, done, total, elapsed_ms, short_url)
