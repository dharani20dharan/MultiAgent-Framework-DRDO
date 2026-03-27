"""
main.py
CLI entry point. Run directly or import pipeline.run() in your own code.

Usage:
    python main.py --urls https://example.com/article1 https://example.com/article2
    python main.py --file urls.txt --output output/articles.jsonl --concurrency 8
    python main.py --file urls.txt --scroll --no-block-resources
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from crawler import CrawlerConfig
from extractor import ExtractionConfig
from pipeline import PipelineConfig, run


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Quieten noisy libraries
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Article extractor — Playwright + trafilatura pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Input
    inp = p.add_mutually_exclusive_group(required=True)
    inp.add_argument(
        "--urls", nargs="+", metavar="URL",
        help="One or more URLs to crawl",
    )
    inp.add_argument(
        "--file", metavar="PATH",
        help="Path to a plain-text file with one URL per line",
    )

    # Output
    p.add_argument(
        "--output", default="output/articles.jsonl",
        help="Output path (.jsonl)",
    )

    # Crawler options
    p.add_argument("--concurrency", type=int, default=5, help="Max concurrent pages")
    p.add_argument("--browsers", type=int, default=2, help="Browser pool size")
    p.add_argument("--timeout", type=int, default=45_000, help="Page timeout (ms)")
    p.add_argument(
        "--wait", default="domcontentloaded",
        choices=["networkidle", "domcontentloaded", "load"],
        help="Navigation wait strategy (default: domcontentloaded — use networkidle only for fully static sites)",
    )
    p.add_argument("--scroll", action="store_true", help="Scroll to load lazy content")
    p.add_argument(
        "--no-block-resources", action="store_true",
        help="Disable resource blocking (slower but sometimes needed)",
    )
    p.add_argument("--no-headless", action="store_true", help="Show browser window")

    # Extractor options
    p.add_argument(
        "--format", default="txt",
        choices=["txt", "markdown", "xml"],
        help="Extracted text output format",
    )
    p.add_argument("--precision", action="store_true", help="Favour precision over recall")
    p.add_argument("--min-words", type=int, default=50, help="Minimum word count to keep")

    # Pipeline options
    p.add_argument(
        "--keep-failed", action="store_true",
        help="Keep articles with failed extraction in output",
    )
    p.add_argument("--no-dedup", action="store_true", help="Disable URL deduplication")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    return p


def _load_urls(args) -> list[str]:
    if args.urls:
        return args.urls
    path = Path(args.file)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)
    lines = path.read_text(encoding="utf-8").splitlines()
    urls = [ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")]
    if not urls:
        print("Error: no URLs found in file", file=sys.stderr)
        sys.exit(1)
    return urls


async def _main(args) -> None:
    urls = _load_urls(args)
    print(f"Loaded {len(urls)} URL(s)")

    crawler_cfg = CrawlerConfig(
        max_concurrent_pages=args.concurrency,
        browser_pool_size=args.browsers,
        page_timeout_ms=args.timeout,
        navigation_timeout_ms=args.timeout,
        wait_strategy=args.wait,
        scroll_to_load=args.scroll,
        block_resources=not args.no_block_resources,
        headless=not args.no_headless,
    )

    extractor_cfg = ExtractionConfig(
        output_format=args.format,
        favour_precision=args.precision,
        favour_recall=not args.precision,
        min_word_count=args.min_words,
    )

    pipeline_cfg = PipelineConfig(
        output_path=args.output,
        crawler=crawler_cfg,
        extractor=extractor_cfg,
        skip_failed_extractions=not args.keep_failed,
        deduplicate=not args.no_dedup,
    )

    articles = await run(urls, pipeline_cfg)
    print(f"\nDone. {len(articles)} article(s) saved to {args.output}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    _setup_logging(args.verbose)

    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
