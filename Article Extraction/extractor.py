"""
extractor.py
Wraps trafilatura for fast, accurate article extraction from raw HTML.
Returns a clean, typed Article dataclass. No network calls — pure parsing.
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import trafilatura
from trafilatura.settings import use_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Trafilatura config — tuned for performance and quality
# ---------------------------------------------------------------------------
_traf_config = use_config()
_traf_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")   # disable internal timeout
_traf_config.set("DEFAULT", "MIN_EXTRACTED_SIZE", "200") # min chars to accept extraction
_traf_config.set("DEFAULT", "MIN_OUTPUT_SIZE", "100")


@dataclass
class Article:
    # Identity
    url: str
    id: str                             # sha256 of final URL, stable dedup key

    # Content
    title: str = ""
    text: str = ""
    summary: str = ""
    language: str = ""

    # Authorship & dates
    authors: list[str] = field(default_factory=list)
    date_published: Optional[str] = None
    date_modified: Optional[str] = None

    # Classification
    categories: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    hostname: str = ""
    sitename: str = ""

    # Media
    image: str = ""
    comments: str = ""

    # Pipeline metadata
    extracted_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z"
    )
    extraction_ok: bool = False
    word_count: int = 0
    char_count: int = 0


@dataclass
class ExtractionConfig:
    # Trafilatura knobs
    include_comments: bool = False
    include_tables: bool = True
    no_fallback: bool = False           # if True, skip fallback extractors (faster)
    favour_precision: bool = False      # True = less text but higher quality
    favour_recall: bool = True          # True = more text, may include boilerplate
    output_format: str = "txt"          # "txt" | "markdown" | "xml" | "json"

    # Post-processing
    min_word_count: int = 50            # discard extractions shorter than this
    strip_emoji: bool = True


def extract(
    html: str,
    url: str,
    config: Optional[ExtractionConfig] = None,
) -> Article:
    """
    Extract article content from raw HTML string.
    Returns an Article dataclass — never raises, errors are encoded in the result.
    """
    cfg = config or ExtractionConfig()
    article_id = _url_to_id(url)
    hostname = _hostname(url)

    base = Article(url=url, id=article_id, hostname=hostname)

    if not html or not html.strip():
        logger.warning("Empty HTML for %s", url)
        return base

    try:
        # --- Primary extraction: structured metadata via trafilatura ---
        extracted = trafilatura.extract(
            html,
            url=url,
            include_comments=cfg.include_comments,
            include_tables=cfg.include_tables,
            no_fallback=cfg.no_fallback,
            favor_precision=cfg.favour_precision,
            favor_recall=cfg.favour_recall,
            output_format=cfg.output_format,
            config=_traf_config,
        )

        # --- Metadata extraction (separate call, lightweight) ---
        meta = trafilatura.extract_metadata(html, default_url=url)

        text = extracted or ""
        if cfg.strip_emoji:
            text = _strip_emoji(text)

        word_count = len(text.split()) if text else 0

        if word_count < cfg.min_word_count:
            logger.warning(
                "Extraction too short (%d words, need %d) for %s",
                word_count, cfg.min_word_count, url,
            )
            base.text = text
            base.word_count = word_count
            base.char_count = len(text)
            if meta:
                _apply_metadata(base, meta)
            return base

        _apply_metadata(base, meta)
        base.text = text
        base.word_count = word_count
        base.char_count = len(text)
        base.extraction_ok = True

    except Exception as exc:
        logger.error("Extraction error for %s: %s", url, exc, exc_info=True)

    return base


def extract_batch(
    items: list[tuple[str, str]],   # list of (html, url)
    config: Optional[ExtractionConfig] = None,
) -> list[Article]:
    """
    Extract a batch synchronously. Fast because trafilatura is CPU-bound
    with no I/O — use process pool for CPU-parallel batches if needed.
    """
    return [extract(html, url, config) for html, url in items]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _apply_metadata(article: Article, meta) -> None:
    if meta is None:
        return
    article.title = meta.title or ""
    article.authors = list(meta.author.split("; ")) if meta.author else []
    article.date_published = meta.date or None
    article.sitename = meta.sitename or ""
    article.image = meta.image or ""
    article.language = meta.language or ""
    article.categories = list(meta.categories) if meta.categories else []
    article.tags = list(meta.tags) if meta.tags else []
    article.description = getattr(meta, "description", "") or ""


def _url_to_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _hostname(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def _strip_emoji(text: str) -> str:
    return _EMOJI_RE.sub("", text)
