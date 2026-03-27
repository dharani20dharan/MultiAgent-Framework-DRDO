# Article Extractor

Async article extraction pipeline using **Playwright** (JS rendering + stealth)
and **trafilatura** (fast, accurate text extraction). Pure Python — no IPC boundary.

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

### CLI

```bash
# Single URL
python main.py --urls https://www.theguardian.com/some-article

# Multiple URLs
python main.py --urls https://site1.com/a https://site2.com/b

# From file (one URL per line, # lines ignored)
python main.py --file urls.txt --output output/articles.jsonl

# Tune concurrency
python main.py --file urls.txt --concurrency 8 --browsers 3

# Markdown output, higher precision
python main.py --file urls.txt --format markdown --precision

# Scroll to load lazy content, show browser window
python main.py --file urls.txt --scroll --no-headless

# Verbose logging
python main.py --file urls.txt -v
```

### Python API

```python
import asyncio
from crawler import CrawlerConfig
from extractor import ExtractionConfig
from pipeline import PipelineConfig, run

async def main():
    urls = [
        "https://example.com/article1",
        "https://example.com/article2",
    ]

    articles = await run(
        urls,
        PipelineConfig(
            output_path="output/articles.jsonl",
            crawler=CrawlerConfig(
                max_concurrent_pages=5,
                wait_strategy="networkidle",
                block_resources=True,   # faster: skip images/fonts
            ),
            extractor=ExtractionConfig(
                output_format="markdown",
                favour_recall=True,
                min_word_count=50,
            ),
        ),
    )

    for article in articles:
        print(article.title, "—", article.word_count, "words")

asyncio.run(main())
```

### Callback per article

```python
def on_article(article):
    print(f"[{article.hostname}] {article.title[:60]}")

await run(urls, PipelineConfig(on_article=on_article))
```

## Output format

Each line in the `.jsonl` output is one JSON object:

```json
{
  "url": "https://...",
  "id": "a3f9c1d2b4e5f6a7",
  "title": "Article title",
  "text": "Full article text...",
  "authors": ["Jane Doe"],
  "date_published": "2024-05-01",
  "hostname": "example.com",
  "sitename": "Example",
  "language": "en",
  "tags": ["tech", "ai"],
  "categories": ["Technology"],
  "image": "https://example.com/image.jpg",
  "word_count": 843,
  "char_count": 4821,
  "extraction_ok": true,
  "extracted_at": "2024-05-10T12:34:56Z"
}
```

## Architecture

```
URLs
 │
 ▼
crawler.py          ← Playwright async browser pool
 │  - Stealth JS patches (navigator.webdriver, plugins, etc.)
 │  - User-agent rotation
 │  - Resource blocking (images/fonts) for speed
 │  - Configurable wait strategy & scroll
 │  - Semaphore-limited concurrency
 │
 ▼  raw HTML + metadata (in-process, no IPC)
 │
extractor.py        ← trafilatura extraction
 │  - extract() parses text content
 │  - extract_metadata() parses title/authors/dates
 │  - Fallback handling, min word-count filter
 │  - Emoji stripping, output format choice
 │
 ▼  Article dataclass
 │
pipeline.py         ← Orchestrator
    - asyncio task scheduling
    - run_in_executor for CPU-bound extraction
    - Deduplication by URL hash
    - JSONL writer
    - Progress logging
```

## Performance tips

- Increase `--concurrency` and `--browsers` for faster crawls on large lists
- Keep `--block-resources` on (default) — skipping images/fonts cuts page load time ~40%
- Use `--wait domcontentloaded` instead of `networkidle` for simpler pages (faster)
- Use `--format markdown` if you need structure; `txt` is fastest
- For CPU-heavy extraction batches, wrap `extract_batch()` in a `ProcessPoolExecutor`
