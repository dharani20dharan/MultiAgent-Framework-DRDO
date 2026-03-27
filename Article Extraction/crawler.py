"""
crawler.py
Async Playwright browser pool with stealth, JS rendering,
and smart wait strategies. Yields raw HTML + metadata per URL.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stealth JS injected before every page load to mask automation signals
# ---------------------------------------------------------------------------
_STEALTH_SCRIPT = """
() => {
    // Mask webdriver property
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

    // Mask automation-related Chrome properties
    window.chrome = { runtime: {} };

    // Realistic plugin list
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5],
    });

    // Realistic language
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
    });

    // Remove HeadlessChrome from UA products
    Object.defineProperty(navigator, 'appVersion', {
        get: () => navigator.appVersion.replace('HeadlessChrome', 'Chrome'),
    });

    // Permissions API fix (headless returns denied for notifications)
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) =>
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters);
}
"""

# Realistic desktop user agents to rotate
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


@dataclass
class CrawlResult:
    url: str
    final_url: str          # after redirects
    html: str
    status_code: int
    elapsed_ms: float
    error: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class CrawlerConfig:
    # Concurrency
    max_concurrent_pages: int = 5
    browser_pool_size: int = 2          # number of persistent browser instances

    # Timing
    page_timeout_ms: int = 60_000
    navigation_timeout_ms: int = 45_000
    min_delay_ms: int = 500             # polite delay between requests per worker
    max_delay_ms: int = 1_500

    # Behaviour
    headless: bool = True
    block_resources: bool = True        # block images and media to speed up crawl
    wait_strategy: str = "domcontentloaded"  # "domcontentloaded" | "load" | "networkidle"
    # NOTE: avoid "networkidle" for sites with continuous background requests
    # (analytics, ads, telemetry) — it will time out. Use "domcontentloaded"
    # and let content_selectors handle waiting for the article to render.
    scroll_to_load: bool = False        # scroll down to trigger lazy-loaded content

    # Content-aware waiting: wait for one of these CSS selectors to appear
    # before capturing HTML. Tried in order; first match wins.
    # None = skip selector waiting and rely solely on wait_strategy.
    content_selectors: tuple[str, ...] = (
        "article",
        "[class*='article-body']",
        "[class*='ArticleBody']",
        "[class*='story-body']",
        "[class*='post-content']",
        "[class*='entry-content']",
        "main p",
    )
    # How long to wait for a content selector before giving up (ms)
    content_selector_timeout_ms: int = 10_000
    # Extra settle time after content selector matches (ms) — lets React
    # finish any in-progress renders before we capture HTML
    post_selector_settle_ms: int = 500

    # Viewport (mimic a real desktop)
    viewport_width: int = 1440
    viewport_height: int = 900


class BrowserPool:
    """Manages a fixed pool of browser instances shared across async workers."""

    def __init__(self, playwright: Playwright, config: CrawlerConfig):
        self._playwright = playwright
        self._config = config
        self._browsers: list[Browser] = []
        self._semaphore = asyncio.Semaphore(config.max_concurrent_pages)

    async def start(self) -> None:
        for _ in range(self._config.browser_pool_size):
            browser = await self._playwright.chromium.launch(
                headless=self._config.headless,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-infobars",
                    "--window-size=1440,900",
                ],
            )
            self._browsers.append(browser)
        logger.info("Browser pool started (%d browsers)", len(self._browsers))

    async def stop(self) -> None:
        for browser in self._browsers:
            await browser.close()
        logger.info("Browser pool stopped")

    def _pick_browser(self) -> Browser:
        return random.choice(self._browsers)

    async def new_context(self) -> BrowserContext:
        ua = random.choice(_USER_AGENTS)
        ctx = await self._pick_browser().new_context(
            user_agent=ua,
            viewport={"width": self._config.viewport_width, "height": self._config.viewport_height},
            locale="en-US",
            timezone_id="America/New_York",
            java_script_enabled=True,
            accept_downloads=False,
            ignore_https_errors=True,
        )
        # Inject stealth script before every page navigation
        await ctx.add_init_script(_STEALTH_SCRIPT)

        if self._config.block_resources:
            await ctx.route(
                "**/*",
                lambda route: (
                    route.abort()
                    # Only block images and media — never fonts or stylesheets.
                    # Some sites load article JSON via dynamically-injected tags
                    # that Playwright classifies as 'stylesheet' or 'other';
                    # blocking those silently empties the article content.
                    if route.request.resource_type in {"image", "media"}
                    else route.continue_()
                ),
            )
        return ctx

    async def fetch(self, url: str) -> CrawlResult:
        """Fetch a single URL, returning CrawlResult. Thread-safe via semaphore."""
        async with self._semaphore:
            ctx = await self.new_context()
            page: Page = await ctx.new_page()
            page.set_default_timeout(self._config.page_timeout_ms)
            page.set_default_navigation_timeout(self._config.navigation_timeout_ms)

            start = time.monotonic()
            status_code = 0
            error = None
            html = ""
            final_url = url

            try:
                response = await page.goto(
                    url,
                    wait_until=self._config.wait_strategy,
                    timeout=self._config.navigation_timeout_ms,
                )
                if response:
                    status_code = response.status
                    final_url = page.url

                # --- Content-aware waiting ---
                # After the network-level wait, try to confirm that actual
                # article content is present in the DOM before capturing.
                # This handles React/Vue sites where networkidle fires before
                # the framework has finished its render commit.
                if self._config.content_selectors:
                    matched = await _wait_for_content(
                        page,
                        self._config.content_selectors,
                        self._config.content_selector_timeout_ms,
                    )
                    if matched:
                        logger.debug("Content selector matched '%s' for %s", matched, url)
                        # Give the framework a moment to finish rendering any
                        # siblings that appear in the same render cycle.
                        if self._config.post_selector_settle_ms > 0:
                            await asyncio.sleep(self._config.post_selector_settle_ms / 1000)
                    else:
                        logger.warning(
                            "No content selector matched for %s — capturing anyway", url
                        )

                if self._config.scroll_to_load:
                    await _scroll_page(page)

                html = await page.content()
                logger.debug("Captured %d bytes of HTML for %s", len(html), url)

            except Exception as exc:
                error = str(exc)
                logger.warning("Fetch failed for %s: %s", url, exc)
            finally:
                await page.close()
                await ctx.close()

            elapsed_ms = (time.monotonic() - start) * 1000

            # Polite delay — jitter to avoid rhythm-based detection
            delay = random.uniform(
                self._config.min_delay_ms, self._config.max_delay_ms
            ) / 1000
            await asyncio.sleep(delay)

            return CrawlResult(
                url=url,
                final_url=final_url,
                html=html,
                status_code=status_code,
                elapsed_ms=elapsed_ms,
                error=error,
            )


async def _wait_for_content(
    page: Page,
    selectors: tuple[str, ...],
    timeout_ms: int,
) -> Optional[str]:
    """
    Try each CSS selector in order. Return the first one that appears in the
    DOM within timeout_ms, or None if none matched. Non-raising: a timeout on
    one selector just means we try the next.
    """
    per_selector_timeout = max(timeout_ms // len(selectors), 1_000)
    for selector in selectors:
        try:
            await page.wait_for_selector(
                selector,
                state="attached",      # present in DOM, not necessarily visible
                timeout=per_selector_timeout,
            )
            return selector
        except Exception:
            continue
    return None


async def _scroll_page(page: Page, steps: int = 4) -> None:
    """Scroll incrementally to trigger lazy-loaded content."""
    for i in range(1, steps + 1):
        await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {i / steps})")
        await asyncio.sleep(0.3)
    await page.evaluate("window.scrollTo(0, 0)")


# ---------------------------------------------------------------------------
# High-level API: crawl a list of URLs with full async pipeline
# ---------------------------------------------------------------------------

async def crawl(
    urls: list[str],
    config: Optional[CrawlerConfig] = None,
) -> AsyncIterator[CrawlResult]:
    """
    Async generator. Yields CrawlResult for each URL as soon as it completes.
    Usage:
        async for result in crawl(urls):
            process(result)
    """
    config = config or CrawlerConfig()

    async with async_playwright() as pw:
        pool = BrowserPool(pw, config)
        await pool.start()

        try:
            tasks = [asyncio.create_task(pool.fetch(url)) for url in urls]
            for coro in asyncio.as_completed(tasks):
                result = await coro
                yield result
        finally:
            await pool.stop()
