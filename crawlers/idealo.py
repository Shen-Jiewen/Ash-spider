"""
Crawler for idealo.de - Price comparison portal.
"""
import asyncio
import csv
import os
import re
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlsplit, unquote, urljoin

from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

# Configuration
SEARCH_QUERY = "anker solix"
BASE_URL = "https://www.idealo.de/preisvergleich/MainSearchProductCategory.html"
IDEALO_ORIGIN = "https://www.idealo.de"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}

JS_WAIT = """
(() => {
  window.scrollTo(0, document.body.scrollHeight);
  return new Promise(r => setTimeout(r, 4000));
})();
"""

# Set output path to data folder (parent directory)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_PATH = OUTPUT_DIR / "idealo.csv"

# ============================================================================
# Utility Functions
# ============================================================================


def to_absolute(url: str) -> str:
    """Convert relative URLs to absolute URLs."""
    if not url:
        return ""
    if url.startswith("http"):
        return url
    return urljoin(IDEALO_ORIGIN, url)


def resolve_idealo_redirect(url: str) -> str:
    """Resolve idealo redirect URLs to get the real target URL."""
    try:
        parsed = urlsplit(url)
        # Support multiple idealo redirect paths: /Redirect and /relocator/relocate
        if "idealo." in (parsed.netloc or "") and (
            "/relocator/relocate" in (parsed.path or "")
            or "/Redirect" in (parsed.path or "")
        ):
            q = parse_qs(parsed.query or "")
            for key in ("targetUrl", "url", "redirectUrl"):
                if key in q and q[key]:
                    return unquote(q[key][0])
        return url
    except Exception:
        return url


def extract_host(url: str) -> str:
    """Extract hostname from URL."""
    if not url:
        return ""
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def extract_merchant_from_alt(alt_text: str) -> str:
    """Extract merchant name from alt text."""
    if not alt_text:
        return ""
    merchant = alt_text.split(" - ", 1)[0].strip()
    m = re.search(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}", merchant)
    if m:
        merchant = m.group(0)
    merchant = merchant.lower()
    if merchant.startswith("www."):
        merchant = merchant[4:]
    return merchant


def parse_price_from_text(txt: str) -> str:
    """Extract price from text."""
    if not txt:
        return ""
    m = re.search(r"\b(?:ab|ao)\s*([\d\.\,]+)\s*€", txt, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1)} €"
    return ""


def clean_price(txt: str) -> str:
    """Clean price text by removing prefixes and extra characters."""
    if not txt:
        return ""
    txt = re.sub(r"^\s*(ab|ao)\s*", "", txt, flags=re.IGNORECASE)
    txt = re.sub(r"\b(ab|ao)\b\s*", "", txt, flags=re.IGNORECASE)
    return txt.strip()


def clean_merchant_name(merchant: str) -> str:
    """Clean merchant name by removing common suffixes."""
    if not merchant:
        return ""

    merchant = merchant.lower().strip()
    if merchant.startswith("www."):
        merchant = merchant[4:]

    unwanted_suffixes = [
        "de", "com", "net", "org", "eu", "uk", "us", "ca", "au",
        "media", "shop", "store", "market", "mall", "direct", "direkt",
        "online", "web", "site", "portal", "center", "group", "corp",
        "inc", "ltd", "gmbh", "ag", "kg", "co", "llc",
    ]

    separators = ['.', '-', '_', ' ']
    for sep in separators:
        if sep in merchant:
            parts = merchant.split(sep)
            cleaned_parts = [
                p.strip() for p in parts
                if p.strip() and p.strip() not in unwanted_suffixes
            ]
            if cleaned_parts:
                return cleaned_parts[0]

    if merchant not in unwanted_suffixes:
        return merchant
    return ""


# ============================================================================
# Parsing Functions
# ============================================================================


def parse_search_html(html: str) -> list[dict]:
    """Parse search results from idealo."""
    soup = BeautifulSoup(html, "lxml")
    products = []
    
    for a in soup.select('a[href*="/preisvergleich/OffersOfProduct/"]'):
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 6:
            continue
        href = a.get("href", "")
        if not href:
            continue
        link = href if href.startswith("http") else f"{IDEALO_ORIGIN}{href}"

        block_text = " ".join(a.get_text(" ", strip=True).split())
        price_from = parse_price_from_text(block_text)
        if not price_from and a.parent:
            parent_text = " ".join(a.parent.get_text(" ", strip=True).split())
            price_from = parse_price_from_text(parent_text)

        products.append({
            "name": title,
            "link": link,
            "price_from": price_from,
        })

    dedup = {}
    for p in products:
        dedup.setdefault(p["link"], p)
    return list(dedup.values())


def find_first_offer_container(soup: BeautifulSoup):
    """Find the container with product offers."""
    return (
        soup.select_one("#offerList")
        or soup.select_one("div[data-test='productOffers']")
        or soup.select_one("section[data-test='offers']")
        or soup.select_one("div.productOffers")
        or soup.select_one("div[id*='productOffers']")
        or soup
    )


def find_first_offer_item(container: BeautifulSoup):
    """Find the first offer item in the container."""
    return (
        container.select_one("li.productOffers-listItem")
        or container.select_one("div.productOffers-listItem")
        or None
    )


def pick_cta_anchor(scope: BeautifulSoup):
    """Pick a CTA anchor for the offer."""
    return (
        scope.select_one(
            "a.productOffers-listItemOfferCtaLeadout.button.button--leadout[data-shop-name]"
        )
        or scope.select_one("a.button--leadout[data-shop-name]")
        or scope.select_one("a[data-shop-name]")
        or scope.select_one("a.button--leadout")
    )


def find_first_offer_url(scope: BeautifulSoup) -> str:
    """Find the URL of the first offer."""
    cta = pick_cta_anchor(scope)
    if cta and cta.get("href"):
        return to_absolute(resolve_idealo_redirect(cta.get("href")))
    candidates = scope.select("a[href]")
    for a in candidates:
        href = a.get("href", "")
        if href:
            return to_absolute(resolve_idealo_redirect(href))
    return ""


def parse_detail_html(html: str, product: dict) -> dict:
    """Parse product detail page."""
    soup = BeautifulSoup(html, "lxml")

    # Price & Shipping
    pv_el = (
        soup.select_one("div.productOffers-listItemOfferPrice")
        or soup.select_one("div.productOffers-listItemPrice")
        or soup.select_one("a.productOffers-listItemOfferPrice")
        or soup.select_one("span.price")
    )
    preis_versand = pv_el.get_text(" ", strip=True) if pv_el else ""
    if not preis_versand:
        preis_versand = product.get("price_from", "")
    product["preis_versand"] = clean_price(preis_versand)

    # Find first offer
    offers_container = find_first_offer_container(soup)
    first_item = find_first_offer_item(offers_container) or offers_container

    # Get external link
    first_offer_url = find_first_offer_url(first_item)
    product["first_offer_url"] = first_offer_url

    # Merchant parsing priority:
    # 1) CTA's data-shop-name attribute
    # 2) Image alt text
    # 3) Real redirect target hostname (must not be idealo domain)
    merchant = ""

    cta = pick_cta_anchor(first_item)
    if cta:
        ds = (cta.get("data-shop-name") or "").strip().lower()
        if ds.startswith("www."):
            ds = ds[4:]
        merchant = clean_merchant_name(ds)

    if not merchant:
        logo_img = (
            first_item.select_one("img.productOffers-listItemOfferShopV2LogoImage[alt]")
            or first_item.select_one("img[alt][class*='Logo'], img[alt][class*='Shop']")
        )
        if logo_img:
            merchant = clean_merchant_name(
                extract_merchant_from_alt(logo_img.get("alt", ""))
            )

    if not merchant and first_offer_url:
        # Try to parse the real redirect target
        resolved = resolve_idealo_redirect(first_offer_url)
        host = extract_host(resolved)
        if (
            host
            and not host.endswith("idealo.de")
            and not host.endswith("idealo.co.uk")
            and "idealo." not in host
        ):
            merchant = clean_merchant_name(host)

    product["merchant"] = merchant
    product.pop("price_from", None)
    return product


# ============================================================================
# Crawling Functions
# ============================================================================


async def fetch_detail(crawler, prod: dict) -> dict:
    """Fetch product detail page."""
    res = await crawler.arun(
        url=prod["link"],
        headers=HEADERS,
        timeout=90,
        wait_until="networkidle",
        js_code=JS_WAIT,
    )
    return parse_detail_html(res.html, prod)


async def crawl() -> list[dict]:
    """Main crawling function for idealo."""
    async with AsyncWebCrawler(concurrency=2) as crawler:
        url = f"{BASE_URL}?q={SEARCH_QUERY.replace(' ', '%20')}&page=1"
        
        products = []
        for attempt in range(2):  # Try up to 2 times
            res = await crawler.arun(
                url=url,
                headers=HEADERS,
                timeout=90,
                wait_until="networkidle",
                js_code=JS_WAIT,
            )
            products = parse_search_html(res.html)
            if products:
                print(f"[idealo] 第{attempt + 1}次解析成功，解析到 {len(products)} 个产品。")
                break
            else:
                print(f"[idealo] 第{attempt + 1}次解析失败，正在重试...")
                await asyncio.sleep(2) # Wait a bit before retrying

        if not products:
            print("[idealo] 多次尝试后未解析到任何产品。")
            return []

        detailed = await asyncio.gather(
            *[fetch_detail(crawler, prod) for prod in products]
        )

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fields = ["name", "link", "preis_versand", "first_offer_url", "merchant"]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in detailed:
            writer.writerow({k: item.get(k, "") for k in fields})
    print(f"[idealo] 已将 {len(detailed)} 条记录保存到 {OUTPUT_PATH}")
    
    return detailed


async def main():
    """Main function."""
    try:
        await crawl()
    except KeyboardInterrupt:
        raise


if __name__ == "__main__":
    import sys

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)