"""
Crawler for kleineskraftwerk.de - Small power station products.
"""
import asyncio
import csv
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler

# Configuration
URLS = [
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-flachdach",
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-ziegeldach",
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-gitterbalkon",
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-wandmontage-wandhalterung",
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-ohne-halterung",
    "https://kleineskraftwerk.de/collections/balkonkraftwerk-garten",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Set output path to data folder (parent directory)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"
OUTPUT_PATH = OUTPUT_DIR / "kleineskraftwerk.csv"

def parse_products(html: str, base_url: str) -> list[dict]:
    """Parse product listings from HTML."""
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    products: list[dict] = []
    
    for wrapper in soup.select("div.text-wrapper"):
        title_el = wrapper.select_one(".product-title a")
        if not title_el:
            continue

        title = title_el.get_text(strip=True)
        href = title_el.get("href") or ""
        detail_url = urljoin(base_url, href)
        
        if not title:
            continue

        products.append({
            "title": title,
            "detail_url": detail_url,
            "original_price": "",
            "discount_price": "",
            "discount_rate": "",
        })

    return products


def clean_price_text(price: str) -> str:
    if not price:
        return ""
    price = re.sub(r"^\s*ab\s*", "", price, flags=re.IGNORECASE)
    price = price.replace("€", "").strip()
    match = re.search(r"(\d+[\d\.]*,\d{2})", price)
    if match:
        numeric = match.group(1)
        normalized = numeric.replace(".", "").replace(",", ".")
        try:
            return f"{Decimal(normalized).quantize(Decimal('0.01'))}"
        except InvalidOperation:
            return normalized
    match = re.search(r"\d+", price)
    if match:
        digits = match.group(0)
        try:
            return f"{Decimal(digits).quantize(Decimal('0.01'))}"
        except InvalidOperation:
            return digits
    return price


def price_to_float(price: str) -> float | None:
    if not price:
        return None
    normalized = price.replace(".", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def compute_discount_rate(original_price: str, discount_price: str) -> str:
    orig_val = price_to_float(original_price)
    disc_val = price_to_float(discount_price)
    if orig_val is None or disc_val is None or orig_val <= 0:
        return ""
    rate = (orig_val - disc_val) / orig_val * 100
    return f"{rate:.2f}%"


def extract_prices_from_detail(html: str) -> tuple[str, str]:
    if not html:
        return "", ""

    soup = BeautifulSoup(html, "lxml")
    price_block = (
        soup.select_one("div.product-price-wrapper span.price")
        or soup.select_one("span.price")
    )

    original_price = ""
    discount_price = ""

    if price_block:
        del_el = price_block.select_one("del span.amount")
        ins_el = price_block.select_one("ins span.amount")

        if del_el:
            original_price = clean_price_text(del_el.get_text(strip=True))
        if ins_el:
            discount_price = clean_price_text(ins_el.get_text(strip=True))

        if not discount_price:
            first_amount = price_block.select_one("span.amount")
            if first_amount:
                discount_price = clean_price_text(first_amount.get_text(strip=True))

        if not original_price:
            amounts: list[str] = []
            for span in price_block.select("span.amount"):
                cleaned = clean_price_text(span.get_text(strip=True))
                if cleaned:
                    amounts.append(cleaned)
            if len(amounts) >= 2:
                original_price = amounts[0]
                discount_price = discount_price or amounts[1]
            elif amounts:
                original_price = amounts[0]
                discount_price = discount_price or amounts[0]

    return original_price, discount_price


async def fetch_products(crawler: AsyncWebCrawler, url: str) -> list[dict]:
    print(f"🔍 正在爬取 {url} ...")
    try:
        res = await crawler.arun(
            url=url,
            headers=HEADERS,
            timeout=60,
            wait_until="networkidle"
        )
    except Exception as exc:
        print(f"❌ 获取 {url} 失败：{exc}")
        return []

    if not res or not res.html:
        print(f"❌ {url} 返回空响应")
        return []

    products = parse_products(res.html, url)
    if not products:
        print(f"⚠️ 未从 {url} 解析到产品")
    return products


async def enrich_product_with_detail(crawler: AsyncWebCrawler, product: dict) -> dict:
    detail_url = product.get("detail_url")
    if not detail_url:
        return product

    print(f"  ↪️ 正在获取详情页：{detail_url}")
    try:
        res = await crawler.arun(
            url=detail_url,
            headers=HEADERS,
            timeout=60,
            wait_until="networkidle"
        )
    except Exception as exc:
        print(f"❌ 获取详情页 {detail_url} 失败：{exc}")
        return product

    if not res or not res.html:
        print(f"❌ {detail_url} 详情页返回空响应")
        return product

    original_price, discount_price = extract_prices_from_detail(res.html)

    if not original_price and discount_price:
        original_price = discount_price
    if not discount_price and original_price:
        discount_price = original_price

    product["original_price"] = original_price
    product["discount_price"] = discount_price
    product["discount_rate"] = compute_discount_rate(original_price, discount_price)
    return product


# ============================================================================
# Crawling Functions
# ============================================================================


async def crawl() -> list[dict]:
    """Main crawling function for kleineskraftwerk."""
    async with AsyncWebCrawler(concurrency=2) as crawler:
        tasks = [fetch_products(crawler, url) for url in URLS]
        results = await asyncio.gather(*tasks)

        all_items: list[dict] = []
        for url, items in zip(URLS, results):
            for item in items:
                item["source_url"] = url
                all_items.append(item)

        if all_items:
            print(f"\n🔄 正在获取 {len(all_items)} 个产品的详情页 ...")
            enriched_items = await asyncio.gather(*[
                enrich_product_with_detail(crawler, item) for item in all_items
            ])
        else:
            enriched_items = []

    total = 0
    all_products: list[dict] = []
    grouped: dict[str, list[dict]] = {url: [] for url in URLS}
    for item in enriched_items:
        source = item.get("source_url", "")
        grouped.setdefault(source, []).append(item)

    for url in URLS:
        items = grouped.get(url, [])
        if not items:
            continue
        print(f"\n📦 来自 {url} 的产品：")
        for item in items:
            total += 1
            src = item.get("source_url", url)
            print(
                f"- {item['title']} | 原价: {item['original_price']} | "
                f"优惠价: {item['discount_price']} | 折扣: {item['discount_rate']} | {item['detail_url']}"
            )
            all_products.append({
                "source_url": src,
                "title": item.get("title", ""),
                "detail_url": item.get("detail_url", ""),
                "original_price": item.get("original_price", ""),
                "discount_price": item.get("discount_price", ""),
                "discount_rate": item.get("discount_rate", ""),
            })

    if all_products:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "source_url",
            "title",
            "detail_url",
            "original_price",
            "discount_price",
            "discount_rate",
        ]
        with OUTPUT_PATH.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_products)
        print(f"\n💾 已保存 {len(all_products)} 条记录到 {OUTPUT_PATH}")
    else:
        print("\n⚠️ 无产品数据可保存。")

    print(f"\n✅ 完成。共提取 {total} 个产品，覆盖 {len(URLS)} 个页面。")
    return all_products


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