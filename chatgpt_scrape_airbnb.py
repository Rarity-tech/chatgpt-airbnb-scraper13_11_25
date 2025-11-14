import asyncio
import csv
import datetime as dt
import os
import re
from typing import List, Dict, Any

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


SEARCH_URL_FILE = "search_urls.txt"
OUTPUT_CSV = "results_chatgpt.csv"

MAX_CONCURRENT_PAGES = int(os.getenv("MAX_CONCURRENT_PAGES", "8"))
MAX_LISTINGS_PER_SEARCH = int(os.getenv("MAX_LISTINGS_PER_SEARCH", "200"))


async def read_search_urls() -> List[str]:
    urls: List[str] = []
    if not os.path.exists(SEARCH_URL_FILE):
        return urls
    with open(SEARCH_URL_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                urls.append(line)
    return urls


async def collect_listing_urls(page, search_url: str) -> List[str]:
    await page.goto(search_url, wait_until="networkidle")
    seen = set()
    last_height = -1

    while True:
        cards = page.locator('a[href*="/rooms/"]')
        hrefs = await cards.evaluate_all(
            "els => Array.from(new Set(els.map(e => e.href)))"
        )
        for h in hrefs:
            if "/rooms/" in h:
                clean = h.split("?")[0]
                seen.add(clean)

        # stop if enough
        if len(seen) >= MAX_LISTINGS_PER_SEARCH:
            break

        # scroll down
        await page.mouse.wheel(0, 1500)
        await page.wait_for_timeout(1000)

        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    return list(seen)[:MAX_LISTINGS_PER_SEARCH]


async def click_more_buttons(page) -> None:
    # ouvre les descriptions longues, etc.
    labels = [
        "Lire la suite",
        "Afficher plus",
        "En savoir plus",
    ]
    for label in labels:
        buttons = page.locator(f'button:has-text("{label}")')
        count = await buttons.count()
        for i in range(count):
            try:
                await buttons.nth(i).click(timeout=2000)
            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue
    await page.wait_for_timeout(500)


async def safe_inner_text(page, selector: str) -> str:
    try:
        loc = page.locator(selector)
        if await loc.count() == 0:
            return ""
        return (await loc.first.inner_text()).strip()
    except Exception:
        return ""


def extract_license_from_text(text: str) -> str:
    """
    Essaie de trouver un code licence dans le texte complet.
    Exemples : BUS-MAG-42KDF, BUR-BEL-DW8VZ, 1333701, etc.
    """
    # pattern type BUS-AAA-12345
    m = re.search(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b", text)
    if m:
        return m.group(0)

    # pattern pure chiffres de 6–8 caractères
    m = re.search(r"\b\d{6,8}\b", text)
    if m:
        return m.group(0)

    return ""


def parse_host_stats(text: str) -> Dict[str, Any]:
    """
    Extrait host_rating, host_reviews_count, host_years à partir du bloc "Hôte".
    Version simple mais efficace.
    """
    rating = ""
    reviews = ""
    years = ""

    # note : 4,95 sur 5
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*sur\s*5", text)
    if m:
        rating = m.group(1).replace(",", ".")

    # 1 378 commentaires
    m = re.search(r"(\d[\d\s ]*)\s+commentaire", text, re.IGNORECASE)
    if m:
        reviews = re.sub(r"[^\d]", "", m.group(1))

    # Hôte depuis 2015
    m = re.search(r"H[oô]te depuis\s+(\d{4})", text, re.IGNORECASE)
    if m:
        try:
            start_year = int(m.group(1))
            current_year = dt.datetime.utcnow().year
            years_val = max(0, current_year - start_year)
            years = str(years_val)
        except Exception:
            years = ""

    return {
        "host_rating": rating,
        "host_reviews_count": reviews,
        "host_years": years,
    }


async def extract_listing(page, url: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "listing_url": url,
        "listing_title": "",
        "license_code": "",
        "host_url": "",
        "host_name": "",
        "host_rating": "",
        "host_years": "",
        "host_reviews_count": "",
        "scraped_at": dt.datetime.utcnow().isoformat(),
    }

    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        return data

    await click_more_buttons(page)

    # titre
    title = await safe_inner_text(page, "h1")
    if not title:
        title = await page.title()
    data["listing_title"] = title

    # texte complet pour la licence et les stats
    try:
        body_text = await page.inner_text("body")
    except Exception:
        body_text = ""

    # licence
    data["license_code"] = extract_license_from_text(body_text)

    # section hôte
    host_section = page.locator('section:has-text("Hôte")')
    if await host_section.count() == 0:
        host_section = page.locator('section:has-text("Votre hôte")')

    host_text = ""
    if await host_section.count() > 0:
        try:
            host_text = await host_section.first.inner_text()
        except Exception:
            host_text = ""

        # nom de l'hôte
        name_candidate = await safe_inner_text(
            host_section.first, "h2, h3, span"
        )
        if name_candidate:
            data["host_name"] = name_candidate

        # URL de l'hôte (dans la section hôte uniquement)
        try:
            host_link = host_section.first.locator(
                'a[href*="/users/"]'
            )
            if await host_link.count() > 0:
                href = await host_link.first.get_attribute("href")
                if href:
                    data["host_url"] = href.split("?")[0]
        except Exception:
            pass

    # si toujours rien pour le nom / host_url, fallback global
    if not data["host_name"] or not data["host_url"]:
        try:
            link = page.locator('a[href*="/users/"]').first
            href = await link.get_attribute("href")
            text = await link.inner_text()
            if href:
                data["host_url"] = href.split("?")[0]
            if text:
                data["host_name"] = text.strip()
        except Exception:
            pass

    # stats hôte
    stats = parse_host_stats(host_text or body_text)
    for k, v in stats.items():
        if v and not data[k]:
            data[k] = v

    return data


async def scrape_all() -> None:
    search_urls = await read_search_urls()
    if not search_urls:
        print("Aucune URL dans search_urls.txt")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="fr-CA",
            timezone_id="America/Toronto",
        )

        all_listing_urls: List[str] = []

        # étape 1 : récupérer toutes les annonces depuis chaque page de recherche
        for search_url in search_urls:
            page = await context.new_page()
            try:
                listing_urls = await collect_listing_urls(page, search_url)
                all_listing_urls.extend(listing_urls)
            finally:
                await page.close()

        # supprimer doublons
        all_listing_urls = list(dict.fromkeys(all_listing_urls))

        print(f"{len(all_listing_urls)} annonces trouvées.")

        # étape 2 : scraper chaque annonce en parallèle
        sem = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        results: List[Dict[str, Any]] = []

        async def worker(listing_url: str):
            async with sem:
                page = await context.new_page()
                try:
                    row = await extract_listing(page, listing_url)
                    results.append(row)
                finally:
                    await page.close()

        await asyncio.gather(*(worker(u) for u in all_listing_urls))

        await browser.close()

    # écriture CSV
    fieldnames = [
        "listing_url",
        "listing_title",
        "license_code",
        "host_url",
        "host_name",
        "host_rating",
        "host_years",
        "host_reviews_count",
        "scraped_at",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"Terminé. {len(results)} lignes écrites dans {OUTPUT_CSV}.")


if __name__ == "__main__":
    asyncio.run(scrape_all())
