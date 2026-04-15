import logging
import os
import re
import time
import random
from datetime import datetime
from typing import Generator, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from anti_block import AntiBlock
from config import BASE_URL
import playwright_scraper

DEBUG_LISTING_HTML = "debug_listing_page.html"
DEBUG_FIRM_HTML    = "debug_firm_page.html"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _strip_fragment(url: str) -> str:
    return url.split("#")[0]


def _build_page_url(base_url: str, page: int) -> str:
    base_url = _strip_fragment(base_url)
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query, fragment=""))


# ---------------------------------------------------------------------------
# Pagination detection
# ---------------------------------------------------------------------------

def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    next_page = current_page + 1
    pattern = re.compile(rf"[?&]page={next_page}(?:&|$|#)")
    if soup.find("a", href=pattern):
        return True
    if soup.find("a", rel=re.compile(r"next", re.I)):
        return True
    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        if text in ("Следующая", "›", "»", ">", ">>"):
            return True
    return False


# ---------------------------------------------------------------------------
# Firm link extraction
# ---------------------------------------------------------------------------

_FIRM_URL_RE = re.compile(r"^/firms/[\w-]+/\d+/$")


def _extract_firm_links(soup: BeautifulSoup) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        raw = a["href"]
        href = raw.split("#")[0].split("?")[0]
        if not href.endswith("/"):
            href += "/"
        if not _FIRM_URL_RE.match(href):
            continue
        full_url = BASE_URL + href
        if full_url not in seen:
            seen.add(full_url)
            links.append(full_url)
    return links


# ---------------------------------------------------------------------------
# Debug helpers
# ---------------------------------------------------------------------------

def _save_debug(path: str, html: str) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info("Debug HTML сохранён: %s (%d байт)", path, len(html))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Requisites extractor  (runs on already-fetched HTML string)
# ---------------------------------------------------------------------------

def _extract_requisites(html: str, firm_url: str) -> dict:
    result = {
        "Название организатора": "",
        "ИНН": "",
        "КПП": "",
        "ОГРН": "",
        "ОКПО": "",
        "Юридический адрес": "",
        "URL страницы организатора": firm_url,
        "Дата парсинга": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

    if not html:
        return result

    _save_debug(DEBUG_FIRM_HTML, html)
    soup = BeautifulSoup(html, "lxml")

    # Company name
    h1 = soup.find("h1")
    if h1:
        result["Название организатора"] = h1.get_text(strip=True)

    _FIELD_MAP = {"ИНН", "КПП", "ОГРН", "ОКПО", "Юридический адрес"}

    # Strategy 1: <dl>/<dt>/<dd>
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True).rstrip(":").strip()
        match = next((f for f in _FIELD_MAP if label == f or label.startswith(f)), None)
        if match:
            dd = dt.find_next_sibling("dd")
            if dd and not result[match]:
                result[match] = dd.get_text(strip=True)

    # Strategy 2: table rows
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).rstrip(":").strip()
            match = next((f for f in _FIELD_MAP if label == f or label.startswith(f)), None)
            if match and not result[match]:
                result[match] = cells[1].get_text(strip=True)

    # Strategy 3: regex on plain text
    page_text = soup.get_text(separator="\n")
    _REGEX = {
        "ИНН":  r"ИНН[:\s]+(\d{10}(?:\d{2})?)",
        "КПП":  r"КПП[:\s]+(\d{9})",
        "ОГРН": r"ОГРН[:\s]+(\d{13}(?:\d{2})?)",
        "ОКПО": r"ОКПО[:\s]+(\d{8}(?:\d{2})?)",
    }
    for field, pat in _REGEX.items():
        if not result[field]:
            m = re.search(pat, page_text)
            if m:
                result[field] = m.group(1)

    if not result["Юридический адрес"]:
        m = re.search(
            r"Юридический адрес[:\s]+(.+?)(?:\n|КПП|ИНН|ОГРН|ОКПО|Фактический)",
            page_text, re.DOTALL,
        )
        if m:
            result["Юридический адрес"] = m.group(1).strip()

    return result


# ---------------------------------------------------------------------------
# Phase 1: listing page fetch (requests — no JS protection here)
# ---------------------------------------------------------------------------

def _fetch_listing(url: str, client: AntiBlock) -> str:
    return client.get(url, referer=BASE_URL + "/")


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def parse_tenders(url: str, max_pages: int, delay: float) -> Generator[dict, None, None]:
    clean_url = _strip_fragment(url)
    host = urlparse(clean_url).netloc.lstrip("www.")
    if "b2b-center.ru" not in host:
        yield {"type": "warning", "message": "Введите корректный URL b2b-center.ru"}
        return

    client = AntiBlock(base_delay=delay)
    seen_firms: set[str] = set()
    firm_urls: list[str] = []
    tenders_seen = orgs_found = inns_found = 0
    page = 1
    unlimited = (max_pages == 0)

    # ── Phase 1: walk listing pages via requests ──────────────────────────
    while unlimited or page <= max_pages:
        page_url = _build_page_url(clean_url, page)
        logger.info("Листинг страница %d: %s", page, page_url)

        try:
            html = _fetch_listing(page_url, client)
        except Exception as e:
            yield {"type": "warning", "message": f"Ошибка загрузки страницы {page}: {e}"}
            break

        _save_debug(DEBUG_LISTING_HTML, html)
        soup = BeautifulSoup(html, "lxml")

        new_links = _extract_firm_links(soup)
        added = 0
        for link in new_links:
            if link not in seen_firms:
                seen_firms.add(link)
                firm_urls.append(link)
                added += 1

        tenders_seen += len(new_links)
        pct = min(0.4 * page / (max_pages if not unlimited else max(page, 1)), 0.4)
        yield _progress(page, pct, tenders_seen, orgs_found, inns_found,
                        f"Листинг стр. {page} — найдено организаторов: {len(firm_urls)}")

        if added == 0 and page > 1:
            break
        if not _has_next_page(soup, page):
            break

        page += 1
        client.delay()

    if not firm_urls:
        yield {
            "type": "warning",
            "message": (
                "Организаторы не найдены. "
                f"Сохранён debug HTML: {DEBUG_LISTING_HTML} — "
                "откройте его, чтобы проверить что страница загрузилась корректно."
            ),
        }
        yield {"type": "done"}
        return

    # ── Phase 2: scrape firm pages via Playwright subprocess ─────────────
    if not playwright_scraper.is_available():
        yield {
            "type": "warning",
            "message": "Playwright не установлен — реквизиты не будут загружены.",
        }
        yield {"type": "done"}
        return

    yield _progress(page, 0.4, tenders_seen, orgs_found, inns_found,
                    f"Запускаю Playwright для {len(firm_urls)} организаторов...")

    html_map: dict[str, str] = {}

    for event in playwright_scraper.scrape_firm_pages(firm_urls, clean_url):
        etype = event.get("type")

        if etype == "warmup_done":
            yield _progress(page, 0.45, tenders_seen, orgs_found, inns_found,
                            "Браузер прогрет, загружаю реквизиты...")

        elif etype == "progress":
            idx   = event.get("idx", 0)
            total = event.get("total", len(firm_urls))
            pct   = 0.45 + 0.55 * (idx / max(total, 1))
            status_txt = "✓" if event.get("status") == "ok" else "✗"
            url_short = event.get("url", "").split("/")[-2]
            yield _progress(page, pct, tenders_seen, orgs_found, inns_found,
                            f"{status_txt} {idx}/{total} — {url_short}")

        elif etype == "done":
            html_map = event.get("results", {})

        elif etype == "error":
            yield {"type": "warning", "message": event.get("message", "Ошибка Playwright")}

    # Process collected HTML
    for firm_url in firm_urls:
        html = html_map.get(firm_url, "")
        data = _extract_requisites(html, firm_url)
        orgs_found += 1
        if data["ИНН"]:
            inns_found += 1

        yield {"type": "result", "data": data}
        pct = 0.45 + 0.55 * (orgs_found / max(len(firm_urls), 1))
        yield _progress(
            page, pct, tenders_seen, orgs_found, inns_found,
            f"{data['Название организатора'] or firm_url} — ИНН: {data['ИНН'] or 'не найден'}",
        )

    yield {"type": "done"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _progress(page, pct, tenders_seen, orgs_found, inns_found, current_org) -> dict:
    return {
        "type": "progress",
        "page": page,
        "pct": float(max(0.0, min(1.0, pct))),
        "current_org": current_org,
        "tenders_seen": tenders_seen,
        "orgs_found": orgs_found,
        "inns_found": inns_found,
    }
