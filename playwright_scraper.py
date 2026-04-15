"""
Playwright scraper that runs in a SUBPROCESS to avoid Windows asyncio
event-loop conflicts with Streamlit.

Each call to scrape_firm_pages() launches a fresh Python process that:
  1. Warms up a browser session (homepage → search page)
  2. Loads every firm URL with full JS rendering
  3. Prints JSON {url: html} to stdout
  4. Writes progress lines to stderr (read in real-time by the parent)
"""

import glob
import json
import logging
import os
import subprocess
import sys
from typing import Generator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# The script that runs INSIDE the subprocess
# ---------------------------------------------------------------------------
_RUNNER = r"""
import asyncio, json, random, sys, time

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

STEALTH_JS = '''
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}, app: {}};
const _origPermQuery = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (p) =>
    p.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : _origPermQuery(p);
'''

async def main(warmup_url: str, firm_urls: list):
    import os
    from playwright.async_api import async_playwright

    token = os.environ.get("BROWSERLESS_TOKEN", "").strip()
    use_remote = bool(token)

    results = {}
    async with async_playwright() as p:
        if use_remote:
            # --- Remote browser via browserless.io (Playwright WebSocket protocol) ---
            ws_url = f"wss://chrome.browserless.io/playwright?token={token}"
            sys.stderr.write(f"WARMUP_ERR\tINFO\tПодключение к browserless.io...\n")
            sys.stderr.flush()
            browser = await p.chromium.connect(ws_url)
            ctx = await browser.new_context(
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent=UA,
                viewport={"width": 1440, "height": 900},
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
        else:
            # --- Local Chromium ---
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-extensions",
                ],
            )
            ctx = await browser.new_context(
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent=UA,
                viewport={"width": 1440, "height": 900},
                extra_http_headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )

        await ctx.add_init_script(STEALTH_JS)

        # --- Warmup: homepage → search page ---
        for url, ref in [
            ("https://www.b2b-center.ru/", "https://www.google.com/"),
            (warmup_url, "https://www.b2b-center.ru/"),
        ]:
            pg = await ctx.new_page()
            try:
                await pg.set_extra_http_headers({"Referer": ref})
                await pg.goto(url, wait_until="networkidle", timeout=30_000)
                await asyncio.sleep(random.uniform(2.0, 3.5))
            except Exception as e:
                sys.stderr.write(f"WARMUP_ERR\t{url}\t{e}\n"); sys.stderr.flush()
            finally:
                await pg.close()

        sys.stderr.write("WARMUP_DONE\n"); sys.stderr.flush()

        # --- Fetch each firm page ---
        for url in firm_urls:
            pg = await ctx.new_page()
            try:
                await pg.goto(url, wait_until="networkidle", timeout=40_000)
                # Wait for spinner to disappear (ServicePipe challenge)
                try:
                    await pg.wait_for_selector(
                        "#id_spinner", state="hidden", timeout=15_000
                    )
                    # Extra pause after challenge completes
                    await asyncio.sleep(1.5)
                except Exception:
                    pass
                html = await pg.content()
                results[url] = html if html else ""
                sys.stderr.write(f"OK\t{url}\n"); sys.stderr.flush()
            except Exception as e:
                results[url] = ""
                sys.stderr.write(f"ERR\t{url}\t{e}\n"); sys.stderr.flush()
            finally:
                await pg.close()
            await asyncio.sleep(random.uniform(1.5, 2.5))

        await browser.close()

    # Output HTML as JSON to stdout — use sys.stdout.buffer for safe encoding
    out = json.dumps(results, ensure_ascii=False)
    sys.stdout.buffer.write(out.encode("utf-8"))
    sys.stdout.buffer.flush()

data = json.loads(sys.argv[1])
asyncio.run(main(data["warmup_url"], data["firm_urls"]))
"""


# ---------------------------------------------------------------------------
# Availability / Chromium check
# ---------------------------------------------------------------------------

def is_available() -> bool:
    try:
        import playwright  # noqa: F401
        return True
    except ImportError:
        return False


def _find_chromium_exe() -> str | None:
    local_app = os.environ.get("LOCALAPPDATA", "")
    home = os.path.expanduser("~")
    patterns = []
    for base in filter(None, [local_app, os.path.join(home, "AppData", "Local")]):
        for folder in ("chrome-win64", "chrome-win"):
            patterns.append(os.path.join(base, "ms-playwright", "chromium-*", folder, "chrome.exe"))
    patterns.append(os.path.join(home, ".cache", "ms-playwright", "chromium-*", "chrome-linux", "chrome"))
    patterns.append(os.path.join(
        home, "Library", "Caches", "ms-playwright", "chromium-*",
        "chrome-mac", "Chromium.app", "Contents", "MacOS", "Chromium",
    ))
    for p in patterns:
        m = glob.glob(p)
        if m:
            return m[0]
    return None


def get_mode() -> tuple[str, str]:
    """
    Returns (mode, detail):
      mode = "remote"  — browserless.io token found in env
      mode = "local"   — local Chromium found
      mode = "none"    — nothing available
    """
    token = os.environ.get("BROWSERLESS_TOKEN", "").strip()
    if token:
        masked = token[:6] + "..." + token[-4:] if len(token) > 10 else "***"
        return "remote", f"browserless.io (токен: {masked})"

    exe = _find_chromium_exe()
    if exe:
        return "local", exe

    local_app = os.environ.get("LOCALAPPDATA", "N/A")
    return "none", (
        f"Chromium не найден (LOCALAPPDATA={local_app}).\n"
        "Запустите: python -m playwright install chromium\n"
        "Или задайте переменную BROWSERLESS_TOKEN для работы через облако."
    )


def check_chromium() -> tuple[bool, str]:
    mode, detail = get_mode()
    if mode in ("remote", "local"):
        return True, detail
    return False, detail


# ---------------------------------------------------------------------------
# Main entry point used by parser.py
# ---------------------------------------------------------------------------

def scrape_firm_pages(
    firm_urls: list[str],
    warmup_url: str,
) -> Generator[dict, None, None]:
    """
    Generator — yields event dicts while Playwright runs in a subprocess:
      {"type": "progress", "url": url, "status": "ok"|"err", "idx": N, "total": M}
      {"type": "done",     "results": {url: html}}
      {"type": "error",    "message": str}
    """
    payload = json.dumps(
        {"warmup_url": warmup_url, "firm_urls": firm_urls},
        ensure_ascii=False,
    )

    cmd = [sys.executable, "-c", _RUNNER, payload]
    logger.info("Запускаю Playwright subprocess: %d URL", len(firm_urls))

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,           # binary — we decode manually
            stderr=subprocess.PIPE,
            text=False,                        # binary mode for stdout safety
        )
        # Wrap stderr in text mode for line-by-line reading
        import io
        proc.stderr = io.TextIOWrapper(proc.stderr, encoding="utf-8", errors="replace")
    except Exception as e:
        yield {"type": "error", "message": f"Не удалось запустить subprocess: {e}"}
        return

    total = len(firm_urls)
    idx = 0
    stderr_lines = []

    # Read stderr in real-time for progress; stdout is read once at end
    import threading, queue as _queue

    stdout_q: _queue.Queue = _queue.Queue()

    # Read stdout in background (separate from stderr to avoid deadlock)
    def _read_stdout():
        try:
            data = proc.stdout.read()   # reads until EOF, does NOT touch stderr
            stdout_q.put(data or b"")
        except Exception as e:
            stdout_q.put(b"")
            logger.error("Ошибка чтения stdout subprocess: %s", e)

    t = threading.Thread(target=_read_stdout, daemon=True)
    t.start()

    # Read stderr line by line for real-time progress events
    for raw_line in proc.stderr:
        line = (raw_line or "").rstrip("\n")
        if not line:
            continue
        stderr_lines.append(line)
        parts = line.split("\t")
        tag = parts[0] if parts else ""

        if tag == "OK" and len(parts) >= 2:
            idx += 1
            yield {"type": "progress", "url": parts[1], "status": "ok",
                   "idx": idx, "total": total}
        elif tag == "ERR" and len(parts) >= 2:
            idx += 1
            err_msg = parts[2] if len(parts) >= 3 else ""
            yield {"type": "progress", "url": parts[1], "status": "err",
                   "idx": idx, "total": total, "err": err_msg}
            logger.warning("Playwright ERR %s: %s", parts[1], err_msg)
        elif tag == "WARMUP_DONE":
            yield {"type": "warmup_done"}
        elif tag == "WARMUP_ERR":
            logger.warning("Warmup error: %s", line)
        else:
            logger.debug("subprocess stderr: %s", line)

    proc.wait()   # ensure returncode is set
    t.join(timeout=30)

    raw_bytes: bytes = stdout_q.get(timeout=5) if not stdout_q.empty() else b""
    raw_stdout: str = raw_bytes.decode("utf-8", errors="replace").strip()

    if proc.returncode != 0 or not raw_stdout:
        err_detail = "\n".join(stderr_lines[-30:])
        yield {
            "type": "error",
            "message": (
                f"Playwright subprocess завершился с ошибкой "
                f"(код {proc.returncode}).\n\n{err_detail}"
            ),
        }
        return

    try:
        results = json.loads(raw_stdout)
    except json.JSONDecodeError as e:
        yield {"type": "error", "message": f"Не удалось разобрать вывод subprocess: {e}\n\nСырой вывод: {raw_stdout[:500]}"}
        return

    yield {"type": "done", "results": results}
