import logging
import os
import subprocess
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

from exporter import to_csv, to_excel
from parser import parse_tenders
import playwright_scraper

# ── Auto-install Playwright Chromium on cloud (Streamlit Cloud has no binary) ──
@st.cache_resource(show_spinner="Подготовка браузера (первый запуск ~1 мин)...")
def _ensure_chromium():
    if playwright_scraper._find_chromium_exe():
        return "already_installed"
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            return "installed"
        return f"failed: {result.stderr[-300:]}"
    except Exception as e:
        return f"error: {e}"

_chromium_status = _ensure_chromium()

# Load browserless token from Streamlit secrets if available
try:
    _secret_token = st.secrets.get("BROWSERLESS_TOKEN", "")
    if _secret_token:
        os.environ.setdefault("BROWSERLESS_TOKEN", _secret_token)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

st.set_page_config(
    page_title="Парсер тендеров b2b-center.ru",
    page_icon="📋",
    layout="wide",
)

st.title("Парсер организаторов тендеров b2b-center.ru")

# ── URL Input ──────────────────────────────────────────────────────────────────
url_input = st.text_input(
    label="Ссылка на поиск b2b-center",
    placeholder="https://www.b2b-center.ru/market/?f_keyword=...&searching=1",
    help="Скопируйте URL страницы поиска из браузера и вставьте сюда",
)

# ── Settings row ──────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
max_pages = col1.number_input(
    "Максимум страниц",
    min_value=0,
    max_value=500,
    value=10,
    step=1,
    help="Одна страница ≈ 20 тендеров. Поставьте 0 — парсить все страницы",
)
delay = col2.slider(
    "Задержка между запросами (сек)",
    min_value=1.0,
    max_value=10.0,
    value=3.0,
    step=0.5,
)

# ── Playwright status ─────────────────────────────────────────────────────────
if not playwright_scraper.is_available():
    st.error(
        "**Playwright не установлен.**\n\n"
        "```\npip install playwright\nplaywright install chromium\n```",
        icon="⚠️",
    )
else:
    mode, detail = playwright_scraper.get_mode()
    if mode == "remote":
        st.warning(f"Режим: **облачный браузер** (browserless.io) — возможны блокировки по IP", icon="☁️")
    elif mode == "local":
        st.success(f"Режим: **локальный Chromium** (`{detail}`)", icon="✅")
    else:
        st.error(
            "**Chromium не найден.**\n\n"
            f"Статус установки: `{_chromium_status}`",
            icon="⚠️",
        )

if st.button("🔄 Обновить статус", key="refresh_pw"):
    st.rerun()

# ── Run button ────────────────────────────────────────────────────────────────
run_clicked = st.button("▶ Запустить парсинг", type="primary")

if run_clicked:
    if not url_input.strip():
        st.warning("Введите ссылку на страницу поиска b2b-center.ru")
    elif "b2b-center.ru" not in url_input:
        st.warning(
            "Введите корректный URL b2b-center.ru. "
            "Пример: https://www.b2b-center.ru/market/?f_keyword=металл&searching=1"
        )
    else:
        st.session_state["results"] = []
        st.session_state["df"] = None

        progress_bar = st.progress(0.0, text="Инициализация...")
        status_box = st.empty()

        metric_cols = st.columns(3)
        m_tenders = metric_cols[0].empty()
        m_orgs = metric_cols[1].empty()
        m_inns = metric_cols[2].empty()

        m_tenders.metric("Тендеров просмотрено", 0)
        m_orgs.metric("Организаторов найдено", 0)
        m_inns.metric("ИНН собрано", 0)

        warnings_box = st.empty()

        try:
            for event in parse_tenders(url_input.strip(), int(max_pages), delay):
                etype = event.get("type")

                if etype == "progress":
                    pct = float(event.get("pct", 0))
                    pct = max(0.0, min(1.0, pct))
                    current_org = event.get("current_org", "")
                    page_num = event.get("page", "?")
                    progress_bar.progress(pct, text=f"Страница {page_num} — {current_org}")
                    status_box.text(f"Страница {page_num} — {current_org}")
                    m_tenders.metric("Тендеров просмотрено", event.get("tenders_seen", 0))
                    m_orgs.metric("Организаторов найдено", event.get("orgs_found", 0))
                    m_inns.metric("ИНН собрано", event.get("inns_found", 0))

                elif etype == "result":
                    st.session_state["results"].append(event["data"])

                elif etype == "debug":
                    st.caption(f"🔍 {event.get('msg', '')}")

                elif etype == "warning":
                    warnings_box.warning(event.get("message", "Неизвестное предупреждение"))

                elif etype == "done":
                    progress_bar.progress(1.0, text="Готово!")
                    status_box.text("Парсинг завершён.")

        except Exception as exc:
            import traceback
            st.error(f"Ошибка во время парсинга: {exc}\n\n```\n{traceback.format_exc()}\n```")
            logging.exception("Unhandled parser exception")

        results = st.session_state.get("results", [])
        if results:
            st.session_state["df"] = pd.DataFrame(results)
        elif not st.session_state.get("df"):
            st.info("Тендеры не найдены. Попробуйте другой поисковый запрос или увеличьте число страниц.")

# ── Results table (persists across interactions) ───────────────────────────────
df: pd.DataFrame = st.session_state.get("df")
if df is not None and not df.empty:
    st.subheader(f"Результаты: {len(df)} организаторов")
    st.dataframe(df, use_container_width=True)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    dl_col1, dl_col2 = st.columns(2)

    dl_col1.download_button(
        label="⬇ Скачать Excel",
        data=to_excel(df),
        file_name=f"inn_results_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    dl_col2.download_button(
        label="⬇ Скачать CSV",
        data=to_csv(df),
        file_name=f"inn_results_{ts}.csv",
        mime="text/csv",
    )
