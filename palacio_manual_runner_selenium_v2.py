# palacio_manual_runner_selenium_v2.py
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import smtplib
import time
import random
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import (
    urlparse,
    urlunparse,
    parse_qs,
    urlencode,
    urljoin,
    quote_plus,
)

import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException


# -----------------------------
# Helpers
# -----------------------------
def _now_cdmx_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _to_bool(x: str) -> bool:
    return str(x).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _slug(s: str, max_len: int = 60) -> str:
    s = s.strip().lower()
    s = re.sub(r"https?://", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return (s[:max_len]).strip("-") if s else "site"


def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def _pct_discount(list_price: Optional[float], sale_price: Optional[float]) -> Optional[float]:
    if list_price is None or sale_price is None:
        return None
    if list_price <= 0:
        return None
    return round((list_price - sale_price) / list_price * 100.0, 2)


def _read_lines_env(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    parts = []
    for chunk in re.split(r"[\n,;]+", raw):
        c = chunk.strip()
        if c:
            parts.append(c)
    return parts


def _normalize_brand(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()


# -----------------------------
# Category discovery (from homepage menu)
# -----------------------------
@dataclass
class CategoryLink:
    label: str
    url: str
    level: int


def discover_from_home(home_url: str = "https://www.elpalaciodehierro.com/") -> List[CategoryLink]:
    r = requests.get(home_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    out: List[CategoryLink] = []
    for lvl in (1, 2, 3):
        for a in soup.select(f"a.b-categories_navigation-link_{lvl}[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            if href.startswith("/"):
                href = urljoin(home_url, href)
            label = re.sub(r"\s+", " ", a.get_text(" ", strip=True))
            if label:
                out.append(CategoryLink(label=label, url=href, level=lvl))

    seen = set()
    uniq = []
    for x in out:
        key = (x.level, x.url)
        if key not in seen:
            seen.add(key)
            uniq.append(x)
    return uniq


# -----------------------------
# Pagination by URL (?params={"page":N})
# -----------------------------
def build_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    u = urlparse(base_url)
    qs = parse_qs(u.query, keep_blank_values=True)

    params_raw = None
    if "params" in qs and qs["params"]:
        params_raw = qs["params"][0]

    params_obj = {}
    if params_raw:
        try:
            params_obj = json.loads(params_raw)
        except Exception:
            params_obj = {}

    params_obj["page"] = page
    qs["params"] = [json.dumps(params_obj, separators=(",", ":"))]

    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


# -----------------------------
# Selenium setup
# -----------------------------
def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=es-MX")

    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")

    opts.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    return webdriver.Chrome(options=opts)


def gentle_scroll(driver: webdriver.Chrome, steps: int = 4) -> None:
    try:
        h = driver.execute_script("return document.body.scrollHeight") or 0
        for i in range(1, steps + 1):
            y = int(h * i / (steps + 1))
            driver.execute_script("window.scrollTo(0, arguments[0]);", y)
            time.sleep(0.15)
    except Exception:
        pass


def _looks_blocked(page_source: str) -> bool:
    s = (page_source or "").lower()
    bad = [
        "access denied",
        "request blocked",
        "unusual traffic",
        "i am not a robot",
        "captcha",
        "/cdn-cgi/",
    ]
    return any(x in s for x in bad)


def _looks_end_of_results(page_source: str) -> bool:
    s = (page_source or "").lower()
    markers = [
        "no se encontraron",
        "sin resultados",
        "0 resultados",
        "ningún resultado",
    ]
    return any(x in s for x in markers)


def wait_for_plp(driver: webdriver.Chrome, timeout: int = 35) -> bool:
    """
    True => hay productos.
    False => página sin productos (fin/no-results). No crashea.
    Lanza TimeoutException solo si parece bloqueo/interstitial.
    """
    w = WebDriverWait(driver, timeout)
    w.until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))

    deadline = time.time() + timeout
    tile_css = "article.l-plp-grid_item.m-product, article.b-product_tile_item"

    while time.time() < deadline:
        if driver.find_elements(By.CSS_SELECTOR, tile_css):
            return True

        src = driver.page_source or ""
        if _looks_blocked(src):
            raise TimeoutException("Blocked / interstitial detected (no product tiles).")

        if _looks_end_of_results(src):
            return False

        time.sleep(0.4)

    return False


# -----------------------------
# Product extraction
# -----------------------------
def extract_product_from_article(article) -> Dict:
    out: Dict = {}

    pdata = {}
    try:
        prod = article.find_element(By.CSS_SELECTOR, ".b-product[data-analytics]")
        da = prod.get_attribute("data-analytics") or ""
        if da:
            pdata = json.loads(da).get("product", {}) or {}
        out["product_id"] = prod.get_attribute("data-pid") or pdata.get("id")
    except Exception:
        out["product_id"] = None

    out["brand"] = pdata.get("brand")
    out["name"] = pdata.get("name")
    out["category"] = pdata.get("category") or pdata.get("departmentName")
    out["department_id"] = pdata.get("departmentID")
    out["department_name"] = pdata.get("departmentName")

    out["sale_price"] = _safe_float(pdata.get("price"))
    out["list_price"] = _safe_float(pdata.get("metric1"))

    try:
        tile = article.find_element(By.CSS_SELECTOR, ".b-product_tile")
    except Exception:
        tile = article

    if not out.get("brand"):
        try:
            out["brand"] = tile.find_element(By.CSS_SELECTOR, ".b-product_tile-brand").text.strip()
        except Exception:
            out["brand"] = None

    if not out.get("name"):
        try:
            out["name"] = tile.find_element(By.CSS_SELECTOR, ".b-product_tile-name").text.strip()
        except Exception:
            out["name"] = None

    out["url"] = None
    for sel in ("a[data-js-product-tile-name]", "a.b-product_tile-image", "h3.b-product_tile-name a", "a[href*='/p/']"):
        try:
            a = tile.find_element(By.CSS_SELECTOR, sel)
            href = a.get_attribute("href")
            if href:
                out["url"] = href
                break
        except Exception:
            continue

    try:
        out["promo"] = tile.find_element(By.CSS_SELECTOR, ".b-product_tile-badge_promo").text.strip()
    except Exception:
        out["promo"] = None

    def _dom_price(css: str) -> Optional[float]:
        try:
            el = tile.find_element(By.CSS_SELECTOR, css)
            val = el.get_attribute("content") or el.text
            val = (val or "").replace("$", "").replace(",", "").strip()
            return _safe_float(re.sub(r"[^\d.]", "", val))
        except Exception:
            return None

    if out.get("list_price") is None:
        out["list_price"] = _dom_price(".b-product_price-old .b-product_price-value")

    if out.get("sale_price") is None:
        out["sale_price"] = _dom_price(".b-product_price-sales .b-product_price-value")

    out["brand"] = _normalize_brand(out.get("brand") or "")
    out["product_id"] = str(out["product_id"]) if out.get("product_id") is not None else None
    return out


def scrape_category(
    driver: webdriver.Chrome,
    base_url: str,
    max_pages: int,
    stop_after_empty: int = 1,
    scroll_steps: int = 4,
) -> pd.DataFrame:
    all_rows: List[Dict] = []
    seen_keys = set()

    empty_streak = 0
    run_ts = _now_cdmx_iso()

    for page in range(1, max_pages + 1):
        page_url = build_page_url(base_url, page)
        print(f"[page {page}/{max_pages}] {page_url}")

        time.sleep(random.uniform(0.3, 1.0))

        loaded = False
        has_products = False

        for attempt in range(1, 4):
            driver.get(page_url)
            try:
                has_products = wait_for_plp(driver, timeout=35)
                loaded = True
                break
            except TimeoutException as e:
                print(f"[warn] wait timeout (attempt {attempt}/3) on page {page}: {e}")
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(2)

        if not loaded:
            has_products = False

        if has_products:
            gentle_scroll(driver, steps=scroll_steps)

        articles = driver.find_elements(By.CSS_SELECTOR, "article.l-plp-grid_item.m-product, article.b-product_tile_item")
        page_rows = []

        for art in articles:
            try:
                row = extract_product_from_article(art)
            except Exception:
                continue

            row["page"] = page
            row["page_url"] = page_url
            row["run_ts"] = run_ts

            key = row.get("product_id") or row.get("url")
            if key and key not in seen_keys:
                seen_keys.add(key)
                page_rows.append(row)

        if not page_rows:
            empty_streak += 1
            print(f"[info] empty page_rows on page={page} (empty_streak={empty_streak}/{stop_after_empty})")
        else:
            empty_streak = 0
            all_rows.extend(page_rows)
            print(f"[info] rows page={page}: {len(page_rows)} (total={len(all_rows)})")

        if empty_streak >= stop_after_empty:
            print("[info] stop_after_empty reached; stopping pagination.")
            break

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    df["list_price"] = pd.to_numeric(df["list_price"], errors="coerce")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    df["discount_pct"] = df.apply(lambda r: _pct_discount(r["list_price"], r["sale_price"]), axis=1)
    df.replace({"": None}, inplace=True)
    return df


# -----------------------------
# Diffs
# -----------------------------
def _latest_snapshot(history_dir: Path, prefix: str) -> Optional[Path]:
    if not history_dir.exists():
        return None
    pats = list(history_dir.glob(f"{prefix}_snapshot_*.parquet"))
    if not pats:
        return None
    pats.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pats[0]


def compute_diffs(
    prev_df: Optional[pd.DataFrame],
    cur_df: pd.DataFrame,
    key_col: str = "product_id",
    compare_cols: Tuple[str, ...] = ("list_price", "sale_price", "discount_pct", "promo"),
) -> Tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if cur_df is None or cur_df.empty:
        empty = pd.DataFrame()
        return key_col, empty, empty, empty

    cur = cur_df.copy()
    cur_key = key_col if key_col in cur.columns else ("url" if "url" in cur.columns else cur.columns[0])
    cur[cur_key] = cur[cur_key].astype(str)

    if prev_df is None or prev_df.empty:
        return cur_key, pd.DataFrame(), cur, pd.DataFrame()

    prev = prev_df.copy()
    if cur_key not in prev.columns:
        alt = "url" if "url" in prev.columns else None
        if alt:
            cur_key = alt
        else:
            return cur_key, pd.DataFrame(), cur, pd.DataFrame()

    prev[cur_key] = prev[cur_key].astype(str)

    cur_keys = set(cur[cur_key].dropna())
    prev_keys = set(prev[cur_key].dropna())

    new_keys = cur_keys - prev_keys
    removed_keys = prev_keys - cur_keys

    new_df = cur[cur[cur_key].isin(new_keys)].copy()
    removed_df = prev[prev[cur_key].isin(removed_keys)].copy()

    common_prev = prev[prev[cur_key].isin(cur_keys)].copy()
    common_cur = cur[cur[cur_key].isin(prev_keys)].copy()

    merged = common_cur.merge(
        common_prev[[cur_key, *compare_cols]],
        on=cur_key,
        how="left",
        suffixes=("", "_prev"),
    )

    changed_rows = []
    for _, r in merged.iterrows():
        changed_fields = []
        for c in compare_cols:
            a = r.get(c)
            b = r.get(f"{c}_prev")

            if isinstance(a, (int, float)) or isinstance(b, (int, float)):
                if pd.isna(a) and pd.isna(b):
                    continue
                if (pd.isna(a) and not pd.isna(b)) or (not pd.isna(a) and pd.isna(b)):
                    changed_fields.append(c)
                else:
                    if abs(float(a) - float(b)) > 1e-6:
                        changed_fields.append(c)
            else:
                if (a or None) != (b or None):
                    changed_fields.append(c)

        if changed_fields:
            row = r.to_dict()
            row["changed_fields"] = ", ".join(changed_fields)
            changed_rows.append(row)

    changes_df = pd.DataFrame(changed_rows)
    return cur_key, changes_df, new_df, removed_df


# -----------------------------
# Outputs
# -----------------------------
def write_outputs(
    out_dir: Path,
    prefix: str,
    cur_df: pd.DataFrame,
    key_col: str,
    changes_df: pd.DataFrame,
    new_df: pd.DataFrame,
    removed_df: pd.DataFrame,
) -> Tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    xlsx_path = out_dir / f"{prefix}_report_{ts}.xlsx"
    snap_path = out_dir / f"{prefix}_snapshot_{ts}.parquet"

    summary = pd.DataFrame(
        [{
            "prefix": prefix,
            "run_ts": _now_cdmx_iso(),
            "key_col": key_col,
            "snapshot_rows": int(len(cur_df)),
            "new_rows": int(len(new_df)),
            "changed_rows": int(len(changes_df)),
            "removed_rows": int(len(removed_df)),
        }]
    )

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        cur_df.to_excel(writer, sheet_name="snapshot", index=False)
        summary.to_excel(writer, sheet_name="summary", index=False)
        new_df.to_excel(writer, sheet_name="new_items", index=False)
        changes_df.to_excel(writer, sheet_name="changed_items", index=False)
        removed_df.to_excel(writer, sheet_name="removed_items", index=False)

        wb = writer.book
        fmt_money = wb.add_format({"num_format": "$#,##0.00"})
        fmt_pct = wb.add_format({"num_format": "0.00"})

        for sh in ["snapshot", "new_items", "changed_items", "removed_items"]:
            ws = writer.sheets.get(sh)
            if not ws:
                continue
            try:
                df = {"snapshot": cur_df, "new_items": new_df, "changed_items": changes_df, "removed_items": removed_df}[sh]
                for i, col in enumerate(df.columns):
                    w = min(max(10, int(df[col].astype(str).str.len().fillna(0).max()) + 2), 60)
                    ws.set_column(i, i, w)
                if "list_price" in df.columns:
                    ws.set_column(df.columns.get_loc("list_price"), df.columns.get_loc("list_price"), 14, fmt_money)
                if "sale_price" in df.columns:
                    ws.set_column(df.columns.get_loc("sale_price"), df.columns.get_loc("sale_price"), 14, fmt_money)
                if "discount_pct" in df.columns:
                    ws.set_column(df.columns.get_loc("discount_pct"), df.columns.get_loc("discount_pct"), 12, fmt_pct)
            except Exception:
                pass

    cur_df.to_parquet(snap_path, index=False)
    return xlsx_path, snap_path


# -----------------------------
# Email alerts
# -----------------------------
def send_email(subject: str, html_body: str, attachments: Optional[List[Path]] = None) -> None:
    host = os.getenv("SMTP_HOST", "")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    pwd = os.getenv("SMTP_PASS", "")
    to = os.getenv("EMAIL_TO", "")

    if not (host and user and pwd and to):
        print("[email] SMTP_* o EMAIL_TO no configurados; skip.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.set_content("Tu cliente de correo no soporta HTML.")
    msg.add_alternative(html_body, subtype="html")

    for p in attachments or []:
        if not p.exists():
            continue
        data = p.read_bytes()
        msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=p.name)

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)


def build_alert_df(df: pd.DataFrame, watchlist: List[str], threshold: float) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    wl = {_normalize_brand(x) for x in watchlist}
    d = df.copy()
    d["brand_norm"] = d["brand"].apply(_normalize_brand)
    d["discount_pct"] = pd.to_numeric(d["discount_pct"], errors="coerce")
    alert = d[(d["brand_norm"].isin(wl)) & (d["discount_pct"] >= float(threshold))].copy()
    keep = [c for c in ["brand", "name", "list_price", "sale_price", "discount_pct", "promo", "url", "page"] if c in alert.columns]
    return alert[keep].sort_values(["brand", "discount_pct"], ascending=[True, False])


# -----------------------------
# Main
# -----------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()

    ap.add_argument("--presets", default="", help="Comma-separated presets (hombre,mujer,ofertas,...)")
    ap.add_argument("--urls", default="", help="URLs multiline (manual).")
    ap.add_argument("--urls-file", default="", help="Archivo con 1 URL por línea.")
    ap.add_argument("--discover", default="false", help="true => imprime links del home y sale.")
    ap.add_argument("--home-url", default="https://www.elpalaciodehierro.com/")
    ap.add_argument("--search-term", default="", help="Termino(s) para buscar en /buscar?q=. Admite coma o newline.")

    ap.add_argument("--max-pages", default="10")
    ap.add_argument("--stop-after-empty", default="1")
    ap.add_argument("--scroll-steps", default="4")
    ap.add_argument("--headless", default="true")

    ap.add_argument("--out-dir", default="outputs")
    ap.add_argument("--history-dir", default="outputs/history")
    ap.add_argument("--discount-threshold", default="51")

    ap.add_argument("--send-email", default="false")
    ap.add_argument("--upload-drive", default="false")

    return ap.parse_args()


PRESET_URLS = {
    "hombre": "https://www.elpalaciodehierro.com/hombre/",
    "mujer": "https://www.elpalaciodehierro.com/mujer/",
    "belleza": "https://www.elpalaciodehierro.com/belleza/",
    "hogar": "https://www.elpalaciodehierro.com/hogar/",
    "gourmet": "https://www.elpalaciodehierro.com/gourmet/",
    "marcas": "https://www.elpalaciodehierro.com/marcas/",

    "ofertas": "https://www.elpalaciodehierro.com/ofertas",
    "lo_mas_vendido": "https://www.elpalaciodehierro.com/lo-mas-vendido",
    "deportes": "https://www.elpalaciodehierro.com/deportes",
    "marcas_nuevas": "https://www.elpalaciodehierro.com/marcas-nuevas",

    "calzado": "https://www.elpalaciodehierro.com/calzado/",
    "calzado_mujer": "https://www.elpalaciodehierro.com/mujer/calzado/",
    "zapatos_hombre": "https://www.elpalaciodehierro.com/hombre/zapatos/",
}


def collect_urls(args: argparse.Namespace) -> List[str]:
    urls: List[str] = []

    if args.urls_file:
        p = Path(args.urls_file)
        if p.exists():
            for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)

    if args.urls.strip():
        for line in args.urls.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)

    presets = [x.strip().lower() for x in (args.presets or "").split(",") if x.strip()]
    unknown = []
    for pr in presets:
        if pr in PRESET_URLS:
            urls.append(PRESET_URLS[pr])
        else:
            unknown.append(pr)

    if unknown:
        print(f"[error] Preset(s) inválido(s): {', '.join(unknown)}")
        print("[info] Presets disponibles:", ", ".join(sorted(PRESET_URLS.keys())))

    if (args.search_term or "").strip():
        for term in re.split(r"[\n,;]+", args.search_term):
            term = term.strip()
            if not term:
                continue
            urls.append(f"https://www.elpalaciodehierro.com/buscar?q={quote_plus(term)}")

    out = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def main() -> int:
    args = parse_args()

    if _to_bool(args.discover):
        links = discover_from_home(args.home_url)
        print(f"Found {len(links)} links from home menu:")
        for x in links[:250]:
            print(f"[lvl{x.level}] {x.label} -> {x.url}")
        return 0

    urls = collect_urls(args)
    if not urls:
        print("No URLs provided. Usa --urls / --urls-file / --presets o --discover=true o --search-term.")
        print("[info] Presets disponibles:", ", ".join(sorted(PRESET_URLS.keys())))
        return 2

    out_dir = Path(args.out_dir)
    history_dir = Path(args.history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    driver = make_driver(headless=_to_bool(args.headless))

    watchlist = _read_lines_env("BRAND_WATCHLIST")
    threshold = float(args.discount_threshold)

    try:
        for base_url in urls:
            prefix = _slug(urlparse(base_url).path or base_url)
            print(f"\n=== SCRAPE: {base_url} (prefix={prefix}) ===")

            cur_df = scrape_category(
                driver=driver,
                base_url=base_url,
                max_pages=int(args.max_pages),
                stop_after_empty=int(args.stop_after_empty),
                scroll_steps=int(args.scroll_steps),
            )

            prev_path = _latest_snapshot(history_dir, prefix)
            prev_df = pd.read_parquet(prev_path) if prev_path else None

            key_col, changes_df, new_df, removed_df = compute_diffs(prev_df, cur_df)

            xlsx_path, snap_path = write_outputs(
                out_dir=out_dir,
                prefix=prefix,
                cur_df=cur_df,
                key_col=key_col,
                changes_df=changes_df,
                new_df=new_df,
                removed_df=removed_df,
            )

            snap_hist = history_dir / snap_path.name
            snap_path.replace(snap_hist)

            print(f"[ok] snapshot={len(cur_df)} new={len(new_df)} changed={len(changes_df)} removed={len(removed_df)}")
            print(f"[files] {xlsx_path.name} + {snap_hist.name}")

            # ✅ CAMBIO: mandar correo SIEMPRE (si send_email=true), sin importar NEW/CHANGED/watchlist
            if _to_bool(args.send_email):
                alert_new = build_alert_df(new_df, watchlist, threshold) if watchlist else pd.DataFrame()
                alert_changed = build_alert_df(changes_df, watchlist, threshold) if watchlist else pd.DataFrame()

                def _html_table(df: pd.DataFrame, title: str) -> str:
                    if df is None or df.empty:
                        return f"<p><i>{title}: (sin registros)</i></p>"
                    return f"<h3>{title}</h3>" + df.to_html(index=False, escape=False)

                html = (
                    f"<p><b>URL:</b> {base_url}</p>"
                    f"<p><b>threshold:</b> {threshold}%</p>"
                    f"<p><b>snapshot:</b> {len(cur_df)} | <b>new:</b> {len(new_df)} | "
                    f"<b>changed:</b> {len(changes_df)} | <b>removed:</b> {len(removed_df)}</p>"
                    + _html_table(alert_new, "Watchlist + threshold (Nuevos)")
                    + _html_table(alert_changed, "Watchlist + threshold (Cambiados)")
                )

                send_email(
                    subject=f"[Palacio Report] {prefix} | snap={len(cur_df)} new={len(new_df)} chg={len(changes_df)} rm={len(removed_df)}",
                    html_body=html,
                    attachments=[xlsx_path],
                )

    finally:
        driver.quit()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
