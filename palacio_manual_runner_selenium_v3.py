# Palacio Manual Runner (Selenium v3)
# - Manual: preset o URLs (una por línea)
# - Histórico: NEW/CHANGES/REMOVED vs último PARQUET por prefix
# - Alertas por marca (WATCHLIST_BRANDS): solo si NEW o CHANGES y discount_pct >= umbral
# - Exporta XLSX (tabs SNAPSHOT/NEW/CHANGES/REMOVED) + PARQUET snapshot
# - Pensado para GitHub Actions (headless) y local

import os, re, io, json, html, time, random, argparse, glob, math, smtplib
from pathlib import Path
from datetime import datetime, timezone
from email.message import EmailMessage
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from tenacity import retry, stop_after_attempt, wait_fixed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException,
    ElementClickInterceptedException, WebDriverException
)
from selenium.webdriver.common.action_chains import ActionChains


# ─────────────────────────────────────────────────────────────
# Email (reusa tus secrets EMAIL_*)
# ─────────────────────────────────────────────────────────────
EMAIL_HOST  = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT  = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER  = os.getenv("EMAIL_USER", "")
EMAIL_PASS  = os.getenv("EMAIL_PASS", "")
EMAIL_TO    = os.getenv("EMAIL_TO", "")
EMAIL_DEBUG = os.getenv("EMAIL_DEBUG", "0") not in ("", "0", "false", "False")

def _split_emails(x: str):
    return [e.strip() for e in re.split(r"[;,]", (x or "")) if e.strip()]

EMAIL_TO_LIST = _split_emails(EMAIL_TO)

def send_email(subject: str, body: str, attachments=None):
    if not (EMAIL_USER and EMAIL_PASS and EMAIL_TO_LIST):
        print("⚠️ EMAIL_* incompletos: no se envía correo.")
        return

    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(EMAIL_TO_LIST)
    msg["Subject"] = subject
    msg.set_content(body)

    for (fname, data, mime) in (attachments or []):
        mt, st = (mime.split("/", 1) if mime else ("application", "octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as smtp:
        if EMAIL_DEBUG:
            smtp.set_debuglevel(1)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg, from_addr=EMAIL_USER, to_addrs=EMAIL_TO_LIST)

    print(f"📧 Email enviado: {subject}")


# ─────────────────────────────────────────────────────────────
# Watchlist brands (Secret WATCHLIST_BRANDS)
# ─────────────────────────────────────────────────────────────
def load_watchlist_brands():
    raw = os.getenv("WATCHLIST_BRANDS", "")
    brands = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            brands.append(s)
    # unique, preserve order
    seen = set()
    out = []
    for b in brands:
        k = b.casefold()
        if k not in seen:
            seen.add(k)
            out.append(b)
    return out

WATCHLIST = load_watchlist_brands()


# ─────────────────────────────────────────────────────────────
# Presets (puedes ajustar/expandir)
# ─────────────────────────────────────────────────────────────
PRESETS = {
    "hombre":  {"url": "https://www.elpalaciodehierro.com/hombre/",  "prefix": "palacio_hombre"},
    "mujer":   {"url": "https://www.elpalaciodehierro.com/mujer/",   "prefix": "palacio_mujer"},
    "hogar":   {"url": "https://www.elpalaciodehierro.com/hogar/",   "prefix": "palacio_hogar"},
    "gourmet": {"url": "https://www.elpalaciodehierro.com/gourmet/", "prefix": "palacio_gourmet"},
    "calzado": {"url": "https://www.elpalaciodehierro.com/calzado/", "prefix": "palacio_calzado"},
    "ofertas": {"url": "https://www.elpalaciodehierro.com/ofertas/", "prefix": "palacio_ofertas"},
    "electronica": {"url": "https://www.elpalaciodehierro.com/electronica/", "prefix": "palacio_electronica"},
}


# ─────────────────────────────────────────────────────────────
# Selenium config
# ─────────────────────────────────────────────────────────────
WAIT = 25
SCROLL_STEP = 800
SCROLL_ROUNDS = 3
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

# selectores tolerantes para tiles (PLP)
LIST_SELECTORS = [
    "article.b-product_tile_item",
    "div.b-product_tile[data-component*='ProductTile']",
    "div.b-product_tile",
    "div.b-product",
    "li.product",
    "div.product-tile",
]

def setup_driver(headless: bool = True):
    co = ChromeOptions()
    if headless:
        co.add_argument("--headless=new")
    co.add_argument("--disable-gpu")
    co.add_argument("--window-size=1366,900")
    co.add_argument("--disable-dev-shm-usage")
    co.add_argument("--no-sandbox")
    co.add_argument("--lang=es-MX")
    co.add_argument("--disable-blink-features=AutomationControlled")
    co.add_argument("--remote-debugging-port=9222")
    co.add_argument(f"--user-agent={random.choice(UA_LIST)}")

    # IMPORTANT: evita webdriver-manager (mismatch). Selenium Manager se encarga.
    # En GitHub Actions normalmente basta con esto:
    try:
        driver = webdriver.Chrome(options=co)
    except WebDriverException:
        # fallback por si tu runner necesita Service explícito
        driver = webdriver.Chrome(service=Service(), options=co)

    driver.set_page_load_timeout(80)
    return driver

def _css_tiles():
    return ", ".join(LIST_SELECTORS)

def dismiss_banners(driver):
    # Cookies / popups típicos (tolerante: intenta y sigue)
    candidates = [
        "button#onetrust-accept-btn-handler",
        "button[aria-label*='Aceptar']",
        "button[aria-label*='Accept']",
        "button[data-testid*='accept']",
        "button[class*='accept']",
        "button:has(span)",  # no siempre soportado, pero no rompe
    ]
    texts = {"aceptar", "acepto", "accept", "entendido", "ok"}
    for _ in range(3):
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, "button, a")
            for b in btns[:80]:
                try:
                    t = (b.text or "").strip().lower()
                    if t and t in texts and b.is_displayed() and b.is_enabled():
                        driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.3)
                        return
                except Exception:
                    continue
        except Exception:
            pass
        time.sleep(0.2)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def wait_for_plp(driver):
    # espera tiles o detecta bloqueo CF
    WebDriverWait(driver, WAIT).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
    dismiss_banners(driver)

    css = _css_tiles()
    try:
        WebDriverWait(driver, WAIT).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, css)) > 0)
    except TimeoutException:
        title = (driver.title or "").lower()
        src = (driver.page_source or "").lower()
        if "just a moment" in title or "cloudflare" in src:
            raise TimeoutException("Parece bloqueo (Cloudflare).")
        raise

def current_tiles(driver):
    return driver.find_elements(By.CSS_SELECTOR, _css_tiles())

def gentle_scroll(driver, rounds=SCROLL_ROUNDS):
    for _ in range(rounds):
        driver.execute_script(f"window.scrollBy(0, {SCROLL_STEP});")
        time.sleep(0.25)
        driver.execute_script("window.scrollBy(0, -200);")
        time.sleep(0.25)

def _robust_click(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.05)
        el.click()
        return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            ActionChains(driver).move_to_element(el).pause(0.05).click().perform()
            return True
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", el)
                return True
            except Exception:
                return False

def click_next_page(driver):
    # 1) rel=next
    for sel in ["a[rel='next']", "a[aria-label*='Siguiente']", "a[aria-label*='Next']"]:
        try:
            a = driver.find_element(By.CSS_SELECTOR, sel)
            if a.is_displayed() and a.is_enabled():
                return _robust_click(driver, a)
        except Exception:
            pass

    # 2) pagination data attr
    try:
        a = driver.find_element(By.CSS_SELECTOR, "a[data-js-pagination-link].b-pagination-elements_next, a[data-js-pagination-link][data-page-number]")
        if a.is_displayed() and a.is_enabled():
            return _robust_click(driver, a)
    except Exception:
        pass

    # 3) icon arrow-right
    try:
        icon = driver.find_element(By.CSS_SELECTOR, "i.i-arrow-right-after")
        parent = icon.find_element(By.XPATH, "./ancestor::a[1]")
        if parent and parent.is_displayed() and parent.is_enabled():
            return _robust_click(driver, parent)
    except Exception:
        pass

    # 4) texto "Siguiente"
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, "a, button"):
            t = (a.text or "").strip().lower()
            if "siguiente" in t or t == "next":
                if a.is_displayed() and a.is_enabled():
                    if _robust_click(driver, a):
                        return True
    except Exception:
        pass

    return False

def wait_page_changed(driver, prev_url, prev_first_key, timeout=WAIT):
    start = time.time()
    while time.time() - start < timeout:
        time.sleep(0.3)
        try:
            if driver.current_url != prev_url:
                return True
            tiles = current_tiles(driver)
            if tiles:
                k = extract_tile_key(tiles[0])
                if k and prev_first_key and k != prev_first_key:
                    return True
        except Exception:
            pass
    return False


# ─────────────────────────────────────────────────────────────
# Parse helpers
# ─────────────────────────────────────────────────────────────
_money_clean = re.compile(r"[^\d.,]")
def parse_price(txt):
    if not txt:
        return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None

def extract_tile_key(tile):
    # intenta id o link
    try:
        da = tile.get_attribute("data-analytics")
        if da:
            try:
                data = json.loads(html.unescape(da))
                prod = (data.get("product") or {}) if isinstance(data, dict) else {}
                pid = prod.get("id")
                if pid is not None:
                    return str(pid)
            except Exception:
                pass
    except Exception:
        pass

    try:
        href = tile.find_element(By.CSS_SELECTOR, "a[href]").get_attribute("href")
        if href:
            m = re.search(r"/(\d{5,})", href)
            if m:
                return m.group(1)
            return href
    except Exception:
        pass
    return None

def parse_tile(tile, page_idx: int):
    href = None
    try:
        a = tile.find_element(By.CSS_SELECTOR, "a[href]")
        href = a.get_attribute("href")
    except Exception:
        pass

    name = None
    brand = None

    # (A) data-analytics
    product_id = None
    try:
        da = tile.get_attribute("data-analytics") or ""
        if da:
            data = json.loads(html.unescape(da))
            prod = (data.get("product") or {}) if isinstance(data, dict) else {}
            if prod.get("id") is not None:
                product_id = str(prod.get("id"))
            if prod.get("name"):
                name = prod.get("name")
            if prod.get("brand"):
                brand = prod.get("brand")
    except Exception:
        pass

    # (B) fallback: visible fields
    if not name:
        for sel in [".b-product_tile-name h4", ".b-product_tile-title", ".b-product_tile-name", "h3.b-product_tile-name"]:
            try:
                name = tile.find_element(By.CSS_SELECTOR, sel).text.strip()
                if name:
                    break
            except Exception:
                continue
    if not brand:
        for sel in [".b-product_tile-brand h4", ".b-product_tile-brand"]:
            try:
                brand = tile.find_element(By.CSS_SELECTOR, sel).text.strip()
                if brand:
                    break
            except Exception:
                continue

    # id from href if needed
    if not product_id and href:
        m = re.search(r"/(\d{5,})", href)
        if m:
            product_id = m.group(1)

    # prices
    list_p = None
    sale_p = None
    numeric_values = []

    price_block = None
    for sel in [".b-product_tile-price", ".b-product_price", ".b-product_tile .b-product_price", ".product-pricing"]:
        try:
            price_block = tile.find_element(By.CSS_SELECTOR, sel)
            break
        except Exception:
            continue

    if price_block:
        spans = price_block.find_elements(By.CSS_SELECTOR, ".b-product_price-value")
        for sp in spans:
            v = parse_price(sp.get_attribute("content") or sp.text)
            if v is not None:
                numeric_values.append(v)
            # por clases padre
            try:
                sp.find_element(By.XPATH, "./ancestor::*[contains(@class,'b-product_price-old')][1]")
                if v is not None:
                    list_p = v
            except Exception:
                pass
            try:
                sp.find_element(By.XPATH, "./ancestor::*[contains(@class,'b-product_price-sales')][1]")
                if v is not None:
                    sale_p = v
            except Exception:
                pass

    if (list_p is None or sale_p is None) and len(numeric_values) >= 2:
        mx, mn = max(numeric_values), min(numeric_values)
        if list_p is None:
            list_p = mx
        if sale_p is None:
            sale_p = mn

    if list_p is None and sale_p is None and len(numeric_values) == 1:
        list_p = sale_p = numeric_values[0]

    discount = None
    if list_p is not None and sale_p is not None and sale_p < list_p:
        discount = round((1 - sale_p / list_p) * 100, 2)

    img = None
    for sel in ["img[data-js-product-image]", "img.b-product_image", "picture img", "img"]:
        try:
            el = tile.find_element(By.CSS_SELECTOR, sel)
            src = el.get_attribute("src") or el.get_attribute("data-src")
            if src and src.startswith("http"):
                img = src
                break
        except Exception:
            continue

    return {
        "product_id": product_id,
        "sku": product_id,
        "name": name,
        "brand": brand,
        "price_currency": "MXN",
        "list_price": list_p,
        "sale_price": sale_p,
        "discount_pct": discount,
        "image_url": img,
        "enlace": href,
        "page_idx": page_idx,
        "captured_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────
# Diff + Excel
# ─────────────────────────────────────────────────────────────
def _latest_previous_parquet(prefix: str, history_dir: Path) -> Path | None:
    files = sorted(glob.glob(str(history_dir / f"{prefix}_snapshot_*.parquet")))
    return Path(files[-1]) if files else None

def _normalize_numeric(df: pd.DataFrame):
    for c in ["list_price", "sale_price", "discount_pct"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _compute_diffs(prev_df: pd.DataFrame, cur_df: pd.DataFrame):
    # retorna: key, changes_df, new_df, removed_df
    # key preferido: product_id si existe, si no enlace
    key = "product_id"
    if key not in cur_df.columns or cur_df[key].isna().all():
        key = "enlace"

    p = prev_df.copy()
    c = cur_df.copy()
    for d in (p, c):
        if key in d.columns:
            d[key] = d[key].astype("string")

    merged = p.merge(c, on=key, suffixes=("_old", "_new"), how="outer", indicator=True)

    def changed_num(a, b, atol=0.01):
        if pd.isna(a) or pd.isna(b):
            return False
        try:
            return not math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=atol)
        except Exception:
            return a != b

    both = merged[merged["_merge"] == "both"].copy()

    mask = (
        both.apply(lambda r: changed_num(r.get("list_price_old"), r.get("list_price_new")), axis=1) |
        both.apply(lambda r: changed_num(r.get("sale_price_old"), r.get("sale_price_new")), axis=1) |
        both.apply(lambda r: changed_num(r.get("discount_pct_old"), r.get("discount_pct_new")), axis=1)
    )
    changes = both.loc[mask].copy()

    new_items = merged[merged["_merge"] == "right_only"].copy()
    removed_items = merged[merged["_merge"] == "left_only"].copy()

    def _strip_side(df_side, side):
        if df_side.empty:
            return df_side
        keep = [c for c in df_side.columns if c == key or c.endswith(f"_{side}")]
        out = df_side[keep].rename(columns=lambda x: x.replace(f"_{side}", ""))
        return out

    changes = changes  # se deja con columnas _old/_new para auditoría (excel)
    new_items = _strip_side(new_items, "new")
    removed_items = _strip_side(removed_items, "old")

    return key, changes, new_items, removed_items

def build_xlsx_bytes(snapshot_df: pd.DataFrame, changes_df: pd.DataFrame, new_df: pd.DataFrame, removed_df: pd.DataFrame):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        snapshot_df.to_excel(w, index=False, sheet_name="SNAPSHOT")
        (new_df if not new_df.empty else pd.DataFrame({"info": ["Sin NEW"]})).to_excel(w, index=False, sheet_name="NEW")
        (changes_df if not changes_df.empty else pd.DataFrame({"info": ["Sin CHANGES"]})).to_excel(w, index=False, sheet_name="CHANGES")
        (removed_df if not removed_df.empty else pd.DataFrame({"info": ["Sin REMOVED"]})).to_excel(w, index=False, sheet_name="REMOVED")

        wb = w.book
        money = wb.add_format({"num_format": "#,##0.00"})
        pct = wb.add_format({'num_format': '0.00"%"'})
        link = wb.add_format({"font_color": "blue", "underline": 1})

        def fmt(ws, df):
            cols = list(df.columns)
            ws.set_column(0, max(len(cols) - 1, 0), 18)
            if "list_price" in cols:
                i = cols.index("list_price"); ws.set_column(i, i, 14, money)
            if "sale_price" in cols:
                i = cols.index("sale_price"); ws.set_column(i, i, 14, money)
            if "discount_pct" in cols:
                i = cols.index("discount_pct"); ws.set_column(i, i, 12, pct)
            if "enlace" in cols and len(df) > 0:
                i = cols.index("enlace")
                for r, val in enumerate(df["enlace"].fillna(""), start=2):
                    if isinstance(val, str) and val.startswith("http"):
                        ws.write_url(r - 1, i, val, link, string=val)
            ws.autofilter(0, 0, max(len(df), 1), max(len(cols) - 1, 0))
            ws.freeze_panes(1, 0)

        fmt(w.sheets["SNAPSHOT"], snapshot_df)
        fmt(w.sheets["NEW"], new_df if not new_df.empty else pd.DataFrame({"info": ["Sin NEW"]}))
        fmt(w.sheets["CHANGES"], changes_df if not changes_df.empty else pd.DataFrame({"info": ["Sin CHANGES"]}))
        fmt(w.sheets["REMOVED"], removed_df if not removed_df.empty else pd.DataFrame({"info": ["Sin REMOVED"]}))

    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────
def _watch_hit(brand: str) -> bool:
    if not brand:
        return False
    b = brand.casefold()
    return any(b == w.casefold() for w in WATCHLIST)

def send_watchlist_alert(prefix: str, discount_threshold: float, new_df: pd.DataFrame, changes_df: pd.DataFrame):
    if not WATCHLIST:
        return

    def _filter(df):
        if df is None or df.empty:
            return df
        out = df.copy()
        if "brand" in out.columns:
            out = out[out["brand"].fillna("").apply(_watch_hit)]
        if "discount_pct" in out.columns:
            out = out[pd.to_numeric(out["discount_pct"], errors="coerce").fillna(0) >= discount_threshold]
        return out

    hits_new = _filter(new_df)
    hits_chg = _filter(changes_df)

    if (hits_new is None or hits_new.empty) and (hits_chg is None or hits_chg.empty):
        return

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    subject = f"[Scraper][ALERTA] Watchlist {prefix} ({stamp})"
    body = (
        f"Watchlist brands: {', '.join(WATCHLIST)}\n"
        f"Umbral descuento: {discount_threshold}%\n\n"
        f"Hits NEW: {0 if hits_new is None else len(hits_new)}\n"
        f"Hits CHANGES: {0 if hits_chg is None else len(hits_chg)}\n"
    )

    # adjunta un xlsx pequeño con hits
    out_name = f"{prefix}_watchlist_hits_{stamp}.xlsx"
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        (hits_new if hits_new is not None and not hits_new.empty else pd.DataFrame({"info":["Sin hits NEW"]})).to_excel(w, index=False, sheet_name="NEW_HITS")
        (hits_chg if hits_chg is not None and not hits_chg.empty else pd.DataFrame({"info":["Sin hits CHANGES"]})).to_excel(w, index=False, sheet_name="CHANGES_HITS")
    buf.seek(0)
    send_email(subject, body, attachments=[(out_name, buf.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])


# ─────────────────────────────────────────────────────────────
# Scrape loop
# ─────────────────────────────────────────────────────────────
COLUMNS = [
    "product_id","sku","name","brand","price_currency",
    "list_price","sale_price","discount_pct",
    "image_url","enlace","page_idx","captured_at"
]

def scrape_category(url: str, headless: bool, max_pages: int):
    driver = setup_driver(headless=headless)
    rows = []
    seen = set()

    try:
        print(f"Abriendo: {url}")
        driver.get(url)
        wait_for_plp(driver)

        page = 1
        while page <= max_pages:
            dismiss_banners(driver)
            wait_for_plp(driver)

            gentle_scroll(driver, rounds=SCROLL_ROUNDS)
            tiles = current_tiles(driver)
            print(f"— Página {page} — tiles={len(tiles)}")

            new_here = 0
            for t in tiles:
                try:
                    data = parse_tile(t, page_idx=page)
                    key = data.get("product_id") or data.get("enlace")
                    if key and key not in seen:
                        seen.add(key)
                        rows.append(data)
                        new_here += 1
                except Exception:
                    continue
            print(f"   nuevos={new_here}")

            prev_url = driver.current_url
            prev_first = extract_tile_key(tiles[0]) if tiles else None

            moved = click_next_page(driver)
            if not moved:
                print("   (No hay más paginación / next)")
                break

            if not wait_page_changed(driver, prev_url=prev_url, prev_first_key=prev_first, timeout=WAIT):
                time.sleep(1.0)

            page += 1
            time.sleep(random.uniform(0.3, 0.7))

        df = pd.DataFrame(rows, columns=COLUMNS)
        _normalize_numeric(df)
        return df

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def run_one(url: str, prefix: str, headless: bool, max_pages: int,
            out_dir: Path, history_dir: Path,
            discount_threshold: float,
            send_email_flag: bool,
            watchlist_alerts_flag: bool):

    out_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)

    cur_df = scrape_category(url=url, headless=headless, max_pages=max_pages)

    # prev
    prev_pq = _latest_previous_parquet(prefix, history_dir)
    prev_df = None
    if prev_pq and prev_pq.exists():
        try:
            prev_df = pd.read_parquet(prev_pq)
            _normalize_numeric(prev_df)
        except Exception as e:
            print(f"⚠️ No pude leer previo {prev_pq.name}: {e}")

    if prev_df is not None and not prev_df.empty:
        key, changes_df, new_df, removed_df = _compute_diffs(prev_df, cur_df)
    else:
        changes_df = pd.DataFrame()
        new_df = cur_df.copy()
        removed_df = pd.DataFrame()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # archivo principal por corrida
    xlsx_bytes = build_xlsx_bytes(cur_df, changes_df, new_df, removed_df)
    xlsx_name = f"{prefix}_snapshot_{stamp}.xlsx"
    pq_name   = f"{prefix}_snapshot_{stamp}.parquet"

    (out_dir / xlsx_name).write_bytes(xlsx_bytes)
    cur_df.to_parquet(history_dir / pq_name, index=False)

    # alertas watchlist (solo NEW o CHANGES y >= threshold)
    if watchlist_alerts_flag and WATCHLIST:
        # Nota: changes_df trae columnas *_old/_new, pero también puede traer brand_new.
        # Para alertas: preferimos brand_new / discount_pct_new si existe.
        chg_for_alert = changes_df.copy()
        if not chg_for_alert.empty:
            # intenta mapear columnas new a estándar
            if "brand_new" in chg_for_alert.columns and "brand" not in chg_for_alert.columns:
                chg_for_alert["brand"] = chg_for_alert["brand_new"]
            if "discount_pct_new" in chg_for_alert.columns and "discount_pct" not in chg_for_alert.columns:
                chg_for_alert["discount_pct"] = chg_for_alert["discount_pct_new"]
            if "enlace_new" in chg_for_alert.columns and "enlace" not in chg_for_alert.columns:
                chg_for_alert["enlace"] = chg_for_alert["enlace_new"]

        send_watchlist_alert(prefix, discount_threshold, new_df, chg_for_alert)

    # correo principal
    if send_email_flag:
        big_disc = int((cur_df.get("discount_pct", pd.Series(dtype=float)).fillna(0) >= discount_threshold).sum())
        subj = f"[Scraper] {prefix} listo ({stamp})"
        body = (
            f"URL: {url}\n"
            f"Filas snapshot: {len(cur_df)}\n"
            f"NEW: {0 if new_df is None else len(new_df)}\n"
            f"CHANGES: {0 if changes_df is None else len(changes_df)}\n"
            f"REMOVED: {0 if removed_df is None else len(removed_df)}\n"
            f"≥{discount_threshold}%: {big_disc}\n"
            f"Histórico: {'sí' if prev_df is not None else 'no (primer snapshot)'}\n"
            f"Out: {out_dir}\nHistory: {history_dir}\n"
        )
        send_email(subj, body, attachments=[(xlsx_name, xlsx_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])

    print(f"✅ OK: {xlsx_name} | {pq_name}")
    return 0


def parse_args():
    p = argparse.ArgumentParser("Palacio Manual Runner (Selenium v3)")

    p.add_argument("--presets", default="", help="Lista separada por coma: ej 'hombre,gourmet'. Ignorado si pasas --urls.")
    p.add_argument("--urls", default="", help="URLs (una por línea). Si se llena, sobre-escribe presets.")
    p.add_argument("--max-pages", type=int, default=2, help="Máx páginas por URL (paginación).")
    p.add_argument("--headless", type=str, default="true", help="true/false")
    p.add_argument("--discount-threshold", type=float, default=51.0, help="Umbral % descuento (para stats y watchlist).")

    p.add_argument("--send-email", type=str, default="true", help="true/false")
    p.add_argument("--watchlist-alerts", type=str, default="true", help="true/false (solo NEW/CHANGES y >= umbral)")

    p.add_argument("--out-dir", default="outputs", help="Carpeta salida XLSX")
    p.add_argument("--history-dir", default="outputs/history", help="Carpeta histórico PARQUET")
    return p.parse_args()

def _as_bool(x: str) -> bool:
    return str(x).strip().lower() in ("1", "true", "yes", "y", "si")

def main():
    args = parse_args()

    headless = _as_bool(args.headless)
    send_email_flag = _as_bool(args.send_email)
    watch_alerts = _as_bool(args.watchlist_alerts)

    out_dir = Path(args.out_dir)
    history_dir = Path(args.history_dir)

    url_list = []
    if args.urls.strip():
        url_list = [u.strip() for u in args.urls.splitlines() if u.strip()]
        items = [(u, "palacio_manual") for u in url_list]
    else:
        presets = [x.strip() for x in (args.presets or "").split(",") if x.strip()]
        if not presets:
            raise SystemExit("Debes pasar --presets (ej: hombre) o --urls (una por línea).")
        items = []
        for k in presets:
            if k not in PRESETS:
                raise SystemExit(f"Preset no existe: {k}. Opciones: {', '.join(PRESETS.keys())}")
            items.append((PRESETS[k]["url"], PRESETS[k]["prefix"]))

    for (url, prefix) in items:
        print(f"\n=== SCRAPE: {url} (prefix={prefix}) ===")
        run_one(
            url=url, prefix=prefix,
            headless=headless, max_pages=args.max_pages,
            out_dir=out_dir, history_dir=history_dir,
            discount_threshold=args.discount_threshold,
            send_email_flag=send_email_flag,
            watchlist_alerts_flag=watch_alerts
        )

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
