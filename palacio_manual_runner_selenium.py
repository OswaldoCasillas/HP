# -*- coding: utf-8 -*-
# ──────────────────────────────────────────────────────────────────────────────
# Palacio de Hierro – Runner MANUAL (elige categoría o URL) con Selenium
# - PLP nueva: selectores robustos (tiles/precios/paginación)
# - Histórico .parquet + diffs
# - Alertas por marca (NEW/CHANGES con descuento ≥ ALERT_MIN_DISC) -> email
# - Genera XLSX (hoja SNAPSHOT)
# Reqs: selenium webdriver-manager pandas xlsxwriter openpyxl
# Secrets/ENV (opcional): 
#   SAVE_DIR=/tmp/palacio_out
#   EMAIL_HOST, EMAIL_PORT, EMAIL_USER, EMAIL_PASS, EMAIL_TO, EMAIL_DEBUG=0/1
#   PALACIO_ALERT_BRANDS="Le Labo,Saint Laurent Paris,Marc Jacobs,..."
#   ALERT_MIN_DISC=30
# ──────────────────────────────────────────────────────────────────────────────

import os, re, time, random, smtplib, io, sys
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException,
    ElementClickInterceptedException
)
from webdriver_manager.chrome import ChromeDriverManager

# ───────────── Config ─────────────
WAIT = 25
SCROLL_STEP = 700
SCROLL_ROUNDS = 2
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

LIST_SELECTORS = [
    # PLP nueva / variantes vistas en el HTML
    "article.b-product_tile_item",
    "div.b-product_tile[data-component='search/ProductTile']",
    "div.b-product_tile",
    "div.b-product",
    "li.product",
    "div.product-tile",
]

PRICE_BLOCKS = [
    ".b-product_tile-price", ".b-product_price",
    ".b-product_tile .b-product_price", ".product-pricing"
]

OLD_PRICE_HINT = "[class*='price-old'], .b-product_price-old"
SALE_PRICE_HINT = "[class*='price-sales'], .b-product_price-sales"

PAGINATION_NEXT = [
    "a[data-js-pagination-link][aria-label*='Siguiente']",
    ".b-pagination-elements_list .b-next-btn a",
    ".b-pagination-elements_next a",
    "li.b-pagination-elements_list.b-next-btn a",
    "a.b-pagination-elements_number[aria-label*='Siguiente']",
    "a[rel='next']",
]

_money_clean = re.compile(r"[^\d.,]")
def parse_price(txt):
    if not txt:
        return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None

def _env_bool(name, default=False):
    v = os.environ.get(name, "")
    if v == "": 
        return default
    return v not in ("0", "false", "False", "no", "NO")

def setup_driver(headless=True):
    co = ChromeOptions()
    if headless:
        co.add_argument("--headless=new")
    co.add_argument("--disable-gpu")
    co.add_argument("--window-size=1366,900")
    co.add_argument("--disable-dev-shm-usage")
    co.add_argument("--no-sandbox")
    co.add_argument("--lang=es-MX")
    co.add_argument("--disable-blink-features=AutomationControlled")
    co.add_argument(f"--user-agent={random.choice(UA_LIST)}")
    # endurecer contra OOM
    co.add_argument("--disable-background-timer-throttling")
    co.add_argument("--disable-renderer-backgrounding")
    co.add_argument("--disable-backgrounding-occluded-windows")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=co)
    driver.set_page_load_timeout(60)
    return driver

def wait_grid(driver):
    css = ", ".join(LIST_SELECTORS)
    WebDriverWait(driver, WAIT).until(lambda d: len(d.find_elements(By.CSS_SELECTOR, css)) > 0)

def current_tiles(driver):
    css = ", ".join(LIST_SELECTORS)
    return driver.find_elements(By.CSS_SELECTOR, css)

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

def click_next_page(driver, next_page_number):
    # 1) enlace con data-page-number
    try:
        sel = f"a[data-js-pagination-link][data-page-number='{next_page_number}']"
        link = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
        if _robust_click(driver, link):
            return True
    except TimeoutException:
        pass
    # 2) botón Siguiente
    for sel in PAGINATION_NEXT:
        try:
            a = driver.find_element(By.CSS_SELECTOR, sel)
            if a.is_displayed() and a.is_enabled():
                if _robust_click(driver, a): 
                    return True
        except Exception:
            continue
    # 3) ícono flecha (fallback)
    try:
        icon = driver.find_element(By.CSS_SELECTOR, "i.i-arrow-right-after")
        parent_link = icon.find_element(By.XPATH, "./ancestor::a[1]")
        if parent_link and _robust_click(driver, parent_link):
            return True
    except Exception:
        pass
    return False

def wait_page_changed(driver, prev_count, timeout=WAIT):
    start = time.time()
    prev_url = driver.current_url
    css = ", ".join(LIST_SELECTORS)
    while time.time() - start < timeout:
        time.sleep(0.25)
        if driver.current_url != prev_url:
            return True
        try:
            curr = len(driver.find_elements(By.CSS_SELECTOR, css))
            if curr != prev_count and curr > 0:
                return True
        except Exception:
            pass
    return False

def _extract_text(el, selectors):
    for sel in selectors:
        try:
            node = el.find_element(By.CSS_SELECTOR, sel)
            t = (node.text or "").strip()
            if t:
                return t
        except Exception:
            continue
    return None

def _extract_img(el):
    for sel in ["img[data-js-product-image]", "img.b-product_image", "picture img", "img"]:
        try:
            img_el = el.find_element(By.CSS_SELECTOR, sel)
            src = img_el.get_attribute("src") or img_el.get_attribute("data-src")
            if src and src.startswith("http"):
                return src
        except Exception:
            continue
    return None

def parse_tile(el, page_idx):
    # enlace / id
    href = None
    try:
        a = el.find_element(By.CSS_SELECTOR, "a[href]")
        href = a.get_attribute("href")
    except Exception:
        pass
    pid = None
    if href:
        m = re.search(r"/(\d{5,})", href)
        if m:
            pid = m.group(1)

    name = _extract_text(el, [
        ".b-product_tile-name h4", ".b-product_tile-title",
        ".b-product_tile-name", "h3.b-product_tile-name"
    ])
    brand = _extract_text(el, [".b-product_tile-brand h4", ".b-product_tile-brand"])

    # precios
    list_p = sale_p = None
    nums = []

    price_block = None
    for sel in PRICE_BLOCKS:
        try:
            price_block = el.find_element(By.CSS_SELECTOR, sel)
            break
        except Exception:
            continue

    if price_block:
        spans = price_block.find_elements(By.CSS_SELECTOR, ".b-product_price-value, [itemprop='price'], span")
        for sp in spans:
            v = parse_price(sp.get_attribute("content") or sp.text)
            if v is not None:
                nums.append(v)
            try:
                if sp.find_elements(By.XPATH, f"./ancestor::*[self::{OLD_PRICE_HINT}]"):
                    if v is not None:
                        list_p = v
                if sp.find_elements(By.XPATH, f"./ancestor::*[self::{SALE_PRICE_HINT}]"):
                    if v is not None:
                        sale_p = v
            except Exception:
                pass

    if (list_p is None or sale_p is None) and len(nums) >= 2:
        mx, mn = max(nums), min(nums)
        if list_p is None: list_p = mx
        if sale_p is None: sale_p = mn
    if list_p is None and sale_p is None and len(nums) == 1:
        list_p = sale_p = nums[0]

    discount = None
    if list_p and sale_p and sale_p < list_p:
        discount = round((1 - (sale_p / list_p)) * 100, 2)

    return {
        "product_id": pid,
        "sku": pid,
        "name": name,
        "brand": brand,
        "price_currency": "MXN",
        "list_price": list_p,
        "sale_price": sale_p,
        "discount_pct": discount,
        "image_url": _extract_img(el),
        "enlace": href,
        "page_idx": page_idx,
        "captured_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }

def scrape_category(url, headless=True, max_pages=50):
    driver = setup_driver(headless=headless)
    rows, seen = [], set()
    try:
        print(f"Abriendo: {url}")
        driver.get(url)
        wait_grid(driver)

        page = 1
        while page <= max_pages:
            print(f"— Página {page} —")
            wait_grid(driver)
            gentle_scroll(driver, rounds=SCROLL_ROUNDS)

            tiles = current_tiles(driver)
            print(f"   tiles={len(tiles)}")
            new_here = 0
            for t in tiles:
                try:
                    data = parse_tile(t, page)
                    key = data.get("product_id") or data.get("enlace")
                    if key and key not in seen:
                        seen.add(key)
                        rows.append(data)
                        new_here += 1
                except Exception:
                    continue
            print(f"   nuevos={new_here}")

            prev_cnt = len(tiles)
            moved = click_next_page(driver, page + 1)
            if not moved:
                print("   (No hay más paginación visible)")
                break
            if not wait_page_changed(driver, prev_cnt, timeout=WAIT):
                time.sleep(1.0)
            page += 1
            time.sleep(random.uniform(0.3, 0.7))
        return rows
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ─────────── Histórico + diffs ───────────
def _normalize_numeric(df: pd.DataFrame):
    for col in ("list_price", "sale_price", "discount_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

def _latest_previous_parquet(out_prefix: str, save_dir: Path) -> Path | None:
    patt = f"{out_prefix}_snapshot_*.parquet"
    cand = sorted(save_dir.glob(patt))
    return cand[-1] if cand else None

def _compute_diffs(prev_df: pd.DataFrame | None, curr_df: pd.DataFrame):
    key = "product_id" if "product_id" in curr_df.columns and curr_df["product_id"].notna().any() else "enlace"
    curr = curr_df.copy()
    if key not in curr.columns:
        curr[key] = curr["enlace"].astype("string")
    curr.set_index(key, inplace=True, drop=False)

    if prev_df is None or prev_df.empty:
        return key, pd.DataFrame(columns=curr.columns), curr.copy()

    prev = prev_df.copy()
    if key not in prev.columns:
        prev[key] = prev["enlace"].astype("string")
    prev.set_index(key, inplace=True, drop=False)

    new_idx = curr.index.difference(prev.index)
    removed_idx = prev.index.difference(curr.index)
    common = curr.index.intersection(prev.index)

    # changes: precio/lista/descuento distintos (tolerancia)
    tol = 0.01
    cols_to_check = [c for c in ("list_price", "sale_price", "discount_pct") if c in curr.columns and c in prev.columns]
    changed_idx = []
    for k in common:
        diffs = []
        for c in cols_to_check:
            a, b = curr.at[k, c], prev.at[k, c]
            try:
                if pd.isna(a) and pd.isna(b):
                    eq = True
                elif pd.isna(a) != pd.isna(b):
                    eq = False
                else:
                    eq = abs(float(a) - float(b)) <= tol
            except Exception:
                eq = (a == b)
            diffs.append(eq)
        if not all(diffs):
            changed_idx.append(k)

    changes = curr.loc[changed_idx].copy()
    new = curr.loc[new_idx].copy()
    removed = prev.loc[removed_idx].copy()
    return key, changes, new, removed

# ─────────── Alertas por marca (email) ───────────
def _load_alert_brands() -> set[str]:
    raw = os.environ.get("PALACIO_ALERT_BRANDS", "")
    if not raw:
        return set()
    parts = []
    # soporta CSV o multilínea
    for line in raw.splitlines():
        parts.extend([p.strip() for p in line.split(",")])
    return {p for p in (s.strip() for s in parts) if p}

def _filter_alert_hits_new(new_df: pd.DataFrame, min_disc: float, brands: set[str]) -> pd.DataFrame:
    if new_df is None or new_df.empty:
        return new_df
    df = new_df.copy()
    if "discount_pct" in df.columns:
        df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")
        df = df[df["discount_pct"].fillna(0) >= float(min_disc)]
    if "brand" in df.columns and brands:
        df = df[df["brand"].fillna("").isin(brands)]
    return df

def _filter_alert_hits_changes(chg_df: pd.DataFrame, min_disc: float, brands: set[str]) -> pd.DataFrame:
    if chg_df is None or chg_df.empty:
        return chg_df
    df = chg_df.copy()
    if "discount_pct" in df.columns:
        df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce")
        df = df[df["discount_pct"].fillna(0) >= float(min_disc)]
    if "brand" in df.columns and brands:
        df = df[df["brand"].fillna("").isin(brands)]
    return df

def _send_email(subject: str, body: str, attachments: list[tuple[str, bytes, str]] | None = None):
    host = os.environ.get("EMAIL_HOST", "")
    port = int(os.environ.get("EMAIL_PORT", "587"))
    user = os.environ.get("EMAIL_USER", "")
    pwd  = os.environ.get("EMAIL_PASS", "")
    to   = os.environ.get("EMAIL_TO", "")
    if not (host and port and user and pwd and to):
        dbg = _env_bool("EMAIL_DEBUG", False)
        if dbg:
            print("⚠️ EMAIL_* incompletos: no se envía correo.")
        return
    rec = [x.strip() for x in re.split(r"[;,]", to) if x.strip()]
    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = ",".join(rec)
    msg["Subject"] = subject
    msg.set_content(body)
    for (name, data, mime) in (attachments or []):
        mt, st = (mime.split("/", 1) if mime else ("application", "octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=name)
    with smtplib.SMTP(host, port) as s:
        s.ehlo(); s.starttls(); s.ehlo(); s.login(user, pwd)
        s.send_message(msg, from_addr=user, to_addrs=rec)
    print("📧 Alerta enviada:", subject)

# ─────────── XLSX ───────────
def save_xlsx(df: pd.DataFrame, out_prefix: str, save_dir: Path) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{out_prefix}_snapshot_{stamp}.xlsx"
    fpath = save_dir / fname
    with pd.ExcelWriter(fpath, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="SNAPSHOT")
        wb = writer.book; ws = writer.sheets["SNAPSHOT"]
        money = wb.add_format({"num_format": "#,##0.00"})
        pct   = wb.add_format({'num_format': '0.00"%"'})
        cols = list(df.columns)
        ws.set_column(0, len(cols)-1, 18)
        for c in ("list_price", "sale_price"):
            if c in cols:
                idx = cols.index(c); ws.set_column(idx, idx, 14, money)
        if "discount_pct" in cols:
            idx = cols.index("discount_pct"); ws.set_column(idx, idx, 12, pct)
        ws.autofilter(0, 0, len(df), len(cols)-1); ws.freeze_panes(1, 0)
    print(f"📄 Guardado: {fpath}")
    return str(fpath)

def save_parquet(df: pd.DataFrame, out_prefix: str, save_dir: Path) -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{out_prefix}_snapshot_{stamp}.parquet"
    fpath = save_dir / fname
    df.to_parquet(fpath, index=False)
    print(f"🧱 Parquet: {fpath}")
    return str(fpath)

# ─────────── Orquestación (una sola categoría/URL) ───────────
def run_one(url: str, out_prefix: str, headless: bool, max_pages: int, save_dir: Path):
    save_dir.mkdir(parents=True, exist_ok=True)

    rows = scrape_category(url=url, headless=headless, max_pages=max_pages)
    df = pd.DataFrame(rows)
    _normalize_numeric(df)
    if "product_id" in df.columns: df["product_id"] = df["product_id"].astype("string")
    if "sku" in df.columns: df["sku"] = df["sku"].astype("string")

    # leer previo
    prev_pq = _latest_previous_parquet(out_prefix, save_dir)
    prev_df = None
    if prev_pq and prev_pq.exists():
        try:
            prev_df = pd.read_parquet(prev_pq)
            _normalize_numeric(prev_df)
            for d in (df, prev_df):
                if "product_id" in d.columns: d["product_id"] = d["product_id"].astype("string")
                if "sku" in d.columns: d["sku"] = d["sku"].astype("string")
        except Exception as e:
            print(f"⚠️ No pude leer previo {prev_pq.name}: {e}")

    # diffs
    key, changes_df, new_df, removed_df = _compute_diffs(prev_df, df)

    # alertas por marca con descuento
    brands = _load_alert_brands()
    min_disc = float(os.environ.get("ALERT_MIN_DISC", "30"))
    hits_new = _filter_alert_hits_new(new_df, min_disc, brands)
    hits_changes = _filter_alert_hits_changes(changes_df, min_disc, brands)

    if (hits_new is not None and not hits_new.empty) or (hits_changes is not None and not hits_changes.empty):
        # armar adjuntos mini
        atts = []
        if hits_new is not None and not hits_new.empty:
            bio = io.BytesIO()
            hits_new.to_excel(bio, index=False)
            atts.append((f"{out_prefix}_ALERT_NEW.xlsx", bio.getvalue(),
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
        if hits_changes is not None and not hits_changes.empty:
            bio = io.BytesIO()
            hits_changes.to_excel(bio, index=False)
            atts.append((f"{out_prefix}_ALERT_CHANGES.xlsx", bio.getvalue(),
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))

        body = (f"Categoría/URL: {url}\n"
                f"Prefijo: {out_prefix}\n"
                f"Umbral descuento: {min_disc}%\n"
                f"Marcas filtradas: {', '.join(sorted(brands)) or '(todas)'}\n"
                f"NEW >= {min_disc}%: {0 if hits_new is None else len(hits_new)}\n"
                f"CHANGES >= {min_disc}%: {0 if hits_changes is None else len(hits_changes)}\n")
        _send_email(f"[Scraper] Alertas {out_prefix}", body, atts)

    # persistencia
    xlsx_path = save_xlsx(df, out_prefix, save_dir)
    pq_path   = save_parquet(df, out_prefix, save_dir)

    # resumen consola
    big51 = int(df.get("discount_pct", pd.Series(dtype=float)).fillna(0).ge(51).sum())
    print(f"🔢 Filas: {len(df)} | ≥51%: {big51}")
    print(f"Último previo: {prev_pq.name if prev_pq else '—'}")
    print(f"NEW={len(new_df)} CHANGES={len(changes_df)} REMOVED={len(removed_df)}")
    return df, xlsx_path, pq_path

# ─────────── CLI manual ───────────
CATEGORIES = {
    "mujer":  "https://www.elpalaciodehierro.com/mujer/",
    "hombre": "https://www.elpalaciodehierro.com/hombre/",
    "nina":   "https://www.elpalaciodehierro.com/nina/",
    "nino":   "https://www.elpalaciodehierro.com/nino/",
    "hogar":  "https://www.elpalaciodehierro.com/hogar/",
    "belleza":"https://www.elpalaciodehierro.com/belleza/",
    "ofertas":"https://www.elpalaciodehierro.com/outlet/"
}

def prompt_menu():
    print("Elige una categoría o escribe una URL completa:")
    keys = list(CATEGORIES.keys())
    for i, k in enumerate(keys, 1):
        print(f" {i}) {k} → {CATEGORIES[k]}")
    raw = input("Número/URL: ").strip()
    if raw.isdigit():
        ix = int(raw)
        if 1 <= ix <= len(keys):
            return keys[ix-1], CATEGORIES[keys[ix-1]]
        else:
            print("Opción inválida, usando 'hombre'.")
            return "hombre", CATEGORIES["hombre"]
    if raw.lower().startswith("http"):
        return "custom", raw
    print("Opción inválida, usando 'hombre'.")
    return "hombre", CATEGORIES["hombre"]

if __name__ == "__main__":
    save_dir = Path(os.environ.get("SAVE_DIR", "/tmp/palacio_out"))

    cat_key, url = prompt_menu()
    headless = _env_bool("HEADLESS", True)
    try:
        max_pages = int(os.environ.get("MAX_PAGES", "50"))
    except Exception:
        max_pages = 50
    out_prefix = os.environ.get("OUT_PREFIX", f"palacio_{cat_key}")

    # permite override por prompt
    hp = input(f"Headless? [Y/n] (actual={headless}): ").strip().lower()
    if hp in ("n", "no", "0"): headless = False
    mp = input(f"Max pages? (actual={max_pages}): ").strip()
    if mp.isdigit(): max_pages = int(mp)
    op = input(f"Prefijo salida? (actual={out_prefix}): ").strip()
    if op: out_prefix = op

    print(f"\n>>> RUN <<<\n  cat/url: {url}\n  headless={headless}\n  max_pages={max_pages}\n  prefix={out_prefix}\n  SAVE_DIR={save_dir}\n")
    run_one(url=url, out_prefix=out_prefix, headless=headless, max_pages=max_pages, save_dir=save_dir)
