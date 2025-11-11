# palacio_manual_runner_selenium.py
# Runner manual con Selenium (nuevo layout de Palacio):
# - Scrapea con Selenium (paginación robusta + descuento robusto)
# - Mantiene: categorías, SAVE_DIR, XLSX + PARQUET, histórico (NEW/CHANGES/REMOVED),
#             alertas por marca (solo NEW/CHANGES con descuento).
# Requisitos: pip install selenium webdriver-manager pandas xlsxwriter pyarrow

import os, re, io, math, time, random, argparse, glob, smtplib
from pathlib import Path
from datetime import datetime, timezone
from email.message import EmailMessage

import pandas as pd

# ───────────────── Email / env ─────────────────
SAVE_DIR = Path(os.getenv("SAVE_DIR", "/tmp/palacio_out")); SAVE_DIR.mkdir(parents=True, exist_ok=True)

EMAIL_HOST  = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT  = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER  = os.getenv("EMAIL_USER", "")
EMAIL_PASS  = os.getenv("EMAIL_PASS", "")
EMAIL_TO    = os.getenv("EMAIL_TO", "")
EMAIL_DEBUG = os.getenv("EMAIL_DEBUG", "0") not in ("", "0", "false", "False")

def _split_list(s: str):
    return [x.strip() for x in re.split(r"[;,]", s or "") if x and x.strip()]

EMAIL_TO_LIST = _split_list(EMAIL_TO)

ALERT_BRANDS = [b.lower() for b in _split_list(os.getenv("ALERT_BRANDS",""))]
ALERT_BRANDS = list(dict.fromkeys(ALERT_BRANDS))

def send_email(subject: str, body: str, to_addr, attachments=None):
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO_LIST:
        print("⚠️ EMAIL_* incompletos: no envío.")
        return
    rec = EMAIL_TO_LIST if not isinstance(to_addr, str) else _split_list(to_addr)
    msg = EmailMessage()
    msg["From"] = EMAIL_USER; msg["To"] = ", ".join(rec); msg["Subject"] = subject
    msg.set_content(body)
    for (fname, data, mime) in (attachments or []):
        mt, st = (mime.split("/",1) if mime else ("application","octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as s:
        if EMAIL_DEBUG: s.set_debuglevel(1)
        s.ehlo(); s.starttls(); s.ehlo(); s.login(EMAIL_USER, EMAIL_PASS)
        s.send_message(msg, from_addr=EMAIL_USER, to_addrs=rec)
    print(f"📧 Enviado: {subject}")

# ───────────────── Categorías ─────────────────
CATEGORIES = {
    "ofertas": {"base_url": "https://www.elpalaciodehierro.com/ofertas/", "prefix": "palacio_ofertas"},
    "electronica": {"base_url": "https://www.elpalaciodehierro.com/electronica/", "prefix": "palacio_electronica"},
    "deportes": {"base_url": "https://www.elpalaciodehierro.com/deportes/", "prefix": "palacio_deportes"},
    "gourmet": {"base_url": "https://www.elpalaciodehierro.com/gourmet/", "prefix": "palacio_gourmet"},
    "nuevos-productos": {"base_url": "https://www.elpalaciodehierro.com/nuevos-productos/", "prefix": "palacio_nuevos_productos"},
    "mujer": {"base_url": "https://www.elpalaciodehierro.com/mujer/", "prefix": "palacio_mujer"},
    "productos-liquidacion": {"base_url": "https://www.elpalaciodehierro.com/productos-liquidacion/", "prefix": "palacio_productos_liquidacion"},
    "hombre": {"base_url": "https://www.elpalaciodehierro.com/hombre/", "prefix": "palacio_hombre"},
    "calzado": {"base_url": "https://www.elpalaciodehierro.com/calzado/", "prefix": "palacio_calzado"},
    "hogar": {"base_url": "https://www.elpalaciodehierro.com/hogar/", "prefix": "palacio_hogar"},
    "juguetes": {"base_url": "https://www.elpalaciodehierro.com/juguetes/", "prefix": "palacio_juguetes"},
    "categorias": {"base_url": "https://www.elpalaciodehierro.com/categorias/", "prefix": "palacio_categorias"},
    "tendencias": {"base_url": "https://www.elpalaciodehierro.com/tendencias/", "prefix": "palacio_tendencias"},
    "más vendido": {"base_url": "https://www.elpalaciodehierro.com/lo-mas-vendido/", "prefix": "palacio_vendido"},
}

# ───────────────── Selenium (nuevo sitio) ─────────────────
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager

WAIT = 25
SCROLL_STEP = 700
SCROLL_ROUNDS = 2
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]
LIST_SELECTORS = [
    "article.b-product_tile_item",
    "div.b-product_tile[data-component='search/ProductTile']",
    "div.b-product",
    "li.product", "div.product-tile"
]
_money_clean = re.compile(r"[^\d.,]")
def parse_price(txt):
    if not txt: return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try: return float(s)
    except ValueError: return None

def setup_driver(headless=True):
    co = ChromeOptions()
    if headless: co.add_argument("--headless=new")
    co.add_argument("--disable-gpu"); co.add_argument("--window-size=1366,900")
    co.add_argument("--disable-dev-shm-usage"); co.add_argument("--no-sandbox")
    co.add_argument("--lang=es-MX"); co.add_argument("--disable-blink-features=AutomationControlled")
    co.add_argument(f"--user-agent={random.choice(UA_LIST)}")
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
        driver.execute_script(f"window.scrollBy(0, {SCROLL_STEP});"); time.sleep(0.25)
        driver.execute_script("window.scrollBy(0, -200);"); time.sleep(0.25)

def _robust_click(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el); time.sleep(0.05); el.click(); return True
    except (ElementClickInterceptedException, StaleElementReferenceException):
        try:
            ActionChains(driver).move_to_element(el).pause(0.05).click().perform(); return True
        except Exception:
            try: driver.execute_script("arguments[0].click();", el); return True
            except Exception: return False

def click_page(driver, next_page_number):
    try:
        sel = f"a[data-js-pagination-link][data-page-number='{next_page_number}']"
        link = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
        if _robust_click(driver, link): return True
    except TimeoutException:
        pass
    try:
        for a in driver.find_elements(By.CSS_SELECTOR, "a[data-js-pagination-link]"):
            if "siguiente" in (a.text or "").strip().lower():
                if _robust_click(driver, a): return True
    except Exception: pass
    try:
        icon = driver.find_element(By.CSS_SELECTOR, "i.i-arrow-right-after")
        parent_link = icon.find_element(By.XPATH, "./ancestor::a[1]")
        if parent_link and _robust_click(driver, parent_link): return True
    except Exception: pass
    for sel in [".b-pagination-elements_list .b-next-btn a",".b-pagination-elements_next a",
                "li.b-pagination-elements_list.b-next-btn a","a.b-pagination-elements_number[aria-label*='Siguiente']"]:
        try:
            a = driver.find_element(By.CSS_SELECTOR, sel)
            if a.is_displayed() and a.is_enabled() and _robust_click(driver, a): return True
        except Exception: continue
    try:
        a = driver.find_element(By.CSS_SELECTOR, "a[rel='next']")
        if _robust_click(driver, a): return True
    except Exception: pass
    return False

def wait_page_changed(driver, prev_count, timeout=WAIT):
    start = time.time(); prev_url = driver.current_url
    css = ", ".join(LIST_SELECTORS)
    while time.time() - start < timeout:
        time.sleep(0.25)
        if driver.current_url != prev_url: return True
        try:
            curr = len(driver.find_elements(By.CSS_SELECTOR, css))
            if curr != prev_count and curr > 0: return True
        except Exception: pass
    return False

def parse_tile(driver, el, page_idx):
    href = None
    try:
        a = el.find_element(By.CSS_SELECTOR, "a[href]"); href = a.get_attribute("href")
    except Exception: pass

    name = brand = None
    try:
        name_el = el.find_element(By.CSS_SELECTOR, ".b-product_tile-name h4, .b-product_tile-title, .b-product_tile-name, h3.b-product_tile-name")
        name = name_el.text.strip()
    except Exception: pass
    try:
        brand_el = el.find_element(By.CSS_SELECTOR, ".b-product_tile-brand h4, .b-product_tile-brand")
        brand = brand_el.text.strip()
    except Exception: pass

    list_p = sale_p = None; values = []
    price_block = None
    for sel in [".b-product_tile-price",".b-product_price",".b-product_tile .b-product_price",".product-pricing"]:
        try: price_block = el.find_element(By.CSS_SELECTOR, sel); break
        except Exception: continue
    if price_block:
        spans = price_block.find_elements(By.CSS_SELECTOR, ".b-product_price-value")
        for sp in spans:
            v = parse_price(sp.get_attribute("content") or sp.text)
            if v is not None: values.append(v)
            try:
                if sp.find_element(By.XPATH, "./ancestor::*[contains(@class,'b-product_price-old')][1]"): 
                    list_p = v if v is not None else list_p
            except Exception: pass
            try:
                if sp.find_element(By.XPATH, "./ancestor::*[contains(@class,'b-product_price-sales')][1]"): 
                    sale_p = v if v is not None else sale_p
            except Exception: pass
    if (list_p is None or sale_p is None) and len(values) >= 2:
        mx, mn = max(values), min(values)
        if list_p is None: list_p = mx
        if sale_p is None: sale_p = mn
    if list_p is None and sale_p is None and len(values) == 1:
        list_p = sale_p = values[0]

    img = None
    for sel in ["img[data-js-product-image]","img.b-product_image","picture img","img"]:
        try:
            i = el.find_element(By.CSS_SELECTOR, sel)
            src = i.get_attribute("src") or i.get_attribute("data-src")
            if src and src.startswith("http"): img = src; break
        except Exception: continue

    pid = None
    if href:
        m = re.search(r"/(\d{5,})", href)
        if m: pid = m.group(1)

    discount = None
    if list_p and sale_p and sale_p < list_p:
        discount = round((1 - sale_p / list_p) * 100, 2)

    return {
        "product_id": pid, "sku": pid,
        "name": name, "brand": brand,
        "price_currency": "MXN",
        "list_price": list_p, "sale_price": sale_p, "discount_pct": discount,
        "image_url": img, "enlace": href, "page_idx": page_idx,
        "captured_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
    }

def scrape_category(url: str, max_pages: int = 50, headless: bool = True):
    driver = setup_driver(headless=headless)
    rows, seen = [], set()
    try:
        print(f"Abriendo: {url}"); driver.get(url); wait_grid(driver)
        page = 1
        while page <= max_pages:
            print(f"— Página {page} —"); wait_grid(driver); gentle_scroll(driver)
            tiles = current_tiles(driver); print(f"   tiles={len(tiles)}")
            new_here = 0
            for t in tiles:
                try:
                    data = parse_tile(driver, t, page)
                    key = data.get("product_id") or data.get("enlace")
                    if key and key not in seen:
                        seen.add(key); rows.append(data); new_here += 1
                except Exception: continue
            print(f"   nuevos={new_here}")
            prev_count = len(tiles)
            moved = click_page(driver, page + 1)
            if not moved: print("   (No hay más paginación)"); break
            if not wait_page_changed(driver, prev_count, timeout=WAIT): time.sleep(1.0)
            page += 1; time.sleep(random.uniform(0.3, 0.7))
        return rows
    finally:
        try: driver.quit()
        except Exception: pass

# ───────────────── Histórico / diffs / excel ─────────────────
HIGHLIGHT_BANDS = [(70,"#FFCDD2"), (60,"#FFE0B2"), (50,"#FFF59D"), (30,"#DCEDC8")]
COLUMNS_EXPORT = ["product_id","sku","name","brand","price_currency","list_price","sale_price","discount_pct",
                  "image_url","enlace","page_idx","captured_at"]

def _latest_previous_parquet(out_prefix: str, folder: Path) -> Path | None:
    files = sorted(glob.glob(str(folder / f"{out_prefix}_snapshot_*.parquet")))
    return Path(files[-1]) if files else None

def _normalize_numeric(df: pd.DataFrame, cols=("list_price","sale_price","discount_pct")):
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

def _changes_merge(prev_df: pd.DataFrame, cur_df: pd.DataFrame):
    if prev_df is None or prev_df.empty:
        return "product_id", pd.DataFrame(), cur_df.copy(), pd.DataFrame()
    use_pid = (cur_df["product_id"].notna().sum() > 0) and (prev_df["product_id"].notna().sum() > 0)
    key = "product_id" if use_pid else "sku"
    merged = prev_df.merge(cur_df, on=key, suffixes=("_old","_new"), how="outer", indicator=True)

    def changed_num(a,b,atol=0.01):
        if pd.isna(a) or pd.isna(b): return False
        try: return not math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=atol)
        except Exception: return a != b

    both = merged[merged["_merge"]=="both"].copy()
    mask = (
        both.apply(lambda r: changed_num(r.get("list_price_old"), r.get("list_price_new")), axis=1) |
        both.apply(lambda r: changed_num(r.get("sale_price_old"), r.get("sale_price_new")), axis=1) |
        both.apply(lambda r: changed_num(r.get("discount_pct_old"), r.get("discount_pct_new")), axis=1) |
        (both["sale_price_old"].isna() ^ both["sale_price_new"].isna())
    )
    changes = both.loc[mask].copy()
    new_items = merged[merged["_merge"]=="right_only"].copy()
    removed_items = merged[merged["_merge"]=="left_only"].copy()
    if not new_items.empty:
        keep = [c for c in new_items.columns if c.endswith("_new") or c == key]
        new_items = new_items[keep].rename(columns=lambda c: c.replace("_new",""))
    if not removed_items.empty:
        keep = [c for c in removed_items.columns if c.endswith("_old") or c == key]
        removed_items = removed_items[keep].rename(columns=lambda c: c.replace("_old",""))
    return key, changes, new_items, removed_items

def build_xlsx_bytes(df: pd.DataFrame, prev_df: pd.DataFrame|None, out_prefix: str) -> bytes:
    if prev_df is not None and not prev_df.empty and not df.empty:
        key, changes, new_items, removed_items = _changes_merge(prev_df, df)
    else:
        changes = pd.DataFrame({"info":["Sin cambios de precio (primer snapshot o vacío)"]})
        new_items = df.copy(); removed_items = pd.DataFrame(columns=df.columns)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="SNAPSHOT")
        (changes if "info" not in changes.columns else changes).to_excel(w, index=False, sheet_name="CHANGES")
        (new_items if not new_items.empty else pd.DataFrame({"info":["Sin nuevos productos"]})).to_excel(w, index=False, sheet_name="NEW")
        (removed_items if not removed_items.empty else pd.DataFrame({"info":["Sin productos removidos"]})).to_excel(w, index=False, sheet_name="REMOVED")
        wb = w.book; money = wb.add_format({"num_format":"#,##0.00"}); pct=wb.add_format({'num_format':'0.00"%"'}); link=wb.add_format({"font_color":"blue","underline":1})
        ws = w.sheets["SNAPSHOT"]; cols=list(df.columns); ws.set_column(0,len(cols)-1,18)
        if "list_price" in cols: ws.set_column(cols.index("list_price"), cols.index("list_price"), 14, money)
        if "sale_price" in cols: ws.set_column(cols.index("sale_price"), cols.index("sale_price"), 14, money)
        if "discount_pct" in cols: ws.set_column(cols.index("discount_pct"), cols.index("discount_pct"), 12, pct)
        if "enlace" in cols:
            ei = cols.index("enlace")
            for r, val in enumerate(df.get("enlace", pd.Series()).fillna(""), start=2):
                if isinstance(val,str) and val.startswith("http"):
                    ws.write_url(r-1, ei, val, link, string=val)
        ws.autofilter(0,0,len(df),len(cols)-1); ws.freeze_panes(1,0)
        if "discount_pct" in cols and not df.empty:
            last=len(df)+1; col=chr(65+cols.index("discount_pct"))
            for thr, hexc in HIGHLIGHT_BANDS:
                ws.conditional_format(1,0,last,len(cols)-1,{"type":"formula","criteria":f"=${col}2>={thr}","format":wb.add_format({"bg_color":hexc}),"stop_if_true":True})
    buf.seek(0); return buf.read()

# ───────────────── Alertas de marca ─────────────────
def _brand_hit(txt: str) -> bool:
    if not ALERT_BRANDS: return False
    s = (txt or "").lower()
    return any(b in s for b in ALERT_BRANDS)

def _filter_hits_new(df_new: pd.DataFrame) -> pd.DataFrame:
    if df_new is None or df_new.empty: return df_new
    f = (df_new.get("discount_pct", pd.Series(dtype=float)).fillna(0) > 0)
    in_brand = df_new.apply(lambda r: _brand_hit(str(r.get("brand") or "")) or _brand_hit(str(r.get("name") or "")), axis=1)
    return df_new.loc[f & in_brand].copy()

def _filter_hits_changes(df_ch: pd.DataFrame) -> pd.DataFrame:
    if df_ch is None or df_ch.empty: return df_ch
    f = (pd.to_numeric(df_ch.get("discount_pct_new"), errors="coerce").fillna(0) > 0)
    in_brand = df_ch.apply(lambda r: _brand_hit(str(r.get("brand_new") or r.get("brand_old") or "")) or
                                   _brand_hit(str(r.get("name_new") or r.get("name_old") or "")), axis=1)
    return df_ch.loc[f & in_brand].copy()

def _send_brand_alerts(cat_key: str, new_hits: pd.DataFrame, chg_hits: pd.DataFrame):
    if (new_hits is None or new_hits.empty) and (chg_hits is None or chg_hits.empty): return
    parts=[f"Categoría: {cat_key}"]
    if new_hits is not None and not new_hits.empty: parts.append(f"NEW con descuento (marcas): {len(new_hits)}")
    if chg_hits is not None and not chg_hits.empty: parts.append(f"CHANGES con descuento (marcas): {len(chg_hits)}")
    body="\n".join(parts); atts=[]
    if new_hits is not None and not new_hits.empty: atts.append((f"alerts_{cat_key}_NEW.csv", new_hits.to_csv(index=False).encode("utf-8-sig"), "text/csv"))
    if chg_hits is not None and not chg_hits.empty: atts.append((f"alerts_{cat_key}_CHANGES.csv", chg_hits.to_csv(index=False).encode("utf-8-sig"), "text/csv"))
    send_email(f"[Alert] NEW/CHANGES · {cat_key}", body, EMAIL_TO_LIST, attachments=atts)

# ───────────────── Runner ─────────────────
def run_single_category(cat_key: str, max_pages: int = 50, headless: bool = True):
    cfg = CATEGORIES[cat_key]; url = cfg["base_url"]; out_prefix = cfg["prefix"]
    print(f"\n=== {cat_key} ===\nURL: {url}\nmax_pages={max_pages} headless={headless}")
    rows = scrape_category(url=url, max_pages=max_pages, headless=headless)
    df = pd.DataFrame(rows, columns=COLUMNS_EXPORT); _normalize_numeric(df)

    # cargar previo
    prev_df=None; prev_pq=_latest_previous_parquet(out_prefix, SAVE_DIR)
    if prev_pq and prev_pq.exists():
        try:
            prev_df=pd.read_parquet(prev_pq); _normalize_numeric(prev_df)
            for d in (df, prev_df):
                if "product_id" in d.columns: d["product_id"]=d["product_id"].astype("string")
                if "sku" in d.columns: d["sku"]=d["sku"].astype("string")
        except Exception as e:
            print(f"⚠️ No pude leer previo {prev_pq.name}: {e}")

    # alertas
    _key, changes_df, new_df, removed_df = _changes_merge(prev_df, df)
    if not new_df.empty and "category" not in new_df.columns: new_df.insert(0,"category",cat_key)
    if not changes_df.empty and "category" not in changes_df.columns: changes_df.insert(0,"category",cat_key)
    _send_brand_alerts(cat_key, _filter_hits_new(new_df), _filter_hits_changes(changes_df))

    # guardar
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_bytes=build_xlsx_bytes(df, prev_df, out_prefix)
    xlsx_name=f"{out_prefix}_snapshot_{stamp}.xlsx"; pq_name=f"{out_prefix}_snapshot_{stamp}.parquet"
    (SAVE_DIR / xlsx_name).write_bytes(xlsx_bytes); df.to_parquet(SAVE_DIR / pq_name, index=False)
    big51=int((df.get("discount_pct", pd.Series(dtype=float)).fillna(0) >= 51).sum())
    print(f"📄 Guardado: {xlsx_name} | 🔢 Filas: {len(df)} | ≥51%: {big51}")

def pick_category_interactively():
    keys=list(CATEGORIES.keys()); print("Elige categoría:")
    for i,k in enumerate(keys, start=1): print(f"  {i}) {k}  →  {CATEGORIES[k]['base_url']}")
    try:
        idx=int(input(f"Número (1..{len(keys)}): ").strip())
        if 1 <= idx <= len(keys): return keys[idx-1]
    except Exception: pass
    print("Opción inválida; usaré 'deportes'."); return "deportes"

def parse_args():
    p=argparse.ArgumentParser(description="Runner manual Selenium (nuevo sitio) con histórico y alertas por marca.")
    p.add_argument("-c","--category", choices=CATEGORIES.keys(), help="Categoría a correr")
    p.add_argument("--max-pages", type=int, default=50, help="Límite de páginas a recorrer")
    p.add_argument("--no-headless", action="store_true", help="Muestra navegador")
    return p.parse_args()

def main():
    args=parse_args()
    cat=args.category or pick_category_interactively()
    headless=not args.no_headless
    run_single_category(cat_key=cat, max_pages=args.max_pages, headless=headless)

if __name__ == "__main__":
    main()
