# palacio_category_snapshot_ramonly.py
# Scraper Palacio → ahora: histórico real con NEW/CHANGES/REMOVED contra el snapshot previo por categoría.
# - Guarda XLSX + PARQUET en SAVE_DIR (env), adjunta XLSX por correo.
# - Si hay PARQUET previo en SAVE_DIR que matchea el prefijo, calcula diferencias.
# - "marcas": sigue omitiendo image_url si la columna existe.
# - Tolerancia para cambios numéricos: 1 centavo.

import os, re, io, math, json, time, random, argparse, smtplib, html, glob
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin
from email.message import EmailMessage

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────────────── Config email ─────────────────
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO   = os.getenv("EMAIL_TO", "")
EMAIL_DEBUG = os.getenv("EMAIL_DEBUG", "0") not in ("", "0", "false", "False")
EMAIL_TO_LIST = [e.strip() for e in re.split(r"[;,]", EMAIL_TO) if e.strip()]

def send_email(subject: str, body: str, to_addr, attachments=None):
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO_LIST:
        print("⚠️ EMAIL_* incompletos: no se envía correo.")
        return
    recipients = EMAIL_TO_LIST if not isinstance(to_addr, str) else [e.strip() for e in re.split(r"[;,]", to_addr) if e.strip()]
    recipients = list(dict.fromkeys(recipients))
    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)
    for (fname, data, mime) in (attachments or []):
        mt, st = (mime.split("/",1) if mime else ("application","octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)
    import smtplib
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as smtp:
        if EMAIL_DEBUG: smtp.set_debuglevel(1)
        smtp.ehlo(); smtp.starttls(); smtp.ehlo(); smtp.login(EMAIL_USER, EMAIL_PASS)
        refused = smtp.send_message(msg, from_addr=EMAIL_USER, to_addrs=recipients)
    if refused: print("⚠️ Rechazados:", refused)
    print(f"📧 Email enviado a {', '.join(recipients)}: {subject}")

# ──────────────── Config salida ────────────────
SAVE_DIR = Path(os.getenv("SAVE_DIR", "/tmp/palacio_out"))
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# ──────────────── Categorías ────────────────
CATEGORIES = {
    "ofertas": {"base_url": "https://www.elpalaciodehierro.com/ofertas/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 200, "prefix": "palacio_ofertas"},
    "electronica": {"base_url": "https://www.elpalaciodehierro.com/electronica/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 400, "prefix": "palacio_electronica"},
    "deportes": {"base_url": "https://www.elpalaciodehierro.com/deportes/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_deportes"},
    "gourmet": {"base_url": "https://www.elpalaciodehierro.com/gourmet/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_gourmet"},
    "nuevos-productos": {"base_url": "https://www.elpalaciodehierro.com/nuevos-productos/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_nuevos_productos"},
    "mujer": {"base_url": "https://www.elpalaciodehierro.com/mujer/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 1000, "prefix": "palacio_mujer"},
    "productos-liquidacion": {"base_url": "https://www.elpalaciodehierro.com/productos-liquidacion/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 80, "prefix": "palacio_productos_liquidacion"},
    "hombre": {"base_url": "https://www.elpalaciodehierro.com/hombre/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_hombre"},
    "calzado": {"base_url": "https://www.elpalaciodehierro.com/calzado/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_calzado"},
    "hogar": {"base_url": "https://www.elpalaciodehierro.com/hogar/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_hogar"},
    "juguetes": {"base_url": "https://www.elpalaciodehierro.com/juguetes/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_juguetes"},
    "categorias": {"base_url": "https://www.elpalaciodehierro.com/categorias/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_categorias"},
    "tendencias": {"base_url": "https://www.elpalaciodehierro.com/tendencias/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_tendencias"},
    "más vendido": {"base_url": "https://www.elpalaciodehierro.com/lo-mas-vendido/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_vendido"},
}

# ──────────────── Red/tiempos ────────────────
CONNECT_TIMEOUT = 20
READ_TIMEOUT    = 180
JITTER_MIN, JITTER_MAX = 0.08, 0.25
LONG_PAUSE_EVERY = (12, 18)
LONG_PAUSE_RANGE = (1.5, 4.0)
STOP_AFTER_EMPTY = 1
HIGHLIGHT_DISCOUNT = 51

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]
def random_headers():
    return {
        "user-agent": random.choice(UA_LIST),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": random.choice(["es-MX,es;q=0.9,en;q=0.8","es-ES,es;q=0.9,en;q=0.6","en-US,en;q=0.9"]),
        "cache-control": "no-cache",
    }

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=6, connect=3, read=3, backoff_factor=1.2,
                  status_forcelist=[429,500,502,503,504,520,522,523,524],
                  allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s

def fetch_page(session: requests.Session, base_url: str, start: int, page_size: int):
    params = {"start": start, "sz": page_size}
    time.sleep(random.uniform(0.05, 0.20))
    headers = random_headers()
    resp = session.get(base_url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        try: wait_s = float(resp.headers["Retry-After"])
        except Exception: wait_s = 2.0
        print(f"⏳ 429 Retry-After {wait_s}s…"); time.sleep(wait_s)
        resp = session.get(base_url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code in (520,522,523,524):
        print(f"↻ CF {resp.status_code} start={start}, sz={page_size}. Reintentando…")
        time.sleep(random.uniform(1.0, 2.5))
        resp = session.get(base_url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return resp.text, resp.url

def _fetch_with_fallback(session, base_url, start, page_size):
    try_sizes = [page_size] if page_size else []
    for sz in (200,120,80):
        if sz not in try_sizes: try_sizes.append(sz)
    last_err = None
    for sz_try in try_sizes or [200,120,80]:
        try:
            html_text, real_url = fetch_page(session, base_url, start, sz_try)
            if page_size and sz_try != page_size:
                print(f"↩︎ Recuperado start={start} con sz={sz_try} (falló {page_size})")
            return html_text, real_url, sz_try
        except requests.HTTPError as e:
            last_err = e
            if e.response is not None and e.response.status_code in (520,522,523,524):
                time.sleep(random.uniform(1.0, 2.0)); continue
            else: raise
    raise last_err if last_err else RuntimeError("Fallo de red sin respuesta HTTP")

# ──────────────── Parse helpers ────────────────
_money_clean = re.compile(r"[^\d.,]")
def parse_price(txt):
    if not txt: return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try: return float(s)
    except ValueError: return None

def text_or_none(el): return el.get_text(" ", strip=True) if el else None

def nearest_b_product(node):
    cur = node
    for _ in range(8):
        if cur is None: break
        classes = cur.get("class") or []
        if "b-product" in classes: return cur
        cur = cur.parent
    return node

def extract_from_analytics(bprod):
    out = {}
    if not bprod: return out
    da = bprod.get("data-analytics")
    if da:
        try:
            data = json.loads(html.unescape(da))
            prod = data.get("product", {}) if isinstance(data, dict) else {}
            out["product_id_analytics"] = str(prod.get("id")) if prod.get("id") is not None else None
            out["name_analytics"]       = prod.get("name")
            out["brand_analytics"]      = prod.get("brand")
            out["category_analytics"]   = prod.get("category")
            out["department_analytics"] = prod.get("departmentName")
            out["price_analytics"]      = prod.get("price")
            out["currency_analytics"]   = prod.get("priceCurrency") or "MXN"
            out["availability_analytics"]= prod.get("availability")
        except Exception: pass
    for k in ["data-pid","data-cnstrc-item-id","data-cnstrc-item-name"]:
        if bprod.get(k): out[k] = bprod.get(k)
    return out

def parse_products_from_html(html_text, page_url, page_start, page_idx, captured_at_iso):
    soup = BeautifulSoup(html_text, "html.parser")
    tiles = soup.select("article.b-product_tile_item, div.b-product, li.product, div.product-tile")
    rows = []
    for t in tiles:
        bprod = nearest_b_product(t)
        info = extract_from_analytics(bprod)

        meta_product_id = (bprod.select_one("meta[itemprop='productID']")["content"].strip()
                           if bprod and bprod.select_one("meta[itemprop='productID']") else None)
        meta_sku = (bprod.select_one("meta[itemprop='sku']")["content"].strip()
                    if bprod and bprod.select_one("meta[itemprop='sku']") else None)

        product_id = (meta_product_id or info.get("data-pid") or info.get("data-cnstrc-item-id") or info.get("product_id_analytics"))
        sku = meta_sku or product_id

        meta_name = (bprod.select_one("meta[itemprop='name']")["content"].strip()
                     if bprod and bprod.select_one("meta[itemprop='name']") else None)
        name = meta_name or info.get("data-cnstrc-item-name") or info.get("name_analytics")
        if not name:
            brand_vis = text_or_none(t.select_one(".b-product_tile-brand"))
            title_el  = t.select_one(".b-product_tile-name, .b-product_tile-title a, .b-product_tile-title, a.b-product_tile-title-link")
            title_vis = text_or_none(title_el)
            name = " ".join(x for x in [brand_vis, title_vis] if x) or None

        brand = (bprod.get("data-brand") if bprod and bprod.get("data-brand") else None) \
                or info.get("brand_analytics") \
                or text_or_none(t.select_one(".b-product_tile-brand h4"))
        category   = info.get("category_analytics")
        department = info.get("department_analytics")

        a = t.select_one("a[href]")
        enlace = urljoin(page_url, a["href"]) if a and a.has_attr("href") else None

        image_meta = bprod.select_one("meta[itemprop='image']") if bprod else None
        image_url = image_meta["content"].strip() if image_meta and image_meta.get("content") else None

        currency_meta = (bprod.select_one("meta[itemprop='priceCurrency']")["content"].strip()
                         if bprod and bprod.select_one("meta[itemprop='priceCurrency']") else None)
        availability_meta = (bprod.select_one("meta[itemprop='availability']")["content"].strip()
                             if bprod and bprod.select_one("meta[itemprop='availability']") else None)
        price_currency = currency_meta or info.get("currency_analytics") or "MXN"
        availability   = availability_meta or info.get("availability_analytics")

        list_span = t.select_one("div.b-product_price-old span.b-product_price-value")
        sale_span = t.select_one("div.b-product_price-sales.m-reduced span.b-product_price-value") \
                  or t.select_one("div.b-product_price-sales span.b-product_price-value")
        def _num(el):
            if not el: return None
            txt = el.get("content") or el.text
            return parse_price(txt) if txt else None
        list_price = _num(list_span)
        sale_price = _num(sale_span)

        discount_pct = None
        if list_price is not None and sale_price is not None and sale_price < list_price:
            discount_pct = round((1 - sale_price / list_price) * 100, 2)

        rows.append({
            "product_id": str(product_id) if product_id is not None else None,
            "sku": str(sku) if sku is not None else None,
            "name": name,
            "brand": brand,
            "category": category,
            "department": department,
            "price_currency": price_currency,
            "list_price": list_price,
            "sale_price": sale_price,
            "discount_pct": discount_pct,
            "availability": availability,
            "image_url": image_url,
            "enlace": enlace,
            "page_start": page_start,
            "page_idx": page_idx,
            "captured_at": captured_at_iso,
        })
    return rows, len(tiles)

# ──────────────── Diferencias + Excel ────────────────
HIGHLIGHT_BANDS = [(70,"#FFCDD2"), (60,"#FFE0B2"), (50,"#FFF59D"), (30,"#DCEDC8")]

def _latest_previous_parquet(out_prefix: str, folder: Path) -> Path | None:
    files = sorted(glob.glob(str(folder / f"{out_prefix}_snapshot_*.parquet")))
    return Path(files[-1]) if files else None

def _normalize_numeric(df: pd.DataFrame, cols=("list_price","sale_price","discount_pct")):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _changes_merge(prev_df: pd.DataFrame, cur_df: pd.DataFrame):
    # decidir clave
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
        keep_cols = [c for c in new_items.columns if c.endswith("_new") or c == key]
        new_items = new_items[keep_cols].rename(columns=lambda c: c.replace("_new",""))
    if not removed_items.empty:
        keep_cols = [c for c in removed_items.columns if c.endswith("_old") or c == key]
        removed_items = removed_items[keep_cols].rename(columns=lambda c: c.replace("_old",""))

    return key, changes, new_items, removed_items

def build_xlsx_bytes(df: pd.DataFrame, prev_df: pd.DataFrame|None, out_prefix: str) -> bytes:
    key = "product_id"
    if prev_df is not None and not prev_df.empty and not df.empty:
        key, changes, new_items, removed_items = _changes_merge(prev_df, df)
    else:
        changes = pd.DataFrame({ "info": ["Sin cambios de precio (primer snapshot o vacío)"] })
        new_items = df.copy()
        removed_items = pd.DataFrame(columns=df.columns)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="SNAPSHOT")
        (changes if "info" not in changes.columns else changes).to_excel(writer, index=False, sheet_name="CHANGES")
        (new_items if not new_items.empty else pd.DataFrame({"info":["Sin nuevos productos"]})).to_excel(writer, index=False, sheet_name="NEW")
        (removed_items if not removed_items.empty else pd.DataFrame({"info":["Sin productos removidos"]})).to_excel(writer, index=False, sheet_name="REMOVED")

        wb = writer.book
        money  = wb.add_format({"num_format": "#,##0.00"})
        pctfmt = wb.add_format({'num_format': '0.00"%"'})
        link   = wb.add_format({"font_color": "blue", "underline": 1})

        def fmt_snapshot(ws, df_ref):
            cols = list(df_ref.columns)
            ws.set_column(0, len(cols)-1, 18)
            for nm in ("list_price","sale_price"):
                if nm in cols:
                    i = cols.index(nm); ws.set_column(i, i, 14, money)
            if "discount_pct" in cols:
                di = cols.index("discount_pct"); ws.set_column(di, di, 12, pctfmt)
            if "enlace" in cols:
                ei = cols.index("enlace")
                for r, val in enumerate(df_ref.get("enlace", pd.Series()).fillna(""), start=2):
                    if isinstance(val, str) and val.startswith("http"):
                        ws.write_url(r-1, ei, val, link, string=val)
            ws.autofilter(0, 0, len(df_ref), len(cols)-1)
            ws.freeze_panes(1, 0)
            if "discount_pct" in cols and not df_ref.empty:
                last_row = len(df_ref) + 1
                col_letter = chr(65 + cols.index("discount_pct"))
                for thresh, hexcolor in HIGHLIGHT_BANDS:
                    fmt = wb.add_format({"bg_color": hexcolor})
                    ws.conditional_format(1, 0, last_row, len(cols)-1,
                        {"type":"formula","criteria":f"=${col_letter}2>={thresh}","format":fmt,"stop_if_true":True})

        def fmt_simple(ws, df_ref):
            cols = list(df_ref.columns) if df_ref is not None and not df_ref.empty else ["info"]
            ws.set_column(0, len(cols)-1, 18); ws.freeze_panes(1,0)
            ws.autofilter(0,0, max(len(df_ref),1) if df_ref is not None else 1, len(cols)-1)

        fmt_snapshot(writer.sheets["SNAPSHOT"], df)
        fmt_simple(writer.sheets["CHANGES"], changes)
        fmt_simple(writer.sheets["NEW"], new_items)
        fmt_simple(writer.sheets["REMOVED"], removed_items)

    buf.seek(0)
    return buf.read()

# ──────────────── Runner ────────────────
COLUMNS_EXPORT = ["product_id","sku","name","brand","category","department","price_currency",
                  "list_price","sale_price","discount_pct","availability","image_url","enlace",
                  "page_start","page_idx","captured_at"]

def run_single_category(cat_key: str, cfg: dict, args: argparse.Namespace):
    session = build_session()
    base_url  = args.url or cfg["base_url"]
    page_size = args.page_size or cfg["default_page_size"]
    page_step = args.page_step or cfg["default_page_step"]
    max_pages = args.max_pages or cfg["default_max_pages"]
    start     = args.start if args.start is not None else 0
    out_prefix = cfg["prefix"]

    print(f"\n=== {cat_key} ===")
    print(f"URL base: {base_url}")
    print(f"start={start}, sz={page_size}, step={page_step}, max_pages={max_pages}")

    all_rows, seen_ids = [], set()
    page_idx = 0
    empty_streak = 0
    next_long_pause_at = random.randint(*LONG_PAUSE_EVERY)
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    while page_idx < max_pages:
        try:
            html_text, real_url, used_sz = _fetch_with_fallback(session, base_url, start, page_size)
        except Exception as e:
            print(f"⚠️ Error de red start={start}: {type(e).__name__}: {e}")
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("Fin por errores consecutivos."); break
            page_idx += 1; start += page_step; continue

        page_rows, tiles_count = parse_products_from_html(html_text, real_url, page_start=start, page_idx=page_idx, captured_at_iso=captured_at)

        new_rows = []
        for r in page_rows:
            key = r.get("product_id") or r.get("enlace")
            if key and key not in seen_ids:
                seen_ids.add(key); new_rows.append(r)

        print(f"Página {page_idx} (start={start}, sz={used_sz}): tiles={tiles_count}, nuevos={len(new_rows)}")

        if tiles_count == 0 or len(new_rows) == 0:
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("Fin: sin más resultados nuevos."); break
        else:
            empty_streak = 0; all_rows.extend(new_rows)

        page_idx += 1; start += page_step

        pause = random.uniform(JITTER_MIN, JITTER_MAX)
        if random.random() < 0.2: pause += random.uniform(0.6, 1.2)
        print(f"⏳ Pausa {pause:.2f}s…"); time.sleep(pause)

        if page_idx == next_long_pause_at:
            long_pause = random.uniform(*LONG_PAUSE_RANGE)
            print(f"⏳⏳ Pausa larga {long_pause:.2f}s…"); time.sleep(long_pause)
            next_long_pause_at += random.randint(*LONG_PAUSE_EVERY)

    df = pd.DataFrame(all_rows, columns=COLUMNS_EXPORT)
    _normalize_numeric(df)

    if (cfg.get("omit_image_url") or cat_key == "marcas") and "image_url" in df.columns:
        df.drop(columns=["image_url"], inplace=True)

    # histórico previo (si rclone ya bajó .parquet al SAVE_DIR)
    prev_pq = _latest_previous_parquet(out_prefix, SAVE_DIR)
    prev_df = None
    if prev_pq and prev_pq.exists():
        try:
            prev_df = pd.read_parquet(prev_pq)
            _normalize_numeric(prev_df)
            # normaliza claves
            for d in (df, prev_df):
                if "product_id" in d.columns: d["product_id"] = d["product_id"].astype("string")
                if "sku" in d.columns: d["sku"] = d["sku"].astype("string")
        except Exception as e:
            print(f"⚠️ No pude leer previo {prev_pq.name}: {e}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_bytes = build_xlsx_bytes(df, prev_df, out_prefix)
    xlsx_name  = f"{out_prefix}_snapshot_{stamp}.xlsx"
    pq_name    = f"{out_prefix}_snapshot_{stamp}.parquet"

    # Guarda en disco para el workflow (y para histórico en Drive)
    (SAVE_DIR / xlsx_name).write_bytes(xlsx_bytes)
    df.to_parquet(SAVE_DIR / pq_name, index=False)

    # correo
    total_rows = len(df)
    big_disc = int((df.get("discount_pct", pd.Series(dtype=float)).fillna(0) >= (args.highlight or HIGHLIGHT_DISCOUNT)).sum())
    subj = f"[Scraper] {out_prefix} listo (RAM-only {stamp})"
    body = (f"Categoría: {cat_key}\nFilas: {total_rows}\n≥{args.highlight or HIGHLIGHT_DISCOUNT}% desc.: {big_disc}\n"
            f"Adjunto: {xlsx_name}\nHistórico: {'sí' if prev_df is not None else 'no (primer snapshot)'}\n"
            f"Guardado: {SAVE_DIR}")
    send_email(subj, body, EMAIL_TO_LIST, attachments=[(xlsx_name, xlsx_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])

    return {"category": cat_key, "ok": True, "rows": total_rows, "big_disc": big_disc}

# ──────────────── CLI ────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Scraper Palacio (RAM-only + histórico, envía XLSX por correo).")
    p.add_argument("--all", action="store_true")
    p.add_argument("--category", "-c", choices=CATEGORIES.keys())
    p.add_argument("--url"); p.add_argument("--start", type=int, default=None)
    p.add_argument("--page-size", type=int, default=None); p.add_argument("--page-step", type=int, default=None)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--highlight", type=float, default=HIGHLIGHT_DISCOUNT)
    args, unknown = p.parse_known_args()
    if unknown: print("⚠️ Ignorando args:", unknown)
    return args

def pick_category_interactively():
    print("Elige una categoría:"); keys = list(CATEGORIES.keys())
    for i,k in enumerate(keys, start=1): print(f"  {i}) {k}  →  {CATEGORIES[k]['base_url']}")
    try:
        idx = int(input(f"Número (1..{len(keys)}): ").strip())
        if 1 <= idx <= len(keys): return keys[idx-1]
    except Exception: pass
    print("Opción inválida, usando 'deportes'."); return "deportes"

def main():
    args = parse_args()
    if args.all:
        print("▶ Ejecutando TODAS las categorías…")
        for idx,(cat_key,cfg) in enumerate(CATEGORIES.items(), start=1):
            run_single_category(cat_key, cfg, args)
            if idx < len(CATEGORIES):
                cat_pause = random.uniform(0.6, 1.5)
                if random.random() < 0.2: cat_pause += random.uniform(0.8, 1.6)
                print(f"⏸️ Pausa entre categorías: {cat_pause:.2f}s…"); time.sleep(cat_pause)
        print("🎉 Terminaron todas.")
    else:
        cat_key = args.category or pick_category_interactively()
        run_single_category(cat_key, CATEGORIES[cat_key], args)

# Helpers Jupyter
def run_one_quick(CAT="ofertas", MAX_PAGES=None, PAGE_SIZE=None, PAGE_STEP=None, HIGHLIGHT=None):
    ns = argparse.Namespace(url=None, page_size=PAGE_SIZE, page_step=PAGE_STEP, max_pages=MAX_PAGES, start=None,
                            highlight=(HIGHLIGHT if HIGHLIGHT is not None else HIGHLIGHT_DISCOUNT), all=False, category=CAT)
    return run_single_category(CAT, CATEGORIES[CAT], ns)

def run_all_quick(MAX_PAGES=None, PAGE_SIZE=None, PAGE_STEP=None, HIGHLIGHT=None):
    ns = argparse.Namespace(url=None, page_size=PAGE_SIZE, page_step=PAGE_STEP, max_pages=MAX_PAGES, start=None,
                            highlight=(HIGHLIGHT if HIGHLIGHT is not None else HIGHLIGHT_DISCOUNT), all=True, category=None)
    print("▶ Ejecutando TODAS (helper)…")
    for idx,(cat_key,cfg) in enumerate(CATEGORIES.items(), start=1):
        run_single_category(cat_key, cfg, ns)
        if idx < len(CATEGORIES):
            cat_pause = random.uniform(0.6, 1.5)
            if random.random() < 0.2: cat_pause += random.uniform(0.8, 1.6)
            print(f"⏸️ Pausa entre categorías: {cat_pause:.2f}s…"); time.sleep(cat_pause)

if __name__ == "__main__":
    main()
