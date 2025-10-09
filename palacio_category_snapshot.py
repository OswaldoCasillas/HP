# palacio_category_snapshot.py
# Scraper Palacio (DISCO): guarda snapshots, compara con historial y envía correo por categoría.

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

# ────────── Email envs ──────────
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO   = os.getenv("EMAIL_TO", "")  # coma/; separados
EMAIL_DEBUG = os.getenv("EMAIL_DEBUG", "0") not in ("", "0", "false", "False", "FALSE")

def _to_list(to_addr):
    if not to_addr: return []
    if isinstance(to_addr, (list, tuple)): vals = [str(x) for x in to_addr]
    else: vals = re.split(r"[;,]", str(to_addr))
    return [x.strip() for x in vals if x and x.strip()]

def send_email(subject, body, to_addr, attachments=None):
    recipients = _to_list(to_addr or EMAIL_TO)
    if not (EMAIL_USER and EMAIL_PASS and recipients):
        print("⚠️ Falta EMAIL_USER/PASS/TO; no se envía correo.")
        return
    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)
    for (fname, data, mime) in (attachments or []):
        mt, st = (mime.split("/", 1) if mime else ("application","octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as smtp:
        if EMAIL_DEBUG: smtp.set_debuglevel(1)
        smtp.ehlo(); smtp.starttls(); smtp.ehlo()
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)
    print(f"📧 Email enviado: {subject} → {', '.join(recipients)}")

# ────────── Config scraping ──────────
CATEGORIES = {
    "ofertas": {"base_url": "https://www.elpalaciodehierro.com/ofertas/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 200, "prefix": "palacio_ofertas"},
    "electronica": {"base_url": "https://www.elpalaciodehierro.com/electronica/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 400, "prefix": "palacio_electronica"},
    "deportes": {"base_url": "https://www.elpalaciodehierro.com/deportes/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_deportes"},
    # "marcas": {...}  ← la quitaste por tiempos
    "gourmet": {"base_url": "https://www.elpalaciodehierro.com/gourmet/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_gourmet"},
    "casapalacio": {"base_url": "https://www.elpalaciodehierro.com/casapalacio/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_casapalacio"},
    "nuevos-productos": {"base_url": "https://www.elpalaciodehierro.com/nuevos-productos/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_nuevos_productos"},
    "mujer": {"base_url": "https://www.elpalaciodehierro.com/mujer/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 1000, "prefix": "palacio_mujer"},
    "productos-liquidacion": {"base_url": "https://www.elpalaciodehierro.com/productos-liquidacion/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 80, "prefix": "palacio_productos_liquidacion"},
    "hombre": {"base_url": "https://www.elpalaciodehierro.com/hombre/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_hombre"},
    "calzado": {"base_url": "https://www.elpalaciodehierro.com/calzado/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_calzado"},
    "mujer-multimarcas": {"base_url": "https://www.elpalaciodehierro.com/mujer/adolfo-dominguez%7Cburberry%7Ccoach2%7Cgerard-darel%7Chugo-boss2%7Cjimmy-choo2%7Clauren%7Clauren-ralph-lauren%7Cpolo-ralph-lauren2%7Cpolo-woman%7Cpucci%7Cralph-lauren%7Cray-ban%7Cugg/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_mujer_multimarcas"},
    "hombre-multimarcas": {"base_url": "https://www.elpalaciodehierro.com/hombre/ropa/adolfo-dominguez%7Cboss2%7Cburberry%7Ccalderoni%7Ccoach2%7Cecoalf%7Chugo-boss2%7Cpolo-ralph-lauren2%7Cralph-lauren%7Csaint-laurent-paris%7Cugg/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_hombre_multimarcas"},
    "electronica-tablets": {"base_url": "https://www.elpalaciodehierro.com/electronica/tablets/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_electronica_tablets"},
    "disenadores": {"base_url": "https://www.elpalaciodehierro.com/disenadores/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 80, "prefix": "palacio_disenadores"},
    "hogar": {"base_url": "https://www.elpalaciodehierro.com/hogar/", "default_page_size": 200, "default_page_step": 201, "default_max_pages": 800, "prefix": "palacio_hogar"},
    "juguetes": {"base_url": "https://www.elpalaciodehierro.com/juguetes/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_juguetes"},
    "categorias": {"base_url": "https://www.elpalaciodehierro.com/categorias/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_categorias"},
    "salas": {"base_url": "https://www.elpalaciodehierro.com/hogar/muebles/salas/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_salas"},
    "tendencias": {"base_url": "https://www.elpalaciodehierro.com/tendencias/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_tendencias"},
    "más vendido": {"base_url": "https://www.elpalaciodehierro.com/lo-mas-vendido/", "default_page_size": 200, "default_page_step": 200, "default_max_pages": 200, "prefix": "palacio_vendido"},
}

CONNECT_TIMEOUT, READ_TIMEOUT = 20, 180
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]
def random_headers():
    import random
    return {
        "user-agent": random.choice(UA_LIST),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": random.choice(["es-MX,es;q=0.9,en;q=0.8","es-ES,es;q=0.9,en;q=0.6","en-US,en;q=0.9"]),
        "cache-control": "no-cache",
    }

def build_session():
    s = requests.Session()
    retry = Retry(total=6, connect=3, read=3, backoff_factor=1.2,
                  status_forcelist=[429,500,502,503,504,520,522,523,524],
                  allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s

def fetch_page(session, base_url, start, page_size):
    params = {"start": start, "sz": page_size}
    time.sleep(random.uniform(0.05, 0.20))
    resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        try: wait_s = float(resp.headers["Retry-After"])
        except: wait_s = 2.0
        print(f"⏳ 429 Retry-After {wait_s}s…"); time.sleep(wait_s)
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code in (520,522,523,524):
        print(f"↻ CF {resp.status_code} retry…"); time.sleep(random.uniform(1.0,2.0))
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return resp.text, resp.url

_money_clean = re.compile(r"[^\d.,]")
def parse_price(txt):
    if not txt: return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try: return float(s)
    except: return None

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
        except: pass
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
            if not txt: return None
            return parse_price(txt)
        list_price = _num(list_span); sale_price = _num(sale_span)

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

# ────────── Comparación y Excel ──────────
COLUMNS_EXPORT = [
    "product_id","sku","name","brand","category","department","price_currency",
    "list_price","sale_price","discount_pct","availability","image_url","enlace",
    "page_start","page_idx","captured_at"
]
HIGHLIGHT_BANDS = [
    (70, "#FFCDD2"), (60, "#FFE0B2"), (50, "#FFF59D"), (30, "#DCEDC8"),
]

def latest_previous_parquet(cat_prefix_dir: Path):
    """Busca el parquet más reciente en out_palacio/<prefix>/<YYYY-MM>."""
    files = sorted(cat_prefix_dir.glob("*/*.parquet"))
    return files[-1] if files else None

def build_changes(prev: pd.DataFrame | None, now: pd.DataFrame, key="product_id"):
    if key not in now.columns:
        now[key] = now["enlace"].fillna("")

    now_idx = now.set_index(key, drop=False)
    if prev is None or prev.empty:
        return now.copy(), pd.DataFrame(columns=now.columns), pd.DataFrame(columns=now.columns), pd.DataFrame(columns=[
            key, "name_old","name_new","brand_old","brand_new",
            "list_price_old","list_price_new","sale_price_old","sale_price_new",
            "discount_pct_old","discount_pct_new","enlace_old","enlace_new"
        ])

    if key not in prev.columns:
        prev[key] = prev["enlace"].fillna("")
    prev_idx = prev.set_index(key, drop=False)

    # NEW & REMOVED
    new_keys = sorted(set(now_idx.index) - set(prev_idx.index))
    rem_keys = sorted(set(prev_idx.index) - set(now_idx.index))
    new_items = now_idx.loc[new_keys].reset_index(drop=True) if new_keys else pd.DataFrame(columns=now.columns)
    removed_items = prev_idx.loc[rem_keys].reset_index(drop=True) if rem_keys else pd.DataFrame(columns=now.columns)

    # CHANGES: compara columnas relevantes
    common = sorted(set(now_idx.index) & set(prev_idx.index))
    diffs = []
    for k in common:
        n = now_idx.loc[k]
        p = prev_idx.loc[k]
        changed = []
        fields = ["name","brand","list_price","sale_price","discount_pct","enlace"]
        row = {
            key: k,
            "name_old": p.get("name"), "name_new": n.get("name"),
            "brand_old": p.get("brand"), "brand_new": n.get("brand"),
            "list_price_old": p.get("list_price"), "list_price_new": n.get("list_price"),
            "sale_price_old": p.get("sale_price"), "sale_price_new": n.get("sale_price"),
            "discount_pct_old": p.get("discount_pct"), "discount_pct_new": n.get("discount_pct"),
            "enlace_old": p.get("enlace"), "enlace_new": n.get("enlace")
        }
        for f in fields:
            if (p.get(f) != n.get(f)): changed.append(f)
        if changed:
            diffs.append(row)
    changes = pd.DataFrame(diffs, columns=list(diffs[0].keys())) if diffs else pd.DataFrame(columns=[
        key,"name_old","name_new","brand_old","brand_new",
        "list_price_old","list_price_new","sale_price_old","sale_price_new",
        "discount_pct_old","discount_pct_new","enlace_old","enlace_new"
    ])
    return now.copy(), new_items, removed_items, changes

def save_snapshot_pack(df_now, prev_df, out_dir: Path, out_prefix: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    for col in ["list_price","sale_price","discount_pct"]:
        if col in df_now.columns:
            df_now[col] = pd.to_numeric(df_now[col], errors="coerce")

    _, new_items, removed_items, changes = build_changes(prev_df, df_now, key="product_id")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{out_prefix}_snapshot_{stamp}"
    csv_path = out_dir / f"{base}.csv"
    pq_path  = out_dir / f"{base}.parquet"
    xlsx_path= out_dir / f"{base}.xlsx"

    # CSV / Parquet
    df_now.to_csv(csv_path, index=False)
    try:
        df_now.to_parquet(pq_path, index=False)
    except Exception as e:
        print("⚠️ Parquet no disponible:", e)

    # Excel con 4 hojas
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df_now.to_excel(w, index=False, sheet_name="SNAPSHOT")
        (changes if not changes.empty else pd.DataFrame({"info":["Sin cambios"]})) \
            .to_excel(w, index=False, sheet_name="CHANGES")
        (new_items if not new_items.empty else pd.DataFrame({"info":["Sin nuevos"]})) \
            .to_excel(w, index=False, sheet_name="NEW")
        (removed_items if not removed_items.empty else pd.DataFrame({"info":["Sin removidos"]})) \
            .to_excel(w, index=False, sheet_name="REMOVED")

        wb = w.book
        def fmt_snapshot(ws, df_ref):
            cols = list(df_ref.columns) if not df_ref.empty else []
            ws.set_column(0, max(0, len(cols)-1), 18)
            if "discount_pct" in cols and not df_ref.empty:
                last_row = len(df_ref)+1
                col_letter = chr(65 + cols.index("discount_pct"))
                for thr, color in HIGHLIGHT_BANDS:
                    ws.conditional_format(
                        1, 0, last_row, len(cols)-1,
                        {"type": "formula", "criteria": f"=${col_letter}2>={thr}", "format": wb.add_format({"bg_color": color}), "stop_if_true": True}
                    )
        fmt_snapshot(w.sheets["SNAPSHOT"], df_now)

    with open(xlsx_path, "wb") as f:
        f.write(buf.getvalue())

    return csv_path, pq_path, xlsx_path, dict(
        rows=len(df_now),
        new=len(new_items),
        removed=len(removed_items),
        changed=len(changes)
    )

# ────────── Runner de categoría ──────────
def run_single_category(cat_key: str, cfg: dict, args: argparse.Namespace):
    session = build_session()
    base_url  = args.url or cfg["base_url"]
    page_size = args.page_size or cfg["default_page_size"]
    page_step = args.page_step or cfg["default_page_step"]
    max_pages = args.max_pages or cfg["default_max_pages"]
    start     = args.start if args.start is not None else 0
    out_prefix = cfg["prefix"]
    out_base = Path(args.out_dir or "out_palacio") / out_prefix / datetime.now().strftime("%Y-%m")
    print(f"\n=== {cat_key} → {base_url}")
    print(f"start={start}, sz={page_size}, step={page_step}, max_pages={max_pages}")
    print(f"Carpeta salida: {out_base}")

    all_rows, seen_ids = [], set()
    page_idx = 0
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    while page_idx < max_pages:
        try:
            html_text, real_url = fetch_page(session, base_url, start, page_size)
        except Exception as e:
            print(f"⚠️ Error red start={start}: {e}")
            break
        page_rows, tiles = parse_products_from_html(html_text, real_url, start, page_idx, captured_at)
        # de-dup por product_id o enlace
        new_rows = []
        for r in page_rows:
            key = r.get("product_id") or r.get("enlace")
            if key and key not in seen_ids:
                seen_ids.add(key); new_rows.append(r)
        print(f"Página {page_idx} (start={start}): tiles={tiles}, nuevos={len(new_rows)}")
        if tiles == 0 or len(new_rows) == 0: break
        all_rows.extend(new_rows)
        page_idx += 1
        start += page_step
        time.sleep(random.uniform(0.08, 0.25))

    df_now = pd.DataFrame(all_rows, columns=COLUMNS_EXPORT)
    prev_pq = latest_previous_parquet(out_base.parent)  # busca en el prefijo (meses previos incluidos)
    prev_df = pd.read_parquet(prev_pq) if prev_pq and prev_pq.exists() else None
    csv_path, pq_path, xlsx_path, stats = save_snapshot_pack(df_now, prev_df, out_base, out_prefix)

    # email por categoría
    subj = f"[Scraper] {out_prefix}: {stats['rows']} filas | +{stats['new']} / -{stats['removed']} / Δ{stats['changed']}"
    body = (f"Categoría: {cat_key}\n"
            f"Filas: {stats['rows']}\nNuevos: {stats['new']}\nRemovidos: {stats['removed']}\nCambios: {stats['changed']}\n"
            f"CSV: {csv_path.name}\nPARQUET: {pq_path.name}\nXLSX: {xlsx_path.name}")
    with open(xlsx_path, "rb") as f:
        send_email(subj, body, EMAIL_TO, attachments=[(xlsx_path.name, f.read(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])

    return {"category": cat_key, "ok": True, **stats, "out_dir": str(out_base)}

# ────────── CLI ──────────
def parse_args():
    p = argparse.ArgumentParser(description="Scraper Palacio (DISCO: compara con snapshot previo).")
    p.add_argument("--all", action="store_true", help="Ejecuta todas las categorías.")
    p.add_argument("--category","-c", choices=CATEGORIES.keys(), help="Categoría única.")
    p.add_argument("--url")
    p.add_argument("--start", type=int, default=None)
    p.add_argument("--page-size", type=int, default=None)
    p.add_argument("--page-step", type=int, default=None)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--out-dir", default="out_palacio")
    return p.parse_args()

def main():
    args = parse_args()
    if args.all:
        res = []
        for k,cfg in CATEGORIES.items():
            try: res.append(run_single_category(k, cfg, args))
            except Exception as e:
                print("✗", k, e); res.append({"category":k,"ok":False,"error":str(e)})
        # Resumen final por correo
        ok  = [r for r in res if r.get("ok")]
        bad = [r for r in res if not r.get("ok")]
        lines = [f"OK: {len(ok)}  |  Fallidas: {len(bad)}"]
        for r in ok: lines.append(f"  ✓ {r['category']}: filas={r['rows']}  +{r['new']}  -{r['removed']}  Δ{r['changed']}")
        for r in bad: lines.append(f"  ✗ {r['category']}: {r['error']}")
        send_email("[Scraper] Resumen ALL (DISCO)", "\n".join(lines), EMAIL_TO, attachments=[])
    else:
        cat = args.category or "deportes"
        run_single_category(cat, CATEGORIES[cat], args)

if __name__ == "__main__":
    main()
