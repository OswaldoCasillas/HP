# palacio_manual_runner.py
# Runner manual para una categoría de Palacio con parches:
# - Warm-up de cookies
# - Detección de muros/login/captcha y reintento
# - Selectores ampliados para tiles
# - Fallback agresivo cuando la primera página trae 0 tiles
# Uso:
#   python palacio_manual_runner.py -c gourmet --page-size 200 --page-step 200 --max-pages 20

import os, re, io, math, json, time, random, argparse, html
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────── Configuración ───────────────────────────
SAVE_DIR = Path(os.getenv("SAVE_DIR", "/tmp/palacio_out"))
SAVE_DIR.mkdir(parents=True, exist_ok=True)

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

CONNECT_TIMEOUT = 20
READ_TIMEOUT    = 180
JITTER_MIN, JITTER_MAX = 0.08, 0.25
LONG_PAUSE_EVERY = (12, 18)
LONG_PAUSE_RANGE = (1.5, 4.0)
STOP_AFTER_EMPTY = 1

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

# ─────────────────────────── Sesión/Fetch ───────────────────────────
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=[429,500,502,503,504,520,522,523,524],
        allowed_methods=["GET"], raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch_page(session: requests.Session, base_url: str, start: int | None, page_size: int | None):
    params = {}
    if start is not None: params["start"] = start
    if page_size is not None: params["sz"] = page_size
    time.sleep(random.uniform(0.05, 0.20))
    headers = random_headers()
    resp = session.get(base_url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        try: wait_s = float(resp.headers["Retry-After"])
        except Exception: wait_s = 2.0
        print(f"⏳ 429 Retry-After {wait_s}s…"); time.sleep(wait_s)
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if resp.status_code in (520,522,523,524):
        print(f"↻ CF {resp.status_code} start={start}, sz={page_size}. Reintentando…")
        time.sleep(random.uniform(1.0, 2.5))
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
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
            else:
                raise
    raise last_err if last_err else RuntimeError("Fallo de red sin respuesta HTTP")

# ─────────────────────────── Parse helpers ───────────────────────────
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
        except Exception:
            pass
    for k in ["data-pid","data-cnstrc-item-id","data-cnstrc-item-name"]:
        if bprod.get(k): out[k] = bprod.get(k)
    return out

def parse_products_from_html(html_text, page_url, page_start, page_idx, captured_at_iso):
    soup = BeautifulSoup(html_text, "html.parser")
    tiles = soup.select(
        "article.b-product_tile_item, div.b-product, li.product, div.product-tile, "
        "div.c-product, article.product, li.grid-tile, div.product-grid__item"
    )
    if not tiles:
        tiles = soup.select("[data-analytics]")

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

# ─────────────────────────── Anti-muros ───────────────────────────
def looks_like_block(html_text: str) -> bool:
    t = (html_text or "").lower()
    if any(k in t for k in ["cf-error", "cloudflare", "captcha", "acceso denegado"]):
        return True
    # señal del botón "Iniciar sesión"
    if "iniciar sesión" in t and "data-testid" in t:
        return True
    # No afirmamos bloqueo si simplemente es un landing; el fallback lo resolverá
    return False

# ─────────────────────────── Runner ───────────────────────────
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

    # Warm-up de cookies (algunos landings necesitan 1er GET sin params)
    try:
        session.get(base_url, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    except Exception:
        pass

    all_rows, seen_ids = [], set()
    page_idx = 0
    empty_streak = 0
    next_long_pause_at = random.randint(*LONG_PAUSE_EVERY)
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    while page_idx < max_pages:
        try:
            html_text, real_url, used_sz = _fetch_with_fallback(session, base_url, start, page_size)
            if looks_like_block(html_text):
                print("⚠️ Página parece muro/login/captcha. Reintentando con pausa…")
                time.sleep(random.uniform(1.5, 3.0))
                html_text, real_url, used_sz = _fetch_with_fallback(session, base_url, start, page_size)
        except Exception as e:
            print(f"⚠️ Error de red start={start}: {type(e).__name__}: {e}")
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("Fin por errores consecutivos.")
                break
            page_idx += 1; start += page_step; continue

        page_rows, tiles_count = parse_products_from_html(html_text, real_url, page_start=start, page_idx=page_idx, captured_at_iso=captured_at)

        # Fallback agresivo si la primera página viene vacía
        if page_idx == 0 and tiles_count == 0:
            try:
                print("↻ Fallback: GET sin parámetros (landing podría traer grid)…")
                html0, url0 = fetch_page(session, base_url, start=None, page_size=None)
                page_rows0, tiles0 = parse_products_from_html(html0, url0, page_start=0, page_idx=0, captured_at_iso=captured_at)
                if tiles0 > 0:
                    page_rows = page_rows0; tiles_count = tiles0
                    print(f"   Recuperado sin params: tiles={tiles0}")
            except Exception:
                pass
            if tiles_count == 0:
                page_size = 80
                page_step = 80
                print("↘︎ Ajuste temporal: page_size=80, page_step=80")

        new_rows = []
        for r in page_rows:
            key = r.get("product_id") or r.get("enlace")
            if key and key not in seen_ids:
                seen_ids.add(key); new_rows.append(r)

        print(f"Página {page_idx} (start={start}, sz={used_sz}): tiles={tiles_count}, nuevos={len(new_rows)}")

        if tiles_count == 0 or len(new_rows) == 0:
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("Fin: sin más resultados nuevos.")
                break
        else:
            empty_streak = 0
            all_rows.extend(new_rows)

        page_idx += 1
        start += page_step

        pause = random.uniform(JITTER_MIN, JITTER_MAX)
        if random.random() < 0.2: pause += random.uniform(0.6, 1.2)
        print(f"⏳ Pausa {pause:.2f}s…"); time.sleep(pause)

        if page_idx == next_long_pause_at:
            long_pause = random.uniform(*LONG_PAUSE_RANGE)
            print(f"⏳⏳ Pausa larga {long_pause:.2f}s…"); time.sleep(long_pause)
            next_long_pause_at += random.randint(*LONG_PAUSE_EVERY)

    df = pd.DataFrame(all_rows, columns=COLUMNS_EXPORT)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_name = f"{out_prefix}_snapshot_{stamp}.xlsx"
    pq_name   = f"{out_prefix}_snapshot_{stamp}.parquet"

    # XLSX simple (una sola hoja con snapshot)
    with pd.ExcelWriter(SAVE_DIR / xlsx_name, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="SNAPSHOT")

    df.to_parquet(SAVE_DIR / pq_name, index=False)
    print(f"✓ Guardados: {xlsx_name}, {pq_name} en {SAVE_DIR}")
    return len(df)

# ─────────────────────────── CLI ───────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Runner manual de categorías Palacio (parches anti tiles=0).")
    p.add_argument("-c", "--category", choices=CATEGORIES.keys(), required=True, help="Categoría a correr.")
    p.add_argument("--url", help="URL base personalizada (opcional).")
    p.add_argument("--start", type=int, default=None, help="start inicial (default 0).")
    p.add_argument("--page-size", type=int, default=None, help="sz por página (default por categoría).")
    p.add_argument("--page-step", type=int, default=None, help="step de start (default por categoría).")
    p.add_argument("--max-pages", type=int, default=None, help="máximo de páginas (default por categoría).")
    return p.parse_args()

def main():
    args = parse_args()
    cfg = CATEGORIES[args.category]
    run_single_category(args.category, cfg, args)

if __name__ == "__main__":
    main()
