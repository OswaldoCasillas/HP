# palacio_category_snapshot.py
# (Versión DISCO: crea CSV + PARQUET + un XLSX con SNAPSHOT/CHANGES/NEW/REMOVED)
# - Usa OUT_BASE_DIR = out_palacio
# - Calcula difs vs el último PARQUET previo de la MISMA categoría (en toda la carpeta).
# - Pausas cortas; reintentos en red; resaltado de descuentos en el XLSX.

import re, time, random, json, html as ihtml, glob, os, argparse, math
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────────────────────── Categorías (puedes quitar/añadir) ─────────────────────────
CATEGORIES = {
    "ofertas": {
        "base_url": "https://www.elpalaciodehierro.com/ofertas/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 200,
        "prefix": "palacio_ofertas",
    },
    "electronica": {
        "base_url": "https://www.elpalaciodehierro.com/electronica/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 400,
        "prefix": "palacio_electronica",
    },
    "deportes": {
        "base_url": "https://www.elpalaciodehierro.com/deportes/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 800,
        "prefix": "palacio_deportes",
    },
    # Si no quieres procesar marcas, simplemente comenta este bloque.
    "marcas": {
        "base_url": "https://www.elpalaciodehierro.com/marcas/",
        "default_page_size": 200,
        "default_page_step": 200,   # si ves duplicados, prueba 53
        "default_max_pages": 400,
        "prefix": "palacio_marcas",
        "omit_image_url": True,     # ← no guardar columna imagen
    },
    "gourmet": {
        "base_url": "https://www.elpalaciodehierro.com/gourmet/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 800,
        "prefix": "palacio_gourmet",
    },
    "casapalacio": {
        "base_url": "https://www.elpalaciodehierro.com/casapalacio/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 800,
        "prefix": "palacio_casapalacio",
    },
    "nuevos-productos": {
        "base_url": "https://www.elpalaciodehierro.com/nuevos-productos/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_nuevos_productos",
    },
    "mujer": {
        "base_url": "https://www.elpalaciodehierro.com/mujer/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 1000,
        "prefix": "palacio_mujer",
    },
    "productos-liquidacion": {
        "base_url": "https://www.elpalaciodehierro.com/productos-liquidacion/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 80,
        "prefix": "palacio_productos_liquidacion",
    },
    "hombre": {
        "base_url": "https://www.elpalaciodehierro.com/hombre/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_hombre",
    },
    "calzado": {
        "base_url": "https://www.elpalaciodehierro.com/calzado/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_calzado",
    },
    "mujer-multimarcas": {
        "base_url": "https://www.elpalaciodehierro.com/mujer/adolfo-dominguez%7Cburberry%7Ccoach%7Cgerard-darel%7Chugo-boss2%7Cjimmy-choo2%7Clauren%7Clauren-ralph-lauren%7Cpolo-ralph-lauren2%7Cpolo-woman%7Cpucci%7Cralph-lauren%7Cray-ban%7Cugg/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_mujer_multimarcas",
    },
    "electronica-tablets": {
        "base_url": "https://www.elpalaciodehierro.com/electronica/tablets/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_electronica_tablets",
    },
    "disenadores": {
        "base_url": "https://www.elpalaciodehierro.com/disenadores/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 80,
        "prefix": "palacio_disenadores",
    },
    "hogar": {
        "base_url": "https://www.elpalaciodehierro.com/hogar/",
        "default_page_size": 200,
        "default_page_step": 201,
        "default_max_pages": 800,
        "prefix": "palacio_hogar",
    },
    "juguetes": {
        "base_url": "https://www.elpalaciodehierro.com/juguetes/",
        "default_page_size": 200,
        "default_page_step": 200,
        "default_max_pages": 200,
        "prefix": "palacio_juguetes",
    },
}

# ───────────────────────── Config de red / timing ─────────────────────────
CONNECT_TIMEOUT = 20
READ_TIMEOUT    = 180

# Pausas más cortas (puedes subir si quieres ser más “humano”)
JITTER_MIN = 0.08
JITTER_MAX = 0.25
LONG_PAUSE_EVERY = (12, 18)
LONG_PAUSE_RANGE = (1.5, 4.0)

STOP_AFTER_EMPTY = 1
HIGHLIGHT_DISCOUNT = 51  # para resaltar en XLSX

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# ── base de salidas en DISCO ──
OUT_BASE_DIR = Path("out_palacio")
OUT_BASE_DIR.mkdir(exist_ok=True)

def random_headers():
    return {
        "user-agent": random.choice(UA_LIST),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": random.choice(["es-MX,es;q=0.9,en;q=0.8","es-ES,es;q=0.9,en;q=0.6","en-US,en;q=0.9"]),
        "cache-control": "no-cache",
    }

# ───────────────────── Helpers de parseo ────────────────────
_money_clean = re.compile(r"[^\d.,]")

def parse_price(txt):
    if not txt: return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else None

def nearest_b_product(node):
    cur = node
    for _ in range(8):
        if cur is None: break
        classes = cur.get("class") or []
        if "b-product" in classes:
            return cur
        cur = cur.parent
    return node

def extract_from_analytics(bprod):
    """Extrae dict desde data-analytics (si existe) y atributos data-* útiles."""
    out = {}
    if not bprod: return out
    da = bprod.get("data-analytics")
    if da:
        try:
            data = json.loads(ihtml.unescape(da))
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
        if bprod.get(k):
            out[k] = bprod.get(k)
    return out

def parse_products_from_html(html, page_url, page_start, page_idx, captured_at_iso):
    """Devuelve rows (uno por producto) y tiles_count."""
    soup = BeautifulSoup(html, "html.parser")
    tiles = soup.select("article.b-product_tile_item, div.b-product, li.product, div.product-tile")
    rows = []

    for t in tiles:
        bprod = nearest_b_product(t)
        info = extract_from_analytics(bprod)

        # IDs
        meta_product_id = (bprod.select_one("meta[itemprop='productID']")["content"].strip()
                           if bprod and bprod.select_one("meta[itemprop='productID']") else None)
        meta_sku = (bprod.select_one("meta[itemprop='sku']")["content"].strip()
                    if bprod and bprod.select_one("meta[itemprop='sku']") else None)

        product_id = (meta_product_id
                      or info.get("data-pid")
                      or info.get("data-cnstrc-item-id")
                      or info.get("product_id_analytics"))
        sku = meta_sku or product_id

        # Nombre
        meta_name = (bprod.select_one("meta[itemprop='name']")["content"].strip()
                     if bprod and bprod.select_one("meta[itemprop='name']") else None)
        name = meta_name or info.get("data-cnstrc-item-name") or info.get("name_analytics")
        if not name:
            brand_vis = text_or_none(t.select_one(".b-product_tile-brand"))
            title_el  = t.select_one(".b-product_tile-name, .b-product_tile-title a, .b-product_tile-title, a.b-product_tile-title-link")
            title_vis = text_or_none(title_el)
            name = " ".join(x for x in [brand_vis, title_vis] if x) or None

        # Brand / Category / Department
        brand = (bprod.get("data-brand") if bprod and bprod.get("data-brand") else None) \
                or info.get("brand_analytics") \
                or text_or_none(t.select_one(".b-product_tile-brand h4"))
        category   = info.get("category_analytics")
        department = info.get("department_analytics")

        # Enlace e imagen
        a = t.select_one("a[href]")
        enlace = urljoin(page_url, a["href"]) if a and a.has_attr("href") else None
        image_meta = bprod.select_one("meta[itemprop='image']") if bprod else None
        image_url = image_meta["content"].strip() if image_meta and image_meta.get("content") else None

        # Moneda / disponibilidad
        currency_meta = (bprod.select_one("meta[itemprop='priceCurrency']")["content"].strip()
                         if bprod and bprod.select_one("meta[itemprop='priceCurrency']") else None)
        availability_meta = (bprod.select_one("meta[itemprop='availability']")["content"].strip()
                             if bprod and bprod.select_one("meta[itemprop='availability']") else None)
        price_currency = currency_meta or info.get("currency_analytics") or "MXN"
        availability   = availability_meta or info.get("availability_analytics")

        # Precios
        list_span = t.select_one("div.b-product_price-old span.b-product_price-value")
        sale_span = t.select_one("div.b-product_price-sales.m-reduced span.b-product_price-value") \
                    or t.select_one("div.b-product_price-sales span.b-product_price-value")
        list_price = parse_price((list_span.get("content") if list_span else None) or (list_span.text if list_span else None))
        sale_price = parse_price((sale_span.get("content") if sale_span else None) or (sale_span.text if sale_span else None))

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

# ───────────────────── Red con reintentos ────────────────────
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504, 520, 522, 523, 524],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def fetch_page(session: requests.Session, base_url: str, start: int, page_size: int):
    params = {"start": start, "sz": page_size}
    time.sleep(random.uniform(0.05, 0.20))  # jitter antes del GET
    resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    # Manejo de 429 con Retry-After si aparece
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        try:
            wait_s = float(resp.headers["Retry-After"])
        except Exception:
            wait_s = 2.0
        print(f"⏳ 429 Retry-After {wait_s:.1f}s…")
        time.sleep(wait_s)
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    # Algunos CF intermitentes
    if resp.status_code in (520, 522, 523, 524):
        print(f"↻ CF {resp.status_code} start={start} reintentando…")
        time.sleep(random.uniform(1.0, 2.0))
        resp = session.get(base_url, params=params, headers=random_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    resp.raise_for_status()
    return resp.text, resp.url

# ───────────────────── Utilidades de archivo ─────────────────────
def latest_previous_parquet(out_prefix: str, search_root_dir: Path, exclude_stamp: str | None = None):
    """Busca el snapshot previo en TODAS las subcarpetas de la categoría (recursivo)."""
    search_root_dir.mkdir(parents=True, exist_ok=True)
    pattern = str(search_root_dir / f"**/{out_prefix}_snapshot_*.parquet")
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        return None
    if exclude_stamp:
        files = [f for f in files if exclude_stamp not in f]
    return Path(files[-1]) if files else None

# ───────────────────── Guardado (XLSX con 4 hojas) ─────────────────────
def save_snapshot(df: pd.DataFrame, stamp: str, out_prefix: str, out_dir: Path,
                  search_root_dir: Path | None = None):
    """
    Guarda snapshot en CSV, PARQUET y un único XLSX con hojas: SNAPSHOT, CHANGES, NEW, REMOVED.
    Si existe snapshot previo (buscado recursivamente en search_root_dir), calcula diferencias.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Rutas de salida
    csv_path  = out_dir / f"{out_prefix}_snapshot_{stamp}.csv"
    pq_path   = out_dir / f"{out_prefix}_snapshot_{stamp}.parquet"
    xlsx_path = out_dir / f"{out_prefix}_snapshot_{stamp}.xlsx"

    # Localiza snapshot previo ANTES de escribir el actual
    prev_pq = latest_previous_parquet(out_prefix, search_root_dir) if search_root_dir is not None else None

    # Normaliza numéricos del actual
    for col in ["list_price", "sale_price", "discount_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Prepara dataframes de diferencias
    changes = pd.DataFrame()
    new_items = pd.DataFrame()
    removed_items = pd.DataFrame()
    key = "product_id"

    if prev_pq is not None:
        prev_df = pd.read_parquet(prev_pq)

        # Normaliza tipos y claves
        for d in (df, prev_df):
            if "product_id" in d.columns:
                d["product_id"] = d["product_id"].astype("string")
            if "sku" in d.columns:
                d["sku"] = d["sku"].astype("string")
            for c in ("list_price", "sale_price", "discount_pct"):
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")

        # Clave preferida: product_id si existe en ambos; si no, sku
        use_pid = (df["product_id"].notna().sum() > 0) and (prev_df["product_id"].notna().sum() > 0)
        key = "product_id" if use_pid else "sku"

        merged = prev_df.merge(
            df, on=key, suffixes=("_old", "_new"),
            how="outer", indicator=True
        )

        def changed_num(a, b, atol=0.01):
            if pd.isna(a) or pd.isna(b):
                return False
            try:
                return not math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=atol)
            except Exception:
                return a != b

        both = merged[merged["_merge"] == "both"].copy()
        change_mask = (
            both.apply(lambda r: changed_num(r.get("list_price_old"), r.get("list_price_new")), axis=1)
            | both.apply(lambda r: changed_num(r.get("sale_price_old"), r.get("sale_price_new")), axis=1)
            | both.apply(lambda r: changed_num(r.get("discount_pct_old"), r.get("discount_pct_new")), axis=1)
            | (both["sale_price_old"].isna() ^ both["sale_price_new"].isna())
        )
        changes = both.loc[change_mask].copy()

        new_items     = merged[merged["_merge"] == "right_only"].copy()
        removed_items = merged[merged["_merge"] == "left_only"].copy()

        # Conforma columnas de NEW/REMOVED sin sufijos
        if not new_items.empty:
            keep_cols = [c for c in new_items.columns if c.endswith("_new") or c == key]
            new_items = new_items[keep_cols].rename(columns=lambda c: c.replace("_new", ""))
        if not removed_items.empty:
            keep_cols = [c for c in removed_items.columns if c.endswith("_old") or c == key]
            removed_items = removed_items[keep_cols].rename(columns=lambda c: c.replace("_old", ""))

    else:
        # Sin previo → NEW = todo; REMOVED vacío; CHANGES informativo
        new_items = df.copy()
        removed_items = pd.DataFrame(columns=df.columns)
        changes = pd.DataFrame(columns=[
            key, "name_old", "name_new", "brand_old", "brand_new",
            "list_price_old", "list_price_new", "sale_price_old", "sale_price_new",
            "discount_pct_old", "discount_pct_new", "enlace_new", "enlace_old"
        ])

    # ── Escribe el XLSX con 4 hojas ──
    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        # SNAPSHOT
        df.to_excel(writer, index=False, sheet_name="SNAPSHOT")

        # CHANGES
        cols_order = [
            key, "name_old", "name_new", "brand_old", "brand_new",
            "list_price_old", "list_price_new",
            "sale_price_old", "sale_price_new",
            "discount_pct_old", "discount_pct_new",
            "enlace_new", "enlace_old"
        ]
        if not changes.empty:
            for c in cols_order:
                if c not in changes.columns:
                    changes[c] = None
            changes[cols_order].to_excel(writer, index=False, sheet_name="CHANGES")
        else:
            pd.DataFrame({"info": ["Sin cambios de precio"]}).to_excel(writer, index=False, sheet_name="CHANGES")

        # NEW
        if not new_items.empty:
            new_items.to_excel(writer, index=False, sheet_name="NEW")
        else:
            pd.DataFrame({"info": ["Sin nuevos productos"]}).to_excel(writer, index=False, sheet_name="NEW")

        # REMOVED
        if not removed_items.empty:
            removed_items.to_excel(writer, index=False, sheet_name="REMOVED")
        else:
            pd.DataFrame({"info": ["Sin productos removidos"]}).to_excel(writer, index=False, sheet_name="REMOVED")

        # ── Formatos bonitos ──
        wb = writer.book
        money  = wb.add_format({"num_format": "#,##0.00"})
        pctfmt = wb.add_format({'num_format': '0.00"%"'})
        link   = wb.add_format({"font_color": "blue", "underline": 1})

        def format_snapshot(ws, df_ref: pd.DataFrame):
            cols = list(df_ref.columns)
            ws.set_column(0, len(cols)-1, 18)
            for nm in ("list_price", "sale_price"):
                if nm in cols:
                    i = cols.index(nm)
                    ws.set_column(i, i, 14, money)
            if "discount_pct" in cols:
                di = cols.index("discount_pct")
                ws.set_column(di, di, 12, pctfmt)
            if "enlace" in cols:
                ei = cols.index("enlace")
                for r, val in enumerate(df_ref.get("enlace", pd.Series()).fillna(""), start=2):
                    if isinstance(val, str) and val.startswith("http"):
                        ws.write_url(r-1, ei, val, link, string=val)
            ws.autofilter(0, 0, len(df_ref), len(cols)-1)
            ws.freeze_panes(1, 0)
            # Resaltar descuentos ≥ HIGHLIGHT_DISCOUNT
            if "discount_pct" in cols:
                last_row = len(df_ref) + 1
                col_letter = chr(65 + cols.index("discount_pct"))
                yellow = wb.add_format({"bg_color": "#FFF59D"})
                ws.conditional_format(1, 0, last_row, len(cols)-1, {
                    "type": "formula",
                    "criteria": f"=${col_letter}2>={HIGHLIGHT_DISCOUNT}",
                    "format": yellow,
                })

        def format_changes(ws, df_ref: pd.DataFrame):
            cols = list(df_ref.columns) if df_ref is not None and not df_ref.empty else ["info"]
            ws.set_column(0, len(cols)-1, 18)
            for nm in ["list_price_old","list_price_new","sale_price_old","sale_price_new"]:
                if nm in cols:
                    i = cols.index(nm)
                    ws.set_column(i, i, 14, money)
            for nm in ["discount_pct_old","discount_pct_new"]:
                if nm in cols:
                    i = cols.index(nm)
                    ws.set_column(i, i, 12, pctfmt)
            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, max(len(df_ref), 1), len(cols)-1)

        def format_simple(ws, df_ref: pd.DataFrame):
            cols = list(df_ref.columns) if df_ref is not None and not df_ref.empty else ["info"]
            ws.set_column(0, len(cols)-1, 18)
            ws.freeze_panes(1, 0)
            ws.autofilter(0, 0, max(len(df_ref), 1) if df_ref is not None else 1, len(cols)-1)

        ws_snap = writer.sheets["SNAPSHOT"]; format_snapshot(ws_snap, df)
        ws_changes = writer.sheets["CHANGES"]; format_changes(ws_changes, changes if not changes.empty else pd.DataFrame(columns=cols_order))
        ws_new = writer.sheets["NEW"]; format_simple(ws_new, new_items)
        ws_removed = writer.sheets["REMOVED"]; format_simple(ws_removed, removed_items)

    # CSV + Parquet (después del XLSX)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(pq_path, index=False)

    # Mensaje final
    if prev_pq is not None:
        print(
            f"📝 Cambios reales: {0 if changes is None else len(changes)} | "
            f"Nuevos: {0 if new_items is None else len(new_items)} | "
            f"Removidos: {0 if removed_items is None else len(removed_items)}\n"
            f"   Archivo: {xlsx_path.resolve()}"
        )
    else:
        print(f"ℹ️ Primer snapshot de la categoría. Archivo: {xlsx_path.resolve()}")

    return csv_path, pq_path, xlsx_path

# ───────────────────────── CLI / Main ─────────────────────────
def pick_category_interactively():
    print("Elige una categoría:")
    keys = list(CATEGORIES.keys())
    for i, k in enumerate(keys, start=1):
        print(f"  {i}) {k}  →  {CATEGORIES[k]['base_url']}")
    try:
        idx = int(input("Número (1..{}): ".format(len(keys))).strip())
        if 1 <= idx <= len(keys):
            return keys[idx-1]
    except Exception:
        pass
    print("Opción inválida, usando 'deportes' por defecto.")
    return "deportes"

def parse_args():
    p = argparse.ArgumentParser(
        description="Scraper Palacio (DISCO) con snapshots y comparación (soporta --all)."
    )
    p.add_argument("--all", action="store_true",
                   help="Ejecuta TODAS las categorías en el orden definido.")
    p.add_argument("--category", "-c", choices=CATEGORIES.keys(),
                   help="Categoría individual (si no se pasa y no hay --all, se pedirá por menú).")
    p.add_argument("--url", help="URL base personalizada (solo para una categoría).")
    p.add_argument("--start", type=int, default=None, help="Offset inicial 'start=' de la paginación (default 0).")
    p.add_argument("--page-size", type=int, default=None, help="Ítems por página 'sz=' (default según categoría).")
    p.add_argument("--page-step", type=int, default=None, help="Incremento de start entre páginas (default según categoría).")
    p.add_argument("--max-pages", type=int, default=None, help="Máximo de páginas a recorrer (default según categoría).")
    p.add_argument("--highlight", type=float, default=HIGHLIGHT_DISCOUNT, help="Umbral % para resaltar en XLSX (default 51).")

    args, unknown = p.parse_known_args()
    if unknown:
        print("⚠️ Ignorando argumentos no reconocidos (p.ej. de Jupyter):", unknown)
    return args

def run_single_category(cat_key, cfg, args):
    """Ejecuta scraping+export para una categoría."""
    session = build_session()

    base_url  = args.url or cfg["base_url"]
    page_size = args.page_size or cfg["default_page_size"]
    page_step = args.page_step or cfg["default_page_step"]
    max_pages = args.max_pages or cfg["default_max_pages"]
    start     = args.start if args.start is not None else 0
    out_prefix = cfg["prefix"]

    # Carpetas: raíz de la categoría y subcarpeta de mes (YYYY-MM)
    category_root_dir = OUT_BASE_DIR / out_prefix
    month_slug = datetime.now().strftime("%Y-%m")
    out_dir = category_root_dir / month_slug
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== {cat_key} ===")
    print(f"URL base: {base_url}")
    print(f"start={start}, sz={page_size}, step={page_step}, max_pages={max_pages}, highlight={HIGHLIGHT_DISCOUNT}%")
    print(f"Guardando en: {out_dir.resolve()}")

    all_rows, seen_ids = [], set()
    page_idx = 0
    empty_streak = 0
    next_long_pause_at = random.randint(*LONG_PAUSE_EVERY)
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    while page_idx < max_pages:
        try:
            html, real_url = fetch_page(session, base_url, start, page_size)
        except Exception as e:
            print(f"⚠️ Error de red en start={start}: {type(e).__name__}: {e}")
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("Fin por errores consecutivos.")
                break
            page_idx += 1; start += page_step
            continue

        page_rows, tiles_count = parse_products_from_html(
            html, real_url, page_start=start, page_idx=page_idx, captured_at_iso=captured_at
        )

        # de-dup por product_id (o enlace si no hay id)
        new_rows = []
        for r in page_rows:
            key = r.get("product_id") or r.get("enlace")
            if key and key not in seen_ids:
                seen_ids.add(key)
                new_rows.append(r)

        print(f"Página {page_idx} (start={start}): tiles={tiles_count}, nuevos={len(new_rows)}")

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

        # pausas aleatorias
        pause = random.uniform(JITTER_MIN, JITTER_MAX)
        if random.random() < 0.2:
            pause += random.uniform(0.6, 1.2)
        print(f"⏳ Pausa {pause:.2f}s…")
        time.sleep(pause)

        if page_idx == next_long_pause_at:
            long_pause = random.uniform(*LONG_PAUSE_RANGE)
            print(f"⏳⏳ Pausa larga {long_pause:.2f}s…")
            time.sleep(long_pause)
            next_long_pause_at += random.randint(*LONG_PAUSE_EVERY)

    # DataFrame final
    df = pd.DataFrame(all_rows, columns=[
        "product_id","sku","name","brand","category","department","price_currency",
        "list_price","sale_price","discount_pct","availability","image_url","enlace",
        "page_start","page_idx","captured_at"
    ])

    # Normaliza tipos
    for col in ["list_price","sale_price","discount_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Opcional: quitar imagen en marcas
    if (cfg.get("omit_image_url") or cat_key == "marcas") and "image_url" in df.columns:
        df.drop(columns=["image_url"], inplace=True)

    # Guardar snapshot en CSV + PARQUET + XLSX (en carpeta del mes)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path, pq_path, xlsx_path = save_snapshot(
        df, stamp, out_prefix, out_dir,
        search_root_dir=category_root_dir  # para localizar snapshot previo
    )
    print(f"✅ Snapshot guardado:\n- CSV : {csv_path.resolve()}\n- PQ  : {pq_path.resolve()}\n- XLSX: {xlsx_path.resolve()}")

def main():
    global HIGHLIGHT_DISCOUNT
    args = parse_args()
    HIGHLIGHT_DISCOUNT = args.highlight

    if args.all:
        print("▶ Ejecutando TODAS las categorías en orden…")
        keys = list(CATEGORIES.keys())
        for idx, (cat_key, cfg) in enumerate(CATEGORIES.items(), start=1):
            args_for_cat = argparse.Namespace(
                url=None,
                page_size=args.page_size,
                page_step=args.page_step,
                max_pages=args.max_pages,
                start=args.start,
                highlight=args.highlight
            )
            run_single_category(cat_key, cfg, args_for_cat)
            if idx < len(keys):
                cat_pause = random.uniform(0.6, 1.5)
                if random.random() < 0.2:
                    cat_pause += random.uniform(0.8, 1.6)
                print(f"⏸️ Pausa entre categorías: {cat_pause:.2f}s…")
                time.sleep(cat_pause)
        print("\n🎉 Terminaron todas las categorías.")
    else:
        cat_key = args.category or pick_category_interactively()
        cfg = CATEGORIES[cat_key]
        run_single_category(cat_key, cfg, args)

if __name__ == "__main__":
    main()
