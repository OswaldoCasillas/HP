#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Scraper Adolfo Domínguez (MX) con snapshots mensuales, comparación,
# hoja CHANGES y columna discount_pct_prev en la hoja principal.
# Además: envía por correo el CSV generado al final de cada job.

import argparse, random, time, re, json, glob, math, os, smtplib
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────────── Config red / tiempos ─────────────
CONNECT_TIMEOUT = 40
READ_TIMEOUT    = 240
JITTER_MIN = 1.5
JITTER_MAX = 5.0
LONG_PAUSE_EVERY = (6, 11)
LONG_PAUSE_RANGE = (20, 50)
STOP_AFTER_EMPTY = 2

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

OUT_BASE_DIR = Path("salidas_adolfodominguez")

# ───────────── Email helper ─────────────
def send_email_with_attachment(
    subject: str,
    body_text: str,
    attachment_path: Path,
    extra_recipients: list[str] | None = None,
):
    """
    Envía un email con el CSV adjunto usando SMTP.
    Usa vars de entorno:
      EMAIL_HOST
      EMAIL_PORT
      EMAIL_USER
      EMAIL_PASS
      EMAIL_TO (puede ser lista separada por coma)
      EMAIL_DEBUG (opcional "1" para imprimir info)
    Además siempre agrega 'oswaldocasillas1@gmail.com' al TO.
    """

    host = os.environ.get("EMAIL_HOST")
    port_txt = os.environ.get("EMAIL_PORT", "587")
    user = os.environ.get("EMAIL_USER")
    pwd  = os.environ.get("EMAIL_PASS")
    to_env = os.environ.get("EMAIL_TO", "")
    debug = os.environ.get("EMAIL_DEBUG", "0") == "1"

    # Build final recipients
    base_list = [t.strip() for t in to_env.split(",") if t.strip()]
    base_list.append("oswaldocasillas1@gmail.com")  # <- requerido
    if extra_recipients:
        base_list.extend(extra_recipients)
    # unique
    to_list = sorted(set(base_list))

    if not host or not user or not pwd or not to_list:
        print("⚠️ No se envía correo: faltan vars EMAIL_HOST/USER/PASS/TO.")
        return

    # Create message
    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = ", ".join(to_list)
    msg["Date"] = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.set_content(body_text)

    # Adjuntar archivo (CSV normalmente, pero soporta cualquiera)
    file_bytes = attachment_path.read_bytes()
    filename = attachment_path.name
    msg.add_attachment(
        file_bytes,
        maintype="text",
        subtype="csv",
        filename=filename
    )

    # Enviar SMTP TLS
    try:
        port = int(port_txt)
    except ValueError:
        port = 587

    print(f"✉️ Enviando email a {to_list} con adjunto {filename} ...")
    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(user, pwd)
        smtp.send_message(msg)

    if debug:
        print("✅ Email enviado OK.")

def rnd_headers():
    return {
        "user-agent": random.choice(UA_LIST),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": random.choice(["es-MX,es;q=0.9,en;q=0.8","es-ES,es;q=0.9,en;q=0.6","en-US,en;q=0.9"]),
        "cache-control": "no-cache",
    }

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.9,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

# ───────────── Utilidades parsing ─────────────
_money_clean = re.compile(r"[^\d.,]")

def parse_price(txt: str | None) -> float | None:
    if not txt: return None
    s = _money_clean.sub("", txt).strip().replace(",", "")
    if not s: return None
    try:
        return float(s)
    except ValueError:
        return None

def pick_text(el) -> str | None:
    return el.get_text(" ", strip=True) if el else None

def first_href_in(node, base_url: str | None):
    a = node.select_one("a[href]")
    if not a or not a.has_attr("href"): return None
    href = a["href"]
    return urljoin(base_url, href) if base_url else href

def find_product_id(tile):
    el = tile.find(attrs={"data-pid": True})
    if el:
        return str(el.get("data-pid")).strip()
    a = tile.select_one("a[href]")
    if a and a.has_attr("href"):
        m = re.search(r"(\d{8,})", a["href"])
        if m: return m.group(1)
    return None

# ✅ EXTRACTOR ESTRICTO (arregla el descuento)
def extract_prices_from_tile(tile):
    """
    AD:
      - list_price  → <del> .value[content] / .strike-through.list .value[content]
      - sale_price  → .discount-wrapper .sales .value[content]
    Fallback: solo dentro de contenedores de precio.
    """
    def _num_from(el):
        if not el:
            return None
        if el.has_attr("content"):
            return parse_price(el["content"])
        return parse_price(pick_text(el))

    list_price = _num_from(tile.select_one(
        "del .value[content], .strike-through.list .value[content], "
        ".prices__value--original[content], .prices__value--original"
    ))
    sale_price = _num_from(tile.select_one(
        ".discount-wrapper .sales .value[content], .discount-wrapper .sales .value, "
        ".prices__value--discount[content], .prices__value--discount, "
        ".sales .value[content], .sales .value"
    ))

    if list_price is None or sale_price is None:
        scope = tile.select_one(".product-tile__price, .product-tile__prices, .prices, .product-price")
        if scope:
            candidates = []
            for el in scope.select(".value"):
                val = el.get("content") or pick_text(el)
                p = parse_price(val)
                if p is not None and 0 < p < 1_000_000:
                    candidates.append(p)
            candidates = sorted(set(candidates))
            if sale_price is None and candidates:
                sale_price = min(candidates)
            if list_price is None:
                list_price = max(candidates) if len(candidates) >= 2 else (candidates[0] if candidates else None)

    return list_price, sale_price

def discount_from_prices(list_price, sale_price):
    if list_price is None or sale_price is None: return None
    if sale_price >= list_price: return None
    return round((1 - sale_price / list_price) * 100, 2)

def infer_gender(enlace: str | None, category: str | None, name: str | None):
    s = " ".join([enlace or "", category or "", name or ""]).lower()
    if any(k in s for k in ["/mujer", "/woman", " mujer "]): return "mujer"
    if any(k in s for k in ["/hombre", "/man", " hombre "]): return "hombre"
    return None

# ───────────── Parse página ─────────────
def parse_products_from_html(html: str, page_url: str, page_idx: int, page_num: int,
                             captured_at: str, forced_gender: str | None):
    soup = BeautifulSoup(html, "html.parser")
    tiles = soup.select(
        "article.product-tile, div.product-tile, li.product-tile, "
        "div.product-list__item, li.grid-tile, article.product"
    )
    rows = []

    for t in tiles:
        product_id = find_product_id(t)
        sku        = product_id
        name_el = t.select_one(".product-tile__name, .product__name, .product-name, h3 a, h3")
        if not name_el:
            meta = t.select_one("meta[itemprop='name']")
            name = meta.get("content").strip() if meta and meta.has_attr("content") else None
        else:
            name = pick_text(name_el)

        enlace = None
        a_name = t.select_one(".product-tile__name a, .product__name a, h3 a")
        if a_name and a_name.has_attr("href"):
            enlace = urljoin(page_url, a_name["href"])
        else:
            enlace = first_href_in(t, page_url)

        img = t.select_one("img[data-src], img[data-original], img[src]")
        image_url = None
        if img:
            if img.get("data-src"): image_url = urljoin(page_url, img["data-src"])
            elif img.get("data-original"): image_url = urljoin(page_url, img["data-original"])
            elif img.get("src"): image_url = urljoin(page_url, img["src"])

        list_price, sale_price = extract_prices_from_tile(t)
        discount_pct = discount_from_prices(list_price, sale_price)

        brand_el = t.select_one(".product-tile__brand, .product__brand, .brand")
        brand = pick_text(brand_el) if brand_el else "Adolfo Domínguez"

        category = None
        dep = None
        da = t.get("data-analytics")
        if da:
            try:
                obj = json.loads(da)
                prod = obj.get("product", obj) if isinstance(obj, dict) else {}
                category = category or prod.get("category")
                dep = dep or prod.get("departmentName") or prod.get("department")
                brand = prod.get("brand") or brand
                if not product_id and prod.get("id"): product_id = str(prod.get("id"))
                if not name and prod.get("name"): name = prod.get("name")
            except Exception:
                pass

        availability = None
        if t.select_one(".out-of-stock, .sold-out, .no-stock"):
            availability = "out_of_stock"

        g = forced_gender or infer_gender(enlace, category, name)

        rows.append({
            "product_id": product_id,
            "sku": sku,
            "name": name,
            "brand": brand,
            "category": category,
            "department": dep,
            "gender": g,
            "price_currency": "MXN",
            "list_price": list_price,
            "sale_price": sale_price,
            "discount_pct": discount_pct,
            "availability": availability,
            "image_url": image_url,
            "enlace": enlace,
            "page_idx": page_idx,
            "page_num": page_num,
            "captured_at": captured_at,
        })

    return rows, len(tiles)

# ───────────── Guardado + comparación ─────────────
def ensure_dirs(alias: str):
    base = OUT_BASE_DIR / alias
    month = datetime.now().strftime("%Y-%m")
    out_dir = base / month
    base.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    return base, out_dir

def latest_previous_parquet(out_prefix: str, base_dir: Path):
    pattern = str(base_dir / f"**/{out_prefix}_snapshot_*.parquet")
    files = sorted(glob.glob(pattern, recursive=True))
    return Path(files[-1]) if files else None

def build_changes(current_df: pd.DataFrame, prev_df: pd.DataFrame, key: str):
    """Devuelve (df_with_prev, changes_df, new_df, removed_df)."""
    df = current_df.copy()

    # columna de descuento previo en la hoja principal
    if "discount_pct" in prev_df.columns:
        prev_disc_map = prev_df.set_index(key)["discount_pct"]
        df["discount_pct_prev"] = df[key].map(prev_disc_map)
    else:
        df["discount_pct_prev"] = None

    merged = prev_df.merge(df, on=key, suffixes=("_old","_new"), how="outer", indicator=True)

    def changed_price(a,b, atol=0.01):
        if pd.isna(a) or pd.isna(b): return False
        try: return not math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=atol)
        except: return a != b

    both = merged[merged["_merge"] == "both"].copy()
    mask_changes = (
        both.apply(lambda r: changed_price(r.get("list_price_old"), r.get("list_price_new")), axis=1)
        | both.apply(lambda r: changed_price(r.get("sale_price_old"), r.get("sale_price_new")), axis=1)
        | (both["sale_price_old"].isna() ^ both["sale_price_new"].isna())
        | (both["discount_pct_old"].fillna(-1) != both["discount_pct_new"].fillna(-1))
    )
    changes = both.loc[mask_changes].copy()

    new_items     = merged[merged["_merge"] == "right_only"].copy()
    removed_items = merged[merged["_merge"] == "left_only"].copy()

    return df, changes, new_items, removed_items

def save_snapshot(df: pd.DataFrame, out_dir: Path, out_prefix: str,
                  highlight: float = 60.0,
                  extras: dict | None = None,
                  stamp: str | None = None):
    stamp = stamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path  = out_dir / f"{out_prefix}_snapshot_{stamp}.csv"
    pq_path   = out_dir / f"{out_prefix}_snapshot_{stamp}.parquet"
    xlsx_path = out_dir / f"{out_prefix}_snapshot_{stamp}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(pq_path, index=False)

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        # SNAPSHOT
        sheet = "SNAPSHOT"
        df.to_excel(writer, index=False, sheet_name=sheet)
        wb, ws = writer.book, writer.sheets[sheet]
        money  = wb.add_format({"num_format": "#,##0.00"})
        pct    = wb.add_format({'num_format': '0.00"%"'})
        yellow = wb.add_format({"bg_color": "#FFF59D"})
        link   = wb.add_format({"font_color": "blue", "underline": 1})

        cols = list(df.columns)
        ws.set_column(0, len(cols)-1, 18)
        for nm in ("list_price","sale_price"):
            if nm in cols: ws.set_column(cols.index(nm), cols.index(nm), 14, money)
        for nm in ("discount_pct","discount_pct_prev"):
            if nm in cols: ws.set_column(cols.index(nm), cols.index(nm), 14, pct)
        if "enlace" in cols:
            ei = cols.index("enlace")
            for r, val in enumerate(df["enlace"].fillna(""), start=2):
                if isinstance(val, str) and val.startswith("http"):
                    ws.write_url(r-1, ei, val, link, string=val)
        ws.autofilter(0, 0, len(df), len(cols)-1)
        ws.freeze_panes(1, 0)
        if "discount_pct" in cols:
            di = cols.index("discount_pct")
            ws.conditional_format(1, 0, len(df)+1, len(cols)-1, {
                "type": "formula",
                "criteria": f"=${chr(65+di)}2>={highlight}",
                "format": yellow,
            })

        # Extras (CHANGES/NEW/REMOVED)
        if extras:
            money2  = wb.add_format({"num_format": "#,##0.00"})
            pct2    = wb.add_format({'num_format': '0.00"%"'})
            for sheet_name, df_extra in extras.items():
                df_extra.to_excel(writer, index=False, sheet_name=sheet_name)
                wsx = writer.sheets[sheet_name]
                colsx = list(df_extra.columns)
                wsx.set_column(0, len(colsx)-1, 18)
                for cn in colsx:
                    if any(tag in cn for tag in ["list_price","sale_price"]):
                        c = colsx.index(cn); wsx.set_column(c, c, 14, money2)
                    if "discount_pct" in cn:
                        c = colsx.index(cn); wsx.set_column(c, c, 12, pct2)

    return stamp, csv_path, pq_path, xlsx_path

# ───────────── Paginación ─────────────
def parse_extra_params(s: str | None):
    if not s: return {}
    return dict(parse_qsl(s, keep_blank_values=True))

def build_page_url(base_url: str, page_param: str, page_value: int, extra_params: dict):
    u = urlparse(base_url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[page_param] = str(page_value)
    q.update(extra_params or {})
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

def fetch_page(session, url):
    time.sleep(random.uniform(0.3, 1.2))
    resp = session.get(url, headers=rnd_headers(), timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return resp.text, resp.url

# ───────────── Prompt interactivo (para Jupyter / local) ─────────────
def pick_gender_interactively(default="mujer"):
    print("Elige género:")
    print("  1) mujer")
    print("  2) hombre")
    print("  3) ambos")
    try:
        val = input("Número (1-3) [1]: ").strip() or "1"
        idx = int(val)
        return ["mujer", "hombre", "ambos"][idx-1]
    except Exception:
        print(f"Entrada inválida. Usando '{default}'.")
        return default

# ───────────── CLI ─────────────
def parse_args():
    p = argparse.ArgumentParser(description="Scraper AD MX (?page=N) con hoja CHANGES y descuento previo. Envía CSV por correo.")
    p.add_argument("--base-url", default="https://www.adolfodominguez.com/es-mx/search",
                   help="URL base del buscador/listado (sin ?page).")
    p.add_argument("--gender", choices=["mujer","hombre","ambos","ask"], default="ask",
                   help="Qué buscar: mujer, hombre, ambos o ask (preguntar).")
    p.add_argument("--page-param", default="page", help="Nombre del parámetro de página (default: page)")
    p.add_argument("--page-start", type=int, default=1, help="Página inicial (default: 1)")
    p.add_argument("--max-pages", type=int, default=100, help="Máximo de páginas a recorrer")
    p.add_argument("--alias", default="ad_mx_search", help="Alias/carpeta base de salida")
    p.add_argument("--out-prefix", default=None, help="Prefijo de archivos (default: alias + '_' + gender)")
    p.add_argument("--highlight", type=float, default=60.0, help="% para resaltar descuentos en XLSX")
    p.add_argument("--also-url", action="append", default=[], help="URL completa adicional a scrapear (repetible).")
    p.add_argument("--extra-params", default="", help="Params extra para base-url (sin '?'), ej: 'lang=null'")
    args, unknown = p.parse_known_args()
    if unknown: print("⚠️ Ignorando argumentos no reconocidos:", unknown)
    return args

# ───────────── Orquestación ─────────────
def run_job(session, job_name: str, base_url: str, fixed_params: dict, page_param: str,
            page_start: int, max_pages: int, out_alias: str, out_prefix: str | None,
            forced_gender: str | None, highlight: float):
    base_dir, out_dir = ensure_dirs(out_alias)
    print(f"\n▶️ Job: {job_name}  |  URL base: {base_url}")
    print(f"   params fijos: {fixed_params}  | pages: {page_start}..{page_start+max_pages-1}")
    print(f"   salida: {out_dir.resolve()}  | gender fijo: {forced_gender or 'auto'}")

    all_rows, seen = [], set()
    empty_streak = 0
    long_pause_at = random.randint(*LONG_PAUSE_EVERY)
    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    for idx, page_num in enumerate(range(page_start, page_start + max_pages), start=0):
        url = build_page_url(base_url, page_param, page_num, fixed_params)
        html, real_url = fetch_page(session, url)
        rows, tiles_count = parse_products_from_html(
            html, real_url, page_idx=idx, page_num=page_num,
            captured_at=captured_at, forced_gender=forced_gender
        )

        new_rows = []
        for r in rows:
            key = r.get("product_id") or r.get("enlace")
            if key and key not in seen:
                seen.add(key)
                new_rows.append(r)

        print(f"   Página {page_num}: tiles={tiles_count}, nuevos={len(new_rows)}")
        if tiles_count == 0 or len(new_rows) == 0:
            empty_streak += 1
            if empty_streak >= STOP_AFTER_EMPTY:
                print("   Fin: sin más resultados nuevos.")
                break
        else:
            empty_streak = 0
            all_rows.extend(new_rows)

        pause = random.uniform(JITTER_MIN, JITTER_MAX)
        if random.random() < 0.25:
            pause += random.uniform(3.0, 12.0)
        print(f"   ⏳ Pausa {pause:.1f}s…")
        time.sleep(pause)
        if (idx + 1) == long_pause_at:
            lp = random.uniform(*LONG_PAUSE_RANGE)
            print(f"   ⏳⏳ Pausa larga {lp:.1f}s…")
            time.sleep(lp)
            long_pause_at += random.randint(*LONG_PAUSE_EVERY)

    df = pd.DataFrame(all_rows, columns=[
        "product_id","sku","name","brand","category","department","gender","price_currency",
        "list_price","sale_price","discount_pct","availability","image_url","enlace",
        "page_idx","page_num","captured_at"
    ])
    for c in ("list_price","sale_price","discount_pct"):
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

    # prefijo final real
    pref = out_prefix or (f"{out_alias}_{forced_gender}" if forced_gender else out_alias)

    # cargar snapshot previo (si existe) y preparar columnas/sheets extra
    prev_pq = latest_previous_parquet(pref, base_dir)
    extras = None
    if prev_pq and not df.empty:
        prev_df = pd.read_parquet(prev_pq)
        for d in (df, prev_df):
            if "product_id" in d.columns: d["product_id"] = d["product_id"].astype("string")
            if "sku" in d.columns:        d["sku"]        = d["sku"].astype("string")
            for c in ("list_price","sale_price","discount_pct"):
                if c in d.columns: d[c] = pd.to_numeric(d[c], errors="coerce")
        use_pid = (df["product_id"].notna().sum() > 0) and (prev_df["product_id"].notna().sum() > 0)
        key = "product_id" if use_pid else "sku"

        df, changes, new_items, removed_items = build_changes(df, prev_df, key=key)

        # Armar hojas extra para el mismo XLSX
        cols = [
            key, "name_old","name_new","brand_old","brand_new",
            "list_price_old","list_price_new","sale_price_old","sale_price_new",
            "discount_pct_old","discount_pct_new","enlace_new","enlace_old"
        ]
        for c in cols:
            if c not in changes.columns: changes[c] = None
        extras = {
            "CHANGES": changes[cols].copy() if not changes.empty else pd.DataFrame({"info":["Sin cambios de precio"]}),
            "NEW":     (new_items[[c for c in new_items.columns if c.endswith("_new") or c==key]]
                        .rename(columns=lambda c: c.replace("_new","")))
                        if not new_items.empty else pd.DataFrame({"info":["Sin nuevos productos"]}),
            "REMOVED": (removed_items[[c for c in removed_items.columns if c.endswith("_old") or c==key]]
                        .rename(columns=lambda c: c.replace("_old","")))
                        if not removed_items.empty else pd.DataFrame({"info":["Sin productos removidos"]}),
        }
    else:
        # si no hay previo, agrega columna vacía para mantener el schema
        if not df.empty and "discount_pct_prev" not in df.columns:
            df["discount_pct_prev"] = None

    # Guardar CSV/PQ/XLSX (el XLSX ya trae SNAPSHOT + EXTRAS)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stamp, csv_path, pq_path, xlsx_path = save_snapshot(
        df, out_dir, pref, highlight=highlight, extras=extras, stamp=stamp
    )
    print(f"   ✅ Snapshot:\n      - CSV : {csv_path.resolve()}\n      - PQ  : {pq_path.resolve()}\n      - XLSX: {xlsx_path.resolve()}")

    # MANDAR EMAIL con el CSV adjunto 📧
    subject = f"[AD MX] {job_name} {stamp} ({pref})"
    body = (
        f"Hola,\n\n"
        f"Adjunto CSV del job '{job_name}'.\n"
        f"Archivo: {csv_path.name}\n"
        f"Productos totales: {len(df)}\n"
        f"Timestamp: {stamp}\n\n"
        f"Saludos.\n"
    )
    try:
        send_email_with_attachment(subject, body, csv_path)
    except Exception as e:
        print(f"❗ Error enviando correo: {e}")

def main():
    args = parse_args()
    session = build_session()

    # Determinar género final (preguntar si toca)
    chosen_gender = args.gender
    if chosen_gender == "ask":
        chosen_gender = pick_gender_interactively()

    jobs = []
    base_fixed = parse_extra_params(args.extra_params)

    if chosen_gender in ("mujer", "ambos"):
        fixed = dict(base_fixed); fixed["q"] = "mujer"
        jobs.append({
            "name": "search_mujer",
            "base_url": args.base_url,
            "fixed_params": fixed,
            "forced_gender": "mujer",
            "alias": f"{args.alias}_mujer",
            "prefix": None,
        })
    if chosen_gender in ("hombre", "ambos"):
        fixed = dict(base_fixed); fixed["q"] = "hombre"
        jobs.append({
            "name": "search_hombre",
            "base_url": args.base_url,
            "fixed_params": fixed,
            "forced_gender": "hombre",
            "alias": f"{args.alias}_hombre",
            "prefix": None,
        })
    for i, full in enumerate(args.also_url, start=1):
        jobs.append({
            "name": f"also_{i}",
            "base_url": full,
            "fixed_params": {},
            "forced_gender": None,
            "alias": f"{args.alias}_extra{i}",
            "prefix": None,
        })

    if not jobs:
        print("⚠️ No hay trabajos por ejecutar (revisa --gender/--also-url).")
        return

    for j in jobs:
        run_job(
            session=session,
            job_name=j["name"],
            base_url=j["base_url"],
            fixed_params=j["fixed_params"],
            page_param=args.page_param,
            page_start=args.page_start,
            max_pages=args.max_pages,
            out_alias=j["alias"],
            out_prefix=j["prefix"],
            forced_gender=j["forced_gender"],
            highlight=args.highlight
        )

if __name__ == "__main__":
    main()
