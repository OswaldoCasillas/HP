#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper de categorías de El Palacio de Hierro (grupo A) con:
- Reintentos y User-Agent rotatorio para reducir 403
- Export a Parquet + Excel
- Envío de correo con adjunto por SMTP (credenciales por variables de entorno)
- CLI con --all o --cat para ejecutar categorías específicas

Requisitos (pip):
  pip install requests beautifulsoup4 lxml pandas openpyxl python-dotenv

Variables de entorno SMTP (ejemplos con Gmail/GSuite):
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=tu_usuario@dominio.com
  SMTP_PASS=tu_password_ou_app_password
  SMTP_FROM=tu_usuario@dominio.com
  SMTP_TO=correo1@dom.com,correo2@dom.com
"""

from __future__ import annotations
import os
import re
import io
import ssl
import sys
import time
import smtplib
import random
import logging
import argparse
from email.message import EmailMessage
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin, urlencode

import requests
import pandas as pd
from bs4 import BeautifulSoup

# =========================
# Configuración y utilidades
# =========================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

HEADERS_BASE = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "es-MX,es;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "upgrade-insecure-requests": "1",
}

SESSION = requests.Session()
SESSION.verify = True


def tz_now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")


def ts_for_filename(prefix: str = "2025") -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default


def sleep_jitter(base: float = 0.7, spread: float = 0.6) -> None:
    time.sleep(max(0.05, base + random.uniform(-spread, spread)))


def log_setup(verbosity: int = 1):
    level = logging.INFO if verbosity <= 1 else logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )


# =========================
# Scraping
# =========================

def fetch_url(url: str, max_retries: int = 4, timeout: int = 20) -> Optional[str]:
    """
    Descarga HTML con reintentos, rotando User-Agent y con backoff.
    Devuelve texto HTML o None si falla.
    """
    for attempt in range(1, max_retries + 1):
        headers = dict(HEADERS_BASE)
        headers["user-agent"] = random.choice(USER_AGENTS)
        if "elpalaciodehierro.com" in url:
            headers["referer"] = "https://www.elpalaciodehierro.com/"

        try:
            resp = SESSION.get(url, headers=headers, timeout=timeout)
            status = resp.status_code
            if status == 200 and resp.text:
                return resp.text

            if status in (403, 429):
                wait = (2 ** attempt) + random.uniform(0.2, 0.9)
                logging.warning(f"HTTP {status} en intento {attempt}/{max_retries} -> backoff {wait:.1f}s")
                time.sleep(wait)
                continue

            if 500 <= status < 600:
                wait = (1.5 ** attempt) + random.uniform(0.1, 0.6)
                logging.warning(f"HTTP {status} en intento {attempt}/{max_retries} -> backoff {wait:.1f}s")
                time.sleep(wait)
                continue

            logging.error(f"HTTP {status} inesperado para {url}")
            return None

        except requests.RequestException as e:
            wait = (1.7 ** attempt) + random.uniform(0.1, 0.8)
            logging.warning(f"Error de red '{e}' intento {attempt}/{max_retries} -> {wait:.1f}s")
            time.sleep(wait)

    logging.error(f"Max reintentos alcanzado para {url}")
    return None


def build_list_url(base: str, start: int, sz: int) -> str:
    params = {"start": start, "sz": sz}
    return f"{base}?{urlencode(params)}"


def parse_listing(html: str, base_url: str) -> List[Dict]:
    """
    Parser conservador: intenta detectar tarjetas de producto comunes.
    Si no logra parsear, devuelve lista vacía (no truena).
    """
    out: List[Dict] = []
    soup = BeautifulSoup(html, "lxml")

    candidates = [
        ("article", {"class": re.compile(r"product.*card|grid-tile|product-tile", re.I)}),
        ("div", {"class": re.compile(r"product.*card|grid-tile|product-tile", re.I)}),
        ("li", {"class": re.compile(r"product.*card|grid-tile|product-tile", re.I)}),
    ]

    product_nodes = []
    for tag, attrs in candidates:
        nodes = soup.find_all(tag, attrs=attrs)
        if nodes:
            product_nodes = nodes
            break

    if not product_nodes:
        product_nodes = soup.select('[data-product-id], [data-sku], [data-pid]')

    if not product_nodes:
        anchors = soup.select("a[href*='/p/'], a[href*='/product/'], a[href*='pid=']")
        for a in anchors:
            product_nodes.append(a.parent if a.parent else a)

    for node in product_nodes:
        a = node.find("a", href=True)
        href = a["href"].strip() if a else None
        if href and href.startswith("/"):
            href = urljoin(base_url, href)

        title = None
        name_node = node.find(attrs={"class": re.compile(r"name|title|product-name", re.I)}) or node.find("h3") or node.find("h2")
        if name_node:
            title = " ".join(name_node.get_text(" ").split())

        price_text = ""
        price_node = node.find(attrs={"class": re.compile(r"price|pricing|sales|value", re.I)})
        if price_node:
            price_text = " ".join(price_node.get_text(" ").split())
        if not price_text:
            m = re.search(r"\$\s?[\d\.,]+", node.get_text(" "), re.I)
            if m:
                price_text = m.group(0)

        brand = None
        brand_node = node.find(attrs={"class": re.compile(r"brand", re.I)})
        if brand_node:
            brand = " ".join(brand_node.get_text(" ").split())

        pid = node.get("data-product-id") or node.get("data-pid") or node.get("data-sku")

        out.append({
            "title": title,
            "price_raw": price_text,
            "brand": brand,
            "url": href,
            "pid": pid,
        })

    cleaned = []
    for r in out:
        if not r.get("title") and not r.get("url"):
            continue
        cleaned.append(r)

    return cleaned


def scrape_category(
    name: str,
    base_url: str,
    page_size: int = 200,
    max_pages: int = 200,
    step: int = 200,
) -> pd.DataFrame:
    """
    Recorre páginas como ?start=0&sz=200, ?start=200&sz=200, ...
    Devuelve un DataFrame (posiblemente vacío si hubo 403 o sin parsear).
    """
    all_rows: List[Dict] = []
    start = 0
    errors_in_row = 0
    max_consecutive_errors = 3

    logging.info(f"=== {name} ===")
    logging.info(f"URL base: {base_url}")
    logging.info(f"start={start}, sz={page_size}, step={step}, max_pages={max_pages}")

    for page in range(max_pages):
        list_url = build_list_url(base_url, start=start, sz=page_size)
        html = fetch_url(list_url)

        if not html:
            errors_in_row += 1
            logging.warning(f"⚠️ Error de red start={start}")
            if errors_in_row >= max_consecutive_errors:
                logging.error("Fin por errores consecutivos.")
                break
            start += step
            sleep_jitter(0.4, 0.3)
            continue

        errors_in_row = 0
        rows = parse_listing(html, base_url=base_url)

        logging.info(f"start={start} -> {len(rows)} filas")
        all_rows.extend(rows)

        if len(rows) == 0:
            next_html = fetch_url(build_list_url(base_url, start=start + step, sz=page_size))
            if not next_html:
                logging.warning("Siguiente página también falló (posible bloqueo). Cortamos.")
                break
            next_rows = parse_listing(next_html, base_url=base_url)
            if len(next_rows) == 0:
                logging.info("Dos páginas seguidas sin resultados. Cortamos.")
                break
            else:
                all_rows.extend(next_rows)
                start += step * 2
                continue

        start += step
        sleep_jitter(0.4, 0.3)

    if not all_rows:
        return pd.DataFrame(columns=["title", "price_raw", "brand", "url", "pid", "categoria"])
    df = pd.DataFrame(all_rows)
    df["categoria"] = name
    return df


# =========================
# Salidas (Excel/Parquet)
# =========================

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_outputs(df: pd.DataFrame, categoria: str, outdir: str, prefix: str = "palacio") -> Tuple[str, str]:
    ensure_dir(outdir)
    stamp = ts_for_filename()

    parquet_name = f"{prefix}_{categoria}_snapshot_{stamp}.parquet".replace("__", "_")
    excel_name = f"{prefix}_{categoria}_snapshot_{stamp}.xlsx".replace("__", "_")

    parquet_path = os.path.join(outdir, parquet_name)
    excel_path = os.path.join(outdir, excel_name)

    try:
        df.to_parquet(parquet_path, index=False)
        logging.info(f"💾 Parquet: {parquet_path}")
    except Exception as e:
        logging.warning(f"No se pudo guardar Parquet: {e}")

    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as xw:
            df.to_excel(xw, index=False, sheet_name="data")
        logging.info(f"💾 Excel:   {excel_path}")
    except Exception as e:
        logging.error(f"No se pudo guardar Excel: {e}")
        excel_path = ""

    return parquet_path, excel_path


# =========================
# Email
# =========================

def smtp_env() -> Dict[str, str]:
    return {
        "host": os.environ.get("SMTP_HOST", ""),
        "port": os.environ.get("SMTP_PORT", "587"),
        "user": os.environ.get("SMTP_USER", ""),
        "password": os.environ.get("SMTP_PASS", ""),
        "from": os.environ.get("SMTP_FROM", os.environ.get("SMTP_USER", "")),
        "to": os.environ.get("SMTP_TO", ""),
    }


def send_email(subject: str, body: str, attachments: List[str]) -> None:
    cfg = smtp_env()
    missing = [k for k, v in cfg.items() if not v and k in ("host", "port", "user", "password", "from")]
    if missing:
        logging.warning(f"⚠️ Email NO enviado, faltan variables: {missing}")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from"]
    msg["To"] = cfg["to"] or cfg["from"]
    msg.set_content(body)

    for path in attachments:
        if not path or not os.path.exists(path):
            continue
        with open(path, "rb") as f:
            data = f.read()
        fname = os.path.basename(path)
        maintype, subtype = ("application", "octet-stream")
        if fname.lower().endswith(".xlsx"):
            maintype, subtype = ("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        elif fname.lower().endswith(".parquet"):
            maintype, subtype = ("application", "octet-stream")

        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)

    context = ssl.create_default_context()
    with smtplib.SMTP(cfg["host"], int(cfg["port"])) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(cfg["user"], cfg["password"])
        server.send_message(msg)
        logging.info(f"📧 Email enviado a {msg['To']}: {subject}")


# =========================
# Orquestación por categoría / grupo
# =========================

CATEGORIAS_GRUPO_A: Dict[str, str] = {
    "mujer": "https://www.elpalaciodehierro.com/mujer/",
    "hogar": "https://www.elpalaciodehierro.com/hogar/",
    # Puedes añadir más aquí:
    # "electronica": "https://www.elpalaciodehierro.com/electronica/",
    # "deportes": "https://www.elpalaciodehierro.com/deportes/",
}


def run_categoria(
    nombre: str,
    base_url: str,
    outdir: str,
    page_size: int = 200,
    max_pages: int = 200,
    step: int = 200,
    enviar_correo: bool = True,
    umbral_desc_porcentaje: int = 50,
    historico: bool = True,
) -> Tuple[pd.DataFrame, str, str]:
    """
    Ejecuta scraping de 1 categoría, guarda outputs y (opcional) manda correo.
    Retorna (df, parquet_path, excel_path).
    """
    t0 = time.time()
    df = scrape_category(nombre, base_url, page_size=page_size, max_pages=max_pages, step=step)

    parquet_path, excel_path = save_outputs(
        df,
        categoria=nombre,
        outdir=outdir,
        prefix="palacio"
    )

    filas = len(df)
    desc_count = 0
    if not df.empty:
        desc_count = df["price_raw"].fillna("").str.contains(
            rf"{umbral_desc_porcentaje}\s?%|{umbral_desc_porcentaje}\s?%\s?desc", flags=re.I, regex=True
        ).sum()

    asunto = f"[Scraper] palacio_{nombre} listo ({ts_for_filename()})"
    cuerpo = (
        f"Categoría: {nombre}\n"
        f"Filas: {filas}\n"
        f"≥{umbral_desc_porcentaje}% desc.: {desc_count}\n"
        f"Adjunto: {os.path.basename(excel_path) if excel_path else '(sin excel)'}\n"
        f"Histórico: {'sí' if historico else 'no'}\n"
        f"Guardado: {outdir}\n"
        f"Fin: {tz_now_iso()}\n"
        f"Duración: {time.time() - t0:.1f}s\n"
    )

    logging.info("\n" + cuerpo)

    if enviar_correo:
        send_email(
            subject=asunto,
            body=cuerpo,
            attachments=[excel_path] if excel_path else []
        )

    return df, parquet_path, excel_path


def run_grupo_a_todo(
    outdir: str,
    enviar_correo: bool = True,
    page_size: int = 200,
    max_pages: int = 200,
    step: int = 200,
) -> None:
    logging.info("▶ Ejecutando TODAS las categorías del grupo…")
    for nombre, base in CATEGORIAS_GRUPO_A.items():
        sleep_jitter(1.2, 0.9)
        try:
            run_categoria(
                nombre=nombre,
                base_url=base,
                outdir=outdir,
                page_size=page_size,
                max_pages=max_pages,
                step=step,
                enviar_correo=enviar_correo,
            )
            sleep_jitter(1.33, 0.4)
        except Exception as e:
            logging.exception(f"Error en categoría {nombre}: {e}")
    logging.info("🎉 Terminaron todas.")


# =========================
# CLI (MAIN)
# =========================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scraper grupo A de El Palacio de Hierro")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="Ejecuta todas las categorías del grupo A")
    g.add_argument("--cat", nargs="+", help="Una o más categorías específicas (p.ej. --cat mujer hogar)")

    p.add_argument("--outdir", default="./salidas_palacio", help="Directorio de salida (parquet/excel)")
    p.add_argument("--no-email", action="store_true", help="No enviar correo")
    p.add_argument("--page-size", type=int, default=200)
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--step", type=int, default=200)
    p.add_argument("-v", "--verbose", action="count", default=0, help="Más logs (-v, -vv)")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    log_setup(args.verbose)

    enviar_correo = not args.no_email
    outdir = args.outdir

    if args.all:
        run_grupo_a_todo(
            outdir=outdir,
            enviar_correo=enviar_correo,
            page_size=args.page_size,
            max_pages=args.max_pages,
            step=args.step,
        )
        return 0

    cats = [c.strip().lower() for c in args.cat]
    invalid = [c for c in cats if c not in CATEGORIAS_GRUPO_A]
    if invalid:
        validos = ", ".join(sorted(CATEGORIAS_GRUPO_A.keys()))
        logging.error(f"Categorías inválidas: {invalid}. Válidas: {validos}")
        return 2

    for c in cats:
        base = CATEGORIAS_GRUPO_A[c]
        run_categoria(
            nombre=c,
            base_url=base,
            outdir=outdir,
            enviar_correo=enviar_correo,
            page_size=args.page_size,
            max_pages=args.max_pages,
            step=args.step,
        )
        sleep_jitter(1.1, 0.5)

    logging.info("✅ Listo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
