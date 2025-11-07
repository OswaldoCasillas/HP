# ===================== PATCH PALACIO GROUP A — COMPLETO =====================
# Imports
from __future__ import annotations
import os, time, random
from datetime import datetime, timezone
from typing import Tuple, Optional, List, Dict, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ===================== Config/Constantes (ajusta si ya existen) =============
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 45
JITTER_MIN, JITTER_MAX = 0.35, 0.9

# Carpeta de salida (usa la tuya si ya tienes otra)
SAVE_DIR = "/tmp/palacio_out"
os.makedirs(SAVE_DIR, exist_ok=True)

# Destinatarios de correo (usa tu lista existente si ya la tienes)
EMAIL_TO_LIST = os.environ.get("SCRAPER_EMAIL_TO", "***@tu-dominio.com").split(",")

# Corta paginación tras N páginas seguidas sin productos
STOP_AFTER_EMPTY = 2  # (antes estaba en 1; subimos a 2 para tolerar PLPs con huecos)

# ===================== User-Agents ==========================================
UA_LIST = [
    # Desktop comunes
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]
# ➕ añadimos móviles (ayudan contra algunos WAF)
UA_LIST.extend([
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
])

def random_headers() -> Dict[str, str]:
    ua = random.choice(UA_LIST)
    return {
        "user-agent": ua,
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "es-MX,es;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
    }

# ===================== Email (usa el tuyo si ya tienes) ======================
def send_email(subject: str, body: str, to_list: List[str], attachments: Optional[List[str]] = None):
    """
    Implementación de correo existente en tu proyecto.
    Aquí sólo se deja la firma; si ya tienes una, ignora esta.
    """
    try:
        # deja el hook a tu imple; aquí sólo logeamos para no romper
        print(f"📧 [EMAIL] {subject}\n{body}\nAdjuntos: {attachments or '—'}")
    except Exception as e:
        print(f"⚠️ send_email falló: {e}")

# ===================== Helpers anti-403 / paginación =========================
def fetch_html(session: requests.Session, url: str) -> Tuple[str, str]:
    """
    GET “humano” con headers realistas y referer básico. Devuelve (html, final_url).
    """
    h = random_headers()
    h.update({
        "upgrade-insecure-requests": "1",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "navigate",
        "sec-fetch-dest": "document",
        "referer": url.split("?")[0],
    })
    resp = session.get(url, headers=h, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    return resp.text, resp.url

def extract_next_page_url(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    """
    Detecta enlace de siguiente página en la PLP.
    Soporta <link rel="next">, <a rel="next"> y varios selectores/textos comunes.
    """
    if not soup:
        return None

    # 1) rel=next
    a = soup.select_one("link[rel='next'], a[rel='next']")
    if a and a.get("href"):
        return urljoin(current_url, a["href"])

    # 2) selectores frecuentes
    candidates = [
        "a.b-pagination_next[href]",
        "a.pagination__next[href]",
        "a.next[href]",
        "a.pager-next[href]",
        "a.b-load_more[href]",
        "button[data-url]",
    ]
    for sel in candidates:
        el = soup.select_one(sel)
        if el:
            href = el.get("href") or el.get("data-url")
            if href:
                return urljoin(current_url, href)

    # 3) por texto
    for lab in ("Siguiente", "Next", "Ver más", "Load more"):
        el = soup.find(lambda tag: tag.name in ("a", "button")
                                 and lab.lower() in tag.get_text(" ", strip=True).lower())
        if el:
            href = el.get("href") or el.get("data-url")
            if href:
                return urljoin(current_url, href)
    return None

# ===================== Fetch con fallback (start/sz) =========================
def _fetch_with_fallback(session: requests.Session, base_url: str, start: int, page_size: int) -> Tuple[str, str, int]:
    """
    Intenta GET con params ?start=&sz=. Si 403, cambia UA/Referer y reintenta 1 vez.
    Devuelve (html, final_url, used_sz).
    """
    params = {"start": start, "sz": page_size}
    headers = random_headers()
    resp = session.get(base_url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    # ▶ retry específico 403
    if resp.status_code == 403:
        headers2 = random_headers()
        headers2["referer"] = base_url
        time.sleep(random.uniform(0.6, 1.1))
        resp = session.get(base_url, params=params, headers=headers2, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

    resp.raise_for_status()
    html_text = resp.text
    real_url = resp.url
    used_sz = page_size

    # si el sitio capó el tamaño, puedes detectar y ajustar used_sz aquí si fuera necesario
    return html_text, real_url, used_sz

# ===================== Parser (se asume existente en tu proyecto) ============
# Debes tener ya algo como:
# def parse_products_from_html(html_text: str, page_url: str, page_start: Optional[int], page_idx: int, captured_at_iso: str) -> Tuple[List[Dict], int]:
#     ...
# Debe devolver (rows:list[dict], tiles_count:int)

# ===================== Runner de categoría (REEMPLAZO TOTAL) =================
def run_single_category(cat_key: str,
                        base_url: str,
                        out_prefix: str,
                        session: requests.Session,
                        start: int = 0,
                        page_size: int = 200,
                        step: int = 200,
                        max_pages: int = 200,
                        stop_after_empty: Optional[int] = None,
                        historical: bool = True) -> Dict:
    """
    Estrategia:
      1) Cargar primera PLP SIN parámetros (evita WAF por querystring).
      2) Si no hay tiles, intentar ?start=&sz= con retry 403.
      3) Paginar preferentemente por enlace real "siguiente".
      4) Si termina con 0 filas y hubo 403, mandar correo de ERROR 403 (sin adjuntos).
    """
    if stop_after_empty is None:
        stop_after_empty = globals().get("STOP_AFTER_EMPTY", 2)

    captured_at = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    all_rows: List[Dict] = []
    seen_keys: Set[str] = set()
    blocked_403: bool = False

    def add_rows(rows: List[Dict]) -> int:
        new_rows = 0
        for r in rows:
            key = (r.get("product_id") or "") + "|" + (r.get("enlace") or "")
            if key and key not in seen_keys:
                seen_keys.add(key)
                all_rows.append(r)
                new_rows += 1
        return new_rows

    # 1) Primera página sin params
    soup0 = None
    next_url = None
    first_tiles = 0
    try:
        html0, url0 = fetch_html(session, base_url)
        soup0 = BeautifulSoup(html0, "html.parser")
        first_rows, first_tiles = parse_products_from_html(
            html0, url0, page_start=0, page_idx=0, captured_at_iso=captured_at
        )
        got = add_rows(first_rows)
        print(f"Página 0 (sin params): tiles={first_tiles}, nuevos={got}")
        next_url = extract_next_page_url(soup0, url0)
    except requests.HTTPError as e:
        if getattr(e, "response", None) is not None and e.response.status_code == 403:
            blocked_403 = True
            print(f"⛔️ 403 primer GET sin params: {base_url}")
        else:
            print(f"⚠️ Primer GET sin params falló: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"⚠️ Primer GET sin params falló: {type(e).__name__}: {e}")

    # 2) Si no hay tiles, intentar con start/sz
    if first_tiles == 0:
        print("↘︎ Sin tiles en primera página; probando ?start=&sz=…")
        try:
            html1, url1, used_sz = _fetch_with_fallback(session, base_url, start, page_size)
            page_rows, tiles_count = parse_products_from_html(
                html1, url1, page_start=start, page_idx=0, captured_at_iso=captured_at
            )
            got = add_rows(page_rows)
            print(f"Página 0 (start/sz={used_sz}): tiles={tiles_count}, nuevos={got}")
            soup0 = BeautifulSoup(html1, "html.parser")
            next_url = extract_next_page_url(soup0, url1)
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            if sc == 403:
                blocked_403 = True
                print("⛔️ 403 con start/sz. Intentaremos paginar por enlaces HTML si hay.")
            else:
                print(f"⚠️ Error de red con start/sz: {e}")
        except Exception as e:
            print(f"⚠️ Error general con start/sz: {e}")

    # 3) Paginación por enlaces reales
    page_idx = 0
    empty_streak = 0
    while page_idx < max_pages and next_url:
        page_idx += 1
        try:
            htmln, urln = fetch_html(session, next_url)
        except requests.HTTPError as e:
            sc = getattr(e.response, "status_code", None)
            print(f"⚠️ Next {page_idx} {sc} en {next_url}")
            if sc in (403, 429):
                time.sleep(random.uniform(1.0, 2.2))
                try:
                    htmln, urln = fetch_html(session, next_url)
                except Exception:
                    if sc == 403:
                        blocked_403 = True
                    break
            else:
                if sc == 403:
                    blocked_403 = True
                break
        except Exception as e:
            print(f"⚠️ Next {page_idx} error: {e}")
            break

        page_rows, tiles_count = parse_products_from_html(
            htmln, urln, page_start=None, page_idx=page_idx, captured_at_iso=captured_at
        )
        got = add_rows(page_rows)
        print(f"Página {page_idx} (link): tiles={tiles_count}, nuevos={got}")

        if tiles_count == 0:
            empty_streak += 1
            if empty_streak >= stop_after_empty:
                print(f"∎ Corte por páginas vacías consecutivas: {empty_streak}")
                break
        else:
            empty_streak = 0

        soup = BeautifulSoup(htmln, "html.parser")
        next_url = extract_next_page_url(soup, urln)

        pause = random.uniform(JITTER_MIN, JITTER_MAX)
        if random.random() < 0.2:
            pause += random.uniform(0.6, 1.2)
        time.sleep(pause)

    # ===================== Salida / Email ====================================
    rows_count = len(all_rows)
    big_disc = sum(1 for r in all_rows if (r.get("descuento_pct") or 0) >= 50)

    df = pd.DataFrame(all_rows)
    saved_path = None
    attachments: List[str] = []

    # Si hubo bloqueo y quedó en 0 filas, correo de error explícito
    if rows_count == 0 and blocked_403:
        subj = f"[Scraper][ERROR 403] {out_prefix} bloqueado"
        body = (f"Categoría: {cat_key}\n"
                f"Resultado: BLOQUEADO (403)\n"
                f"URL base: {base_url}\n"
                f"Filas: 0\n"
                f"Se intentó fallback por enlaces HTML.\n"
                f"Histórico: {'sí' if historical else 'no'}\n"
                f"Guardado: {SAVE_DIR}\n")
        send_email(subj, body, EMAIL_TO_LIST, attachments=None)
        return {"category": cat_key, "ok": False, "rows": 0, "big_disc": 0}

    # Si hay filas, guardamos Excel y notificamos normal
    if rows_count > 0:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"{out_prefix}_{ts}.xlsx"
        saved_path = os.path.join(SAVE_DIR, fname)
        os.makedirs(os.path.dirname(saved_path), exist_ok=True)
        with pd.ExcelWriter(saved_path, engine="xlsxwriter") as xw:
            df.to_excel(xw, index=False)
        attachments.append(saved_path)

    subj = f"[Scraper] {cat_key} listo ({datetime.now().strftime('%Y%m%d_%H%M%S')})"
    body = (f"Categoría: {cat_key}\n"
            f"Filas: {rows_count}\n"
            f"≥50% desc.: {big_disc}\n"
            f"Adjunto: {os.path.basename(saved_path) if saved_path else '—'}\n"
            f"Histórico: {'sí' if historical else 'no'}\n"
            f"Guardado: {SAVE_DIR}\n")
    send_email(subj, body, EMAIL_TO_LIST, attachments=attachments if attachments else None)

    return {"category": cat_key, "ok": True, "rows": rows_count, "big_disc": big_disc}

# ===================== FIN PATCH PALACIO GROUP A ============================
