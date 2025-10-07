# aggregate_master.py
import os, re, glob, time, smtplib
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

# === Config local ===
OUT_BASE_DIR = Path("out_palacio")              # donde tu scraper guarda todo
MASTER_DIR   = OUT_BASE_DIR / "master"          # subcarpeta para los consolidados
MASTER_DIR.mkdir(parents=True, exist_ok=True)

# === Email desde ENV (mismos secrets que ya usas) ===
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASS = os.getenv("EMAIL_PASS", "")
EMAIL_TO   = os.getenv("EMAIL_TO", "")

def _send_email(subject: str, body: str, attachments: list[tuple[str, bytes, str]]):
    if not (EMAIL_USER and EMAIL_PASS and EMAIL_TO):
        print("⚠️ Falta EMAIL_*; no se envía el correo.")
        return
    # normaliza destinatarios
    recipients = [e.strip() for e in re.split(r"[;,]", EMAIL_TO) if e.strip()]
    if not recipients:
        print("⚠️ EMAIL_TO vacío; no se envía.")
        return
    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"]   = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)
    for fname, data, mime in attachments or []:
        mt, st = (mime.split("/", 1) if mime else ("application", "octet-stream"))
        msg.add_attachment(data, maintype=mt, subtype=st, filename=fname)
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as s:
        s.ehlo(); s.starttls(); s.ehlo()
        s.login(EMAIL_USER, EMAIL_PASS)
        s.send_message(msg, from_addr=EMAIL_USER, to_addrs=recipients)
    print(f"📧 Enviado: {subject} → {', '.join(recipients)}")

def _is_placeholder_df(df: pd.DataFrame) -> bool:
    # Detecta hojas "Sin cambios..." que crea el scraper
    return (list(df.columns) == ["info"]) or (df.shape[0] == 0)

def _category_from_path(p: Path) -> str:
    # .../out_palacio/palacio_ofertas/2025-10/palacio_ofertas_snapshot_*.xlsx
    try:
        return (p.parents[2].name or "").replace("palacio_", "")
    except Exception:
        return "desconocido"

def build_master(hours_back: int = 8) -> Path | None:
    """Concatena CHANGES/NEW/REMOVED de los XLSX modificados en las últimas `hours_back` horas."""
    now = time.time()
    xlsx_files = []
    for pat in ("out_palacio/*/*/*.xlsx",):  # todas las categorías/meses
        xlsx_files.extend(Path().glob(pat))
    # filtra por recientes (esta corrida)
    recent = [p for p in xlsx_files if (now - p.stat().st_mtime) <= hours_back*3600]

    if not recent:
        print("⚠️ No encontré XLSX recientes para consolidar.")
        return None

    all_changes, all_new, all_removed = [], [], []

    for p in sorted(recent):
        cat = _category_from_path(p)
        try:
            # Lee si existen; si no, crea vacío
            try:
                df_ch = pd.read_excel(p, sheet_name="CHANGES")
            except Exception:
                df_ch = pd.DataFrame(columns=["info"])
            try:
                df_new = pd.read_excel(p, sheet_name="NEW")
            except Exception:
                df_new = pd.DataFrame(columns=["info"])
            try:
                df_rm  = pd.read_excel(p, sheet_name="REMOVED")
            except Exception:
                df_rm  = pd.DataFrame(columns=["info"])

            if not _is_placeholder_df(df_ch):
                df_ch.insert(0, "category", cat)
                df_ch.insert(1, "source_file", p.name)
                all_changes.append(df_ch)

            if not _is_placeholder_df(df_new):
                df_new.insert(0, "category", cat)
                df_new.insert(1, "source_file", p.name)
                all_new.append(df_new)

            if not _is_placeholder_df(df_rm):
                df_rm.insert(0, "category", cat)
                df_rm.insert(1, "source_file", p.name)
                all_removed.append(df_rm)

        except Exception as e:
            print(f"⚠️ No pude leer {p.name}: {e}")

    # DataFrames finales
    CH = pd.concat(all_changes, ignore_index=True) if all_changes else pd.DataFrame({"info": ["Sin cambios de precio"]})
    NW = pd.concat(all_new,     ignore_index=True) if all_new     else pd.DataFrame({"info": ["Sin nuevos productos"]})
    RM = pd.concat(all_removed, ignore_index=True) if all_removed else pd.DataFrame({"info": ["Sin productos removidos"]})

    stamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d_%H%M%S")
    out_xlsx = MASTER_DIR / f"palacio_master_{stamp}.xlsx"
    out_zipcsv = MASTER_DIR / f"palacio_master_{stamp}.zip"

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as w:
        CH.to_excel(w, index=False, sheet_name="CHANGES_ALL")
        NW.to_excel(w, index=False, sheet_name="NEW_ALL")
        RM.to_excel(w, index=False, sheet_name="REMOVED_ALL")

        # formateo básico
        wb = w.book
        pctfmt = wb.add_format({'num_format': '0.00"%"'})
        for sheet in ("CHANGES_ALL", "NEW_ALL", "REMOVED_ALL"):
            ws = w.sheets[sheet]
            # auto-filter y freeze
            ncols = (CH if sheet=="CHANGES_ALL" else NW if sheet=="NEW_ALL" else RM).shape[1]
            ws.autofilter(0,0, 1_000_000, max(ncols-1,0))
            ws.freeze_panes(1,0)
            # si hay columna discount_pct formatear
            try:
                cols = (CH if sheet=="CHANGES_ALL" else NW if sheet=="NEW_ALL" else RM).columns.tolist()
                if "discount_pct" in cols:
                    idx = cols.index("discount_pct")
                    ws.set_column(idx, idx, 12, pctfmt)
            except Exception:
                pass

    # CSVs opcionales comprimidos
    try:
        import zipfile, io
        with zipfile.ZipFile(out_zipcsv, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for name, df in [("CHANGES_ALL.csv", CH), ("NEW_ALL.csv", NW), ("REMOVED_ALL.csv", RM)]:
                b = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                zf.writestr(name, b)
    except Exception as e:
        print(f"⚠️ No pude crear ZIP de CSV: {e}")

    print(f"✅ Master creado: {out_xlsx}")
    return out_xlsx

def main():
    out = build_master(hours_back=15)
    if out:
        with open(out, "rb") as f:
            data = f.read()
        subject = f"[Scraper] Master consolidado {out.name}"
        body    = f"Adjunto el consolidado maestro con CHANGES/NEW/REMOVED para todas las categorías.\nArchivo: {out.name}"
        _send_email(subject, body, [(out.name, data, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")])

if __name__ == "__main__":
    main()
