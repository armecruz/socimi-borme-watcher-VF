#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SOCIMI BORME Watcher
Descarga el sumario del BORME por fecha, inspecciona la Sección II (C) y detecta anuncios de adopción del régimen SOCIMI.
DB: SQLite local (socimi_borme.db) que quedará versionada en el repo por el workflow.
"""
from __future__ import annotations
import sys, os, re, json, time, sqlite3, argparse, datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests
from dateutil import tz
from bs4 import BeautifulSoup

SUMARIO_URL = "https://www.boe.es/datosabiertos/api/borme/sumario/{date}"  # {YYYYMMDD}
HEADERS = {"Accept": "application/json", "User-Agent": "SOCIMI-BORME-Watcher/1.0"}
DB_FILE = "socimi_borme.db"
CSV_FILE = "socimi_borme.csv"

# Patrones que indican adopción/entrada al régimen especial SOCIMI (Ley 11/2009)
ADOPTION_PATTERNS = [
    r"\b(acog(?:e|er(?:se)?|ida|ido|ieron|iéndose)|acuerda(?:n)?\s+acogerse)\s+(?:al?\s+)?r[ée]gimen(?:\s+fiscal)?\s+especial.*SOCIMI\b",
    r"\bopt(?:a|an|ó|aron|ar)\s+por\s+la\s+aplicaci[óo]n\s+del?\s+r[ée]gimen(?:\s+fiscal)?\s+especial.*SOCIMI\b",
    r"\bpas(?:a|arán|aron|ar)\s+a\s+tributar\s+por\s+el?\s+r[ée]gimen(?:\s+fiscal)?\s+especial.*SOCIMI\b",
    r"\bentrada\s+en\s+el?\s+r[ée]gimen(?:\s+fiscal)?\s+especial.*SOCIMI\b",
    r"\badaptaci[óo]n\s+de\s+estatutos.*(SOCIMI|Ley\s+11/2009).*r[ée]gimen\s+especial\b",
    r"\br[ée]gimen\s+fiscal\s+especial\s+de\s+las?\s+SOCIMI\b",
]
ADOPTION_REGEXES = [re.compile(pat, re.IGNORECASE | re.DOTALL) for pat in ADOPTION_PATTERNS]

def madrid_today() -> dt.date:
    tz_madrid = tz.gettz("Europe/Madrid")
    now = dt.datetime.now(tz=tz_madrid)
    return now.date()

def yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")

def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS socimi_events (
            id TEXT PRIMARY KEY,
            pub_date TEXT NOT NULL,
            company TEXT,
            apartado TEXT,
            url_html TEXT,
            url_pdf TEXT,
            matched_pattern TEXT,
            excerpt TEXT
        );
        """
    )
    conn.commit()

def save_event(conn: sqlite3.Connection, rec: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO socimi_events (id, pub_date, company, apartado, url_html, url_pdf, matched_pattern, excerpt)
        VALUES (:id, :pub_date, :company, :apartado, :url_html, :url_pdf, :matched_pattern, :excerpt)
        ON CONFLICT(id) DO UPDATE SET
            pub_date=excluded.pub_date,
            company=excluded.company,
            apartado=excluded.apartado,
            url_html=excluded.url_html,
            url_pdf=excluded.url_pdf,
            matched_pattern=excluded.matched_pattern,
            excerpt=excluded.excerpt
        """,
        rec,
    )
    conn.commit()

def fetch_sumario(fecha: dt.date) -> Optional[Dict[str, Any]]:
    url = SUMARIO_URL.format(date=yyyymmdd(fecha))
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def _as_list(x) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def iter_section_c_items(sumario: Dict[str, Any]) -> Iterable[Tuple[Dict[str, Any], Optional[str]]]:
    """
    Devuelve tuplas (item, apartado_nombre) para la Sección C (Sección II).
    """
    data = sumario.get("data", {}) if isinstance(sumario, dict) else {}
    sumario_node = data.get("sumario", {})
    diarios = _as_list(sumario_node.get("diario"))
    for diario in diarios:
        secciones = _as_list(diario.get("seccion"))
        for s in secciones:
            if s.get("codigo") != "C":
                continue
            apartados = _as_list(s.get("apartado"))
            if apartados:
                for ap in apartados:
                    ap_name = ap.get("nombre")
                    for item in _as_list(ap.get("item")):
                        yield item, ap_name
            for item in _as_list(s.get("item")):
                yield item, None

def fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.text
        return None
    except requests.RequestException:
        return None

def text_from_borme_html(html: str) -> str:
    soup = BeautifulSoup(html, "html5lib")
    for nav in soup.find_all(["nav", "header", "footer", "script", "style"]):
        nav.decompose()
    text = soup.get_text(" ", strip=True)
    return text

def find_adoption(text: str) -> Optional[Tuple[str, str]]:
    for rx in ADOPTION_REGEXES:
        m = rx.search(text)
        if m:
            start = max(m.start() - 140, 0)
            end = min(m.end() + 140, len(text))
            excerpt = text[start:end]
            return rx.pattern, excerpt
    return None

def process_date(fecha: dt.date, conn: sqlite3.Connection) -> int:
    sumario = fetch_sumario(fecha)
    if not sumario:
        print(f"[{fecha}] No hay sumario BORME (404).")
        return 0
    count = 0
    for item, apartado in iter_section_c_items(sumario):
        url_html = (item.get("url_html") or {}).get("texto") if isinstance(item.get("url_html"), dict) else item.get("url_html")
        url_pdf = (item.get("url_pdf") or {}).get("texto") if isinstance(item.get("url_pdf"), dict) else item.get("url_pdf")
        ident = item.get("identificador")
        company = item.get("titulo")
        if not url_html:
            continue
        html = fetch_html(url_html)
        if not html:
            continue
        txt = text_from_borme_html(html)
        hit = find_adoption(txt)
        if hit:
            matched_pattern, excerpt = hit
            rec = {
                "id": ident,
                "pub_date": fecha.isoformat(),
                "company": company,
                "apartado": apartado,
                "url_html": url_html,
                "url_pdf": url_pdf,
                "matched_pattern": matched_pattern,
                "excerpt": excerpt,
            }
            save_event(conn, rec)
            count += 1
    return count

def cmd_run(_):
    # Define ventana de 12 meses hasta hoy (zona Europe/Madrid)
    tz_madrid = tz.gettz("Europe/Madrid")
    today = dt.datetime.now(tz=tz_madrid).date()
    start = today - relativedelta(months=12)
    total = 0
    with sqlite3.connect(DB_FILE) as conn:
        ensure_db(conn)
        for d in daterange(start, today):
            try:
                total += process_date(d, conn)
                time.sleep(0.4)  # cortesía con el servidor
            except Exception as e:
                print(f"Error en {d}: {e}", file=sys.stderr)
    print(f"Procesado ventana 12 meses: {start} → {today}. Nuevos eventos: {total}")


def daterange(d1: dt.date, d2: dt.date):
    step = dt.timedelta(days=1)
    cur = d1
    while cur <= d2:
        yield cur
        cur += step

def cmd_backfill(args):
    start = dt.date.fromisoformat(args.start)
    tz_madrid = tz.gettz("Europe/Madrid")
    end = dt.datetime.now(tz=tz_madrid).date()
    total = 0
    with sqlite3.connect(DB_FILE) as conn:
        ensure_db(conn)
        for d in daterange(start, end):
            try:
                total += process_date(d, conn)
                time.sleep(0.5)
            except Exception as e:
                print(f"Error en {d}: {e}", file=sys.stderr)
    print(f"Backfill terminado. Eventos nuevos: {total}")

def cmd_export(_):
    import csv
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.execute("SELECT id, pub_date, company, apartado, url_html, url_pdf, matched_pattern, excerpt FROM socimi_events ORDER BY pub_date DESC, id DESC")
        rows = cur.fetchall()
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","pub_date","company","apartado","url_html","url_pdf","matched_pattern","excerpt"])
        for r in rows:
            w.writerow(r)
    print(f"Exportado {len(rows)} filas a {CSV_FILE}")

def build_cli():
    import argparse
    p = argparse.ArgumentParser(description="SOCIMI BORME Watcher")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Procesa el BORME del día (o viernes si es fin de semana)")
    p_run.set_defaults(func=cmd_run)

    p_bf = sub.add_parser("backfill", help="Procesa un rango desde --from hasta hoy (incl.)")
    p_bf.add_argument("--from", dest="start", required=True, help="AAAA-MM-DD")
    p_bf.set_defaults(func=cmd_backfill)

    p_exp = sub.add_parser("export", help="Exporta CSV desde la base de datos")
    p_exp.set_defaults(func=cmd_export)

    return p

if __name__ == "__main__":
    cli = build_cli()
    ns = cli.parse_args()
    ns.func(ns)
