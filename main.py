import os, asyncio, httpx, io, re, json
from collections import Counter
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, UploadFile, File, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import pdfplumber
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    import psycopg2
    HAS_PG = True
except ImportError:
    HAS_PG = False

# ── Config ─────────────────────────────────────────────────────────────────────
DATABASE_URL  = os.getenv("DATABASE_URL", "")
VETA_COOKIE   = os.getenv("VETA_COOKIE", "")
TZ_ARG        = ZoneInfo("America/Argentina/Buenos_Aires")

IAMC_BASE = "https://www.iamc.com.ar/Informe/InformeDiarioOpciones"

app = FastAPI(title="Merlin Opciones API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Estado global ──────────────────────────────────────────────────────────────
state = {
    "opciones": [],
    "resumen": {},
    "fecha": None,
    "updated_at": None,
    "descarga_ok": False,
    "error": None,
}
_scheduler_task = None

# ── PostgreSQL helpers ─────────────────────────────────────────────────────────
def _pg_conn():
    return psycopg2.connect(DATABASE_URL)

def _pg_init():
    if not HAS_PG or not DATABASE_URL: return
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iamc_opciones_pdf (
                id INTEGER PRIMARY KEY DEFAULT 1,
                pdf_bytes BYTEA,
                fecha TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS iamc_opciones_data (
                fecha TEXT NOT NULL,
                symbol TEXT NOT NULL,
                tipo TEXT,
                subyacente TEXT,
                vencimiento TEXT,
                strike NUMERIC,
                moneyness TEXT,
                precio_suby NUMERIC,
                apertura_prima NUMERIC,
                min_prima NUMERIC,
                max_prima NUMERIC,
                ultimo_precio NUMERIC,
                var_prima_pct NUMERIC,
                hora_ultimo TEXT,
                volumen_ars NUMERIC,
                cant_ops INTEGER,
                open_interest INTEGER,
                var_oi_pct NUMERIC,
                precio_teorico NUMERIC,
                desvio_teorico NUMERIC,
                valor_temporal NUMERIC,
                vol_hist_40r NUMERIC,
                vol_implicita NUMERIC,
                delta NUMERIC,
                gamma NUMERIC,
                theta NUMERIC,
                vega NUMERIC,
                rho NUMERIC,
                tasa_libre NUMERIC,
                dias_vto INTEGER,
                PRIMARY KEY (fecha, symbol)
            );
        """)
        conn.commit(); cur.close(); conn.close()
        print("PG init OK")
    except Exception as e:
        print(f"PG init error: {e}")

def _pg_save_pdf(pdf_bytes: bytes, fecha: str):
    if not HAS_PG or not DATABASE_URL: return
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO iamc_opciones_pdf (id, pdf_bytes, fecha, updated_at)
            VALUES (1, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET pdf_bytes=EXCLUDED.pdf_bytes,
            fecha=EXCLUDED.fecha, updated_at=NOW()
        """, (psycopg2.Binary(pdf_bytes), fecha))
        conn.commit(); cur.close(); conn.close()
        print(f"PDF guardado en PG: {fecha}")
    except Exception as e:
        print(f"PG save pdf error: {e}")

def _pg_save_opciones(rows: list, fecha: str):
    if not HAS_PG or not DATABASE_URL or not rows: return
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("DELETE FROM iamc_opciones_data WHERE fecha = %s", (fecha,))
        for r in rows:
            cur.execute("""
                INSERT INTO iamc_opciones_data VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) ON CONFLICT DO NOTHING
            """, (
                fecha, r.get("symbol"), r.get("tipo"), r.get("subyacente"),
                r.get("vencimiento"), r.get("strike"), r.get("moneyness"),
                r.get("precio_suby"), r.get("apertura_prima"), r.get("min_prima"),
                r.get("max_prima"), r.get("ultimo_precio"), r.get("var_prima_pct"),
                r.get("hora_ultimo"), r.get("volumen_ars"), r.get("cant_ops"),
                r.get("open_interest"), r.get("var_oi_pct"), r.get("precio_teorico"),
                r.get("desvio_teorico"), r.get("valor_temporal"), r.get("vol_hist_40r"),
                r.get("vol_implicita"), r.get("delta"), r.get("gamma"),
                r.get("theta"), r.get("vega"), r.get("rho"),
                r.get("tasa_libre"), r.get("dias_vto"),
            ))
        conn.commit(); cur.close(); conn.close()
        print(f"Opciones guardadas en PG: {len(rows)} filas, fecha {fecha}")
    except Exception as e:
        print(f"PG save opciones error: {e}")

def _pg_load_latest():
    if not HAS_PG or not DATABASE_URL: return None, None
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("SELECT pdf_bytes, fecha FROM iamc_opciones_pdf WHERE id=1")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row: return bytes(row[0]), row[1]
    except Exception as e:
        print(f"PG load error: {e}")
    return None, None

def _pg_load_opciones(fecha: str = None):
    if not HAS_PG or not DATABASE_URL: return []
    try:
        conn = _pg_conn(); cur = conn.cursor()
        if fecha:
            cur.execute("SELECT * FROM iamc_opciones_data WHERE fecha=%s ORDER BY subyacente, vencimiento, strike", (fecha,))
        else:
            cur.execute("""SELECT * FROM iamc_opciones_data WHERE fecha=(
                SELECT MAX(fecha) FROM iamc_opciones_data
            ) ORDER BY subyacente, vencimiento, strike""")
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f"PG load opciones error: {e}")
        return []

# ── Mapeo prefijo de opción → subyacente ───────────────────────────────────────
# La convención BYMA es estable: ticker de opción = prefijo + C/V + strike + serie.
# Ej: GFGC4200OC → GFG + C + 4200 + OC (CALL 4200, serie octubre)
# Fuente de verdad para asignar subyacente (el encabezado del PDF se mezcla).
# Si aparece un prefijo nuevo: /admin/debug-symbols → agregar acá.
OPCION_MAP = {
    # Panel Merval
    "GFG":  "GGAL",   # Grupo Financiero Galicia
    "GGAL": "GGAL",
    "YPF":  "YPFD",
    "YPFD": "YPFD",
    "BBA":  "BBAR",
    "BBAR": "BBAR",
    "TXA":  "TXAR",
    "TXAR": "TXAR",
    "MET":  "METR",
    "METR": "METR",
    "CRO":  "CRES",   # Cresud
    "CRE":  "CRES",
    "CRES": "CRES",
    "SUP":  "SUPV",
    "SUPV": "SUPV",
    "TGS":  "TGSU2",
    "TGSU": "TGSU2",
    "EDN":  "EDN",
    "VIST": "VIST",
    "VIS":  "VIST",
    "VST":  "VIST",   # Vista (confirmado por debug-symbols)
    "BMK":  "BYMA",
    "BYM":  "BYMA",   # BYMA (confirmado por debug-symbols)
    "BYMA": "BYMA",
    # Resto del panel / líquidos
    "ALU":  "ALUA",
    "ALUA": "ALUA",
    "BMA":  "BMA",
    "BHI":  "BHIP",   # Banco Hipotecario (confirmado)
    "TEC":  "TECO2",  # Telecom (confirmado)
    "TRA":  "TRAN",
    "TRAN": "TRAN",
    "COM":  "COME",
    "COME": "COME",
    "PAM":  "PAMP",
    "PAMP": "PAMP",
    "LOM":  "LOMA",
    "LOMA": "LOMA",
    "MIR":  "MIRG",
    "MIRG": "MIRG",
    "IRS":  "IRSA",
    "IRSA": "IRSA",
    "IRC":  "IRCP",
    "CEP":  "CEPU",
    "CEPU": "CEPU",
    "CEC":  "CEPU",   # confirmado por debug-symbols (suby_en_pdf: CEPU)
    "VAL":  "VALO",
    "VALO": "VALO",
    "TGN":  "TGNO4",
    "AUS":  "AUSO",
    "OES":  "OEST",
    "GAL":  "GAMI",
}

# ── NUEVO: vencimiento por serie del símbolo (fallback si el texto no lo da) ───
# El PDF usa series OC (octubre) y DI (diciembre). Se usa SOLO si ni el texto de
# la página ni el aprendizaje lograron determinar el vencimiento. Si IAMC agrega
# una serie nueva (ej: enero), aparece en los logs como serie sin mapear.
SERIE_VTO_FALLBACK = {
    "OC": "2026-10-16",
    "O":  "2026-10-16",
    "DI": "2026-12-18",
    "D":  "2026-12-18",
}

# Símbolo BYMA: prefijo + C/V + strike (decimales opcionales) + serie opcional
RE_OPCION = re.compile(r'^([A-Z0-9]+?)([CV])(\d{2,7}(?:\.\d+)?)\.?([A-Z]{1,2})?$')

# Fragmento que parece símbolo de opción (para detectar basura multi-símbolo)
RE_SYM_FRAGMENT = re.compile(r'[A-Z0-9]{2,8}[CV]\d')

# Hora tipo 16:32:25 o 16:32 (ancla para recuperar líneas colapsadas)
RE_HORA = re.compile(r'^\d{1,2}:\d{2}(:\d{2})?$')

# ── NUEVO: detección robusta de vencimiento en texto ───────────────────────────
MESES_NUM = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
    'julio': 7, 'agosto': 8, 'septiembre': 9, 'setiembre': 9, 'octubre': 10,
    'noviembre': 11, 'diciembre': 12,
}
RE_MES       = re.compile(r'\b(' + '|'.join(MESES_NUM) + r')\b', re.IGNORECASE)
RE_FECHA_DMY = re.compile(r'\b(\d{1,2})\s*[/\-.]\s*(\d{1,2})\s*[/\-.]\s*(\d{2,4})\b')
RE_FECHA_TXT = re.compile(r'\b(\d{1,2})\s*[-\./]\s*([A-Za-z]{3,10})\s*[-\./]\s*(\d{2,4})\b')

def _mes_num(nombre: str):
    return MESES_NUM.get(nombre.lower()) or next(
        (v for k, v in MESES_NUM.items() if k[:3] == nombre.lower()[:3]), None)

def _parse_fecha_str(fecha_str: str):
    """'11-sep-26' → date(2026, 9, 11). None si no se puede."""
    if not fecha_str: return None
    m = re.match(r'^(\d{1,2})[.\-/]([A-Za-z]{3})[.\-/](\d{2,4})$', fecha_str)
    if not m: return None
    mes = _mes_num(m.group(2))
    if not mes: return None
    try:
        yy = int(m.group(3)); yy += 2000 if yy < 100 else 0
        return date(yy, mes, int(m.group(1)))
    except ValueError:
        return None

def _detect_vto(text: str, report_date=None):
    """Detecta el vencimiento en el texto de una página. Devuelve ISO o None."""
    lines = text.split('\n')
    # 1) Fuerte: nombre de mes y fecha dd/mm/yyyy en la MISMA línea
    for line in lines:
        if not RE_MES.search(line): continue
        m_f = RE_FECHA_DMY.search(line)
        if m_f:
            try:
                yy = int(m_f.group(3)); yy += 2000 if yy < 100 else 0
                return date(yy, int(m_f.group(2)), int(m_f.group(1))).isoformat()
            except ValueError: pass
    # 2) Medio: mes en una línea, fecha en la siguiente
    for i, line in enumerate(lines):
        if RE_MES.search(line) and i + 1 < len(lines):
            m_f = RE_FECHA_DMY.search(lines[i + 1])
            if m_f:
                try:
                    yy = int(m_f.group(3)); yy += 2000 if yy < 100 else 0
                    return date(yy, int(m_f.group(2)), int(m_f.group(1))).isoformat()
                except ValueError: pass
    # 3) Fecha textual "16-oct-26" (descartando la fecha del informe)
    for m in RE_FECHA_TXT.finditer(text):
        mes = _mes_num(m.group(2))
        if not mes: continue
        try:
            yy = int(m.group(3)); yy += 2000 if yy < 100 else 0
            d = date(yy, mes, int(m.group(1)))
        except ValueError: continue
        if report_date and d <= report_date: continue   # es la fecha del informe
        return d.isoformat()
    # 4) Último recurso: fecha dd/mm cuyo nombre de mes aparece en el texto
    tl = text.lower()
    for m_f in RE_FECHA_DMY.finditer(text):
        mm = int(m_f.group(2))
        nombre = next((k for k, v in MESES_NUM.items() if v == mm), None)
        if not nombre or nombre not in tl: continue
        try:
            yy = int(m_f.group(3)); yy += 2000 if yy < 100 else 0
            d = date(yy, mm, int(m_f.group(1)))
        except ValueError: continue
        if report_date and d <= report_date: continue
        return d.isoformat()
    return None

def parse_symbol(sym: str):
    """Divide el símbolo: GFGC4200OC → ('GFG','CALL',4200.0,'OC')."""
    m = RE_OPCION.match((sym or '').strip().upper())
    if not m: return None
    pref, cv, strike, serie = m.groups()
    return pref, ('CALL' if cv == 'C' else 'PUT'), float(strike), (serie or '')

def resolve_suby(pref: str):
    """Resuelve el subyacente desde el prefijo, probando truncados (TGSU→TGS)."""
    if pref in OPCION_MAP: return OPCION_MAP[pref]
    for L in (4, 3):
        if len(pref) >= L and pref[:L] in OPCION_MAP:
            return OPCION_MAP[pref[:L]]
    return None

# ── Sanity check por fila (IVs/griegas basura del PDF) ─────────────────────────
def _sanity_fix_row(r: dict):
    # Primas negativas no existen
    for f in ("apertura_prima", "min_prima", "max_prima", "ultimo_precio", "precio_teorico"):
        if r.get(f) is not None and r[f] < 0:
            r[f] = None
    # Teórico <= 0 = solver roto → teórico y desvío no confiables
    if r.get("precio_teorico") is not None and r["precio_teorico"] <= 0:
        r["precio_teorico"] = None
        r["desvio_teorico"] = None
    # IV imposible = solver roto → IV y griegas derivadas no confiables
    iv = r.get("vol_implicita")
    if iv is not None and not (0.5 <= iv <= 300):
        r["vol_implicita"] = None
        for f in ("delta", "gamma", "theta", "vega", "rho"):
            r[f] = None
    # Gamma negativa en un CALL es imposible
    if r.get("tipo") == "CALL" and r.get("gamma") is not None and r["gamma"] < 0:
        for f in ("delta", "gamma", "theta", "vega", "rho"):
            r[f] = None
    return r

# ── Parser PDF IAMC ────────────────────────────────────────────────────────────
def _pf(v):
    if v is None: return None
    try:
        s = str(v).replace('%','').replace(',','').strip()
        if s in ('', '-', '—'): return None
        return float(s)
    except: return None

def _pi(v):
    f = _pf(v)
    return int(f) if f is not None else None

def _recover_line_row(line: str, ctx: dict, resolve_vto, unmapped: set):
    """
    Recupera filas que pdfplumber colapsó en una única celda, ej:
      'TXAC700.DI 700 -4.71% OTM 669.5 60.0 ... 16:32:25 60000.0 1 60.00 ... 34.57% 39.80% 0.57 0.0029 -0.49 1.36 0.86'
      'YPFC9400DI 9400 -6.06% OTM 8830.0 0.0 0 30.62%'
    Devuelve dict de fila o None si es basura inservible.
    FIX: SIN ancla moneyness (ITM/OTM/ATM) → None (basura de rankings, etc.)
    """
    toks = line.split()
    if not toks: return None

    # Página de rankings: varios símbolos de opción en la misma línea → basura
    if len(RE_SYM_FRAGMENT.findall(line)) > 1:
        return None

    info = parse_symbol(toks[0])
    if not info: return None
    pref, tipo, strike, serie = info

    suby = resolve_suby(pref)
    if suby is None:
        unmapped.add(pref)
        return None   # sin mapeo no asignamos subyacente (evita contaminación)

    vto = resolve_vto(serie, ctx.get("vto"))

    row = {"symbol": toks[0].upper(), "tipo": tipo, "subyacente": suby,
           "vencimiento": vto, "strike": strike,
           "tasa_libre": ctx.get("tasa"), "dias_vto": ctx.get("dias"),
           "_recovered": True}

    rest = toks[1:]
    mon_idx = next((i for i, t in enumerate(rest) if t in ('ITM', 'OTM', 'ATM')), None)
    if mon_idx is None:
        return None   # FIX: sin moneyness no es una fila de cadena → basura
    if mon_idx > 0 and rest[mon_idx-1].endswith('%'):
        row["distancia_itm_otm"] = rest[mon_idx-1]
    row["moneyness"] = rest[mon_idx]
    if mon_idx + 1 >= len(rest):
        return row
    row["precio_suby"] = _pf(rest[mon_idx + 1])
    after = rest[mon_idx + 2:]

    # ── Cola de griegas completa: ..., VH%, IV%, delta, gamma, theta, vega, rho
    if len(after) >= 7:
        last5 = after[-5:]
        if (all(_pf(t) is not None and not t.endswith('%') for t in last5)
                and after[-7].endswith('%') and after[-6].endswith('%')):
            row["vol_hist_40r"]  = _pf(after[-7])
            row["vol_implicita"] = _pf(after[-6])
            row["delta"] = _pf(after[-5])
            row["gamma"] = _pf(after[-4])
            row["theta"] = _pf(after[-3])
            row["vega"]  = _pf(after[-2])
            row["rho"]   = _pf(after[-1])
            after = after[:-7]
    # ── Cola parcial (sin VH): ..., IV%, delta, gamma, theta, vega, rho
    elif len(after) >= 6:
        last5 = after[-5:]
        if (all(_pf(t) is not None and not t.endswith('%') for t in last5)
                and after[-6].endswith('%')):
            row["vol_implicita"] = _pf(after[-6])
            row["delta"] = _pf(after[-5])
            row["gamma"] = _pf(after[-4])
            row["theta"] = _pf(after[-3])
            row["vega"]  = _pf(after[-2])
            row["rho"]   = _pf(after[-1])
            after = after[:-6]

    # ── Ancla hora: separa [apertura..var%] | hora | [vol..vt]
    hora_idx = next((i for i, t in enumerate(after) if RE_HORA.match(t)), None)
    if hora_idx is not None:
        left, middle = after[:hora_idx], after[hora_idx + 1:]
        row["hora_ultimo"] = after[hora_idx]
        vals, varpct = [], None
        for t in left:
            if t.endswith('%'):
                varpct = _pf(t); break
            f = _pf(t)
            if f is None: break
            vals.append(f)
        for field, v in zip(("apertura_prima", "min_prima", "max_prima", "ultimo_precio"), vals):
            row[field] = v
        if varpct is not None: row["var_prima_pct"] = varpct
        mvals = [_pf(t) for t in middle if _pf(t) is not None]
        for field, v in zip(("volumen_ars", "cant_ops", "open_interest", "var_oi_pct",
                             "precio_teorico", "desvio_teorico", "valor_temporal"), mvals):
            row[field] = v
        if row.get("cant_ops") is not None: row["cant_ops"] = int(row["cant_ops"])
        if row.get("open_interest") is not None: row["open_interest"] = int(row["open_interest"])
    else:
        # Sin hora: números → apertura/min/max/ultimo; % → VH (y IV si hay dos)
        vals, pcts = [], []
        for t in after:
            if t.endswith('%'):
                pcts.append(_pf(t))
            else:
                f = _pf(t)
                if f is not None and len(vals) < 4:
                    vals.append(f)
        for field, v in zip(("apertura_prima", "min_prima", "max_prima", "ultimo_precio"), vals):
            row[field] = v
        if len(pcts) == 1:
            row["vol_hist_40r"] = pcts[0]
        elif len(pcts) >= 2:
            row["vol_hist_40r"]  = pcts[-2]
            row["vol_implicita"] = pcts[-1]

    return row

def parse_iamc_pdf(pdf_bytes: bytes) -> tuple[list, dict, str]:
    """
    Parsea el PDF de IAMC.
    - Subyacente/tipo/strike desde el SÍMBOLO (OPCION_MAP).
    - Vencimiento: texto de página → aprendido por serie → fallback estático OC/DI.
    - Filas colapsadas se recuperan; basura de rankings se descarta.
    - Dedupe por símbolo conservando la fila más completa.
    """
    if not HAS_PDF: return [], {}, None
    rows = []
    resumen_vol, resumen_oi = {}, {}
    resumen_pc, resumen_pc_oi = {}, {}
    fecha_str = None

    # COL_MAP base (78 columnas — GGAL/COME/etc.)
    COL_MAP_78 = {
        0:  "symbol",
        3:  "strike",
        6:  "distancia_itm_otm",
        8:  "moneyness",
        11: "precio_suby",
        14: "apertura_prima",
        19: "min_prima",
        22: "max_prima",
        25: "ultimo_precio",
        28: "var_prima_pct",
        33: "hora_ultimo",
        36: "volumen_ars",
        39: "cant_ops",
        42: "open_interest",
        45: "var_oi_pct",
        48: "precio_teorico",
        51: "desvio_teorico",
        54: "valor_temporal",
        57: "vol_hist_40r",
        60: "vol_implicita",
        63: "delta",
        66: "gamma",
        69: "theta",
        72: "vega",
        75: "rho",
    }
    # COL_MAP para 81 columnas (YPFD/ALUA — 2 nulls extra en pos 15-16 y 29-30)
    # Primera diferencia: +2 a partir de col 19 (min_prima)
    # Segunda diferencia: +2 adicional a partir de col 33 (hora)
    COL_MAP_81 = {
        0:  "symbol",
        3:  "strike",
        6:  "distancia_itm_otm",
        8:  "moneyness",
        11: "precio_suby",
        14: "apertura_prima",   # igual
        19: "min_prima",        # +2 (nulls en 15-16)
        22: "max_prima",        # +2
        25: "ultimo_precio",    # +2
        28: "var_prima_pct",    # +2
        35: "hora_ultimo",      # +4 (nulls en 15-16 y 29-30)
        38: "volumen_ars",      # +4
        41: "cant_ops",         # +4
        44: "open_interest",    # +4
        47: "var_oi_pct",       # +4
        50: "precio_teorico",   # +4
        53: "desvio_teorico",   # +4
        56: "valor_temporal",   # +4
        59: "vol_hist_40r",     # +4
        62: "vol_implicita",    # +4
        65: "delta",            # +4
        68: "gamma",            # +4
        71: "theta",            # +4
        74: "vega",             # +4
        77: "rho",              # +4
    }
    # COL_MAP para 73 columnas (BBAR y subyacentes con menos cols)
    COL_MAP_73 = {
        0:  "symbol",
        3:  "strike",
        6:  "distancia_itm_otm",
        8:  "moneyness",
        11: "precio_suby",
        14: "apertura_prima",
        17: "min_prima",
        20: "max_prima",
        23: "ultimo_precio",
        26: "var_prima_pct",
        29: "hora_ultimo",
        32: "volumen_ars",
        34: "cant_ops",
        37: "open_interest",
        40: "var_oi_pct",
        43: "precio_teorico",
        46: "desvio_teorico",
        49: "valor_temporal",
        52: "vol_hist_40r",
        55: "vol_implicita",
        58: "delta",
        61: "gamma",
        64: "theta",
        67: "vega",
        70: "rho",
    }

    def get_col_map(ncols):
        if ncols >= 80:
            print(f"  [parser] usando COL_MAP_81 para tabla con {ncols} cols")
            return COL_MAP_81
        if ncols >= 75:
            return COL_MAP_78
        print(f"  [parser] usando COL_MAP_73 para tabla con {ncols} cols")
        return COL_MAP_73

    COL_MAP = COL_MAP_78  # default, se sobreescribe por tabla
    STR_FIELDS  = {"symbol", "moneyness", "hora_ultimo", "distancia_itm_otm"}
    INT_FIELDS  = {"cant_ops", "open_interest"}

    mixed, unmapped = [], set()
    learned_series = {}                        # serie → vto (derivado del texto)
    used_static_series = set()                 # series resueltas con fallback estático
    vto_sources = Counter()                    # diagnóstico: de dónde salió cada vto
    garbage_skipped = 0
    recovered_count = 0

    report_date = None   # se setea tras leer la fecha de página 1

    def resolve_vto(serie, current_vto):
        """Prioridad: texto aprendido por serie > fallback estático > contexto de página."""
        if serie:
            if serie in learned_series:
                vto_sources["serie_aprendida"] += 1
                return learned_series[serie]
            if serie[:1] in learned_series:
                vto_sources["serie_aprendida"] += 1
                return learned_series[serie[:1]]
            if serie in SERIE_VTO_FALLBACK:
                used_static_series.add(serie)
                vto_sources["serie_estatica"] += 1
                return SERIE_VTO_FALLBACK[serie]
            if serie[:1] in SERIE_VTO_FALLBACK:
                used_static_series.add(serie[:1])
                vto_sources["serie_estatica"] += 1
                return SERIE_VTO_FALLBACK[serie[:1]]
        vto_sources["contexto_pagina" if current_vto else "none"] += 1
        return current_vto

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            first_text = pdf.pages[0].extract_text() or ""
            m_fecha = re.search(r'(\d{1,2})[.\-/]([A-Za-z]{3})[.\-/](\d{2,4})', first_text)
            if m_fecha:
                fecha_str = f"{m_fecha.group(1)}-{m_fecha.group(2)}-{m_fecha.group(3)}"
                report_date = _parse_fecha_str(fecha_str)

            current_suby, current_tipo, current_vto = None, None, None
            current_tasa, current_dias = None, None

            for page in pdf.pages:
                text = page.extract_text() or ""

                # FIX: "Tasa Libre de Riesgo" — la regex vieja exigía "Tasa Libre Riesgo"
                m_tasa = re.search(
                    r'tasa\s+libre(?:\s+de)?\s+riesgo[^\d%]*([\d.,]+)\s*%',
                    text, re.IGNORECASE)
                if m_tasa:
                    try: current_tasa = float(m_tasa.group(1).replace(',', '.'))
                    except ValueError: pass

                # FIX: IGNORECASE + "al" opcional
                m_dias = re.search(
                    r'd[ií]as\s+(?:al\s+)?vencimiento[^\d%]*?(\d+)',
                    text, re.IGNORECASE)
                if m_dias: current_dias = int(m_dias.group(1))

                # Vencimiento desde el texto (detección robusta multi-formato)
                detected = _detect_vto(text, report_date)
                if detected:
                    current_vto = detected

                if 'OPCIONES DE COMPRA (CALL)' in text:
                    current_tipo = 'CALL'
                elif 'OPCIONES DE VENTA (PUT)' in text:
                    current_tipo = 'PUT'

                # Encabezado de subyacente: solo fallback
                for line in lines_tmp if False else text.split('\n'):
                    m_s = re.match(r'^(.+?)\s*\(([A-Z0-9]{2,6})\)\s*$', line.strip())
                    if m_s and len(m_s.group(2)) <= 6:
                        current_suby = m_s.group(2)

                for table in page.extract_tables():
                    for row in table:
                        if not row or len(row) < 10: continue
                        sym_raw = str(row[0] or '').strip()
                        if not sym_raw: continue

                        r_parsed = None

                        if re.search(r'\s', sym_raw):
                            # ── Celda colapsada (línea entera en row[0]) ──
                            ctx = {"vto": current_vto, "tasa": current_tasa, "dias": current_dias}
                            r_parsed = _recover_line_row(sym_raw, ctx, resolve_vto, unmapped)
                            if r_parsed is None:
                                garbage_skipped += 1
                                continue
                            recovered_count += 1
                        else:
                            # ── Fila normal ──
                            sym = sym_raw.upper()
                            if not re.match(r'^[A-Z0-9]{2,8}[CV]\d', sym): continue

                            info = parse_symbol(sym)
                            if not info:
                                unmapped.add(f"UNPARSED:{sym}")
                                continue
                            pref, tipo_sym, strike_sym, serie = info

                            suby = resolve_suby(pref)
                            if suby is None:
                                unmapped.add(pref)
                                suby = current_suby            # fallback: encabezado
                            elif current_suby and suby != current_suby:
                                mixed.append((sym, current_suby, suby))

                            tipo = tipo_sym or current_tipo
                            strike = strike_sym if strike_sym else (_pf(row[3]) if len(row) > 3 else None)

                            # Aprender serie→vto cuando el texto de la página lo dio
                            vto = None
                            if serie:
                                if current_vto:
                                    learned_series.setdefault(serie, current_vto)
                                    learned_series.setdefault(serie[:1], current_vto)
                                vto = resolve_vto(serie, current_vto)
                            else:
                                vto = current_vto

                            r_parsed = {"symbol": sym, "tipo": tipo, "subyacente": suby,
                                        "vencimiento": vto, "strike": strike,
                                        "tasa_libre": current_tasa, "dias_vto": current_dias}

                            col_map = get_col_map(len(row))
                            if len(row) not in (73, 78, 81):
                                print(f"  [parser] fila {sym} tiene {len(row)} cols — revisar mapeo")
                            for col_idx, field in col_map.items():
                                if field in ("symbol", "strike"): continue
                                val = row[col_idx] if col_idx < len(row) else None
                                val = str(val).strip() if val is not None else None
                                if not val or val in ('', 'None'):
                                    r_parsed[field] = None
                                    continue
                                if field in STR_FIELDS:
                                    r_parsed[field] = val
                                elif field in INT_FIELDS:
                                    r_parsed[field] = _pi(val)
                                else:
                                    r_parsed[field] = _pf(val)

                        r_parsed = _sanity_fix_row(r_parsed)
                        rows.append(r_parsed)

                        # Acumular resumen
                        suby_acc = r_parsed.get("subyacente")
                        if suby_acc:
                            vol = r_parsed.get("volumen_ars") or 0
                            oi  = r_parsed.get("open_interest") or 0
                            resumen_vol[suby_acc] = resumen_vol.get(suby_acc, 0) + vol
                            resumen_oi[suby_acc]  = resumen_oi.get(suby_acc, 0) + oi
                            resumen_pc.setdefault(suby_acc, {"put": 0, "call": 0})
                            resumen_pc_oi.setdefault(suby_acc, {"put": 0, "call": 0})
                            if r_parsed.get("tipo") == 'PUT':
                                resumen_pc[suby_acc]["put"]    += vol
                                resumen_pc_oi[suby_acc]["put"] += oi
                            else:
                                resumen_pc[suby_acc]["call"]    += vol
                                resumen_pc_oi[suby_acc]["call"] += oi

    except Exception as e:
        print(f"PDF parse error: {e}")
        import traceback; traceback.print_exc()

    # ── NUEVO: dedupe por símbolo — conservar la fila más completa ──
    # (la página de rankings puede generar filas fantasma que duplican
    #  símbolos reales de la cadena; nos quedamos con la que tiene más datos)
    by_sym, order, dup_count = {}, [], 0
    for r in rows:
        s = r.get("symbol")
        if not s: continue
        if s not in by_sym:
            by_sym[s] = r
            order.append(s)
        else:
            dup_count += 1
            cur = by_sym[s]
            if sum(v is not None for v in r.values()) > sum(v is not None for v in cur.values()):
                by_sym[s] = r
    rows = [by_sym[s] for s in order]

    # ── Moneyness recalculada con el spot dominante real por subyacente ──
    spots_raw = {}
    for r in rows:
        s = r.get("subyacente"); p = r.get("precio_suby")
        if s and p: spots_raw.setdefault(s, []).append(round(p, 2))
    spots = {s: Counter(v).most_common(1)[0][0] for s, v in spots_raw.items()}
    for r in rows:
        spot = spots.get(r.get("subyacente"))
        if spot and r.get("strike") and r.get("tipo"):
            diff = (r["strike"] - spot) / spot
            r["moneyness"] = ('ATM' if abs(diff) < 0.01 else
                              ('ITM' if diff < 0 else 'OTM') if r["tipo"] == 'CALL' else
                              ('ITM' if diff > 0 else 'OTM'))

    # ── Diagnóstico ──
    if mixed:
        print(f"[parser] {len(mixed)} filas reasignadas por símbolo (mezcla detectada). "
              f"Muestra: {mixed[:10]}")
    if recovered_count:
        print(f"[parser] {recovered_count} filas recuperadas desde línea colapsada")
    if garbage_skipped:
        print(f"[parser] {garbage_skipped} filas de basura descartadas (rankings/otras)")
    if dup_count:
        print(f"[parser] {dup_count} filas duplicadas fusionadas (rankings vs cadena)")
    print(f"[parser] Fuentes de vencimiento: {dict(vto_sources)}")
    if used_static_series:
        print(f"[parser] Series resueltas con fallback estático: {sorted(used_static_series)} — "
              f"actualizar SERIE_VTO_FALLBACK cuando cambien los vencimientos")
    if unmapped:
        print(f"[parser] ⚠ Prefijos sin mapear: {sorted(unmapped)[:20]} — "
              f"completar OPCION_MAP. Ver /admin/debug-symbols")

    resumen = {
        "ranking_volumen": sorted(
            [{"subyacente": k, "volumen_ars": v} for k, v in resumen_vol.items()],
            key=lambda x: x["volumen_ars"], reverse=True
        )[:20],
        "ranking_oi": sorted(
            [{"subyacente": k, "open_interest": v} for k, v in resumen_oi.items()],
            key=lambda x: x["open_interest"], reverse=True
        )[:20],
        "put_call_ratio": {
            k: round(v["put"] / v["call"], 3) if v["call"] > 0 else None
            for k, v in resumen_pc.items()
        },
        "put_call_ratio_oi": {
            k: round(v["put"] / v["call"], 3) if v["call"] > 0 else None
            for k, v in resumen_pc_oi.items()
        },
    }
    return rows, resumen, fecha_str

def _stats_rows(rows: list) -> dict:
    """NUEVO: contadores de completitud para validar el parseo en un vistazo."""
    def nn(f): return sum(1 for r in rows if r.get(f) is not None)
    return {
        "total": len(rows),
        "con_precio_suby": nn("precio_suby"),
        "con_ultimo": nn("ultimo_precio"),
        "con_vencimiento": nn("vencimiento"),
        "con_oi": nn("open_interest"),
        "con_iv": nn("vol_implicita"),
        "con_tasa": nn("tasa_libre"),
    }

IAMC_DIARIO_URL = "https://www.iamc.com.ar/informediario/"

def _iamc_url(d: date) -> str:
    return f"https://www.iamc.com.ar/Informe/InformeDiarioOpciones{d:%d%m%Y}/"

async def descargar_iamc_pdf(target_date: date = None) -> bool:
    """
    Descarga el PDF de opciones del IAMC.
    Flujo:
      1. GET /informediario/ → extraer href del Informe Diario Opciones
      2. GET esa página del informe → extraer URL del PDF embebido
      3. GET el PDF
    """
    if target_date is None:
        target_date = datetime.now(TZ_ARG).date()

    headers_browser = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "es-AR,es;q=0.9",
        "Referer": "https://www.iamc.com.ar/",
    }

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, verify=False) as client:

            # ── Paso 1: /informediario/ → link al informe de opciones ──────────
            print(f"Paso 1: bajando {IAMC_DIARIO_URL}")
            r1 = await client.get(IAMC_DIARIO_URL, headers=headers_browser)
            if r1.status_code != 200:
                state["error"] = f"/informediario/ status {r1.status_code}"
                return False
            html1 = r1.text

            opciones_links = re.findall(
                r'href=["\'](/Informe/InformeDiarioOpciones(\d{8})/?)["\']',
                html1, re.IGNORECASE
            )
            print(f"  Links encontrados: {opciones_links}")

            if not opciones_links:
                state["error"] = "No se encontró link InformeDiarioOpciones en /informediario/"
                state["descarga_ok"] = False
                return False

            hoy = datetime.now(TZ_ARG).date()
            def parse_link_date(ddmmyyyy: str) -> date:
                try:
                    return date(int(ddmmyyyy[4:]), int(ddmmyyyy[2:4]), int(ddmmyyyy[:2]))
                except:
                    return date(2000, 1, 1)

            links_con_fecha = [
                (parse_link_date(ddmmyyyy), path)
                for path, ddmmyyyy in opciones_links
            ]
            # Excluir hoy y ordenar descendente (el del día anterior tiene el PDF completo)
            links_con_fecha = sorted(
                [(d, p) for d, p in links_con_fecha if d < hoy],
                key=lambda x: x[0], reverse=True
            )
            print(f"  Links ordenados (sin hoy): {[(str(d), p) for d, p in links_con_fecha]}")

            if not links_con_fecha:
                links_con_fecha = sorted(
                    [(parse_link_date(ddmmyyyy), path) for path, ddmmyyyy in opciones_links],
                    key=lambda x: x[0], reverse=True
                )

            # Probar cada link hasta encontrar uno con PDF válido (máx 7)
            informe_url = None
            html2 = None
            pdf_url = None
            pdf_bytes = None
            intentos = 0

            for link_date, link in links_con_fecha[:7]:
                intentos += 1
                candidate = f"https://www.iamc.com.ar{link}"
                print(f"  Intento {intentos}/7 — {link_date}: {candidate}")
                r_test = await client.get(candidate, headers={**headers_browser, "Referer": IAMC_DIARIO_URL})
                if r_test.status_code != 200:
                    print(f"  Falla status={r_test.status_code}, siguiente...")
                    continue

                h2 = r_test.text
                print(f"  Página OK ({len(h2)} bytes), buscando PDF...")

                p_url = None
                pdf_patterns = [
                    r'(?:src|data-src|file|url)=["\']([^"\']+\.pdf)["\']',
                    r'(https?://iamcweb[^\s"\'<>]+\.pdf)',
                    r'(https?://[^\s"\'<>]+TempFiles[^\s"\'<>]+\.pdf)',
                    r'(https?://[^\s"\'<>]+/[0-9a-f-]{36}\.pdf)',
                    r'["\']([^"\']+\.pdf)["\']',
                ]
                for pattern in pdf_patterns:
                    matches = re.findall(pattern, h2, re.IGNORECASE)
                    for m2 in matches:
                        u = m2 if m2.startswith('http') else f"https://www.iamc.com.ar{m2}"
                        p_url = u
                        print(f"  PDF candidato: {p_url}")
                        break
                    if p_url: break

                if not p_url:
                    print(f"  No se encontró URL de PDF en esta página, siguiente...")
                    continue

                print(f"  Descargando y validando PDF...")
                r_pdf = await client.get(p_url, headers={
                    **headers_browser,
                    "Accept": "application/pdf,*/*",
                    "Referer": candidate,
                })
                content = r_pdf.content
                es_valido = (
                    r_pdf.status_code == 200
                    and len(content) > 4
                    and content[:4] == b'%PDF'
                    and b'startxref' in content[-4096:]
                    and len(content) > 50000
                )
                if es_valido:
                    print(f"  PDF válido: {len(content)} bytes, fecha={link_date}")
                    informe_url = candidate
                    html2 = h2
                    pdf_url = p_url
                    pdf_bytes = content
                    target_date = link_date
                    break
                else:
                    print(f"  PDF corrupto o inválido (size={len(content)}, header={content[:8]}), siguiente...")

            if not informe_url or not pdf_bytes:
                msg = f"No se encontró ningún PDF válido tras {intentos} intentos"
                print(f"  {msg}")
                state["error"] = msg
                state["descarga_ok"] = False
                return False

            # ── Guardar y procesar ────────────────────────────────────────────
            print(f"Procesando PDF de {target_date}: {len(pdf_bytes)} bytes")
            rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
            print(f"Parseado: {len(rows)} opciones — stats: {_stats_rows(rows)}")
            _pg_save_pdf(pdf_bytes, target_date.isoformat())
            if rows:
                _pg_save_opciones(rows, target_date.isoformat())
            state["opciones"]    = rows
            state["resumen"]     = resumen
            state["fecha"]       = target_date.isoformat()
            state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
            state["descarga_ok"] = True
            state["error"]       = None
            return True

    except Exception as e:
        print(f"Error: {e}")
        import traceback; traceback.print_exc()
        state["error"] = str(e)
        state["descarga_ok"] = False
        return False

# ── Scheduler ─────────────────────────────────────────────────────────────────
async def scheduler():
    """
    Intenta descargar el PDF todos los días hábiles:
    - Al arrancar (para cargar datos del día anterior si no hay nada)
    - A las 18:30 ARG (después del cierre de mercado)
    - Reintenta cada 15 min hasta las 20:00 si no lo consiguió
    """
    print("Scheduler de IAMC iniciado")
    pdf_bytes, fecha = _pg_load_latest()
    if pdf_bytes:
        rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
        state["opciones"]    = rows
        state["resumen"]     = resumen
        state["fecha"]       = fecha
        state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
        state["descarga_ok"] = True
        print(f"Cargado desde PG: {len(rows)} opciones, fecha {fecha} — stats: {_stats_rows(rows)}")
    else:
        await descargar_iamc_pdf()

    while True:
        now = datetime.now(TZ_ARG)
        target = now.replace(hour=18, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        while target.weekday() >= 5:
            target = target + timedelta(days=1)

        wait_secs = (target - now).total_seconds()
        print(f"Próxima descarga IAMC programada: {target.strftime('%Y-%m-%d %H:%M')} ARG (en {wait_secs/3600:.1f}h)")
        await asyncio.sleep(wait_secs)

        ok = False
        for _ in range(10):  # máximo 10 intentos = 150 min
            now = datetime.now(TZ_ARG)
            if now.hour >= 20:
                print("Pasaron las 20:00, dejando de reintentar por hoy")
                break
            ok = await descargar_iamc_pdf()
            if ok:
                break
            print("Reintentando en 15 min...")
            await asyncio.sleep(900)

@app.on_event("startup")
async def startup():
    _pg_init()
    global _scheduler_task
    _scheduler_task = asyncio.create_task(scheduler())

# ── API endpoints ──────────────────────────────────────────────────────────────

SUPABASE_URL  = "https://zqnxkgqalhhybcnzgjfk.supabase.co"
SUPABASE_ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpxbnhrZ3FhbGhoeWJjbnpnamZrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg5Mzk3MDEsImV4cCI6MjA5NDUxNTcwMX0.GD0RHfLGzplLhL1E-_GUHh3HXSNsZsvFi436wcm2tD4"
_token_cache: dict = {}

async def validar_stoken(token: str) -> bool:
    if not token: return False
    cached = _token_cache.get(token)
    if cached and (datetime.now().timestamp() - cached["ts"]) < 300:
        return cached["valid"]
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.post(
                f"{SUPABASE_URL}/rest/v1/rpc/validate_session",
                headers={"Content-Type":"application/json","apikey":SUPABASE_ANON,"Authorization":f"Bearer {SUPABASE_ANON}"},
                json={"p_token": token},
            )
            valid = r.status_code == 200 and r.json() == True
            _token_cache[token] = {"valid": valid, "ts": datetime.now().timestamp()}
            return valid
    except:
        return cached["valid"] if cached else False

HTML_403 = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>body{background:#0e0d0a;color:#e8e0cc;font-family:'Segoe UI',sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center;border:0.5px solid #2a2510;padding:40px 60px;border-radius:12px;background:#12100a}
h2{color:#C9960A;font-size:20px;margin-bottom:8px}p{color:#5a4e2a;font-size:13px}
a{color:#C9960A}</style></head><body>
<div class="box"><h2>Merlin Options</h2>
<p>Acceso solo para suscriptores.<br>Ingresá desde <a href="https://merlin-financial-group.netlify.app">Merlin Financial Group</a></p>
</div></body></html>"""

@app.get("/", response_class=HTMLResponse)
async def frontend(request: Request, stoken: str = ""):
    path = os.getenv("FRONTEND_FILE", "frontend.html")
    if os.path.exists(path):
        from fastapi.responses import Response
        content = open(path, encoding="utf-8").read()
        return Response(
            content=content,
            media_type="text/html",
            headers={"Content-Security-Policy": "script-src 'self' 'unsafe-eval' 'unsafe-inline' https://cdnjs.cloudflare.com https://fonts.googleapis.com https://www.googletagmanager.com; default-src * 'unsafe-inline' 'unsafe-eval' data: blob:"}
        )
    return HTMLResponse("<h1>Merlin Options</h1><p>Frontend no encontrado: " + path + "</p>")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "fecha": state["fecha"],
        "total_opciones": len(state["opciones"]),
        "updated_at": state["updated_at"],
        "descarga_ok": state["descarga_ok"],
    }

@app.get("/api/opciones/cadena")
async def get_cadena(
    subyacente: str = Query(None),
    tipo: str = Query(None),       # CALL / PUT
    vencimiento: str = Query(None), # 2026-10-16 / 2026-12-18
    fecha: str = Query(None),
):
    """Devuelve la cadena de opciones filtrable por subyacente, tipo y vencimiento."""
    rows = state["opciones"]
    if not rows and fecha:
        rows = _pg_load_opciones(fecha)
    if subyacente:
        rows = [r for r in rows if (r.get("subyacente") or "").upper() == subyacente.upper()]
    if tipo:
        rows = [r for r in rows if (r.get("tipo") or "").upper() == tipo.upper()]
    if vencimiento:
        rows = [r for r in rows if r.get("vencimiento") == vencimiento]
    return {"fecha": state["fecha"], "total": len(rows), "data": rows}

@app.get("/api/opciones/subyacentes")
async def get_subyacentes():
    """Lista de subyacentes disponibles con resumen de actividad."""
    subyacentes = {}
    for r in state["opciones"]:
        s = r.get("subyacente")
        if not s: continue
        if s not in subyacentes:
            subyacentes[s] = {"subyacente": s, "calls": 0, "puts": 0,
                               "volumen_ars": 0, "open_interest": 0}
        entry = subyacentes[s]
        if r.get("tipo") == "CALL": entry["calls"] += 1
        else: entry["puts"] += 1
        entry["volumen_ars"]   += r.get("volumen_ars") or 0
        entry["open_interest"] += r.get("open_interest") or 0
    return {
        "fecha": state["fecha"],
        "total": len(subyacentes),
        "data": sorted(subyacentes.values(), key=lambda x: x["volumen_ars"], reverse=True)
    }

@app.get("/api/opciones/resumen")
async def get_resumen():
    """Ranking de volumen, OI y ratio put/call por subyacente."""
    return {"fecha": state["fecha"], "updated_at": state["updated_at"], **state["resumen"]}

@app.get("/api/opciones/symbol/{symbol}")
async def get_symbol(symbol: str):
    """Datos de una opción específica por symbol."""
    rows = [r for r in state["opciones"] if r.get("symbol","").upper() == symbol.upper()]
    if not rows:
        return JSONResponse(status_code=404, content={"error": f"Symbol {symbol} no encontrado"})
    return {"fecha": state["fecha"], "data": rows[0]}

@app.get("/api/opciones/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    """
    Bid/ask en tiempo real desde Veta (BCCH).
    Requiere VETA_COOKIE configurada en Doppler.
    """
    if not VETA_COOKIE:
        return JSONResponse(status_code=503, content={"error": "VETA_COOKIE no configurada"})
    ds = int(datetime.now().timestamp() * 1000)
    url = f"https://matriz.bcch.xoms.com.ar/api/v2/profile?symbol={symbol}&_ds={ds}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers={
                "Cookie": VETA_COOKIE,
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://matriz.bcch.xoms.com.ar/",
            })
            if r.status_code == 200:
                return {"symbol": symbol, "data": r.json()}
            return JSONResponse(status_code=r.status_code, content={"error": f"Veta status {r.status_code}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/opciones/iv_surface/{subyacente}")
async def get_iv_surface(subyacente: str):
    """Superficie de volatilidad implícita: strike vs vencimiento."""
    rows = [r for r in state["opciones"]
            if r.get("subyacente","").upper() == subyacente.upper()
            and r.get("vol_implicita") is not None
            and r.get("vol_implicita", 0) > 0]
    surface = [
        {
            "symbol":       r["symbol"],
            "tipo":         r["tipo"],
            "strike":       r["strike"],
            "vencimiento":  r["vencimiento"],
            "dias_vto":     r["dias_vto"],
            "vol_implicita":r["vol_implicita"],
            "delta":        r["delta"],
            "moneyness":    r["moneyness"],
        }
        for r in rows
    ]
    return {"subyacente": subyacente, "fecha": state["fecha"], "data": surface}

# ── Admin endpoints ────────────────────────────────────────────────────────────
@app.get("/admin/reparse")
@app.post("/admin/reparse")
async def admin_reparse():
    """Reparsea el PDF que ya está en PostgreSQL con el parser actual."""
    pdf_bytes, fecha = _pg_load_latest()
    if not pdf_bytes:
        return {"ok": False, "error": "No hay PDF en PostgreSQL"}
    print(f"Reparsando PDF de {fecha} ({len(pdf_bytes)} bytes)...")
    rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
    print(f"Reparsado: {len(rows)} opciones — stats: {_stats_rows(rows)}")
    if rows:
        _pg_save_opciones(rows, fecha)
    state["opciones"]    = rows
    state["resumen"]     = resumen
    state["fecha"]       = fecha
    state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
    state["descarga_ok"] = True
    state["error"]       = None
    # NUEVO: muestra con datos reales (no filas fantasma) + stats de completitud
    ggal = [r for r in rows if r.get("subyacente") == "GGAL"]
    ggal_con_datos = [r for r in ggal if r.get("ultimo_precio") is not None][:3]
    return {
        "ok": True,
        "fecha": fecha,
        "stats": _stats_rows(rows),
        "muestra_ggal_con_datos": ggal_con_datos or ggal[:3],
    }

@app.get("/admin/refresh")
@app.post("/admin/refresh")
async def admin_refresh(fecha_str: str = Query(None)):
    """Fuerza descarga del PDF de IAMC."""
    target = None
    if fecha_str:
        try: target = date.fromisoformat(fecha_str)
        except: pass
    ok = await descargar_iamc_pdf(target)
    return {
        "ok": ok,
        "fecha": state["fecha"],
        "total_opciones": len(state["opciones"]),
        "error": state["error"],
    }

@app.post("/admin/upload-pdf")
async def admin_upload_pdf(pdf: UploadFile = File(...)):
    """Sube manualmente el PDF de IAMC (fallback si la descarga automática falla)."""
    content = await pdf.read()
    if content[:4] != b'%PDF':
        return {"ok": False, "error": "No es un PDF válido"}
    rows, resumen, fecha_str = parse_iamc_pdf(content)
    if not rows:
        return {"ok": False, "error": "No se encontraron opciones en el PDF"}
    m = re.search(r'(\d{2})(\d{2})(\d{4})', pdf.filename or '')
    if m:
        fecha = date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
    else:
        fecha = date.today().isoformat()
    _pg_save_pdf(content, fecha)
    _pg_save_opciones(rows, fecha)
    state["opciones"]    = rows
    state["resumen"]     = resumen
    state["fecha"]       = fecha
    state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
    state["descarga_ok"] = True
    state["error"]       = None
    return {
        "ok": True,
        "fecha": fecha,
        "stats": _stats_rows(rows),
        "subyacentes": len(set(r["subyacente"] for r in rows if r.get("subyacente"))),
    }

@app.get("/api/opciones/disponibles")
async def get_disponibles():
    """Muestra qué fechas tiene disponibles IAMC en /informediario/ sin descargar nada."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, verify=False) as client:
            r = await client.get(IAMC_DIARIO_URL, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,*/*",
            })
            if r.status_code != 200:
                return {"ok": False, "error": f"IAMC status {r.status_code}"}

            html = r.text
            matches = re.findall(
                r'href=["\'](/Informe/InformeDiarioOpciones(\d{8})/?)["\']',
                html, re.IGNORECASE
            )
            hoy = datetime.now(TZ_ARG).date()

            fechas = []
            for path, ddmmyyyy in matches:
                try:
                    d = date(int(ddmmyyyy[4:]), int(ddmmyyyy[2:4]), int(ddmmyyyy[:2]))
                    fechas.append({
                        "fecha": d.isoformat(),
                        "label": d.strftime("%d/%m/%Y (%A)").replace(
                            "Monday","Lunes").replace("Tuesday","Martes").replace(
                            "Wednesday","Miércoles").replace("Thursday","Jueves").replace(
                            "Friday","Viernes"),
                        "url": f"https://www.iamc.com.ar{path}",
                        "es_hoy": d == hoy,
                        "dias_atras": (hoy - d).days,
                        "descargado": d.isoformat() == state.get("fecha"),
                    })
                except: pass

            fechas = sorted(fechas, key=lambda x: x["fecha"], reverse=True)

            return {
                "ok": True,
                "iamc_fechas_disponibles": fechas,
                "descargado_actualmente": state.get("fecha"),
                "total_opciones_cargadas": len(state["opciones"]),
                "updated_at": state.get("updated_at"),
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/admin/debug-symbols")
async def debug_symbols():
    """Prefijos de símbolos vistos, a qué subyacente resolvieron y conteo.
    Sirve para completar OPCION_MAP cuando aparece un subyacente nuevo."""
    cnt = Counter()
    for r in state["opciones"]:
        info = parse_symbol(r.get("symbol") or "")
        if not info:
            cnt[("??UNPARSED", (r.get("symbol") or "")[:24], r.get("subyacente") or "?")] += 1
        else:
            pref = info[0]
            suby = resolve_suby(pref)
            cnt[(pref, suby or f"?FALLBACK:{r.get('subyacente')}", r.get("subyacente") or "?")] += 1
    return {
        "fecha": state["fecha"],
        "prefijos": [
            {"prefijo": k[0], "resuelto_a": k[1], "suby_en_pdf": k[2], "filas": v}
            for k, v in cnt.most_common()
        ],
    }

@app.get("/admin/debug-parser")
async def debug_parser(suby: str = "GGAL"):
    """Muestra filas crudas extraídas por pdfplumber para debug del parser."""
    if not HAS_PDF:
        return {"error": "pdfplumber no disponible"}
    pdf_bytes, fecha = _pg_load_latest()
    if not pdf_bytes:
        return {"error": "No hay PDF cargado"}
    resultado = []
    page_texts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            total_pages = len(pdf.pages)
            target_pages = []
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if suby.upper() in text.upper():
                    target_pages.append(i)
                    page_texts.append({"pagina": i, "texto_primeras_lineas": text[:400]})

            if target_pages:
                page = pdf.pages[target_pages[0]]
                tables = page.extract_tables()
                for t_idx, table in enumerate(tables[:3]):
                    for r_idx, row in enumerate(table[:10]):
                        resultado.append({
                            "pagina": target_pages[0],
                            "tabla": t_idx,
                            "fila": r_idx,
                            "raw": row,
                            "len": len(row) if row else 0,
                        })

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}
    return {
        "fecha": fecha,
        "suby": suby,
        "total_pages": total_pages,
        "target_pages": target_pages,
        "page_texts": page_texts[:3],
        "filas_muestra": resultado,
    }

@app.get("/admin/debug-page/{page_num}")
async def debug_page(page_num: int):
    """Muestra el texto y tablas crudas de una página específica del PDF."""
    if not HAS_PDF:
        return {"error": "pdfplumber no disponible"}
    pdf_bytes, fecha = _pg_load_latest()
    if not pdf_bytes:
        return {"error": "No hay PDF cargado"}
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if page_num >= len(pdf.pages):
                return {"error": f"Página {page_num} no existe (total: {len(pdf.pages)})"}
            page = pdf.pages[page_num]
            text = page.extract_text() or ""
            tables = page.extract_tables()
            return {
                "pagina": page_num,
                "texto_completo": text,
                "num_tablas": len(tables),
                "tablas": [
                    {
                        "tabla_idx": t_idx,
                        "num_filas": len(table),
                        "num_cols": len(table[0]) if table else 0,
                        "filas": table[:15],
                    }
                    for t_idx, table in enumerate(tables[:3])
                ],
            }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/admin/debug-iamc")
async def debug_iamc_html():
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, verify=False) as client:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            r1 = await client.get(IAMC_DIARIO_URL, headers=headers)
            html1 = r1.text
            opciones_links = re.findall(r'href=["\']([^"\']*[Oo]pciones[^"\']*)["\']', html1)
            informe_link = next((l for l in opciones_links if 'InformeDiario' in l), None)

            result = {
                "paso1_url": IAMC_DIARIO_URL,
                "paso1_status": r1.status_code,
                "paso1_html_length": len(html1),
                "opciones_links": opciones_links[:10],
                "informe_link_encontrado": informe_link,
            }

            if informe_link:
                informe_url = informe_link if informe_link.startswith('http') else f"https://www.iamc.com.ar{informe_link}"
                r2 = await client.get(informe_url, headers=headers)
                html2 = r2.text
                pdf_candidates = re.findall(r'["\']([^"\']+\.pdf)["\']', html2, re.IGNORECASE)
                iamcweb_urls = re.findall(r'https?://iamcweb[^\s"\'<>]+', html2)
                result.update({
                    "paso2_url": informe_url,
                    "paso2_status": r2.status_code,
                    "paso2_html_length": len(html2),
                    "pdf_candidates": pdf_candidates[:10],
                    "iamcweb_urls": iamcweb_urls[:5],
                    "paso2_html_snippet": html2[:3000],
                })

            return result
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/test-iamc-url")
async def test_iamc_url(fecha_str: str = Query(None)):
    """Testea si la URL del PDF de IAMC es accesible (últimos 5 días hábiles)."""
    if fecha_str:
        try: d = date.fromisoformat(fecha_str)
        except: d = datetime.now(TZ_ARG).date()
    else:
        d = datetime.now(TZ_ARG).date()
    results = []
    for delta in range(5):
        test_date = d - timedelta(days=delta)
        if test_date.weekday() >= 5: continue
        url = _iamc_url(test_date)
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False) as client:
                r = await client.head(url, headers={"User-Agent": "Mozilla/5.0"})
                results.append({
                    "fecha": test_date.isoformat(),
                    "url": url,
                    "status": r.status_code,
                    "content_type": r.headers.get("content-type", ""),
                    "content_length": r.headers.get("content-length", ""),
                })
        except Exception as e:
            results.append({"fecha": test_date.isoformat(), "url": url, "error": str(e)})
    return {"results": results}
