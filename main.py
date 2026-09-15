import os, asyncio, httpx, io, re, json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, UploadFile, File, Query
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
    "opciones": [],          # lista de filas parseadas
    "resumen": {},           # ranking volumen / OI / put-call
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

def parse_iamc_pdf(pdf_bytes: bytes) -> tuple[list, dict, str]:
    """
    Parsea el PDF de IAMC y devuelve (rows, resumen, fecha_str).
    Estructura del PDF:
      - Páginas con header: subyacente, tipo (CALL/PUT), vencimiento (OC=Oct, DI=Dic)
      - Columnas: Symbol, Strike, Distancia ITM/OTM, Moneyness, Precio Suby,
                  Apertura Prima, Min Prima, Max Prima, Ultimo Precio Prima,
                  Var Prima%, Hora Ultimo Trade, Volumen Efectivo ARS, Cant Ops,
                  Open Interest, Var OI%, Precio Teorico, Desvio vs Teorico ARS,
                  Valor Temporal, Vol Historica 40r, Vol Implicita,
                  Delta, Gamma, Theta, Vega, Rho
    """
    if not HAS_PDF: return [], {}, None
    rows = []
    resumen_vol = {}
    resumen_oi  = {}
    resumen_pc  = {}
    fecha_str   = None

    # Mapeo de sufijo en symbol → tipo + vencimiento
    # OC = call oct, DI = call/put dic, VxOC = put oct, VxDI = put dic
    # En el PDF el tipo está en el header de sección
    # Inferimos tipo por la C/V en el symbol: GFGCxxOC = call oct, GFGVxxOC = put oct
    # También por el sufijo DI = diciembre, OC = octubre

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Detectar fecha del PDF (título primera página)
            first_text = pdf.pages[0].extract_text() or ""
            m_fecha = re.search(r'(\d{1,2})[.\-/]([A-Za-z]{3})[.\-/](\d{2,4})', first_text)
            if m_fecha:
                fecha_str = f"{m_fecha.group(1)}-{m_fecha.group(2)}-{m_fecha.group(3)}"

            # Variables de contexto para cada página/sección
            current_suby    = None
            current_tipo    = None   # CALL / PUT
            current_vto     = None   # "2026-10-16" / "2026-12-18"
            current_tasa    = None
            current_dias    = None

            for page in pdf.pages:
                text = page.extract_text() or ""
                lines = [l.strip() for l in text.split('\n') if l.strip()]

                # Detectar tasa libre de riesgo y días al vencimiento del header
                m_tasa = re.search(r'Tasa Libre Riesgo\s+([\d.]+)%', text)
                if m_tasa: current_tasa = float(m_tasa.group(1))

                m_dias = re.search(r'Días al Vencimiento\s+(\d+)', text)
                if m_dias: current_dias = int(m_dias.group(1))

                # Detectar vencimiento del header
                if 'Octubre' in text and '16/10/2026' in text:
                    current_vto = "2026-10-16"
                elif 'Diciembre' in text and '18/12/2026' in text:
                    current_vto = "2026-12-18"

                # Detectar tipo en header de sección
                if 'OPCIONES DE COMPRA (CALL)' in text:
                    current_tipo = 'CALL'
                elif 'OPCIONES DE VENTA (PUT)' in text:
                    current_tipo = 'PUT'

                # Detectar subyacente: líneas tipo "GRUPO FINANCIERO GALICIA S.A. (GGAL)"
                for line in lines:
                    m_suby = re.match(r'^(.+?)\s*\(([A-Z0-9]+)\)\s*$', line)
                    if m_suby and len(m_suby.group(2)) <= 6:
                        current_suby = m_suby.group(2)

                # Extraer tabla usando pdfplumber
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if not row or not row[0]: continue
                        sym = str(row[0]).strip()
                        # Symbol tiene formato como GFGC7600OC, COMC41.0OC, etc.
                        if not re.match(r'^[A-Z]{2,6}[CV]?\d', sym): continue

                        # Inferir tipo y vencimiento del symbol si no está en contexto
                        tipo = current_tipo
                        vto  = current_vto
                        if sym.endswith('OC'):
                            vto = "2026-10-16"
                            # C antes del strike → call, V → put
                            tipo = 'PUT' if re.search(r'[A-Z]V\d', sym) else 'CALL'
                        elif sym.endswith('DI') or sym.endswith('D'):
                            vto = "2026-12-18"
                            tipo = 'PUT' if re.search(r'[A-Z]V\d', sym) else 'CALL'

                        # Inferir subyacente del symbol (primeros 3-4 chars antes de C/V)
                        suby = current_suby
                        if not suby:
                            m_s = re.match(r'^([A-Z]{2,4})[CV]\d', sym)
                            if m_s: suby = m_s.group(1)

                        def safe(idx):
                            try: return row[idx]
                            except: return None

                        # Parsear strike del symbol o de columna
                        strike_str = ""
                        m_strike = re.search(r'[CV]([\d.]+)[OD]', sym)
                        if m_strike: strike_str = m_strike.group(1)

                        r_parsed = {
                            "symbol":         sym,
                            "tipo":           tipo,
                            "subyacente":     suby,
                            "vencimiento":    vto,
                            "strike":         _pf(strike_str) or _pf(safe(1)),
                            "moneyness":      str(safe(3) or '').strip() or None,
                            "precio_suby":    _pf(safe(4)),
                            "apertura_prima": _pf(safe(5)),
                            "min_prima":      _pf(safe(6)),
                            "max_prima":      _pf(safe(7)),
                            "ultimo_precio":  _pf(safe(8)),
                            "var_prima_pct":  _pf(safe(9)),
                            "hora_ultimo":    str(safe(10) or '').strip() or None,
                            "volumen_ars":    _pf(safe(11)),
                            "cant_ops":       _pi(safe(12)),
                            "open_interest":  _pi(safe(13)),
                            "var_oi_pct":     _pf(safe(14)),
                            "precio_teorico": _pf(safe(15)),
                            "desvio_teorico": _pf(safe(16)),
                            "valor_temporal": _pf(safe(17)),
                            "vol_hist_40r":   _pf(safe(18)),
                            "vol_implicita":  _pf(safe(19)),
                            "delta":          _pf(safe(20)),
                            "gamma":          _pf(safe(21)),
                            "theta":          _pf(safe(22)),
                            "vega":           _pf(safe(23)),
                            "rho":            _pf(safe(24)),
                            "tasa_libre":     current_tasa,
                            "dias_vto":       current_dias,
                        }
                        rows.append(r_parsed)

                        # Acumular resumen
                        if suby:
                            vol = _pf(safe(11)) or 0
                            oi  = _pi(safe(13)) or 0
                            resumen_vol[suby] = resumen_vol.get(suby, 0) + vol
                            resumen_oi[suby]  = resumen_oi.get(suby, 0) + oi
                            if tipo == 'PUT':
                                resumen_pc.setdefault(suby, {"put": 0, "call": 0})
                                resumen_pc[suby]["put"] += vol
                            else:
                                resumen_pc.setdefault(suby, {"put": 0, "call": 0})
                                resumen_pc[suby]["call"] += vol

    except Exception as e:
        print(f"PDF parse error: {e}")
        import traceback; traceback.print_exc()

    # Construir resumen
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
    }

    return rows, resumen, fecha_str

# ── Descarga automática PDF IAMC ───────────────────────────────────────────────
def _iamc_url(d: date) -> str:
    """URL del PDF para una fecha dada."""
    return f"{IAMC_BASE}{d.day:02d}{d.month:02d}{d.year}"

async def _extraer_pdf_url(html: str) -> str | None:
    """
    Extrae la URL directa del PDF desde el HTML de la página IAMC.
    El PDF está hosteado en iamcweb.prod.ingecloud.com/TempFiles/
    """
    # Buscar URLs de PDF en el HTML
    patterns = [
        r'https?://[^\s"\'<>]+\.pdf',
        r'src=["\']([^"\']+\.pdf)["\']',
        r'href=["\']([^"\']+\.pdf)["\']',
        r'url=["\']([^"\']+\.pdf)["\']',
        r'file=["\']([^"\']+\.pdf)["\']',
        r'(https?://iamcweb[^\s"\'<>]+)',
        r'(https?://[^\s"\'<>]+TempFiles[^\s"\'<>]+)',
    ]
    for pattern in patterns:
        matches = re.findall(pattern, html, re.IGNORECASE)
        for m in matches:
            url = m if m.startswith('http') else m
            if '.pdf' in url.lower() or 'TempFiles' in url:
                print(f"URL PDF encontrada: {url}")
                return url
    return None

async def descargar_iamc_pdf(target_date: date = None) -> bool:
    """
    Intenta descargar el PDF de IAMC para la fecha dada (o hoy).
    Flujo:
      1. GET página HTML de IAMC → extraer URL del PDF (iamcweb.prod.ingecloud.com)
      2. GET PDF directo
    """
    if target_date is None:
        target_date = datetime.now(TZ_ARG).date()

    headers_browser = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9",
    }

    for delta in range(0, 5):
        d = target_date - timedelta(days=delta)
        if d.weekday() >= 5: continue
        page_url = _iamc_url(d)
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True, verify=False) as client:
                # Paso 1: bajar la página HTML
                print(f"Bajando página IAMC: {page_url}")
                r_page = await client.get(page_url, headers=headers_browser)
                if r_page.status_code != 200:
                    print(f"Página no disponible (status={r_page.status_code})")
                    continue

                html = r_page.text
                print(f"Página OK ({len(html)} bytes), buscando URL del PDF...")

                # Paso 2: extraer URL del PDF
                pdf_url = await _extraer_pdf_url(html)
                if not pdf_url:
                    print(f"No se encontró URL de PDF en la página de {d}")
                    # Loguear parte del HTML para debug
                    print(f"HTML snippet: {html[:500]}")
                    continue

                # Paso 3: descargar el PDF
                print(f"Descargando PDF: {pdf_url}")
                r_pdf = await client.get(pdf_url, headers={
                    "User-Agent": headers_browser["User-Agent"],
                    "Referer": page_url,
                    "Accept": "application/pdf,*/*",
                })
                ct = r_pdf.headers.get("content-type", "")
                if r_pdf.status_code == 200 and (
                    "pdf" in ct.lower() or r_pdf.content[:4] == b'%PDF'
                ):
                    pdf_bytes = r_pdf.content
                    print(f"PDF OK: {len(pdf_bytes)} bytes, fecha {d}")
                    rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
                    print(f"Parseado: {len(rows)} opciones")
                    _pg_save_pdf(pdf_bytes, d.isoformat())
                    if rows:
                        _pg_save_opciones(rows, d.isoformat())
                    state["opciones"]    = rows
                    state["resumen"]     = resumen
                    state["fecha"]       = d.isoformat()
                    state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
                    state["descarga_ok"] = True
                    state["error"]       = None
                    return True
                else:
                    print(f"PDF no válido (status={r_pdf.status_code}, ct={ct})")

        except Exception as e:
            print(f"Error en fecha {d}: {e}")
            import traceback; traceback.print_exc()

    state["error"] = f"No se pudo bajar el PDF para {target_date}"
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
    # Al arrancar: intentar cargar desde PG primero
    pdf_bytes, fecha = _pg_load_latest()
    if pdf_bytes:
        rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
        state["opciones"]    = rows
        state["resumen"]     = resumen
        state["fecha"]       = fecha
        state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
        state["descarga_ok"] = True
        print(f"Cargado desde PG: {len(rows)} opciones, fecha {fecha}")
    else:
        # Intentar bajarlo de IAMC
        await descargar_iamc_pdf()

    while True:
        now = datetime.now(TZ_ARG)
        # Calcular próxima descarga: 18:30 del día de hoy o mañana
        target = now.replace(hour=18, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        # Saltar fines de semana
        while target.weekday() >= 5:
            target = target + timedelta(days=1)

        wait_secs = (target - now).total_seconds()
        print(f"Próxima descarga IAMC programada: {target.strftime('%Y-%m-%d %H:%M')} ARG (en {wait_secs/3600:.1f}h)")
        await asyncio.sleep(wait_secs)

        # Intentar descarga, reintentar cada 15 min hasta las 20:00
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
@app.get("/")
async def root():
    return {
        "service": "Merlin Opciones API",
        "fecha": state["fecha"],
        "total_opciones": len(state["opciones"]),
        "updated_at": state["updated_at"],
        "descarga_ok": state["descarga_ok"],
        "error": state["error"],
    }

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
        rows = [r for r in rows if r.get("subyacente","").upper() == subyacente.upper()]
    if tipo:
        rows = [r for r in rows if r.get("tipo","").upper() == tipo.upper()]
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
    # Extraer fecha del nombre del archivo
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
        "total_opciones": len(rows),
        "subyacentes": len(set(r["subyacente"] for r in rows if r.get("subyacente"))),
    }

@app.get("/admin/debug-iamc-html")
async def debug_iamc_html(fecha_str: str = Query(None)):
    """Devuelve el HTML crudo de la página IAMC para debug."""
    if fecha_str:
        try: d = date.fromisoformat(fecha_str)
        except: d = datetime.now(TZ_ARG).date()
    else:
        d = datetime.now(TZ_ARG).date()
    # Ir al día hábil anterior si es finde
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    url = _iamc_url(d)
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, verify=False) as client:
            r = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,*/*",
            })
            # Buscar URLs de PDF en el HTML
            pdf_urls = re.findall(r'https?://[^\s"\'<>]*(?:\.pdf|TempFiles)[^\s"\'<>]*', r.text, re.IGNORECASE)
            ingecloud_urls = re.findall(r'https?://iamcweb[^\s"\'<>]+', r.text, re.IGNORECASE)
            return {
                "fecha": d.isoformat(),
                "url": url,
                "status": r.status_code,
                "content_type": r.headers.get("content-type"),
                "html_length": len(r.text),
                "pdf_urls_found": pdf_urls,
                "ingecloud_urls_found": ingecloud_urls,
                "html_snippet": r.text[:2000],
            }
    except Exception as e:
        return {"error": str(e)}

@app.get("/admin/test-iamc-url")
async def test_iamc_url(fecha_str: str = Query(None)):
    """Testea si la URL del PDF de IAMC es accesible."""
    if fecha_str:
        try: d = date.fromisoformat(fecha_str)
        except: d = date.today()
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
