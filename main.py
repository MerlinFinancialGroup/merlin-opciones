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
    Parsea el PDF de IAMC.
    La tabla tiene 78 columnas, cada campo ocupa 3 celdas (valor, vacío, vacío).
    Mapeo de columnas:
      0=Symbol, 3=Strike, 6=Dist ITM/OTM, 8=Moneyness, 11=PrecioSuby,
      14=AperturaPrima, 19=MinPrima, 22=MaxPrima, 25=UltimoPrecio,
      28=VarPrima%, 33=HoraUltimo, 36=VolumenARS, 39=CantOps,
      42=OI, 45=VarOI%, 48=PrecioTeorico, 51=DesvioTeorico,
      54=ValorTemporal, 57=VolHist40r, 60=VolImplicita,
      63=Delta, 66=Gamma, 69=Theta, 72=Vega, 75=Rho
    """
    if not HAS_PDF: return [], {}, None
    rows = []
    resumen_vol = {}
    resumen_oi  = {}
    resumen_pc  = {}
    fecha_str   = None

    # Mapeo col_index → field_name
    COL_MAP = {
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
    STR_FIELDS  = {"symbol", "moneyness", "hora_ultimo", "distancia_itm_otm"}
    FLOAT_FIELDS = set(COL_MAP.values()) - STR_FIELDS - {"symbol", "cant_ops", "open_interest"}
    INT_FIELDS  = {"cant_ops", "open_interest"}

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Detectar fecha en primera página
            first_text = pdf.pages[0].extract_text() or ""
            m_fecha = re.search(r'(\d{1,2})[.\-/]([A-Za-z]{3})[.\-/](\d{2,4})', first_text)
            if m_fecha:
                fecha_str = f"{m_fecha.group(1)}-{m_fecha.group(2)}-{m_fecha.group(3)}"

            current_suby    = None
            current_tipo    = None
            current_vto     = None
            current_tasa    = None
            current_dias    = None

            for page in pdf.pages:
                text = page.extract_text() or ""

                # Detectar contexto de la página
                m_tasa = re.search(r'Tasa Libre Riesgo\s+([\d.]+)%', text)
                if m_tasa: current_tasa = float(m_tasa.group(1))

                m_dias = re.search(r'Días al Vencimiento\s+(\d+)', text)
                if m_dias: current_dias = int(m_dias.group(1))

                if 'Octubre' in text and '16/10/2026' in text:
                    current_vto = "2026-10-16"
                elif 'Diciembre' in text and '18/12/2026' in text:
                    current_vto = "2026-12-18"

                if 'OPCIONES DE COMPRA (CALL)' in text:
                    current_tipo = 'CALL'
                elif 'OPCIONES DE VENTA (PUT)' in text:
                    current_tipo = 'PUT'

                # Detectar subyacente: "NOMBRE S.A. (TICKER)"
                for line in text.split('\n'):
                    m_s = re.match(r'^(.+?)\s*\(([A-Z0-9]{2,6})\)\s*$', line.strip())
                    if m_s and len(m_s.group(2)) <= 6:
                        current_suby = m_s.group(2)

                # Procesar tablas
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if not row or len(row) < 10: continue
                        sym = str(row[0] or '').strip()
                        # Solo filas de opciones: símbolo tipo GFGCxxxxOC
                        if not re.match(r'^[A-Z]{2,6}[CV]\d', sym):
                            continue

                        # Extraer tipo y vencimiento del símbolo
                        tipo = current_tipo
                        vto  = current_vto
                        if re.search(r'[A-Z]C\d', sym):
                            tipo = 'CALL'
                        elif re.search(r'[A-Z]V\d', sym):
                            tipo = 'PUT'
                        if sym.endswith('OC') or 'OC' in sym[-3:]:
                            vto = "2026-10-16"
                        elif sym.endswith('DI') or sym.endswith('D') or 'DI' in sym[-3:]:
                            vto = "2026-12-18"

                        # Construir dict con el mapeo de columnas
                        r_parsed = {"symbol": sym, "tipo": tipo, "subyacente": current_suby,
                                    "vencimiento": vto, "tasa_libre": current_tasa, "dias_vto": current_dias}

                        for col_idx, field in COL_MAP.items():
                            if field == "symbol": continue
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

                        rows.append(r_parsed)

                        # Acumular resumen
                        suby = current_suby
                        if suby:
                            vol = r_parsed.get("volumen_ars") or 0
                            oi  = r_parsed.get("open_interest") or 0
                            resumen_vol[suby] = resumen_vol.get(suby, 0) + vol
                            resumen_oi[suby]  = resumen_oi.get(suby, 0) + oi
                            resumen_pc.setdefault(suby, {"put": 0, "call": 0})
                            if tipo == 'PUT':
                                resumen_pc[suby]["put"] += vol
                            else:
                                resumen_pc[suby]["call"] += vol

    except Exception as e:
        print(f"PDF parse error: {e}")
        import traceback; traceback.print_exc()

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

IAMC_DIARIO_URL = "https://www.iamc.com.ar/informediario/"

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

            # Buscar href que contenga "InformeDiarioOpciones"
            # Buscar TODOS los links de InformeDiarioOpciones
            opciones_links = re.findall(
                r'href=["\'](/Informe/InformeDiarioOpciones(\d{8})/?)["\']',
                html1, re.IGNORECASE
            )
            print(f"  Links encontrados: {opciones_links}")

            if not opciones_links:
                state["error"] = "No se encontró link InformeDiarioOpciones en /informediario/"
                state["descarga_ok"] = False
                return False

            # Ordenar por fecha DDMMYYYY descendente → el más reciente primero
            # Excluir el de hoy (el del día anterior hábil es el que tiene PDF completo)
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
            # Excluir hoy y ordenar descendente
            links_con_fecha = sorted(
                [(d, p) for d, p in links_con_fecha if d < hoy],
                key=lambda x: x[0], reverse=True
            )
            print(f"  Links ordenados (sin hoy): {[(str(d), p) for d, p in links_con_fecha]}")

            if not links_con_fecha:
                # Si solo hay el de hoy, usarlo igual
                links_con_fecha = sorted(
                    [( parse_link_date(ddmmyyyy), path) for path, ddmmyyyy in opciones_links],
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

                # Buscar URL del PDF en esta página
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

                # Descargar y validar el PDF
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
            print(f"Parseado: {len(rows)} opciones")
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
    print(f"Reparsado: {len(rows)} opciones")
    if rows:
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
        "muestra_ggal": [r for r in rows if r.get("subyacente") == "GGAL"][:3],
    }


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


@app.get("/admin/debug-parser")
async def debug_parser():
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
            # Buscar páginas con GGAL
            ggal_pages = []
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if "GGAL" in text or "GALICIA" in text:
                    ggal_pages.append(i)
                    page_texts.append({"pagina": i, "texto_primeras_lineas": text[:300]})

            # Tomar primera página de GGAL y mostrar tablas crudas
            if ggal_pages:
                page = pdf.pages[ggal_pages[0]]
                tables = page.extract_tables()
                for t_idx, table in enumerate(tables[:3]):
                    for r_idx, row in enumerate(table[:8]):
                        resultado.append({
                            "pagina": ggal_pages[0],
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
        "total_pages": total_pages,
        "ggal_pages": ggal_pages,
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


async def debug_iamc_html():
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, verify=False) as client:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

            # Paso 1
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

            # Paso 2 si encontramos el link
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
