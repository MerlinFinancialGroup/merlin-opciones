import os, asyncio, httpx, io, re, json, logging
from collections import Counter
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, UploadFile, File, Query, Request, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("merlin")

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
VETA_ACCOUNT  = os.getenv("VETA_ACCOUNT", "")
TZ_ARG        = ZoneInfo("America/Argentina/Buenos_Aires")

# ── Tasas libre de riesgo por vencimiento (centralizadas) ─────────────────────
# Fuente: IAMC PDF. Actualizar cuando aparezca un vencimiento nuevo.
_TASA_DEFAULT = 0.2287
_TASA_FIJA: dict[str, float] = {
    "2026-10-16": 0.2287,
    "2026-12-18": 0.2418,
}

def _get_tasa(vto: str, tasas_rt: dict | None = None) -> float:
    """Devuelve la tasa libre para el vencimiento dado. Logguea si no la encuentra."""
    rt = (tasas_rt or {}).get(vto)
    if rt:
        return rt
    fija = _TASA_FIJA.get(vto)
    if fija:
        return fija
    logger.warning(f"Tasa no encontrada para vencimiento {vto!r} — usando default {_TASA_DEFAULT}")
    return _TASA_DEFAULT


IAMC_BASE = "https://www.iamc.com.ar/Informe/InformeDiarioOpciones"

app = FastAPI(title="Merlin Opciones API")

# ── CORS: solo orígenes propios ───────────────────────────────────────────────
_ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS",
    "https://merlin-financial-group.netlify.app,https://web-production-2a938.up.railway.app"
).split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

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
            CREATE TABLE IF NOT EXISTS veta_books (
                security_id TEXT PRIMARY KEY,
                data JSONB,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bid_ask_cierre (
                symbol      TEXT NOT NULL,
                fecha       DATE NOT NULL,
                bid         NUMERIC,
                ask         NUMERIC,
                qty_bid     NUMERIC,
                qty_ask     NUMERIC,
                ultimo      NUMERIC,
                saved_at    TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (symbol, fecha)
            );
            CREATE TABLE IF NOT EXISTS subyacentes_resumen (
                subyacente   TEXT NOT NULL,
                fecha        DATE NOT NULL,
                precio_suby  NUMERIC,
                calls        INTEGER,
                puts         INTEGER,
                vol_ars      NUMERIC,
                open_interest INTEGER,
                put_call     NUMERIC,
                tasa_libre   NUMERIC,
                dias_vto_min INTEGER,
                dias_vto_max INTEGER,
                PRIMARY KEY (subyacente, fecha)
            );
            CREATE TABLE IF NOT EXISTS opciones_cierres (
                symbol          TEXT NOT NULL,
                fecha           DATE NOT NULL,
                subyacente      TEXT,
                tipo            TEXT,
                strike          NUMERIC,
                vencimiento     DATE,
                moneyness       TEXT,
                dias_vto        INTEGER,
                precio_suby     NUMERIC,
                -- Precios
                ultimo          NUMERIC,
                apertura        NUMERIC,
                minimo          NUMERIC,
                maximo          NUMERIC,
                var_pct         NUMERIC,
                -- Volumen y OI
                volumen_ars     NUMERIC,
                cant_ops        INTEGER,
                open_interest   INTEGER,
                var_oi_pct      NUMERIC,
                -- Precio teórico
                precio_teorico  NUMERIC,
                desvio_teorico  NUMERIC,
                valor_temporal  NUMERIC,
                -- Volatilidades
                vi_iamc         NUMERIC,
                vi_calc         NUMERIC,
                vi_bid          NUMERIC,
                vi_offer        NUMERIC,
                vol_hist_40r    NUMERIC,
                -- Bid/Ask al cierre 17hs
                bid_cierre      NUMERIC,
                ask_cierre      NUMERIC,
                qty_bid_cierre  NUMERIC,
                qty_ask_cierre  NUMERIC,
                -- Griegas
                delta           NUMERIC,
                gamma           NUMERIC,
                theta           NUMERIC,
                vega            NUMERIC,
                rho             NUMERIC,
                -- Metadata
                tasa_libre      NUMERIC,
                iv_source       TEXT,
                PRIMARY KEY (symbol, fecha)
            );

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
        # Migraciones — ALTER TABLE IF NOT EXISTS la columna para tablas ya creadas
        migrations = [
            # opciones_cierres — columnas que pueden faltar en tablas creadas antes
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS moneyness TEXT;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS dias_vto INTEGER;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS precio_suby NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS apertura NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS minimo NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS maximo NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS var_pct NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS volumen_ars NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS cant_ops INTEGER;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS open_interest INTEGER;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS var_oi_pct NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS precio_teorico NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS desvio_teorico NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS valor_temporal NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vi_iamc NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vi_calc NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vi_bid NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vi_offer NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vol_hist_40r NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS tasa_libre NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS delta NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS gamma NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS theta NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vega NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS rho NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS subyacente TEXT;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS tipo TEXT;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS strike NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS vencimiento DATE;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS bid_cierre NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS ask_cierre NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS qty_bid_cierre NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS qty_ask_cierre NUMERIC;",
            "ALTER TABLE opciones_cierres ADD COLUMN IF NOT EXISTS iv_source TEXT;",
            # bid_ask_cierre
            "ALTER TABLE bid_ask_cierre ADD COLUMN IF NOT EXISTS ultimo NUMERIC;",
        ]
        for m in migrations:
            try:
                cur.execute(m)
            except Exception as me:
                logger.warning(f"Migración omitida: {me}")
        conn.commit(); cur.close(); conn.close()
        logger.info("PG init OK")
    except Exception as e:
        logger.error(f"PG init error: {e}")

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
        logger.info(f"PDF guardado en PG: {fecha}")
    except Exception as e:
        logger.error(f"PG save pdf error: {e}")

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
        logger.info(f"Opciones guardadas en PG: {len(rows)} filas, fecha {fecha}")
    except Exception as e:
        logger.error(f"PG save opciones error: {e}")

def _pg_save_veta_books():
    """Persiste _veta_books en PG para sobrevivir reinicios."""
    if not HAS_PG or not DATABASE_URL or not _veta_books: return
    try:
        conn = _pg_conn(); cur = conn.cursor()
        for sec_id, data in _veta_books.items():
            cur.execute("""
                INSERT INTO veta_books (security_id, data, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (security_id) DO UPDATE
                SET data = EXCLUDED.data, updated_at = NOW()
            """, (sec_id, json.dumps(data)))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.error(f"PG save veta_books error: {e}")

def _pg_load_veta_books():
    """Restaura _veta_books desde PG al arrancar."""
    if not HAS_PG or not DATABASE_URL: return
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("SELECT security_id, data FROM veta_books")
        rows = cur.fetchall()
        for sec_id, data in rows:
            _veta_books[sec_id] = data if isinstance(data, dict) else json.loads(data)
        cur.close(); conn.close()
        if rows: logger.info(f"[PG] Restaurados {len(rows)} veta_books")
    except Exception as e:
        logger.error(f"PG load veta_books error: {e}")

def _pg_save_cierres(fecha: str):
    """Guarda los últimos operados de _veta_md en opciones_cierres para la fecha dada."""
    if not HAS_PG or not DATABASE_URL or not _veta_md: return
    rows_iamc = {r["symbol"]: r for r in state.get("opciones", [])}
    saved = 0
    try:
        conn = _pg_conn(); cur = conn.cursor()
        TASA_VTO = {vto: _get_tasa(vto, _tasas_rt) for vto in list(_TASA_FIJA) + list(_tasas_rt or {})}
        for sym, snap in _veta_md.items():
            ultimo = snap.get("ultimo")
            if not ultimo: continue
            iamc = rows_iamc.get(sym, {})
            S    = iamc.get("precio_suby")
            K    = iamc.get("strike")
            dias = iamc.get("dias_vto")
            tipo = iamc.get("tipo")
            vto  = iamc.get("vencimiento")
            vi_calc = None
            if S and K and dias and dias > 0 and tipo:
                T = dias / 365.0
                r_rate = _get_tasa(vto, _tasas_rt)
                vi_calc = _calc_iv(ultimo, S, K, T, r_rate, tipo)
            # Bid/ask del cierre de Veta
            bid_c = snap.get("bid")
            ask_c = snap.get("ask")
            cur.execute("""
                INSERT INTO opciones_cierres
                    (symbol, fecha, subyacente, tipo, strike, vencimiento,
                     ultimo, vi_calc, precio_suby, dias_vto,
                     open_interest, volumen_ars, cant_ops,
                     bid_cierre, ask_cierre)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol, fecha) DO UPDATE SET
                    ultimo=EXCLUDED.ultimo, vi_calc=EXCLUDED.vi_calc,
                    precio_suby=EXCLUDED.precio_suby,
                    bid_cierre=EXCLUDED.bid_cierre,
                    ask_cierre=EXCLUDED.ask_cierre
            """, (sym, fecha,
                  iamc.get("subyacente"), tipo, K,
                  vto, ultimo, vi_calc, S, dias,
                  iamc.get("open_interest"),
                  iamc.get("volumen_ars"),
                  iamc.get("cant_ops"),
                  bid_c, ask_c))
            saved += 1
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[PG] Cierres guardados: {saved} opciones para {fecha}")
    except Exception as e:
        logger.error(f"PG save cierres error: {e}")

def _pg_save_bid_ask_cierre(fecha: str):
    """Guarda el último bid/ask de cada opción a las 17:00 para consulta post-rueda."""
    if not HAS_PG or not DATABASE_URL: return
    saved = 0
    try:
        conn = _pg_conn(); cur = conn.cursor()
        for sec_id, book in _veta_books.items():
            # Solo guardar si tiene puntas reales
            bid = book.get("bid")
            ask = book.get("ask")
            if not bid and not ask:
                # Intentar desde _veta_md
                sym = _norm_veta_sym(sec_id)
                md = _veta_md.get(sym)
                if md:
                    bid = md.get("bid")
                    ask = md.get("ask")
            if not bid and not ask: continue
            # Obtener symbol limpio
            sym = _norm_veta_sym(sec_id)
            if not sym: continue
            qty_bid = book.get("qty_bid")
            qty_ask = book.get("qty_ask")
            cur.execute("""
                INSERT INTO bid_ask_cierre (symbol, fecha, bid, ask, qty_bid, qty_ask, saved_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, fecha) DO UPDATE SET
                    bid=EXCLUDED.bid, ask=EXCLUDED.ask,
                    qty_bid=EXCLUDED.qty_bid, qty_ask=EXCLUDED.qty_ask,
                    saved_at=NOW()
            """, (sym, fecha, bid, ask, qty_bid, qty_ask))
            saved += 1
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[PG] Bid/Ask cierre guardados: {saved} opciones para {fecha}")
    except Exception as e:
        logger.error(f"PG save bid_ask_cierre error: {e}")

def _pg_load_bid_ask_cierre(fecha: str) -> dict:
    """Carga bid/ask del cierre de una fecha para mostrar en la tabla."""
    if not HAS_PG or not DATABASE_URL: return {}
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT symbol, bid, ask, qty_bid, qty_ask
            FROM bid_ask_cierre WHERE fecha = %s
        """, (fecha,))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {r[0]: {"bid": r[1], "ask": r[2], "qty_bid": r[3], "qty_ask": r[4]} for r in rows}
    except Exception as e:
        logger.error(f"PG load bid_ask_cierre error: {e}")
        return {}

def _pg_save_resumen_diario(rows: list, fecha: str):
    """Guarda el resumen diario por subyacente (precio, calls, puts, OI, P/C ratio, etc.)."""
    if not HAS_PG or not DATABASE_URL or not rows: return
    from collections import defaultdict
    by_suby = defaultdict(lambda: {"calls":0,"puts":0,"vol_ars":0,"oi":0,
                                    "precio":None,"tasa":None,"dias":[]})
    for r in rows:
        sub = r.get("subyacente")
        if not sub: continue
        tipo = r.get("tipo","")
        d = by_suby[sub]
        if tipo == "CALL": d["calls"] += 1
        elif tipo == "PUT": d["puts"] += 1
        d["vol_ars"] += r.get("volumen_ars") or 0
        d["oi"]      += r.get("open_interest") or 0
        if not d["precio"] and r.get("precio_suby"): d["precio"] = r["precio_suby"]
        if not d["tasa"]   and r.get("tasa_libre"):  d["tasa"]   = r["tasa_libre"]
        if r.get("dias_vto"): d["dias"].append(r["dias_vto"])
    try:
        conn = _pg_conn(); cur = conn.cursor()
        for sub, d in by_suby.items():
            total = d["calls"] + d["puts"]
            pc = round(d["puts"]/d["calls"], 4) if d["calls"] > 0 else None
            dias = d["dias"]
            cur.execute("""
                INSERT INTO subyacentes_resumen
                    (subyacente, fecha, precio_suby, calls, puts,
                     vol_ars, open_interest, put_call, tasa_libre,
                     dias_vto_min, dias_vto_max)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (subyacente, fecha) DO UPDATE SET
                    precio_suby=EXCLUDED.precio_suby,
                    calls=EXCLUDED.calls, puts=EXCLUDED.puts,
                    vol_ars=EXCLUDED.vol_ars,
                    open_interest=EXCLUDED.open_interest,
                    put_call=EXCLUDED.put_call,
                    tasa_libre=EXCLUDED.tasa_libre,
                    dias_vto_min=EXCLUDED.dias_vto_min,
                    dias_vto_max=EXCLUDED.dias_vto_max
            """, (sub, fecha, d["precio"], d["calls"], d["puts"],
                  d["vol_ars"], d["oi"], pc, d["tasa"],
                  min(dias) if dias else None,
                  max(dias) if dias else None))
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[PG] Resumen diario guardado: {len(by_suby)} subyacentes para {fecha}")
    except Exception as e:
        logger.error(f"PG save resumen error: {e}")

def _pg_save_cierres_iamc(rows: list, fecha: str):
    """Guarda todos los datos de la cadena IAMC en opciones_cierres para histórico."""
    if not HAS_PG or not DATABASE_URL or not rows: return
    saved = 0
    try:
        conn = _pg_conn(); cur = conn.cursor()
        TASA_VTO = {vto: _get_tasa(vto, _tasas_rt) for vto in list(_TASA_FIJA) + list(_tasas_rt or {})}
        for r in rows:
            sym = r.get("symbol")
            if not sym: continue
            S    = r.get("precio_suby")
            K    = r.get("strike")
            dias = r.get("dias_vto")
            tipo = r.get("tipo")
            vto  = r.get("vencimiento")
            ultimo = r.get("ultimo_precio")
            r_rate = _get_tasa(vto, _tasas_rt)
            T = dias / 365.0 if dias and dias > 0 else None
            vi_calc = None
            if ultimo and S and K and T and tipo:
                vi_calc = _calc_iv(ultimo, S, K, T, r_rate, tipo)
            cur.execute("""
                INSERT INTO opciones_cierres (
                    symbol, fecha, subyacente, tipo, strike, vencimiento,
                    moneyness, dias_vto, precio_suby,
                    ultimo, apertura, minimo, maximo, var_pct,
                    volumen_ars, cant_ops, open_interest, var_oi_pct,
                    precio_teorico, desvio_teorico, valor_temporal,
                    vi_iamc, vi_calc, vi_bid, vi_offer, vol_hist_40r,
                    delta, gamma, theta, vega, rho,
                    tasa_libre, iv_source
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s
                )
                ON CONFLICT (symbol, fecha) DO UPDATE SET
                    ultimo=COALESCE(opciones_cierres.ultimo, EXCLUDED.ultimo),
                    apertura=EXCLUDED.apertura, minimo=EXCLUDED.minimo,
                    maximo=EXCLUDED.maximo, var_pct=EXCLUDED.var_pct,
                    volumen_ars=EXCLUDED.volumen_ars, cant_ops=EXCLUDED.cant_ops,
                    open_interest=EXCLUDED.open_interest, var_oi_pct=EXCLUDED.var_oi_pct,
                    precio_teorico=EXCLUDED.precio_teorico,
                    desvio_teorico=EXCLUDED.desvio_teorico,
                    valor_temporal=EXCLUDED.valor_temporal,
                    vi_iamc=EXCLUDED.vi_iamc,
                    vi_calc=COALESCE(opciones_cierres.vi_calc, EXCLUDED.vi_calc),
                    vi_bid=EXCLUDED.vi_bid, vi_offer=EXCLUDED.vi_offer,
                    vol_hist_40r=EXCLUDED.vol_hist_40r,
                    delta=EXCLUDED.delta, gamma=EXCLUDED.gamma,
                    theta=EXCLUDED.theta, vega=EXCLUDED.vega, rho=EXCLUDED.rho,
                    tasa_libre=EXCLUDED.tasa_libre, iv_source=EXCLUDED.iv_source
            """, (
                sym, fecha, r.get("subyacente"), tipo, K, vto,
                r.get("moneyness"), dias, S,
                ultimo, r.get("apertura_prima"), r.get("min_prima"),
                r.get("max_prima"), r.get("var_prima_pct"),
                r.get("volumen_ars"), r.get("cant_ops"),
                r.get("open_interest"), r.get("var_oi_pct"),
                r.get("precio_teorico"), r.get("desvio_teorico"),
                r.get("valor_temporal"),
                r.get("vol_implicita"), vi_calc,
                r.get("vi_bid"), r.get("vi_offer"),
                r.get("vol_hist_40r"),
                r.get("delta"), r.get("gamma"),
                r.get("theta"), r.get("vega"), r.get("rho"),
                r.get("tasa_libre"), r.get("iv_source")
            ))
            saved += 1
        conn.commit(); cur.close(); conn.close()
        logger.info(f"[PG] Cierres IAMC guardados: {saved} opciones para {fecha}")
    except Exception as e:
        logger.error(f"PG save cierres IAMC error: {e}")

def _pg_load_cierre_anterior(symbol: str, fecha_hoy: str) -> dict | None:
    """Carga el último cierre disponible para un symbol antes de fecha_hoy."""
    if not HAS_PG or not DATABASE_URL: return None
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("""
            SELECT fecha, ultimo, vi_calc, precio_suby
            FROM opciones_cierres
            WHERE symbol = %s AND fecha < %s
            ORDER BY fecha DESC LIMIT 1
        """, (symbol, fecha_hoy))
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            return {"fecha": str(row[0]), "ultimo": row[1],
                    "vi_calc": row[2], "precio_suby": row[3]}
        return None
    except Exception as e:
        logger.error(f"PG load cierre error: {e}")
        return None

def _pg_load_latest():
    if not HAS_PG or not DATABASE_URL: return None, None
    try:
        conn = _pg_conn(); cur = conn.cursor()
        cur.execute("SELECT pdf_bytes, fecha FROM iamc_opciones_pdf WHERE id=1")
        row = cur.fetchone()
        cur.close(); conn.close()
        if row: return bytes(row[0]), row[1]
    except Exception as e:
        logger.error(f"PG load error: {e}")
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
        logger.error(f"PG load opciones error: {e}")
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
    if iv is not None and not (0.5 <= iv <= 500):
        r["vol_implicita"] = None
        for f in ("delta", "gamma", "theta", "vega", "rho"):
            r[f] = None
    # Gamma negativa en un CALL es imposible
    if r.get("tipo") == "CALL" and r.get("gamma") is not None and r["gamma"] < 0:
        for f in ("delta", "gamma", "theta", "vega", "rho"):
            r[f] = None
    return r

def _norm_cdf(x):
    """CDF de la normal estándar — usa math.erf (stdlib, exacto)."""
    import math
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _bs_price(S, K, T, r, sigma, tipo):
    """Precio Black-Scholes europeo."""
    import math
    if T <= 0 or sigma <= 0: return max(0, S-K) if tipo=='CALL' else max(0, K-S)
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    if tipo == 'CALL':
        return S*_norm_cdf(d1) - K*math.exp(-r*T)*_norm_cdf(d2)
    return K*math.exp(-r*T)*_norm_cdf(-d2) - S*_norm_cdf(-d1)

def _calc_iv(precio_mercado, S, K, T, r, tipo, tol=1e-5, max_iter=100):
    """
    Calcula IV por bisección dado el precio de mercado.
    Devuelve IV en % (ej: 42.5) o None si no converge.
    """
    import decimal as _dec
    try:
        precio_mercado = float(precio_mercado) if precio_mercado is not None else None
        S = float(S) if S is not None else None
        K = float(K) if K is not None else None
        T = float(T) if T is not None else None
        r = float(r) if r is not None else 0.2287
    except (TypeError, ValueError): return None
    if not precio_mercado or precio_mercado <= 0: return None
    if not S or S <= 0 or not K or K <= 0: return None
    if not T or T <= 0: return None
    # Valor intrínseco
    intrinsic = max(0, S-K) if tipo=='CALL' else max(0, K-S)
    if precio_mercado <= intrinsic: return None
    lo, hi = 0.001, 10.0  # 0.1% a 1000%
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        price = _bs_price(S, K, T, r, mid, tipo)
        diff = price - precio_mercado
        if abs(diff) < tol:
            return round(mid * 100, 4)
        if diff > 0:
            hi = mid
        else:
            lo = mid
        if hi - lo < 1e-7:
            break
    iv = (lo + hi) / 2 * 100
    return round(iv, 4) if 0.5 <= iv <= 500 else None

def _enrich_iv(rows: list) -> list:
    """
    Para filas sin IV del PDF:
    1. Intenta calcular IV desde el último precio operado.
    2. Si no hay último precio, calcula el precio teórico BS usando la VH 40r
       (o la IV ATM del subyacente como fallback) y lo asigna como precio_teorico.
       La vol_implicita en ese caso será la VH usada (precio teórico = smile plano).
    """
    TASA_VTO = {k: (_tasas_rt.get(k) or v) for k, v in {"2026-10-16": 0.2287, "2026-12-18": 0.2418}.items()}

    # Pre-calcular IV ATM por subyacente (para usar como fallback de sigma)
    iv_atm_by_suby = {}
    for r in rows:
        iv = r.get("vol_implicita")
        S  = r.get("precio_suby")
        K  = r.get("strike")
        sub = r.get("subyacente","")
        if iv and iv > 0 and S and K and abs(K - S) / S < 0.05:
            prev_k = iv_atm_by_suby.get(sub, {}).get("k") if isinstance(iv_atm_by_suby.get(sub), dict) else None
            if prev_k is None or abs(K - S) < abs(prev_k - S):
                iv_atm_by_suby[sub] = {"iv": iv, "k": K}
    # Aplanar a {sub: iv_value}
    iv_atm_by_suby = {sub: v["iv"] if isinstance(v, dict) else v for sub, v in iv_atm_by_suby.items()}

    for r in rows:
        if r.get("vol_implicita") is not None:
            r["iv_source"] = "iamc"
            continue

        S     = r.get("precio_suby")
        K     = r.get("strike")
        dias  = r.get("dias_vto")
        tipo  = r.get("tipo")
        vto   = r.get("vencimiento")
        sub   = r.get("subyacente", "")
        tasa_pct = r.get("tasa_libre")
        r_rate = (tasa_pct / 100) if tasa_pct else _get_tasa(vto, _tasas_rt)

        if not S or S <= 0 or not K or K <= 0 or not dias or dias <= 0:
            r["iv_source"] = None
            continue

        T = dias / 365.0
        ultimo = r.get("ultimo_precio")

        # 1) IV desde último operado
        if ultimo and ultimo > 0:
            iv = _calc_iv(ultimo, S, K, T, r_rate, tipo)
            if iv is not None:
                r["vol_implicita"] = iv
                r["iv_source"] = "calculada"
                continue

        # 2) Sin precio operado: calcular teórico con sigma = VH 40r o IV ATM
        vh = r.get("vol_hist_40r")
        sigma = None
        if vh and vh > 0:
            sigma = vh / 100
        elif sub in iv_atm_by_suby:
            sigma = iv_atm_by_suby[sub] / 100

        if sigma:
            teorico = _bs_price(S, K, T, r_rate, sigma, tipo)
            if teorico and teorico > 0:
                if not r.get("precio_teorico"):
                    r["precio_teorico"] = round(teorico, 4)
                # IV implícita del teórico = sigma usado (por construcción)
                r["vol_implicita"] = round(sigma * 100, 4)
                r["iv_source"] = "teorico"

    return rows

# ── Parser PDF IAMC ────────────────────────────────────────────────────────────
def _pf(v):
    if v is None: return None
    try:
        s = str(v).replace('%','').replace(',','').strip()
        if s in ('', '-', '—'): return None
        return float(s)
    except Exception as e:
        logger.debug(f"_pf parse error: {e}")
        return None

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

    # COL_MAP para 76 columnas (YPFD puts/calls diciembre, algunos subyacentes)
    # Diferencia con 78: faltan 2 cols en algún punto medio
    # Del debug: col 14=apertura, col 17=min, col 20=max, col 23=ultimo, col 26=var%
    # col 29=hora, col 32=vol, col 34=ops, col 37=OI, col 40=varOI
    # col 43=teorico, col 46=desvio, col 49=valor_temp, col 52=VH, col 55=IV
    # col 58=delta, col 61=gamma, col 64=theta, col 67=vega, col 70=rho
    COL_MAP_76 = {
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
# print(f"  [parser] usando COL_MAP_81 para tabla con {ncols} cols")
            return COL_MAP_81
        if ncols >= 77:
            return COL_MAP_78
        if ncols >= 74:
# print(f"  [parser] usando COL_MAP_76 para tabla con {ncols} cols")
            return COL_MAP_76
# print(f"  [parser] usando COL_MAP_73 para tabla con {ncols} cols")
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
                for line in text.split('\n'):
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
                            # Solo loguear si el número de columnas es muy inusual
                            if len(row) not in (73, 74, 76, 78, 81):
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
        logger.error(f"PDF parse error: {e}")
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

    # ── Calcular IV desde último precio donde no hay IV del PDF ──
    rows = _enrich_iv(rows)
    iv_calc = sum(1 for r in rows if r.get("iv_source") == "calculada")
    iv_iamc = sum(1 for r in rows if r.get("iv_source") == "iamc")
    logger.info(f"[parser] IV: {iv_iamc} del IAMC + {iv_calc} calculadas desde último precio")

    # ── Diagnóstico ──
    if mixed:
        logger.info(f"[parser] {len(mixed)} filas reasignadas por símbolo (mezcla detectada). "
              f"Muestra: {mixed[:10]}")
    if recovered_count:
        logger.info(f"[parser] {recovered_count} filas recuperadas desde línea colapsada")
    if garbage_skipped:
        logger.info(f"[parser] {garbage_skipped} filas de basura descartadas (rankings/otras)")
    if dup_count:
        logger.info(f"[parser] {dup_count} filas duplicadas fusionadas (rankings vs cadena)")
    print(f"[parser] Fuentes de vencimiento: {dict(vto_sources)}")
    if used_static_series:
        logger.info(f"[parser] Series resueltas con fallback estático: {sorted(used_static_series)} — "
              f"actualizar SERIE_VTO_FALLBACK cuando cambien los vencimientos")
    if unmapped:
        logger.info(f"[parser] ⚠ Prefijos sin mapear: {sorted(unmapped)[:20]} — "
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
                _pg_save_cierres_iamc(rows, target_date.isoformat())
                _pg_save_resumen_diario(rows, target_date.isoformat())
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
    # Intentar cargar rows ya parseadas desde iamc_opciones_data (evita reparsear el PDF)
    rows = _pg_load_opciones()
    if rows:
        _, fecha = _pg_load_latest()
        _, resumen, _ = parse_iamc_pdf(b"")  # solo para estructura de resumen
        # Recalcular resumen desde rows
        from collections import defaultdict
        resumen = defaultdict(lambda: {"calls":0,"puts":0,"vol_ars":0,"oi":0})
        for r in rows:
            sub = r.get("subyacente","")
            if r.get("tipo") == "CALL": resumen[sub]["calls"] += 1
            elif r.get("tipo") == "PUT": resumen[sub]["puts"] += 1
            resumen[sub]["vol_ars"] += r.get("volumen_ars") or 0
            resumen[sub]["oi"]      += r.get("open_interest") or 0
        state["opciones"]    = rows
        state["resumen"]     = dict(resumen)
        state["fecha"]       = fecha or ""
        state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
        state["descarga_ok"] = True
        print(f"Cargado desde PG (rows): {len(rows)} opciones, fecha {fecha}")
    else:
        # No hay datos en PG — descargar PDF
        pdf_bytes, fecha = _pg_load_latest()
        if pdf_bytes:
            rows, resumen, fecha_str = parse_iamc_pdf(pdf_bytes)
            state["opciones"]    = rows
            state["resumen"]     = resumen
            state["fecha"]       = fecha
            state["updated_at"]  = datetime.now(TZ_ARG).isoformat()
            state["descarga_ok"] = True
            print(f"Cargado desde PG (pdf): {len(rows)} opciones, fecha {fecha}")
        else:
            await descargar_iamc_pdf()
    # Restaurar último estado de Veta desde PG
    _pg_load_veta_books()

async def _guardar_cierres_si_corresponde():
    """Guarda bid/ask a las 17:00 y cierres de último operado a las 17:05."""
    bid_ask_guardado = False
    cierres_guardado = False
    ultimo_dia = None
    while True:
        now = datetime.now(TZ_ARG)
        fecha_hoy = now.strftime("%Y-%m-%d")
        # Reset flags al cambiar de día
        if ultimo_dia != fecha_hoy:
            ultimo_dia = fecha_hoy
            bid_ask_guardado = False
            cierres_guardado = False
        if now.weekday() < 5:
            # 17:00 — guardar bid/ask de cierre
            if now.hour == 17 and now.minute == 0 and not bid_ask_guardado:
                print(f"[Cierre] Guardando bid/ask del cierre {fecha_hoy}")
                _pg_save_bid_ask_cierre(fecha_hoy)
                bid_ask_guardado = True
                await asyncio.sleep(60)
            # 17:05 — guardar últimos operados
            if now.hour == 17 and now.minute == 5 and not cierres_guardado:
                print(f"[Cierre] Guardando últimos operados {fecha_hoy}")
                _pg_save_cierres(fecha_hoy)
                _pg_save_resumen_diario(state.get("opciones", []), fecha_hoy)
                cierres_guardado = True
                await asyncio.sleep(60)
        await asyncio.sleep(20)

async def _scheduler_iamc():
    """
    Descarga el PDF de IAMC:
    - Al arrancar: si el PDF en PG tiene más de 1 día, reintenta antes de las 10am
    - A las 18:30 ARG después del cierre
    - Reintenta cada 15 min hasta las 20:00
    """
    # Al arrancar: verificar si el PDF es viejo
    pdf_bytes, fecha_pg = _pg_load_latest()
    if pdf_bytes and fecha_pg:
        from datetime import date as _date
        hoy = datetime.now(TZ_ARG).date()
        dias_old = (hoy - fecha_pg).days if hasattr(fecha_pg, 'days') else 0
        try:
            if hasattr(fecha_pg, 'strftime'):
                fecha_dt = fecha_pg
            else:
                fecha_dt = date.fromisoformat(str(fecha_pg))
            dias_old = (hoy - fecha_dt).days
        except: dias_old = 0

        if dias_old > 1:
            now = datetime.now(TZ_ARG)
            print(f"[IAMC] PDF tiene {dias_old} días de antigüedad ({fecha_pg}), intentando actualizar...")
            # Intentar antes de las 10am o en cualquier momento si es muy viejo
            if now.hour < 10 or dias_old > 2:
                for _ in range(4):  # hasta 4 intentos de 15 min
                    ok = await descargar_iamc_pdf()
                    if ok:
                        print(f"[IAMC] PDF actualizado exitosamente")
                        break
                    now = datetime.now(TZ_ARG)
                    if now.hour >= 10:
                        print("[IAMC] Pasaron las 10am, esperando el ciclo de las 18:30")
                        break
                    print("[IAMC] Reintentando en 15 min...")
                    await asyncio.sleep(900)

    while True:
        now = datetime.now(TZ_ARG)
        # Próximo objetivo: 18:30 día hábil
        target = now.replace(hour=18, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        while target.weekday() >= 5:
            target = target + timedelta(days=1)

        wait_secs = (target - now).total_seconds()
        print(f"[IAMC] Próxima descarga: {target.strftime('%Y-%m-%d %H:%M')} ARG (en {wait_secs/3600:.1f}h)")
        await asyncio.sleep(wait_secs)

        for _ in range(10):
            now = datetime.now(TZ_ARG)
            if now.hour >= 20:
                print("[IAMC] Pasaron las 20:00, dejando de reintentar por hoy")
                break
            ok = await descargar_iamc_pdf()
            if ok:
                break
            print("[IAMC] Reintentando en 15 min...")
            await asyncio.sleep(900)

@app.on_event("startup")
async def startup():
    _pg_init()
    global _scheduler_task, _veta_ws_task
    _scheduler_task = asyncio.create_task(scheduler())
    asyncio.create_task(_guardar_cierres_si_corresponde())
    asyncio.create_task(_scheduler_iamc())
    if VETA_COOKIE:
        _veta_ws_task = asyncio.create_task(_veta_ws_loop())
        print("[Veta WS] Task iniciada")

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
    except Exception as e:
        logger.warning(f"validar_stoken error: {e}")
        return cached["valid"] if cached else False

async def require_auth(authorization: str = Header(None)) -> str:
    """
    Auth deshabilitada en desarrollo — habilitar antes de producción.
    Para activar: descomentar las líneas de validación.
    """
    return authorization or ""
    # if not authorization or not authorization.startswith("Bearer "):
    #     raise HTTPException(status_code=401, detail="Token requerido")
    # token = authorization[7:].strip()
    # if not await validar_stoken(token):
    #     raise HTTPException(status_code=401, detail="Token inválido o expirado")
    # return token

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

@app.get("/api/opciones/cadena", dependencies=[Depends(require_auth)])
async def get_cadena(
    subyacente: str = Query(None),
    tipo: str = Query(None),
    vencimiento: str = Query(None),
    fecha: str = Query(None),
):
    """Devuelve la cadena de opciones con bid/ask y VI de bid/offer/último desde Veta."""
    rows = state["opciones"]
    if not rows and fecha:
        rows = _pg_load_opciones(fecha)
    if subyacente:
        rows = [r for r in rows if (r.get("subyacente") or "").upper() == subyacente.upper()]
    if tipo:
        rows = [r for r in rows if (r.get("tipo") or "").upper() == tipo.upper()]
    if vencimiento:
        rows = [r for r in rows if r.get("vencimiento") == vencimiento]

    # Enriquecer con bid/ask y VI de Veta si hay books disponibles
    TASA_VTO = {k: (_tasas_rt.get(k) or v) for k, v in {"2026-10-16": 0.2287, "2026-12-18": 0.2418}.items()}
    result = []
    # Bid/ask del cierre: mostrar si son las 17-23hs de hoy o antes de las 10am del día siguiente
    now_arg = datetime.now(TZ_ARG)
    fecha_iamc = state.get("fecha", "")
    mostrar_cierre_ba = False
    cierre_ba_data = {}
    if fecha_iamc:
        hora = now_arg.hour
        # Post-cierre hoy (17-23hs) o madrugada/mañana hasta las 10am
        if hora >= 17 or hora < 10:
            # Fecha del cierre a mostrar
            if hora >= 17:
                fecha_cierre_ba = now_arg.strftime("%Y-%m-%d")
            else:
                fecha_cierre_ba = (now_arg - timedelta(days=1)).strftime("%Y-%m-%d")
            cierre_ba_data = _pg_load_bid_ask_cierre(fecha_cierre_ba)
            if cierre_ba_data:
                mostrar_cierre_ba = True
                state["_cierre_ba_fecha"] = fecha_cierre_ba

    # Convertir todos los Decimal de PG a float para evitar TypeError
    import decimal as _decimal
    def _tofloat(v):
        return float(v) if isinstance(v, _decimal.Decimal) else v
    rows = [{k: _tofloat(v) for k, v in r.items()} for r in rows]

    matched_md = 0
    for r in rows:
        row = dict(r)
        sym    = (row.get("symbol") or "").upper()
        sec_id = _symbol_to_security_id(row.get("symbol",""))
        # Actualizar precio subyacente con dato RT si disponible
        suby = (row.get("subyacente") or "").upper()
        if suby and suby in _precios_suby:
            row["precio_suby"] = float(_precios_suby[suby])
        elif row.get("precio_suby") is not None:
            row["precio_suby"] = float(row["precio_suby"])
        # Primero buscar en _veta_md (datos M: más frescos)
        md_snap = _veta_md.get(sym)
        # Luego en _veta_books (datos B: con profundidad)
        book = _veta_books.get(sec_id)

        bid = ask = qty_bid = qty_ask = book_ts = None
        if book:
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            bid     = float(bids[0]["price"]) if bids else (float(book["bid"]) if book.get("bid") else None)
            ask     = float(asks[0]["price"]) if asks else (float(book["ask"]) if book.get("ask") else None)
            qty_bid = float(bids[0]["qty"])   if bids else (float(book["qty_bid"]) if book.get("qty_bid") else None)
            qty_ask = float(asks[0]["qty"])   if asks else (float(book["qty_ask"]) if book.get("qty_ask") else None)
            book_ts = book.get("ts")
        if md_snap:
            matched_md += 1
            bid     = md_snap.get("bid") or bid
            ask     = md_snap.get("ask") or ask

        # Si no hay bid/ask en RT y es post-cierre, usar bid/ask del cierre guardado
        if not bid and not ask and mostrar_cierre_ba and sym in cierre_ba_data:
            ba = cierre_ba_data[sym]
            bid     = ba.get("bid")
            ask     = ba.get("ask")
            qty_bid = ba.get("qty_bid")
            qty_ask = ba.get("qty_ask")
            row["bid_ask_source"] = "cierre"

        if bid or ask or md_snap:
            row["bid"]     = bid
            row["ask"]     = ask
            row["qty_bid"] = qty_bid
            row["qty_ask"] = qty_ask
            row["book_ts"] = book_ts

            # Calcular VI de bid, offer y último
            S    = row.get("precio_suby")
            # Fallback: buscar precio_suby de otra fila del mismo subyacente
            if not S and subyacente:
                S = next((x.get("precio_suby") for x in rows if x.get("precio_suby") and x.get("subyacente","").upper() == subyacente.upper()), None)
            K    = row.get("strike")
            dias = row.get("dias_vto")
            tipo_op = row.get("tipo")
            vto  = row.get("vencimiento")
            r_rate = _get_tasa(vto, _tasas_rt)
            if S and K and dias and dias > 0:
                T = dias / 365.0
                S = float(S) if S else S; K = float(K) if K else K
                if bid:    row["vi_bid"]    = _calc_iv(bid,    S, K, T, r_rate, tipo_op)
                if ask:    row["vi_offer"]  = _calc_iv(ask,    S, K, T, r_rate, tipo_op)
                # Último operado: primero de _veta_md, luego de _veta_books, luego cierre PG
                veta_ult = (md_snap.get("ultimo") if md_snap else None) or (book.get("ultimo") if book else None)
                if not veta_ult:
                    cierre_pg = _pg_load_cierre_anterior(sym, state.get("fecha") or datetime.now(TZ_ARG).strftime("%Y-%m-%d"))
                    if cierre_pg:
                        veta_ult = cierre_pg.get("ultimo")
                        row["cierre_fecha"] = cierre_pg.get("fecha")
                if veta_ult:
                    row["veta_ultimo"] = veta_ult
                    row["vi_ultimo"]   = _calc_iv(veta_ult, S, K, T, r_rate, tipo_op)
                else:
                    row["veta_ultimo"] = None
                    row["vi_ultimo"]   = None
        else:
            row["bid"] = row["ask"] = row["qty_bid"] = row["qty_ask"] = row["book_ts"] = None
            row["vi_bid"] = row["vi_offer"] = None
            row["veta_ultimo"] = None
            row["vi_ultimo"]   = None

        result.append(row)

    tasa_oct = _tasas_rt.get("2026-10-16")
    tasa_dic = _tasas_rt.get("2026-12-18")
    # Precio RT del subyacente actual
    suby_actual = state.get("currentSuby") or (result[0].get("subyacente") if result else None)
    precio_suby_rt = _precios_suby.get((suby_actual or "").upper())
    return {"fecha": state["fecha"], "total": len(result), "data": result,
            "precio_suby_rt": precio_suby_rt,
            "tasas_rt": {"oct": round(tasa_oct*100,2) if tasa_oct else None,
                         "dic": round(tasa_dic*100,2) if tasa_dic else None},
            "cierre_ba": mostrar_cierre_ba, "cierre_ba_fecha": state.get("_cierre_ba_fecha")}

@app.get("/api/veta/debug-md", dependencies=[Depends(require_auth)])
async def veta_debug_md(symbol: str = None):
    if symbol:
        s = _norm_veta_sym(symbol)
        return {"lookup": s, "found": _veta_md.get(s)}
    return {"count": len(_veta_md), "keys": list(_veta_md)[:30], "sample": list(_veta_md.values())[:5]}

@app.get("/api/veta/debug-raw", dependencies=[Depends(require_auth)])
async def veta_debug_raw(n: int = 10):
    return {"recent": list(_veta_raw_m)[-n:]}

@app.get("/api/opciones/subyacentes", dependencies=[Depends(require_auth)])
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

@app.get("/api/opciones/resumen", dependencies=[Depends(require_auth)])
async def get_resumen():
    """Ranking de volumen, OI y ratio put/call por subyacente."""
    return {"fecha": state["fecha"], "updated_at": state["updated_at"], **state["resumen"]}

@app.get("/api/opciones/symbol/{symbol}", dependencies=[Depends(require_auth)])
async def get_symbol(symbol: str):
    """Datos de una opción específica por symbol."""
    rows = [r for r in state["opciones"] if r.get("symbol","").upper() == symbol.upper()]
    if not rows:
        return JSONResponse(status_code=404, content={"error": f"Symbol {symbol} no encontrado"})
    return {"fecha": state["fecha"], "data": rows[0]}

VETA_BASE = "https://matriz.bcch.xoms.com.ar/api/v2"
VETA_WS   = "wss://matriz.bcch.xoms.com.ar/ws"

# ── Cache de books en memoria ─────────────────────────────────────────────────
# { "bm_MERV_GFGC7000OC_24hs": { bid, ask, qty_bid, qty_ask, ts } }
_veta_books: dict = {}
_veta_md: dict    = {}   # "GFGV6000OC" → snapshot RT del M:

# Instrumentos de tasa libre de riesgo por vencimiento de opciones
# TEM de emisión y fecha de vencimiento para calcular valor técnico al vto
LECAP_CONFIG = {
    "2026-10-16": {"ticker": "S3006",  "sec_id": "bm_MERV_S3006_CI",  "tem": 0.0255, "fecha_vto_lecap": "2026-10-30"},
    "2026-12-18": {"ticker": "T30J6",  "sec_id": "bm_MERV_T30J6_CI",  "tem": 0.0255, "fecha_vto_lecap": "2026-12-30"},
}
_tasas_rt: dict   = {}  # vencimiento_opcion → tasa_anual_efectiva en tiempo real
_precios_suby: dict = {}  # "GGAL" → precio RT del subyacente
import collections as _collections
_veta_raw_m = _collections.deque(maxlen=80)

def _calc_tasa_lecap(precio_mercado: float, vto_opcion: str) -> float | None:
    """Calcula la tasa anual efectiva implícita desde el precio de mercado de la LECAP.
    precio_mercado: precio por cada $100 VN (ej: 131.885)
    Retorna TEA como decimal (ej: 0.2287)
    """
    cfg = LECAP_CONFIG.get(vto_opcion)
    if not cfg or not precio_mercado or precio_mercado <= 0:
        return None
    try:
        from datetime import date as _date
        hoy = datetime.now(TZ_ARG).date()
        vto_lecap = _date.fromisoformat(cfg["fecha_vto_lecap"])
        dias = (vto_lecap - hoy).days
        if dias <= 0: return None
        # Valor técnico al vencimiento: capitaliza desde emisión con TEM
        # Como capitaliza continuamente, usamos el precio de mercado directamente
        # precio cotiza por $100 VN, al vto paga el valor técnico (≈ precio al vto)
        # Usamos: precio_hoy * (1 + r_periodo) = valor_vto
        # Para simplificar: asumimos que el valor técnico al vto es 1000 por VN 1000
        # y precio_mercado es por VN 100
        # Entonces: r_anual = (VN/precio * 100)^(365/dias) - 1
        # Pero la LECAP capitaliza desde emisión, así que el valor al vto > VN
        # Lo correcto: usar el precio para estimar la TEA del período restante
        # comparando con el valor técnico esperado
        tem = cfg["tem"]
        # Aproximar meses desde emisión hasta vto (≈ 12 meses para S30O6)
        # Valor técnico ≈ 1000 * (1+TEM)^12 para una LECAP anual
        # Usamos interpolación: valor_vto = precio_mercado / precio_emision * VN
        # Más simple y preciso: calcular directamente desde precio de mercado
        # Rendimiento para el período restante:
        # Si comprás a precio_mercado (por cada 100) y al vto recibís valor_tecnico
        # Estimamos valor técnico usando TEM desde la fecha de emisión
        # Fecha de emisión ≈ 1 año antes del vto
        from datetime import date as _date
        fecha_emision_aprox = _date(vto_lecap.year - 1, vto_lecap.month, vto_lecap.day)
        dias_total = (vto_lecap - fecha_emision_aprox).days
        meses_total = dias_total / 30.5
        valor_tecnico_vto = 100 * (1 + tem) ** meses_total  # por cada $100 VN
        # TEA implícita para el período restante
        r_periodo = (valor_tecnico_vto / precio_mercado) - 1
        tea = (1 + r_periodo) ** (365 / dias) - 1
        return round(tea, 6)
    except Exception as e:
        print(f"[LECAP] Error calc tasa: {e}")
        return None

def _norm_veta_sym(security_id: str) -> str:
    """'bm_MERV_GFGV6000OC_CI' → 'GFGV6000OC', 'bm_MERV_GGAL_24hs' → 'GGAL'"""
    s = security_id
    for p in ("bm_MERV_", "MERV_", "bm_"):
        if s.startswith(p): s = s[len(p):]; break
    # Quitar sufijos conocidos: _CI, _24hs, _48hs, _DI, etc.
    for suf in ("_CI", "_24hs", "_48hs", "_DI"):
        if s.upper().endswith(suf.upper()):
            s = s[:-len(suf)]
            break
    return s.upper()

def _pf(x):
    try: return float(x) if x not in ("", None) else None
    except (TypeError, ValueError): return None

def _parse_veta_m(raw_after_prefix: str):
    """Parsea 'bm_MERV_GFGV6000OC_CI|seq|qty_bid|bid|qty_ask|ask|...|ultimo...'"""
    pipe = raw_after_prefix.find("|")
    if pipe == -1: return None
    security_id = raw_after_prefix[:pipe]
    f = raw_after_prefix[pipe+1:].split("|")
    # Según log real: f[0]=seq f[1]=qty_bid f[2]=bid f[3]=qty_ask f[4]=ask ... f[13]=ultimo
    return {
        "symbol":      _norm_veta_sym(security_id),
        "security_id": security_id,
        "bid":         _pf(f[2]) if len(f)>2 else None,
        "ask":         _pf(f[3]) if len(f)>3 else None,
        "ultimo":      _pf(f[14]) if len(f)>14 and f[14] else None,
    }

def _dispatch_veta(item: str):
    if not isinstance(item, str): return
    if item.startswith("M:"):
        _veta_raw_m.append(item)
        snap = _parse_veta_m(item[2:])
        if snap and snap.get("symbol"):
            _veta_md[snap["symbol"]] = snap
            sec_id = snap["security_id"]
            if sec_id not in _veta_books: _veta_books[sec_id] = {}
            if snap.get("bid"):    _veta_books[sec_id]["bid"]    = snap["bid"]
            if snap.get("ask"):    _veta_books[sec_id]["ask"]    = snap["ask"]
            if snap.get("ultimo"): _veta_books[sec_id]["ultimo"] = snap["ultimo"]
            # Detectar LECAPs de referencia y actualizar tasas RT
            for vto_op, cfg in LECAP_CONFIG.items():
                if snap["symbol"].upper() == cfg["ticker"].upper():
                    precio = snap.get("ultimo") or snap.get("bid") or snap.get("ask")
                    if precio and precio > 0:
                        tea = _calc_tasa_lecap(precio, vto_op)
                        if tea:
                            _tasas_rt[vto_op] = tea
            # Detectar subyacentes y actualizar precio RT
            sym_upper = snap["symbol"].upper()
            subyacentes_conocidos = {(r.get("subyacente") or "").upper() for r in state.get("opciones", [])}
            if sym_upper in subyacentes_conocidos:
                ultimo = snap.get("ultimo") or snap.get("bid") or snap.get("ask")
                if ultimo and ultimo > 0:
                    _precios_suby[sym_upper] = float(ultimo)
                    # print(f"[Suby RT] {sym_upper} = {ultimo}")
    elif item.startswith("B:"):
        sec_id, book = _parse_book_msg(item[2:])
        if sec_id and book:
            _veta_books[sec_id] = book
            # También verificar si es LECAP y actualizar tasa con el mid del book
            for vto_op, cfg in LECAP_CONFIG.items():
                if sec_id == cfg["sec_id"]:
                    bids = book.get("bids", [])
                    asks = book.get("asks", [])
                    bid_p = bids[0]["price"] if bids else None
                    ask_p = asks[0]["price"] if asks else None
                    if bid_p and ask_p:
                        mid = (bid_p + ask_p) / 2
                        tea = _calc_tasa_lecap(mid, vto_op)
                        if tea:
                            _tasas_rt[vto_op] = tea

_veta_ws_task = None
_veta_session = {"id": None, "conn_id": None, "csrf": None}

async def _veta_get_session() -> dict:
    """Extrae session_id de la cookie _mtz_web_key y obtiene csrfToken del profile."""
    if not VETA_COOKIE: return {}
    # Extraer _mtz_web_key de la cookie
    session_id = None
    for part in VETA_COOKIE.split(';'):
        part = part.strip()
        if part.startswith('_mtz_web_key='):
            session_id = part[len('_mtz_web_key='):]
            break
    conn_id = str(int(datetime.now().timestamp() * 1000))
    try:
        async with httpx.AsyncClient(timeout=10, verify=False) as c:
            r = await c.get(f"{VETA_BASE}/profile",
                params={"_ds": int(datetime.now().timestamp()*1000)},
                headers={
                    "Cookie": VETA_COOKIE,
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": "https://matriz.bcch.xoms.com.ar/principal/favoritos",
                    "Origin": "https://matriz.bcch.xoms.com.ar",
                })
            if r.status_code == 200:
                data = r.json()
                csrf = data.get("csrfToken")
                print(f"[Veta] Profile OK, session_id={str(session_id)[:20]}..., csrf={str(csrf)[:20]}...")
                return {"csrf": csrf, "session": session_id, "conn_id": conn_id}
    except Exception as e:
        print(f"[Veta] Error obteniendo sesión: {e}")
    return {"session": session_id, "conn_id": conn_id}

def _symbol_to_security_id(symbol: str) -> str:
    """Convierte símbolo BYMA a securityId de Veta: GFGC7000OC → bm_MERV_GFGC7000OC_CI"""
    return f"bm_MERV_{symbol}_CI"

def _parse_book_msg(raw: str):
    """
    Parsea mensaje book de Veta:
    B:securityId!seq!qty_bid|bid|ask|qty_ask!...
    """
    parts = raw.split('!')
    if len(parts) < 3: return None, None
    security_id = parts[0]
    bids, asks = [], []
    for linea in parts[2:]:
        if not linea or linea == '|||': continue
        cols = linea.split('|')
        if len(cols) < 4: continue
        try:
            qb, b, a, qa = float(cols[0]), float(cols[1]), float(cols[2]), float(cols[3])
            if not (b != b) and b > 0 and qb > 0: bids.append({"price": b, "qty": qb})
            if not (a != a) and a > 0 and qa > 0: asks.append({"price": a, "qty": qa})
        except: continue
    bids.sort(key=lambda x: -x["price"])
    asks.sort(key=lambda x:  x["price"])
    return security_id, {"bids": bids[:5], "asks": asks[:5], "ts": datetime.now(TZ_ARG).isoformat()}

def _parse_md_msg(raw: str):
    """
    Parsea mensaje market data de Veta:
    M:securityId|seq|qty_bid|bid|ask|qty_ask|lst|datetime|...|vol|von|...
    fields[0]=seq, [1]=qty_bid, [2]=bid, [3]=ask, [4]=qty_ask,
    [5]=lst (último operado), [6]=datetime, [9]=vol_ars, [10]=von (cant_ops)
    """
    pipe = raw.find('|')
    if pipe == -1: return None, None
    security_id = raw[:pipe]
    fields = raw[pipe+1:].split('|')
    try:
        qty_bid = float(fields[1]) if len(fields)>1 and fields[1] else None
        bid     = float(fields[2]) if len(fields)>2 and fields[2] else None
        ask     = float(fields[3]) if len(fields)>3 and fields[3] else None
        qty_ask = float(fields[4]) if len(fields)>4 and fields[4] else None
        # fields[5] vacío — último operado está en fields[14], fecha en fields[15]
        ultimo  = float(fields[14]) if len(fields)>14 and fields[14] else None
        # vol_ars acumulado: fields[8] o fields[9]
        vol     = float(fields[8]) if len(fields)>8 and fields[8] else None
        von     = None  # cant_ops no disponible en M:
        return security_id, {
            "bid": bid, "ask": ask,
            "qty_bid": qty_bid, "qty_ask": qty_ask,
            "ultimo": ultimo,
            "vol_ars": vol,
            "cant_ops": von,
            "ts": datetime.now(TZ_ARG).isoformat()
        }
    except Exception as e:
        logger.debug(f"_pf parse error: {e}")
        return None, None

async def _veta_ws_loop():
    """Loop WebSocket de Veta — mantiene conexión y actualiza _veta_books."""
    import websockets
    while True:
        if not VETA_COOKIE:
            await asyncio.sleep(60)
            continue
        try:
            sess = await _veta_get_session()
            session_id = sess.get("session") or ""
            conn_id    = sess.get("conn_id") or str(int(datetime.now().timestamp()*1000))
            ws_url = f"{VETA_WS}?session_id={session_id}&conn_id={conn_id}" if session_id else VETA_WS
            print(f"[Veta WS] Conectando: {ws_url[:60]}...")

            async with websockets.connect(
                ws_url,
                extra_headers={
                    "Cookie": VETA_COOKIE,
                    "Origin": "https://matriz.bcch.xoms.com.ar",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                },
                ping_interval=30,
                ping_timeout=10,
            ) as ws:
                print("[Veta WS] Conectado ✅")
                _veta_session["id"] = session_id
                _veta_session["conn_id"] = conn_id

                # Suscribir a todas las opciones — md para último operado, book para bid/offer
                todas = [r["symbol"] for r in state["opciones"] if r.get("symbol")]
                for i in range(0, len(todas), 50):
                    lote = todas[i:i+50]
                    # md: trae bid/ask/último en tiempo real
                    md_topics = [f"md.{_symbol_to_security_id(s)}" for s in lote]
                    await ws.send(json.dumps({"_req": "S", "topicType": "md", "topics": md_topics, "replace": False}))
                    await asyncio.sleep(0.05)
                    # book: trae las puntas del book
                    book_topics = [f"book.{_symbol_to_security_id(s)}" for s in lote]
                    await ws.send(json.dumps({"_req": "S", "topicType": "book", "topics": book_topics, "replace": False}))
                    await asyncio.sleep(0.05)
                # Suscribir LECAPs de referencia de tasa
                # Suscribir LECAPs de tasa
                lecap_sec_ids = [cfg["sec_id"] for cfg in LECAP_CONFIG.values()]
                await ws.send(json.dumps({"_req": "S", "topicType": "md",   "topics": [f"md.{s}"   for s in lecap_sec_ids], "replace": False}))
                await ws.send(json.dumps({"_req": "S", "topicType": "book", "topics": [f"book.{s}" for s in lecap_sec_ids], "replace": False}))
                # Suscribir subyacentes para precio en tiempo real
                subyacentes_uniq = list(set(r["subyacente"] for r in state["opciones"] if r.get("subyacente")))
                suby_sec_ids = [f"bm_MERV_{s}_24hs" for s in subyacentes_uniq]
                await ws.send(json.dumps({"_req": "S", "topicType": "md", "topics": [f"md.{s}" for s in suby_sec_ids], "replace": False}))
                print(f"[Veta WS] Suscrito a {len(todas)} opciones + {len(lecap_sec_ids)} LECAPs + {len(suby_sec_ids)} subyacentes")

                msg_count = 0
                async for message in ws:
                    if isinstance(message, bytes): message = message.decode()
                    if message == 'pong': continue
                    msg_count += 1
                    if msg_count <= 5:
                        print(f"[Veta WS] msg sample #{msg_count}: {message[:120]}")

                    # Los mensajes pueden venir como array JSON o string directo
                    stripped = message.strip()
                    if stripped.startswith('['):
                        try:
                            arr = json.loads(stripped)
                            if isinstance(arr, list):
                                for item in arr: _dispatch_veta(item)
                        except: pass
                    elif stripped.startswith('{'):
                        pass  # clock/fixstatus, ignorar
                    else:
                        _dispatch_veta(stripped)

                    # Persistir en PG cada 200 mensajes
                    if msg_count % 200 == 0:
                        _pg_save_veta_books()

        except Exception as e:
            print(f"[Veta WS] Error: {e}. Reconectando en 10s...")
        await asyncio.sleep(10)

@app.get("/admin/veta-status", dependencies=[Depends(require_auth)])
async def veta_status():
    """Estado del WebSocket de Veta y books recibidos."""
    return {
        "veta_cookie_ok": bool(VETA_COOKIE),
        "ws_task_running": _veta_ws_task is not None and not _veta_ws_task.done(),
        "session_id": str(_veta_session.get("id",""))[:30] if _veta_session.get("id") else None,
        "books_en_cache": len(_veta_books),
        "muestra_books": {k: v for k, v in list(_veta_books.items())[:3]},
        "subs_activas": list(_veta_books.keys())[:10],
    }


async def get_orderbook(symbol: str):
    """Bid/ask en tiempo real desde Veta WebSocket."""
    if not VETA_COOKIE:
        return JSONResponse(status_code=503, content={"error": "VETA_COOKIE no configurada"})
    sec_id = _symbol_to_security_id(symbol)
    book = _veta_books.get(sec_id)
    if book:
        return {"symbol": symbol, "security_id": sec_id, "book": book}
    # Si no está en cache, suscribir on-demand
    return {"symbol": symbol, "security_id": sec_id, "book": None,
            "msg": "Suscribiendo — reintentar en 2s"}

@app.get("/api/opciones/iv_surface/{subyacente}", dependencies=[Depends(require_auth)])
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
@app.get("/admin/reparse", dependencies=[Depends(require_auth)])
@app.post("/admin/reparse", dependencies=[Depends(require_auth)])
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

@app.get("/admin/refresh", dependencies=[Depends(require_auth)])
@app.post("/admin/refresh", dependencies=[Depends(require_auth)])
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

@app.post("/admin/upload-pdf", dependencies=[Depends(require_auth)])
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

@app.get("/api/opciones/disponibles", dependencies=[Depends(require_auth)])
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

@app.get("/admin/debug-symbols", dependencies=[Depends(require_auth)])
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

@app.get("/admin/debug-parser", dependencies=[Depends(require_auth)])
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

@app.get("/admin/debug-page/{page_num}", dependencies=[Depends(require_auth)])
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

@app.get("/admin/debug-iamc", dependencies=[Depends(require_auth)])
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

@app.get("/admin/test-iamc-url", dependencies=[Depends(require_auth)])
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

# ── ESTRATEGIAS ────────────────────────────────────────────────────────────────

def _payoff_array(legs: list, s_range: list) -> list:
    """
    legs: [{"tipo": "CALL"|"PUT", "strike": K, "side": "long"|"short", "prima": p, "qty": 1}]
    Devuelve lista de {s, pnl} para cada precio S en s_range.
    """
    import math
    result = []
    for S in s_range:
        pnl = 0.0
        for leg in legs:
            K     = leg["strike"]
            side  = leg["side"]   # "long" o "short"
            prima = leg["prima"]  # costo de la prima (positivo)
            qty   = leg.get("qty", 1)
            if leg["tipo"] == "CALL":
                intrinsic = max(0.0, S - K)
            else:
                intrinsic = max(0.0, K - S)
            if side == "long":
                pnl += (intrinsic - prima) * qty
            else:
                pnl += (prima - intrinsic) * qty
        result.append({"s": round(S, 2), "pnl": round(pnl, 4)})
    return result

def _chance_estimate(legs: list, spot: float, sigma: float, T: float) -> float | None:
    """
    Estima la probabilidad de que la estrategia expire con ganancia,
    usando distribución log-normal del subyacente.
    """
    import math
    if not spot or not sigma or not T or sigma <= 0 or T <= 0:
        return None

    # Identificar la zona de profit de la estrategia
    # Usamos 200 puntos en ±3sigma del spot
    s_log_std = sigma / 100 * math.sqrt(T)
    lo = spot * math.exp(-3.5 * s_log_std)
    hi = spot * math.exp(+3.5 * s_log_std)
    n_pts = 200
    step = (hi - lo) / n_pts

    mu = math.log(spot) + (0 - 0.5 * (sigma/100)**2) * T  # drift neutro al riesgo
    total_prob = 0.0
    profit_prob = 0.0

    for i in range(n_pts):
        S = lo + (i + 0.5) * step
        # Densidad log-normal
        z = (math.log(S) - mu) / (s_log_std + 1e-10)
        density = math.exp(-0.5 * z * z) / (S * s_log_std * math.sqrt(2 * math.pi) + 1e-10) * step
        # PnL en este punto
        pnl = 0.0
        for leg in legs:
            K     = leg["strike"]
            prima = leg["prima"]
            qty   = leg.get("qty", 1)
            if leg["tipo"] == "CALL":
                intr = max(0.0, S - K)
            else:
                intr = max(0.0, K - S)
            pnl += (intr - prima) * qty if leg["side"] == "long" else (prima - intr) * qty
        total_prob += density
        if pnl > 0:
            profit_prob += density

    if total_prob <= 0:
        return None
    return round(profit_prob / total_prob * 100, 1)

def _get_best_option(opciones: list, tipo: str, strike_target: float, vto: str,
                     prefer: str = "mid", exclude_strikes: list = None) -> dict | None:
    """
    Busca la opción más cercana al strike_target con bid/ask/precio disponible.
    prefer: "mid" usa mid de bid/ask, "ultimo" usa último operado
    exclude_strikes: lista de strikes a excluir (para evitar spreads degenerados)
    """
    candidates = [
        r for r in opciones
        if r.get("tipo") == tipo
        and r.get("vencimiento") == vto
        and r.get("strike") is not None
        and (exclude_strikes is None or r.get("strike") not in exclude_strikes)
    ]
    if not candidates:
        return None

    candidates.sort(key=lambda r: abs((r.get("strike") or 0) - strike_target))

    for r in candidates[:5]:
        bid   = r.get("bid") or r.get("vi_bid")
        ask   = r.get("ask") or r.get("vi_offer")
        ult   = r.get("ultimo_precio") or r.get("veta_ultimo")
        teorico = r.get("precio_teorico")

        # Precio de la prima
        if bid and ask:
            prima = (bid + ask) / 2
        elif ult:
            prima = ult
        elif teorico:
            prima = teorico
        else:
            continue

        if prima <= 0:
            continue

        return {
            "symbol":  r.get("symbol"),
            "tipo":    tipo,
            "strike":  r.get("strike"),
            "vto":     vto,
            "prima":   round(prima, 4),
            "bid":     bid,
            "ask":     ask,
            "ultimo":  ult,
            "vi":      r.get("vol_implicita") or r.get("vi_ultimo"),
            "delta":   r.get("delta"),
            "dias_vto": r.get("dias_vto"),
        }
    return None

def _build_estrategias(opciones: list, subyacente: str, vto: str,
                        sesgo: str, budget: float | None) -> list:
    """
    Construye las estrategias disponibles para el subyacente/vencimiento/sesgo dados.
    sesgo: "very_bearish" | "bearish" | "neutral" | "bullish" | "very_bullish"
    Devuelve lista de estrategias ordenadas por score.
    """
    import math

    rows_suby = [
        r for r in opciones
        if (r.get("subyacente") or "").upper() == subyacente.upper()
        and r.get("vencimiento") == vto
    ]
    if not rows_suby:
        return []

    # Spot y parámetros base
    spot = next((r["precio_suby"] for r in rows_suby if r.get("precio_suby")), None)
    if not spot:
        return []

    dias = next((r["dias_vto"] for r in rows_suby if r.get("dias_vto")), None)
    T    = dias / 365.0 if dias else 30 / 365.0

    # IV ATM para chance estimate
    atm_rows = [r for r in rows_suby if r.get("moneyness") == "ATM" and r.get("vol_implicita")]
    sigma_atm = min(atm_rows, key=lambda r: abs((r.get("strike") or 0) - spot))["vol_implicita"] if atm_rows else 60.0

    # Rango de precios para payoff
    s_std = spot * (sigma_atm / 100) * math.sqrt(T)
    s_lo  = max(1, spot - 3 * s_std)
    s_hi  = spot + 3 * s_std
    s_range = [s_lo + (s_hi - s_lo) * i / 99 for i in range(100)]

    strategies = []

    # ── Helper para agregar estrategia ──
    def add_strategy(name: str, legs_def: list, categoria: str, sesgos_ok: list):
        if sesgo not in sesgos_ok:
            return

        # Resolver patas — excluir strikes ya usados para evitar spreads degenerados
        legs = []
        used_strikes_by_tipo = {}
        for ld in legs_def:
            excluir = used_strikes_by_tipo.get(ld["tipo"], [])
            opt = _get_best_option(rows_suby, ld["tipo"], ld["strike_target"], vto,
                                   exclude_strikes=excluir if excluir else None)
            if not opt:
                return
            # Registrar el strike usado para este tipo
            used_strikes_by_tipo.setdefault(ld["tipo"], []).append(opt["strike"])
            legs.append({
                "tipo":   ld["tipo"],
                "strike": opt["strike"],
                "side":   ld["side"],
                "prima":  opt["prima"],
                "qty":    ld.get("qty", 1),
                "symbol": opt["symbol"],
                "bid":    opt["bid"],
                "ask":    opt["ask"],
            })

        # Validar spreads: si dos patas tienen el mismo strike y tipo, es basura
        if len(legs) >= 2:
            strikes_por_tipo = {}
            for l in legs:
                key = (l["tipo"], l["strike"])
                if key in strikes_por_tipo:
                    return  # mismo strike para long y short → spread degenerado
                strikes_por_tipo[key] = True
            # También descartar si los strikes son iguales entre patas distintas
            strikes = [l["strike"] for l in legs]
            if len(set(strikes)) < len(strikes):
                return

        # Costo neto de la estrategia
        costo_neto = sum(
            l["prima"] * l["qty"] if l["side"] == "long" else -l["prima"] * l["qty"]
            for l in legs
        )

        # Descartar si el costo es exactamente 0 (spread degenerado)
        if len(legs) >= 2 and abs(costo_neto) < 0.01:
            return

        # Max profit / max risk / break-evens
        payoff = _payoff_array(legs, s_range)
        pnl_vals = [p["pnl"] for p in payoff]
        max_profit = max(pnl_vals)
        max_risk   = min(pnl_vals)  # es negativo

        lotes = int(budget) if budget and budget > 0 else 1

        # Break-evens (cruces por cero)
        be_list = []
        for i in range(len(payoff) - 1):
            p1, p2 = pnl_vals[i], pnl_vals[i+1]
            if p1 * p2 < 0:
                # Interpolación lineal
                s1, s2 = payoff[i]["s"], payoff[i+1]["s"]
                be = s1 + (s2 - s1) * (-p1) / (p2 - p1)
                be_list.append(round(be, 2))

        # Chance
        chance = _chance_estimate(legs, spot, sigma_atm, T)

        # Score: ponderación de retorno/riesgo y chance
        ret_risk = abs(max_profit / max_risk) if max_risk < 0 else 999
        score = (chance or 0) * 0.5 + min(ret_risk, 10) * 10

        patas_desc = []
        for l in legs:
            side_txt = "Compra" if l["side"] == "long" else "Venta"
            patas_desc.append(f"{side_txt} {l['tipo']} {subyacente} ${l['strike']:,.0f} @ ${l['prima']:,.2f}")

        strategies.append({
            "nombre":       name,
            "categoria":    categoria,
            "sesgo":        sesgo,
            "subyacente":   subyacente,
            "vencimiento":  vto,
            "dias_vto":     dias,
            "spot":         spot,
            "costo_neto":   round(costo_neto, 4),
            "costo_lote":   round(costo_neto * 100, 2),
            "lotes":        lotes,
            "total":        round(costo_neto * 100 * lotes, 2),
            "max_profit":   round(max_profit, 4) if max_profit < 1e8 else None,
            "max_risk":     round(max_risk,   4) if max_risk > -1e8  else None,
            "break_evens":  be_list,
            "chance":       chance,
            "score":        round(score, 2),
            "patas":        patas_desc,
            "legs":         legs,
            "payoff":       payoff,
        })

    # ── Strikes de referencia ──
    atm   = spot
    otm1c = spot * 1.05   # 5% OTM call
    otm2c = spot * 1.10   # 10% OTM call
    otm1p = spot * 0.95   # 5% OTM put
    otm2p = spot * 0.90   # 10% OTM put
    itm1c = spot * 0.95   # 5% ITM call
    itm1p = spot * 1.05   # 5% ITM put

    # ── Estrategias direccionales ──

    # Long Call
    # Long Call — compra call OTM
    add_strategy("Long Call", [
        {"tipo": "CALL", "strike_target": otm1c, "side": "long"}
    ], "direccional", ["bullish", "very_bullish"])

    # Long Call agresivo — más OTM, más apalancamiento
    add_strategy("Long Call Agresivo", [
        {"tipo": "CALL", "strike_target": otm2c, "side": "long"}
    ], "direccional", ["very_bullish"])

    # Long Put — compra put OTM
    add_strategy("Long Put", [
        {"tipo": "PUT", "strike_target": otm1p, "side": "long"}
    ], "direccional", ["bearish", "very_bearish"])

    # Long Put agresivo
    add_strategy("Long Put Agresivo", [
        {"tipo": "PUT", "strike_target": otm2p, "side": "long"}
    ], "direccional", ["very_bearish"])

    # Bull Call Spread — alcista limitado, menor costo que long call
    add_strategy("Bull Call Spread", [
        {"tipo": "CALL", "strike_target": atm,   "side": "long"},
        {"tipo": "CALL", "strike_target": otm1c, "side": "short"},
    ], "spread", ["bullish", "very_bullish"])

    # Bear Put Spread — bajista limitado
    add_strategy("Bear Put Spread", [
        {"tipo": "PUT", "strike_target": atm,   "side": "long"},
        {"tipo": "PUT", "strike_target": otm1p, "side": "short"},
    ], "spread", ["bearish", "very_bearish"])

    # Bear Call Spread — bajista/neutral: cobra crédito si el papel no sube
    add_strategy("Bear Call Spread", [
        {"tipo": "CALL", "strike_target": otm1c, "side": "short"},
        {"tipo": "CALL", "strike_target": otm2c, "side": "long"},
    ], "spread", ["bearish", "very_bearish", "neutral"])

    # Bull Put Spread — alcista/neutral: cobra crédito si el papel no baja
    add_strategy("Bull Put Spread", [
        {"tipo": "PUT", "strike_target": otm1p, "side": "short"},
        {"tipo": "PUT", "strike_target": otm2p, "side": "long"},
    ], "spread", ["bullish", "very_bullish", "neutral"])

    # Straddle — compra call+put ATM: necesita movimiento GRANDE (pierde si lateral)
    add_strategy("Straddle", [
        {"tipo": "CALL", "strike_target": atm, "side": "long"},
        {"tipo": "PUT",  "strike_target": atm, "side": "long"},
    ], "neutral_movimiento", ["neutral"])

    # Strangle — igual pero más barato, strikes más alejados del spot
    add_strategy("Strangle", [
        {"tipo": "CALL", "strike_target": otm1c, "side": "long"},
        {"tipo": "PUT",  "strike_target": otm1p, "side": "long"},
    ], "neutral_movimiento", ["neutral"])

    # Venta Call OTM — gana si el papel baja o se mantiene (mismo sesgo que Bear Call Spread)
    add_strategy("Venta Call OTM", [
        {"tipo": "CALL", "strike_target": otm1c, "side": "short"},
    ], "generacion_ingreso", ["very_bearish", "bearish", "neutral"])

    # Venta Put OTM — gana si el papel sube o se mantiene (mismo sesgo que Bull Put Spread)
    add_strategy("Venta Put OTM", [
        {"tipo": "PUT", "strike_target": otm1p, "side": "short"},
    ], "generacion_ingreso", ["neutral", "bullish", "very_bullish"])

    # Ordenar por score descendente, top 5
    strategies.sort(key=lambda x: -x["score"])
    return strategies[:4]


@app.get("/api/estrategias", dependencies=[Depends(require_auth)])
async def get_estrategias(
    subyacente: str = Query(...),
    vencimiento: str = Query(None),    # "2026-10-16" o "2026-12-18"; None = ambos
    sesgo: str = Query("neutral"),
    budget: float = Query(None),
):
    """
    Devuelve estrategias sugeridas para el subyacente/sesgo/budget dados.
    sesgo: very_bearish | bearish | neutral | bullish | very_bullish
    """
    opciones = state.get("opciones", [])
    if not opciones:
        return JSONResponse(status_code=503, content={"error": "Sin datos de opciones cargados"})

    SESGOS_VALIDOS = ["very_bearish", "bearish", "neutral", "bullish", "very_bullish"]
    if sesgo not in SESGOS_VALIDOS:
        sesgo = "neutral"

    VTOS = ["2026-10-16", "2026-12-18"]
    vtos_target = [vencimiento] if vencimiento and vencimiento in VTOS else VTOS

    result = {}
    for vto in vtos_target:
        strats = _build_estrategias(opciones, subyacente, vto, sesgo, budget)
        if strats:
            result[vto] = strats

    return {
        "fecha":       state["fecha"],
        "subyacente":  subyacente,
        "sesgo":       sesgo,
        "budget":      budget,
        "estrategias": result,
    }
