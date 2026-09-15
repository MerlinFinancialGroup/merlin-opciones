# Merlin Opciones API

Backend FastAPI para datos de opciones del mercado argentino.

## Variables de entorno (Doppler)

| Variable | Descripción |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `VETA_COOKIE` | Cookie de sesión de Veta (BCCH) para order book |

## Endpoints

| Endpoint | Descripción |
|---|---|
| `GET /` | Estado del servicio |
| `GET /health` | Health check |
| `GET /api/opciones/cadena` | Cadena completa, filtrable por `subyacente`, `tipo`, `vencimiento` |
| `GET /api/opciones/subyacentes` | Lista de papeles con actividad |
| `GET /api/opciones/resumen` | Ranking volumen, OI y put/call ratio |
| `GET /api/opciones/symbol/{sym}` | Datos de una opción por symbol |
| `GET /api/opciones/orderbook/{sym}` | Bid/ask en tiempo real desde Veta |
| `GET /api/opciones/iv_surface/{suby}` | Superficie de IV por subyacente |
| `GET /admin/refresh` | Fuerza descarga del PDF IAMC |
| `POST /admin/upload-pdf` | Sube PDF manualmente (multipart) |
| `GET /admin/test-iamc-url` | Testea accesibilidad de URLs IAMC |

## Descarga automática

El scheduler intenta bajar el PDF de IAMC todos los días hábiles a las **18:30 ARG**.
Si falla, reintenta cada 15 minutos hasta las 20:00.

URL del PDF: `https://www.iamc.com.ar/Informe/InformeDiarioOpcionesDDMMYYYY`

## Vencimientos Argentina (2026)

- **Octubre**: 16/10/2026 (35 días desde 11-Sep)
- **Diciembre**: 18/12/2026 (98 días desde 11-Sep)
