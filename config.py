import json
import logging
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"
ASSETS_DIR = ROOT_DIR / "assets"
FONTS_DIR = ASSETS_DIR / "fonts"
SPONSORS_DIR = ASSETS_DIR / "sponsors"
DB_PATH = DATA_DIR / "historico.db"
GEOJSON_PATH = ASSETS_DIR / "pb_municipios.geojson"
SPONSOR_AGENDA_PATH = SPONSORS_DIR / "agenda.json"

# ---------------------------------------------------------------------------
# Video
# ---------------------------------------------------------------------------
VIDEO_SIZE = (1080, 1920)
FPS = 30
DUR_ABERTURA = 5
DUR_MAPA = 10
DUR_ACUDES = 10
DUR_ALERTAS = 7
DUR_ENCERRAMENTO = 5

# ---------------------------------------------------------------------------
# Brand
# ---------------------------------------------------------------------------
BRAND_NAVY = "#1B2A6B"
BRAND_WHITE = "#FFFFFF"
FONT_REGULAR = "Lato-Regular.ttf"
FONT_BOLD = "Lato-Bold.ttf"

# ---------------------------------------------------------------------------
# Alert thresholds (mm / 24h)
# ---------------------------------------------------------------------------
ALERT_ATENCAO_MM = 50.0
ALERT_CRITICO_MM = 80.0

# ---------------------------------------------------------------------------
# Reservoir thresholds (% capacity)
# ---------------------------------------------------------------------------
RESERV_CRITICO_PCT = 20.0
RESERV_ALERTA_PCT = 40.0
RESERV_NORMAL_PCT = 70.0

RESERV_COLORS = {
    "crítico": "#C0392B",
    "alerta":  "#E67E22",
    "normal":  "#27AE60",
    "cheio":   "#2980B9",
}

# ---------------------------------------------------------------------------
# INMET
# ---------------------------------------------------------------------------
INMET_BASE_URL = "https://apitempo.inmet.gov.br"
INMET_TIMEOUT = 10
INMET_RETRIES = 3

# Keyed by station code → {municipio, ibge_code, lat, lon}
INMET_STATIONS = {
    "A322": {"municipio": "Campina Grande",  "ibge_code": 2504009, "lat": -7.2306,  "lon": -35.8811},
    "A323": {"municipio": "João Pessoa",     "ibge_code": 2507507, "lat": -7.1153,  "lon": -34.8641},
    "A349": {"municipio": "Patos",           "ibge_code": 2510808, "lat": -7.0194,  "lon": -37.2806},
    "A350": {"municipio": "Sousa",           "ibge_code": 2516201, "lat": -6.7597,  "lon": -38.2283},
    "A348": {"municipio": "Cajazeiras",      "ibge_code": 2503209, "lat": -6.8919,  "lon": -38.5597},
    "A339": {"municipio": "Monteiro",        "ibge_code": 2509701, "lat": -7.8886,  "lon": -37.1200},
}

# ---------------------------------------------------------------------------
# CEMADEN
# ---------------------------------------------------------------------------
CEMADEN_CSV_URL = "https://www.cemaden.gov.br/dados-pluviometricos-em-formato-csv/"
CEMADEN_TIMEOUT = 30

# IBGE codes for PB municipalities that appear in CEMADEN data.
# Key: lowercase, accent-stripped name as it appears in the CSV.
# Value: IBGE 7-digit code.
# Extend this table as new municipalities appear in the data.
CEMADEN_IBGE = {
    "campina grande":    2504009,
    "joao pessoa":       2507507,
    "patos":             2510808,
    "sousa":             2516201,
    "cajazeiras":        2503209,
    "monteiro":          2509701,
    "guarabira":         2506301,
    "bayeux":            2502003,
    "santa rita":        2513703,
    "caruaru":           2604106,  # PE — filtered out by UF check, but listed for safety
    "pombal":            2512101,
    "pianco":            2511301,
    "itabaiana":         2506806,
    "esperanca":         2505200,
    "queimadas":         2512507,
    "araruna":           2501104,
    "bananeiras":        2501534,
    "catole do rocha":   2504306,
    "sao bento":         2513802,
    "mamanguape":        2508703,
    "cabedelo":          2503209,  # will be overridden by ibge lookup if wrong
    "itapororoca":       2507002,
    "alagoa grande":     2500304,
    "cuite":             2505238,
    "picui":             2511202,
    "soledad":           2515930,
    "conceicao":         2504801,
    "bonito de santa fe": 2502508,
    "sao jose de piranhas": 2514503,
    "triunfo":           2516805,
    "paulista":          2511004,
    "malta":             2509107,
    "condado":           2504603,
}

# ---------------------------------------------------------------------------
# AESA
# ---------------------------------------------------------------------------
AESA_URL = "http://www.aesa.pb.gov.br/aesa-website/monitoramento/chuvas/"
AESA_TIMEOUT = 20

# Priority reservoirs: display name → ibge_code of the municipality
RESERVOIRS_PRIORITY = [
    {"nome": "Epitácio Pessoa",    "apelido": "Boqueirão",        "ibge_code": 2503704, "capacidade_hm3": 411.0},
    {"nome": "Coremas-Mãe D'Água", "apelido": "Coremas",          "ibge_code": 2504900, "capacidade_hm3": 1358.0},
    {"nome": "São Gonçalo",        "apelido": "São Gonçalo",       "ibge_code": 2516201, "capacidade_hm3": 44.0},
    {"nome": "Engenheiro Ávidos",  "apelido": "Eng. Ávidos",       "ibge_code": 2503704, "capacidade_hm3": 255.0},
    {"nome": "Acauã",              "apelido": "Acauã",             "ibge_code": 2511301, "capacidade_hm3": 253.0},
]

# ---------------------------------------------------------------------------
# Sponsor
# ---------------------------------------------------------------------------

def get_sponsor_for_date(ref_date: date | None = None) -> dict:
    """Return the sponsor dict for the given date, falling back to 'default'."""
    if ref_date is None:
        ref_date = date.today()
    key = ref_date.isoformat()

    if not SPONSOR_AGENDA_PATH.exists():
        return _default_sponsor()

    try:
        agenda = json.loads(SPONSOR_AGENDA_PATH.read_text(encoding="utf-8"))
    except Exception:
        logging.warning("Could not read sponsor agenda; using default.")
        return _default_sponsor()

    return agenda.get(key) or agenda.get("default") or _default_sponsor()


def _default_sponsor() -> dict:
    return {"nome": "VAMO Consultoria", "slug": "vamo", "cta": "vamoconsultoria.com.br"}
