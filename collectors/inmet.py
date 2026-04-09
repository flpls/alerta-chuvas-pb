"""
INMET collector — fetches last-24h rainfall for each configured PB station.

Endpoint: GET /estacao/dados/{codEstacao}
Returns a list of hourly observations; we sum the CHUVA field to get the
24h accumulation. Station coordinates and IBGE codes come from config, not
the API response (INMET's lat/lon can be unreliable for some stations).
"""

import logging
from datetime import date, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import DB_PATH, INMET_BASE_URL, INMET_STATIONS, INMET_TIMEOUT, INMET_RETRIES
from collectors import db

log = logging.getLogger(__name__)

# Brasília = UTC-3
BRT = timezone(timedelta(hours=-3))


def collect(run_date: date | None = None) -> list[dict]:
    """
    Fetch 24h rainfall for all configured INMET stations.

    Returns a list of dicts ready for db.upsert_chuvas(). Stations that are
    offline or return no data are skipped with a warning — the pipeline
    continues regardless.
    """
    if run_date is None:
        run_date = date.today()

    session = _make_session()
    records = []

    for code, meta in INMET_STATIONS.items():
        try:
            mm = _fetch_station(session, code)
        except Exception as exc:
            log.warning("INMET station %s (%s) failed: %s", code, meta["municipio"], exc)
            continue

        records.append({
            "data":       run_date.isoformat(),
            "ibge_code":  meta["ibge_code"],
            "municipio":  meta["municipio"],
            "fonte":      "INMET",
            "chuva_mm":   mm,
            "lat":        meta["lat"],
            "lon":        meta["lon"],
        })
        log.info("INMET %s (%s): %.1f mm", code, meta["municipio"], mm)

    if records:
        db.upsert_chuvas(DB_PATH, records)

    log.info("INMET: collected %d/%d stations", len(records), len(INMET_STATIONS))
    return records


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _fetch_station(session: requests.Session, code: str) -> float:
    """Return the 24h accumulated rainfall (mm) for a single station code."""
    url = f"{INMET_BASE_URL}/estacao/dados/{code}"
    resp = session.get(url, timeout=INMET_TIMEOUT)
    resp.raise_for_status()

    data = resp.json()
    if not data:
        log.debug("INMET %s: empty response", code)
        return 0.0

    total = 0.0
    for obs in data:
        total += _parse_chuva(obs.get("CHUVA"))

    return round(total, 1)


def _parse_chuva(value) -> float:
    """Safely parse the CHUVA field: handles None, 'null', '', and numeric strings."""
    if value is None:
        return 0.0
    s = str(value).strip().lower()
    if s in ("", "null", "none", "-"):
        return 0.0
    try:
        return max(0.0, float(s))
    except ValueError:
        log.debug("INMET: unrecognised CHUVA value %r", value)
        return 0.0


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=INMET_RETRIES,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
