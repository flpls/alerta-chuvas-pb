"""
Rainfall processing — turns raw collector records into rendering-ready metrics.

Primary source: CEMADEN (dense municipal network across PB).
The merge_sources function accepts an optional list of supplementary records
(e.g. from a future ANA/HidroWeb integration) and overlays them on top of
CEMADEN, keeping the higher value per municipality as a conservative estimate.
"""

import logging
from datetime import date

from config import (
    ALERT_ATENCAO_MM,
    ALERT_CRITICO_MM,
    DB_PATH,
)
from collectors import db

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def merge_sources(
    cemaden: list[dict],
    supplementary: list[dict] | None = None,
) -> list[dict]:
    """
    Build a single record per municipality from CEMADEN plus any supplementary
    source (e.g. ANA/HidroWeb in a future phase).

    Strategy when a municipality appears in both:
      - Keep the higher chuva_mm value (conservative for alerts).
      - Mark fonte as 'CEMADEN+<other>' for transparency.

    With no supplementary source this is a simple deduplication pass that
    also resolves any duplicate IBGE codes within the CEMADEN data itself
    (rare, but possible if two station names map to the same code).
    """
    by_ibge: dict[int, dict] = {}

    for r in cemaden:
        code = r["ibge_code"]
        if code not in by_ibge or r["chuva_mm"] > by_ibge[code]["chuva_mm"]:
            by_ibge[code] = dict(r)

    for r in (supplementary or []):
        code = r["ibge_code"]
        if code in by_ibge:
            if r["chuva_mm"] > by_ibge[code]["chuva_mm"]:
                merged = dict(r)
                merged["fonte"] = f"CEMADEN+{r['fonte']}"
                by_ibge[code] = merged
        else:
            by_ibge[code] = dict(r)

    result = list(by_ibge.values())
    log.debug(
        "merge_sources: CEMADEN=%d, supplementary=%d → %d municipalities",
        len(cemaden), len(supplementary or []), len(result),
    )
    return result


def top5_ranking(records: list[dict]) -> list[dict]:
    """
    Return the top-5 municipalities by 24h rainfall, descending.
    Each entry retains all original fields plus a `rank` field (1–5).
    """
    ranked = sorted(records, key=lambda r: r["chuva_mm"], reverse=True)
    return [dict(r, rank=i + 1) for i, r in enumerate(ranked[:5])]


def state_average(records: list[dict]) -> float:
    """Simple mean of all municipal totals. Returns 0.0 if no records."""
    if not records:
        return 0.0
    return round(sum(r["chuva_mm"] for r in records) / len(records), 1)


def classify_alerts(records: list[dict]) -> list[dict]:
    """
    Return records where chuva_mm >= ALERT_ATENCAO_MM, each with a
    `nivel` field: 'atenção' or 'crítico'.
    Sorted descending by chuva_mm.
    """
    alerts = []
    for r in records:
        mm = r["chuva_mm"]
        if mm >= ALERT_CRITICO_MM:
            nivel = "crítico"
        elif mm >= ALERT_ATENCAO_MM:
            nivel = "atenção"
        else:
            continue
        alerts.append(dict(r, nivel=nivel))

    alerts.sort(key=lambda r: r["chuva_mm"], reverse=True)
    log.info(
        "classify_alerts: %d atenção/crítico municipalities",
        len(alerts),
    )
    return alerts


def compute_anomaly(ibge_code: int, chuva_mm: float, run_date: date | None = None) -> float | None:
    """
    Return the percentage anomaly vs. the 5-year average for the same calendar day.

    Positive = above average, negative = below average.
    Returns None if there is insufficient historical data (< 2 years).
    """
    if run_date is None:
        run_date = date.today()

    avg = db.get_chuvas_5yr_avg(DB_PATH, ibge_code, run_date.strftime("%m-%d"))
    if avg is None or avg == 0.0:
        return None

    return round((chuva_mm - avg) / avg * 100, 1)


def enrich_with_anomaly(records: list[dict], run_date: date | None = None) -> list[dict]:
    """
    Add an `anomalia_pct` field to each record. None means no history available.
    Non-mutating — returns new dicts.
    """
    enriched = []
    for r in records:
        anomalia = compute_anomaly(r["ibge_code"], r["chuva_mm"], run_date)
        enriched.append(dict(r, anomalia_pct=anomalia))
    return enriched
