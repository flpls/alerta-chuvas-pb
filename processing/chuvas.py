"""
Rainfall processing — turns raw collector records into rendering-ready metrics.

All inputs are lists of dicts with at minimum:
    {ibge_code, municipio, chuva_mm, fonte, data, lat, lon}

CEMADEN is the primary source when both cover the same municipality.
When they diverge, we keep the higher value as the conservative estimate
(better to flag a potential alert than to miss one).
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
    inmet: list[dict],
    cemaden: list[dict],
) -> list[dict]:
    """
    Combine INMET and CEMADEN records into one record per municipality.

    Strategy per municipality:
      - Only one source present → use it.
      - Both present → prefer CEMADEN, but keep the higher chuva_mm value.
        (CEMADEN has denser coverage; INMET is a single point per station.)
    """
    by_ibge: dict[int, dict] = {}

    # Load INMET first (lower priority)
    for r in inmet:
        by_ibge[r["ibge_code"]] = dict(r)

    # Overlay CEMADEN (higher priority)
    for r in cemaden:
        code = r["ibge_code"]
        if code in by_ibge:
            existing_mm = by_ibge[code]["chuva_mm"]
            # Keep whichever is higher — conservative approach for alerts
            if r["chuva_mm"] >= existing_mm:
                by_ibge[code] = dict(r)
            else:
                # CEMADEN wins on source attribution but INMET had more rain —
                # record both the higher value and the CEMADEN provenance
                merged = dict(r)
                merged["chuva_mm"] = existing_mm
                merged["fonte"] = "INMET+CEMADEN"
                by_ibge[code] = merged
        else:
            by_ibge[code] = dict(r)

    result = list(by_ibge.values())
    log.debug(
        "merge_sources: INMET=%d, CEMADEN=%d → merged=%d municipalities",
        len(inmet), len(cemaden), len(result),
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
