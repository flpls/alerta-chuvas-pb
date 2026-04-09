"""
Reservoir processing — classifies açude records and computes aggregate metrics.

Inputs are lists of dicts as returned by collectors/aesa.py and db.get_today_acudes():
    {nome, ibge_code, capacidade_hm3, volume_hm3, percentual, variacao_24h, ...}
"""

import logging

from config import (
    RESERVOIRS_PRIORITY,
    RESERV_ALERTA_PCT,
    RESERV_COLORS,
    RESERV_CRITICO_PCT,
    RESERV_NORMAL_PCT,
)

log = logging.getLogger(__name__)

# Map canonical reservoir name → display apelido from config
_APELIDO = {r["nome"]: r["apelido"] for r in RESERVOIRS_PRIORITY}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_reservoir(percentual: float) -> str:
    """Return the severity bucket for a given fill percentage."""
    if percentual < RESERV_CRITICO_PCT:
        return "crítico"
    if percentual < RESERV_ALERTA_PCT:
        return "alerta"
    if percentual < RESERV_NORMAL_PCT:
        return "normal"
    return "cheio"


def enrich_reservoirs(records: list[dict]) -> list[dict]:
    """
    Add `classificacao`, `cor`, and `apelido` fields to each reservoir record.
    Non-mutating — returns new dicts.
    Also recomputes `percentual` from volume/capacidade as a sanity check;
    if the scraped value is implausible (> 105%), the computed value wins.
    """
    enriched = []
    for r in records:
        pct = r["percentual"]

        # Sanity check: recompute from volumes if scraped value looks wrong
        if r["capacidade_hm3"] > 0:
            computed_pct = round(r["volume_hm3"] / r["capacidade_hm3"] * 100, 2)
            if pct > 105.0:
                log.warning(
                    "Reservoir '%s': scraped percentual %.1f%% > 105%%, using computed %.1f%%",
                    r["nome"], pct, computed_pct,
                )
                pct = computed_pct

        classificacao = classify_reservoir(pct)

        enriched.append({
            **r,
            "percentual":     round(pct, 2),
            "classificacao":  classificacao,
            "cor":            RESERV_COLORS[classificacao],
            "apelido":        _APELIDO.get(r["nome"], r["nome"]),
        })

    # Preserve priority order from config
    priority_order = {r["nome"]: i for i, r in enumerate(RESERVOIRS_PRIORITY)}
    enriched.sort(key=lambda r: priority_order.get(r["nome"], 999))

    return enriched


def weighted_semiarido_average(records: list[dict]) -> float:
    """
    Capacity-weighted fill percentage across all provided reservoirs.

    weight_i = capacidade_hm3_i / sum(capacidade_hm3)
    result   = sum(percentual_i * weight_i)

    Returns 0.0 if no records or total capacity is zero.
    """
    total_cap = sum(r["capacidade_hm3"] for r in records)
    if total_cap == 0:
        return 0.0

    weighted = sum(r["percentual"] * r["capacidade_hm3"] for r in records)
    return round(weighted / total_cap, 2)


def semiarido_classification(records: list[dict]) -> str:
    """
    Overall classification for the semiárido based on the weighted average.
    Uses the same thresholds as individual reservoirs.
    """
    avg = weighted_semiarido_average(records)
    return classify_reservoir(avg)


def critical_reservoirs(records: list[dict]) -> list[dict]:
    """Return only records classified as 'crítico' or 'alerta', sorted by percentual asc."""
    at_risk = [r for r in records if r.get("classificacao") in ("crítico", "alerta")]
    return sorted(at_risk, key=lambda r: r["percentual"])
