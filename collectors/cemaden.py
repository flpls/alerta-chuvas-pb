"""
CEMADEN collector — downloads the daily pluviometric CSV and aggregates
rainfall by municipality for Paraíba.

The CSV is semicolon-separated, Latin-1 encoded, and published once per day.
If today's date doesn't appear in the file (not yet published at 05:00 BRT),
we use the most recent date available in the file instead.

Municipality names are normalised to lowercase + ASCII and resolved to IBGE
codes via config.CEMADEN_IBGE. Municipalities not in that dict are logged and
skipped rather than silently dropped off the map.
"""

import io
import logging
import unicodedata
from datetime import date

import pandas as pd
import requests

from config import (
    CEMADEN_CSV_URL,
    CEMADEN_IBGE,
    CEMADEN_TIMEOUT,
    DB_PATH,
)
from collectors import db

log = logging.getLogger(__name__)

# Expected column names in the CEMADEN CSV (may drift — validated at parse time)
_REQUIRED_COLS = {"uf", "municipio", "datahora", "valorMedida"}


def collect(run_date: date | None = None) -> list[dict]:
    """
    Download and parse the CEMADEN CSV, returning PB municipality totals.

    Returns a list of dicts ready for db.upsert_chuvas(). Returns [] on
    download failure so the pipeline can continue with INMET data alone.
    """
    if run_date is None:
        run_date = date.today()

    try:
        raw = _download()
    except Exception as exc:
        log.warning("CEMADEN download failed: %s", exc)
        return []

    try:
        df = _parse(raw)
    except Exception as exc:
        log.warning("CEMADEN parse failed: %s", exc)
        return []

    # Use today's data if available; otherwise fall back to the most recent date
    target_date = _resolve_date(df, run_date)
    df_day = df[df["_date"] == target_date]

    if target_date != run_date:
        log.warning(
            "CEMADEN: today (%s) not in CSV; using %s instead",
            run_date.isoformat(),
            target_date.isoformat(),
        )

    records = _aggregate(df_day, target_date)
    if records:
        db.upsert_chuvas(DB_PATH, records)

    log.info(
        "CEMADEN: %d municipalities from %s (date used: %s)",
        len(records),
        run_date.isoformat(),
        target_date.isoformat(),
    )
    return records


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _download() -> bytes:
    resp = requests.get(CEMADEN_CSV_URL, timeout=CEMADEN_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def _parse(raw: bytes) -> pd.DataFrame:
    df = pd.read_csv(
        io.BytesIO(raw),
        sep=";",
        encoding="latin-1",
        dtype=str,
        on_bad_lines="skip",
    )

    # Normalise column names: strip whitespace, lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    # CEMADEN sometimes ships camelCase columns — remap known variants
    _col_aliases = {
        "valormedio":   "valormedicao",
        "valormedida":  "valormedida",
        "valor_medida": "valormedida",
        "datahora":     "datahora",
        "data_hora":    "datahora",
    }
    df.rename(columns=_col_aliases, inplace=True)

    # Validate required columns (after normalisation)
    required = {"uf", "municipio", "datahora", "valormedida"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CEMADEN CSV missing columns: {missing}. Got: {list(df.columns)}")

    df = df[df["uf"].str.strip().str.upper() == "PB"].copy()

    # Parse date from datahora (format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS')
    df["_date"] = pd.to_datetime(df["datahora"], errors="coerce").dt.date
    df = df.dropna(subset=["_date"])

    # Parse rainfall value
    df["valormedida"] = (
        df["valormedida"]
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    df["_mm"] = pd.to_numeric(df["valormedida"], errors="coerce").fillna(0.0).clip(lower=0)

    return df


def _resolve_date(df: pd.DataFrame, desired: date) -> date:
    """Return `desired` if present in the data, else the most recent date."""
    available = df["_date"].dropna().unique()
    if len(available) == 0:
        return desired
    if desired in available:
        return desired
    return max(available)


def _aggregate(df: pd.DataFrame, target_date: date) -> list[dict]:
    """Sum rainfall per municipality and resolve IBGE codes."""
    totals = (
        df.groupby("municipio", as_index=False)["_mm"]
        .sum()
        .rename(columns={"_mm": "chuva_mm"})
    )

    records = []
    unknown = []

    for _, row in totals.iterrows():
        norm = _normalise(row["municipio"])
        ibge = CEMADEN_IBGE.get(norm)
        if ibge is None:
            unknown.append(row["municipio"])
            continue

        records.append({
            "data":      target_date.isoformat(),
            "ibge_code": ibge,
            "municipio": row["municipio"].strip().title(),
            "fonte":     "CEMADEN",
            "chuva_mm":  round(float(row["chuva_mm"]), 1),
            "lat":       None,
            "lon":       None,
        })

    if unknown:
        log.debug(
            "CEMADEN: %d municipalities with no IBGE mapping (skipped): %s",
            len(unknown),
            ", ".join(unknown[:10]) + ("..." if len(unknown) > 10 else ""),
        )

    return records


def _normalise(name: str) -> str:
    """Lowercase + strip accents for dict lookup."""
    nfkd = unicodedata.normalize("NFKD", name.strip().lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))
