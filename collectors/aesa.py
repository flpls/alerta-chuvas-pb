"""
AESA-PB collector — scrapes the reservoir monitoring table.

Policy (05:00 BRT run):
  - Request the page once with a 20-second timeout.
  - Parse whatever is there. No retries, no fallback to cached data.
  - If the page fails or the table can't be parsed, return [] and let the
    pipeline produce a video without the açudes frame.

variacao_24h is computed by comparing today's percentual against the most
recent value stored in the DB for each reservoir.
"""

import logging
import unicodedata
from datetime import date

import requests
from bs4 import BeautifulSoup

from config import AESA_URL, AESA_TIMEOUT, DB_PATH, RESERVOIRS_PRIORITY
from collectors import db

log = logging.getLogger(__name__)


def collect(run_date: date | None = None) -> list[dict]:
    """
    Scrape AESA-PB and return reservoir records for the priority list.

    Returns a list of dicts ready for db.upsert_acudes(). Returns [] if the
    page is unavailable or the table cannot be parsed.
    """
    if run_date is None:
        run_date = date.today()

    try:
        html = _fetch()
    except Exception as exc:
        log.warning("AESA fetch failed: %s", exc)
        return []

    try:
        scraped = _parse(html)
    except Exception as exc:
        log.warning("AESA parse failed: %s", exc)
        return []

    if not scraped:
        log.warning("AESA: table parsed but no matching reservoirs found")
        return []

    records = _build_records(scraped, run_date)
    if records:
        db.upsert_acudes(DB_PATH, records)

    log.info("AESA: collected %d/%d priority reservoirs", len(records), len(RESERVOIRS_PRIORITY))
    return records


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _fetch() -> str:
    resp = requests.get(AESA_URL, timeout=AESA_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _parse(html: str) -> list[dict]:
    """
    Find the reservoir table and return a list of raw row dicts.
    We locate the table by searching for a header cell containing 'açude'
    (case-insensitive, accent-insensitive).
    """
    soup = BeautifulSoup(html, "lxml")

    target_table = None
    for table in soup.find_all("table"):
        headers = [_norm(th.get_text()) for th in table.find_all("th")]
        if any("acude" in h or "reservatorio" in h for h in headers):
            target_table = table
            break

    if target_table is None:
        # Fallback: try the first table that has enough columns
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) > 3:
                cols = rows[0].find_all(["th", "td"])
                if len(cols) >= 5:
                    target_table = table
                    log.debug("AESA: using heuristic table (no 'açude' header found)")
                    break

    if target_table is None:
        raise ValueError("No suitable table found in AESA page")

    # Map header positions
    header_row = target_table.find("tr")
    headers = [_norm(cell.get_text()) for cell in header_row.find_all(["th", "td"])]
    log.debug("AESA table headers: %s", headers)

    col = _map_columns(headers)

    rows = []
    for tr in target_table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if len(cells) <= max(col.values()):
            continue

        try:
            row = {
                "nome":            cells[col["nome"]].strip(),
                "capacidade_hm3":  _parse_float(cells[col["capacidade"]]),
                "volume_hm3":      _parse_float(cells[col["volume"]]),
                "percentual":      _parse_float(cells[col["percentual"]]),
            }
        except (IndexError, KeyError):
            continue

        if row["nome"] and row["capacidade_hm3"] > 0:
            rows.append(row)

    return rows


def _map_columns(headers: list[str]) -> dict:
    """
    Return a dict mapping semantic field name → column index.
    Tries several known variants of each header label.
    """
    _candidates = {
        "nome":       ("acude", "reservatorio", "nome", "barragem"),
        "capacidade": ("capacidade", "cap", "volume_total", "volume total"),
        "volume":     ("volume_atual", "volume atual", "volume_acumulado", "vol"),
        "percentual": ("percentual", "pct", "%", "perc", "percentagem"),
    }

    result = {}
    for field, variants in _candidates.items():
        for i, h in enumerate(headers):
            if any(v in h for v in variants):
                result[field] = i
                break

    missing = set(_candidates) - set(result)
    if missing:
        # Best-effort: assign by position if headers are completely unknown
        log.warning("AESA: could not identify columns %s; using positional fallback", missing)
        defaults = {"nome": 0, "capacidade": 2, "volume": 3, "percentual": 4}
        for f in missing:
            result[f] = defaults[f]

    return result


def _build_records(scraped: list[dict], run_date: date) -> list[dict]:
    """
    Match scraped rows against RESERVOIRS_PRIORITY (by normalised name),
    compute variacao_24h from DB, and return final record list.
    """
    scraped_index = {_norm(r["nome"]): r for r in scraped}
    records = []

    for meta in RESERVOIRS_PRIORITY:
        # Try exact normalised match first, then partial
        key = _norm(meta["nome"])
        row = scraped_index.get(key)
        if row is None:
            # Try partial: scraped name contains the canonical name
            for k, v in scraped_index.items():
                if key in k or k in key:
                    row = v
                    log.debug("AESA: matched '%s' → '%s' (partial)", meta["nome"], v["nome"])
                    break

        if row is None:
            log.warning("AESA: reservoir '%s' not found in scraped data", meta["nome"])
            continue

        # Prefer scraped percentual; fall back to computed
        pct = row["percentual"]
        if pct == 0.0 and row["capacidade_hm3"] > 0:
            pct = round(row["volume_hm3"] / row["capacidade_hm3"] * 100, 2)

        # Compute variation against last stored value
        prev_pct = db.get_previous_percentual(DB_PATH, meta["nome"], run_date.isoformat())
        variacao = round(pct - prev_pct, 2) if prev_pct is not None else None

        records.append({
            "data":            run_date.isoformat(),
            "nome":            meta["nome"],
            "ibge_code":       meta["ibge_code"],
            "capacidade_hm3":  meta["capacidade_hm3"],
            "volume_hm3":      round(row["volume_hm3"], 3),
            "percentual":      round(pct, 2),
            "variacao_24h":    variacao,
        })

    return records


def _parse_float(value: str) -> float:
    """Parse a Brazilian-format number string: '1.234,56' → 1234.56"""
    if not value:
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    # Strip any trailing non-numeric characters (e.g. ' hm³', '%')
    for i, ch in enumerate(cleaned):
        if not (ch.isdigit() or ch in ("-", ".")):
            cleaned = cleaned[:i]
            break
    try:
        return max(0.0, float(cleaned))
    except ValueError:
        log.debug("AESA: could not parse float from %r", value)
        return 0.0


def _norm(text: str) -> str:
    """Lowercase + strip accents for comparison."""
    nfkd = unicodedata.normalize("NFKD", text.strip().lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))
