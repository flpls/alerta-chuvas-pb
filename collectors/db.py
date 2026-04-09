"""
SQLite persistence layer.

All records are keyed by IBGE 7-digit municipal code so that every module
speaks the same canonical identifier — no string matching at query time.
"""

import logging
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS chuvas (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    data        TEXT    NOT NULL,          -- YYYY-MM-DD
    ibge_code   INTEGER NOT NULL,          -- 7-digit IBGE municipal code
    municipio   TEXT    NOT NULL,          -- human-readable, for display only
    fonte       TEXT    NOT NULL,          -- 'INMET' or 'CEMADEN'
    chuva_mm    REAL    NOT NULL DEFAULT 0.0,
    lat         REAL,
    lon         REAL,
    UNIQUE (data, ibge_code, fonte)
);

CREATE TABLE IF NOT EXISTS acudes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    data            TEXT    NOT NULL,      -- YYYY-MM-DD
    nome            TEXT    NOT NULL,      -- reservoir name as scraped
    ibge_code       INTEGER NOT NULL,      -- municipality the dam is in
    capacidade_hm3  REAL    NOT NULL,
    volume_hm3      REAL    NOT NULL,
    percentual      REAL    NOT NULL,
    variacao_24h    REAL,                  -- NULL if no prior day available
    UNIQUE (data, nome)
);

CREATE INDEX IF NOT EXISTS idx_chuvas_data       ON chuvas (data);
CREATE INDEX IF NOT EXISTS idx_chuvas_ibge       ON chuvas (ibge_code);
CREATE INDEX IF NOT EXISTS idx_acudes_data       ON acudes (data);
CREATE INDEX IF NOT EXISTS idx_acudes_nome_data  ON acudes (nome, data);
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> None:
    """Create tables and indexes if they don't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        conn.executescript(_DDL)
    log.debug("DB initialised at %s", db_path)


def purge_old_records(db_path: Path, days: int = 365) -> None:
    """Delete records older than `days` days."""
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM chuvas WHERE data < date('now', ?)", (f"-{days} days",))
        conn.execute("DELETE FROM acudes WHERE data < date('now', ?)", (f"-{days} days",))
    log.debug("Purged records older than %d days", days)


def upsert_chuvas(db_path: Path, records: list[dict]) -> int:
    """
    Insert or replace rainfall records.
    Returns the number of rows affected.
    """
    if not records:
        return 0
    sql = """
        INSERT OR REPLACE INTO chuvas (data, ibge_code, municipio, fonte, chuva_mm, lat, lon)
        VALUES (:data, :ibge_code, :municipio, :fonte, :chuva_mm, :lat, :lon)
    """
    with _connect(db_path) as conn:
        conn.executemany(sql, records)
        count = conn.execute("SELECT changes()").fetchone()[0]
    log.debug("upsert_chuvas: %d rows", count)
    return count


def upsert_acudes(db_path: Path, records: list[dict]) -> int:
    """
    Insert or replace reservoir records.
    Returns the number of rows affected.
    """
    if not records:
        return 0
    sql = """
        INSERT OR REPLACE INTO acudes
            (data, nome, ibge_code, capacidade_hm3, volume_hm3, percentual, variacao_24h)
        VALUES
            (:data, :nome, :ibge_code, :capacidade_hm3, :volume_hm3, :percentual, :variacao_24h)
    """
    with _connect(db_path) as conn:
        conn.executemany(sql, records)
        count = conn.execute("SELECT changes()").fetchone()[0]
    log.debug("upsert_acudes: %d rows", count)
    return count


def get_previous_percentual(db_path: Path, nome: str, before_date: str) -> float | None:
    """
    Return the most recent percentual for a reservoir strictly before `before_date`.
    Used to compute variacao_24h when the source doesn't provide it.
    """
    sql = """
        SELECT percentual FROM acudes
        WHERE nome = ? AND data < ?
        ORDER BY data DESC
        LIMIT 1
    """
    with _connect(db_path) as conn:
        row = conn.execute(sql, (nome, before_date)).fetchone()
    return row[0] if row else None


def get_chuvas_5yr_avg(db_path: Path, ibge_code: int, month_day: str) -> float | None:
    """
    Return the 5-year average rainfall for a municipality on a given MM-DD.
    `month_day` must be in 'MM-DD' format.
    Returns None if fewer than 2 years of data exist (not enough for a meaningful average).
    """
    sql = """
        SELECT AVG(chuva_mm), COUNT(DISTINCT substr(data, 1, 4))
        FROM chuvas
        WHERE ibge_code = ?
          AND strftime('%m-%d', data) = ?
          AND data >= date('now', '-5 years')
    """
    with _connect(db_path) as conn:
        row = conn.execute(sql, (ibge_code, month_day)).fetchone()
    if row and row[1] >= 2:
        return row[0]
    return None


def get_today_chuvas(db_path: Path, data: str) -> list[dict]:
    """Return all rainfall records for a given date."""
    sql = "SELECT * FROM chuvas WHERE data = ?"
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (data,)).fetchall()
    return [dict(r) for r in rows]


def get_today_acudes(db_path: Path, data: str) -> list[dict]:
    """Return all reservoir records for a given date."""
    sql = "SELECT * FROM acudes WHERE data = ?"
    with _connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, (data,)).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
