"""
Unit tests for data collectors.

Network calls are always mocked — these tests must run offline.
"""

import io
import sqlite3
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# CEMADEN
# ---------------------------------------------------------------------------

CEMADEN_CSV_FIXTURE = (
    "codEstacao;uf;municipio;nomeEstacao;latitude;longitude;datahora;valorMedida\n"
    "31001;PB;Campina Grande;Est CG 01;-7.22;-35.90;2026-04-23 06:00:00;12.4\n"
    "31002;PB;Campina Grande;Est CG 02;-7.23;-35.91;2026-04-23 07:00:00;8.0\n"
    "31003;PB;Joao Pessoa;Est JP 01;-7.14;-34.85;2026-04-23 06:00:00;0.0\n"
    "31004;PB;Patos;Est PA 01;-7.07;-37.27;2026-04-23 06:00:00;55.2\n"
    "31005;PE;Recife;Est RE 01;-8.05;-34.87;2026-04-23 06:00:00;3.0\n"   # different UF
    "31006;PB;Municipio Novo;Est MN 01;-6.0;-36.0;2026-04-23 06:00:00;10.0\n"  # not in IBGE map
)

CEMADEN_CSV_YESTERDAY = (
    "codEstacao;uf;municipio;nomeEstacao;latitude;longitude;datahora;valorMedida\n"
    "31001;PB;Campina Grande;Est CG 01;-7.22;-35.90;2026-04-22 06:00:00;5.0\n"
)

CEMADEN_CSV_ALT_COLS = (
    # CEMADEN sometimes ships camelCase column names
    "codEstacao;uf;municipio;nomeEstacao;latitude;longitude;datahora;valorMedida\n"
    "31001;PB;Campina Grande;Est CG 01;-7.22;-35.90;2026-04-23 06:00:00;20,5\n"  # comma decimal
)


class FakeResponse:
    def __init__(self, content: str, status_code: int = 200):
        self.content = content.encode("latin-1")
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class TestCemadenParse:
    def test_filters_to_pb_only(self):
        from collectors.cemaden import _parse
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        assert set(df["uf"].str.strip().str.upper().unique()) == {"PB"}

    def test_aggregates_multiple_stations_same_municipality(self):
        from collectors.cemaden import _parse, _aggregate
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        target = date(2026, 4, 23)
        df_day = df[df["_date"] == target]
        records = _aggregate(df_day, target)
        cg = next(r for r in records if r["ibge_code"] == 2504009)
        assert cg["chuva_mm"] == pytest.approx(20.4, abs=0.1)

    def test_resolves_ibge_codes(self):
        from collectors.cemaden import _parse, _aggregate
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        target = date(2026, 4, 23)
        records = _aggregate(df[df["_date"] == target], target)
        codes = {r["ibge_code"] for r in records}
        assert 2504009 in codes  # Campina Grande
        assert 2507507 in codes  # João Pessoa
        assert 2510808 in codes  # Patos

    def test_skips_unknown_municipalities(self):
        from collectors.cemaden import _parse, _aggregate
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        target = date(2026, 4, 23)
        records = _aggregate(df[df["_date"] == target], target)
        names = [r["municipio"].lower() for r in records]
        assert not any("municipio novo" in n for n in names)

    def test_parses_comma_decimal(self):
        from collectors.cemaden import _parse, _aggregate
        df = _parse(CEMADEN_CSV_ALT_COLS.encode("latin-1"))
        target = date(2026, 4, 23)
        records = _aggregate(df[df["_date"] == target], target)
        cg = next(r for r in records if r["ibge_code"] == 2504009)
        assert cg["chuva_mm"] == pytest.approx(20.5, abs=0.1)

    def test_fonte_is_cemaden(self):
        from collectors.cemaden import _parse, _aggregate
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        target = date(2026, 4, 23)
        records = _aggregate(df[df["_date"] == target], target)
        assert all(r["fonte"] == "CEMADEN" for r in records)

    def test_date_fallback_to_most_recent(self):
        from collectors.cemaden import _parse, _resolve_date
        df = _parse(CEMADEN_CSV_YESTERDAY.encode("latin-1"))
        # Ask for today (2026-04-23) but CSV only has yesterday
        resolved = _resolve_date(df, date(2026, 4, 23))
        assert resolved == date(2026, 4, 22)

    def test_uses_today_when_available(self):
        from collectors.cemaden import _parse, _resolve_date
        df = _parse(CEMADEN_CSV_FIXTURE.encode("latin-1"))
        resolved = _resolve_date(df, date(2026, 4, 23))
        assert resolved == date(2026, 4, 23)

    def test_download_failure_returns_empty(self):
        with patch("collectors.cemaden.requests.get") as mock_get:
            mock_get.side_effect = ConnectionError("timeout")
            from collectors.cemaden import collect
            result = collect(date(2026, 4, 23))
        assert result == []

    def test_collect_persists_to_db(self, tmp_path):
        db_path = tmp_path / "test.db"
        with patch("collectors.cemaden.requests.get") as mock_get, \
             patch("collectors.cemaden.DB_PATH", db_path):
            mock_get.return_value = FakeResponse(CEMADEN_CSV_FIXTURE)
            from collectors import db
            db.init_db(db_path)
            from collectors.cemaden import collect
            records = collect(date(2026, 4, 23))
        assert len(records) > 0
        stored = db.get_today_chuvas(db_path, "2026-04-23")
        assert len(stored) == len(records)


# ---------------------------------------------------------------------------
# AESA
# ---------------------------------------------------------------------------

AESA_HTML_FIXTURE = """
<html><body>
<table>
  <tr>
    <th>Açude</th><th>Município</th><th>Capacidade (hm³)</th>
    <th>Volume Atual (hm³)</th><th>Percentual (%)</th>
  </tr>
  <tr>
    <td>Epitácio Pessoa</td><td>Boqueirão</td>
    <td>411,00</td><td>82,20</td><td>20,00</td>
  </tr>
  <tr>
    <td>Coremas-Mãe D'Água</td><td>Coremas</td>
    <td>1.358,00</td><td>679,00</td><td>50,00</td>
  </tr>
  <tr>
    <td>São Gonçalo</td><td>São Gonçalo</td>
    <td>44,00</td><td>4,40</td><td>10,00</td>
  </tr>
  <tr>
    <td>Engenheiro Ávidos</td><td>Cajazeiras</td>
    <td>255,00</td><td>25,50</td><td>10,00</td>
  </tr>
  <tr>
    <td>Acauã</td><td>Itatuba</td>
    <td>253,00</td><td>126,50</td><td>50,00</td>
  </tr>
</table>
</body></html>
"""

AESA_HTML_BAD = "<html><body><p>Serviço indisponível</p></body></html>"


class TestAesaParse:
    def test_finds_all_priority_reservoirs(self):
        from collectors.aesa import _parse
        rows = _parse(AESA_HTML_FIXTURE)
        names = [r["nome"] for r in rows]
        assert "Epitácio Pessoa" in names
        assert "Coremas-Mãe D'Água" in names
        assert "Acauã" in names

    def test_parses_brazilian_float_thousands_separator(self):
        from collectors.aesa import _parse
        rows = _parse(AESA_HTML_FIXTURE)
        coremas = next(r for r in rows if "Coremas" in r["nome"])
        assert coremas["capacidade_hm3"] == pytest.approx(1358.0, abs=0.1)

    def test_parses_brazilian_float_comma_decimal(self):
        from collectors.aesa import _parse
        rows = _parse(AESA_HTML_FIXTURE)
        ep = next(r for r in rows if "Epitácio" in r["nome"])
        assert ep["volume_hm3"] == pytest.approx(82.2, abs=0.1)
        assert ep["percentual"] == pytest.approx(20.0, abs=0.1)

    def test_returns_empty_on_fetch_failure(self):
        with patch("collectors.aesa.requests.get") as mock_get:
            mock_get.side_effect = ConnectionError("timeout")
            from collectors.aesa import collect
            result = collect(date(2026, 4, 23))
        assert result == []

    def test_returns_empty_when_no_table_found(self):
        with patch("collectors.aesa.requests.get") as mock_get:
            mock_get.return_value = FakeResponse(AESA_HTML_BAD)
            from collectors.aesa import collect
            result = collect(date(2026, 4, 23))
        assert result == []

    def test_build_records_assigns_ibge_codes(self):
        from collectors.aesa import _parse, _build_records
        with patch("collectors.aesa.db.get_previous_percentual", return_value=None):
            rows = _parse(AESA_HTML_FIXTURE)
            records = _build_records(rows, date(2026, 4, 23))
        assert all("ibge_code" in r for r in records)
        assert all(r["ibge_code"] > 0 for r in records)

    def test_variacao_is_none_with_no_prior_data(self, tmp_path):
        from collectors.aesa import _parse, _build_records
        with patch("collectors.aesa.DB_PATH", tmp_path / "test.db"), \
             patch("collectors.aesa.db.get_previous_percentual", return_value=None):
            rows = _parse(AESA_HTML_FIXTURE)
            records = _build_records(rows, date(2026, 4, 23))
        assert all(r["variacao_24h"] is None for r in records)

    def test_variacao_computed_from_prior_data(self):
        from collectors.aesa import _parse, _build_records
        with patch("collectors.aesa.db.get_previous_percentual", return_value=15.0):
            rows = _parse(AESA_HTML_FIXTURE)
            records = _build_records(rows, date(2026, 4, 23))
        ep = next(r for r in records if "Epitácio" in r["nome"])
        assert ep["variacao_24h"] == pytest.approx(20.0 - 15.0, abs=0.01)


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

class TestDb:
    def test_init_creates_tables(self, tmp_path):
        from collectors import db
        db_path = tmp_path / "test.db"
        db.init_db(db_path)
        conn = sqlite3.connect(db_path)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "chuvas" in tables
        assert "acudes" in tables

    def test_upsert_chuvas_insert_and_replace(self, tmp_path):
        from collectors import db
        db_path = tmp_path / "test.db"
        db.init_db(db_path)
        record = {"data": "2026-04-23", "ibge_code": 2504009, "municipio": "Campina Grande",
                  "fonte": "CEMADEN", "chuva_mm": 10.0, "lat": None, "lon": None}
        db.upsert_chuvas(db_path, [record])
        db.upsert_chuvas(db_path, [{**record, "chuva_mm": 20.0}])
        rows = db.get_today_chuvas(db_path, "2026-04-23")
        assert len(rows) == 1
        assert rows[0]["chuva_mm"] == 20.0

    def test_purge_removes_old_records(self, tmp_path):
        from collectors import db
        db_path = tmp_path / "test.db"
        db.init_db(db_path)
        old = {"data": "2020-01-01", "ibge_code": 2504009, "municipio": "CG",
               "fonte": "CEMADEN", "chuva_mm": 5.0, "lat": None, "lon": None}
        db.upsert_chuvas(db_path, [old])
        db.purge_old_records(db_path, days=365)
        rows = db.get_today_chuvas(db_path, "2020-01-01")
        assert rows == []
