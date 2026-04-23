"""
Unit tests for processing modules.

No network calls, no DB I/O — pure logic tests.
"""

import pytest


def _chuva(ibge_code, municipio, chuva_mm, fonte="CEMADEN"):
    return {
        "data": "2026-04-23",
        "ibge_code": ibge_code,
        "municipio": municipio,
        "fonte": fonte,
        "chuva_mm": chuva_mm,
        "lat": None,
        "lon": None,
    }


def _acude(nome, capacidade_hm3, volume_hm3, percentual, variacao_24h=None):
    return {
        "data": "2026-04-23",
        "nome": nome,
        "ibge_code": 2504009,
        "capacidade_hm3": capacidade_hm3,
        "volume_hm3": volume_hm3,
        "percentual": percentual,
        "variacao_24h": variacao_24h,
    }


# ---------------------------------------------------------------------------
# processing.chuvas
# ---------------------------------------------------------------------------

class TestMergeSources:
    def test_cemaden_only_passthrough(self):
        from processing.chuvas import merge_sources
        records = [_chuva(2504009, "Campina Grande", 10.0)]
        result = merge_sources(records)
        assert len(result) == 1
        assert result[0]["chuva_mm"] == 10.0

    def test_deduplicates_same_ibge_code_keeps_higher(self):
        from processing.chuvas import merge_sources
        # Two CEMADEN stations mapping to the same municipality
        records = [
            _chuva(2504009, "Campina Grande", 10.0),
            _chuva(2504009, "Campina Grande", 15.0),
        ]
        result = merge_sources(records)
        assert len(result) == 1
        assert result[0]["chuva_mm"] == 15.0

    def test_supplementary_adds_new_municipality(self):
        from processing.chuvas import merge_sources
        cemaden = [_chuva(2504009, "Campina Grande", 10.0)]
        supplementary = [_chuva(2507507, "João Pessoa", 5.0, fonte="ANA")]
        result = merge_sources(cemaden, supplementary)
        assert len(result) == 2

    def test_supplementary_higher_value_wins(self):
        from processing.chuvas import merge_sources
        cemaden = [_chuva(2504009, "Campina Grande", 10.0)]
        supplementary = [_chuva(2504009, "Campina Grande", 20.0, fonte="ANA")]
        result = merge_sources(cemaden, supplementary)
        assert len(result) == 1
        assert result[0]["chuva_mm"] == 20.0

    def test_cemaden_wins_when_higher_than_supplementary(self):
        from processing.chuvas import merge_sources
        cemaden = [_chuva(2504009, "Campina Grande", 25.0)]
        supplementary = [_chuva(2504009, "Campina Grande", 10.0, fonte="ANA")]
        result = merge_sources(cemaden, supplementary)
        assert result[0]["chuva_mm"] == 25.0
        assert result[0]["fonte"] == "CEMADEN"

    def test_none_supplementary_is_safe(self):
        from processing.chuvas import merge_sources
        records = [_chuva(2504009, "Campina Grande", 10.0)]
        result = merge_sources(records, None)
        assert len(result) == 1


class TestTop5Ranking:
    def test_returns_top5_descending(self):
        from processing.chuvas import top5_ranking
        records = [_chuva(2500000 + i, f"Mun{i}", float(i * 10)) for i in range(10)]
        top5 = top5_ranking(records)
        assert len(top5) == 5
        assert top5[0]["chuva_mm"] == 90.0
        assert top5[4]["chuva_mm"] == 50.0

    def test_rank_field_is_assigned(self):
        from processing.chuvas import top5_ranking
        records = [_chuva(2500000 + i, f"Mun{i}", float(i)) for i in range(5)]
        top5 = top5_ranking(records)
        assert [r["rank"] for r in top5] == [1, 2, 3, 4, 5]

    def test_fewer_than_5_returns_all(self):
        from processing.chuvas import top5_ranking
        records = [_chuva(2504009, "Campina Grande", 10.0)]
        assert len(top5_ranking(records)) == 1

    def test_empty_returns_empty(self):
        from processing.chuvas import top5_ranking
        assert top5_ranking([]) == []


class TestStateAverage:
    def test_correct_mean(self):
        from processing.chuvas import state_average
        records = [
            _chuva(2504009, "A", 10.0),
            _chuva(2507507, "B", 20.0),
            _chuva(2510808, "C", 30.0),
        ]
        assert state_average(records) == pytest.approx(20.0)

    def test_empty_returns_zero(self):
        from processing.chuvas import state_average
        assert state_average([]) == 0.0


class TestClassifyAlerts:
    def test_below_threshold_excluded(self):
        from processing.chuvas import classify_alerts
        assert classify_alerts([_chuva(2504009, "CG", 49.9)]) == []

    def test_exact_atencao_threshold(self):
        from processing.chuvas import classify_alerts
        result = classify_alerts([_chuva(2504009, "CG", 50.0)])
        assert len(result) == 1
        assert result[0]["nivel"] == "atenção"

    def test_exact_critico_threshold(self):
        from processing.chuvas import classify_alerts
        result = classify_alerts([_chuva(2504009, "CG", 80.0)])
        assert result[0]["nivel"] == "crítico"

    def test_above_critico_threshold(self):
        from processing.chuvas import classify_alerts
        result = classify_alerts([_chuva(2504009, "CG", 120.0)])
        assert result[0]["nivel"] == "crítico"

    def test_sorted_descending(self):
        from processing.chuvas import classify_alerts
        records = [
            _chuva(2504009, "A", 55.0),
            _chuva(2507507, "B", 90.0),
            _chuva(2510808, "C", 60.0),
        ]
        result = classify_alerts(records)
        assert result[0]["chuva_mm"] == 90.0
        assert result[-1]["chuva_mm"] == 55.0

    def test_mixed_levels_correct(self):
        from processing.chuvas import classify_alerts
        records = [
            _chuva(2504009, "A", 79.9),   # atenção
            _chuva(2507507, "B", 80.0),   # crítico
        ]
        result = classify_alerts(records)
        by_ibge = {r["ibge_code"]: r for r in result}
        assert by_ibge[2504009]["nivel"] == "atenção"
        assert by_ibge[2507507]["nivel"] == "crítico"


# ---------------------------------------------------------------------------
# processing.acudes
# ---------------------------------------------------------------------------

class TestClassifyReservoir:
    @pytest.mark.parametrize("pct,expected", [
        (0.0,   "crítico"),
        (19.9,  "crítico"),
        (20.0,  "alerta"),
        (39.9,  "alerta"),
        (40.0,  "normal"),
        (69.9,  "normal"),
        (70.0,  "cheio"),
        (100.0, "cheio"),
    ])
    def test_boundaries(self, pct, expected):
        from processing.acudes import classify_reservoir
        assert classify_reservoir(pct) == expected


class TestWeightedSemiaridoAverage:
    def test_capacity_weighted(self):
        from processing.acudes import weighted_semiarido_average
        # 100 hm³ cap at 10%, 900 hm³ cap at 50% → weighted avg = (10 + 450) / 1000 * 100 = 46%
        records = [
            _acude("A", 100.0, 10.0, 10.0),
            _acude("B", 900.0, 450.0, 50.0),
        ]
        assert weighted_semiarido_average(records) == pytest.approx(46.0, abs=0.1)

    def test_equal_capacities_is_simple_mean(self):
        from processing.acudes import weighted_semiarido_average
        records = [
            _acude("A", 100.0, 30.0, 30.0),
            _acude("B", 100.0, 70.0, 70.0),
        ]
        assert weighted_semiarido_average(records) == pytest.approx(50.0, abs=0.1)

    def test_empty_returns_zero(self):
        from processing.acudes import weighted_semiarido_average
        assert weighted_semiarido_average([]) == 0.0

    def test_zero_capacity_returns_zero(self):
        from processing.acudes import weighted_semiarido_average
        records = [_acude("A", 0.0, 0.0, 0.0)]
        assert weighted_semiarido_average(records) == 0.0


class TestEnrichReservoirs:
    def test_adds_classificacao_and_cor(self):
        from processing.acudes import enrich_reservoirs
        records = [_acude("Epitácio Pessoa", 411.0, 82.2, 20.0)]
        enriched = enrich_reservoirs(records)
        assert "classificacao" in enriched[0]
        assert "cor" in enriched[0]
        assert enriched[0]["cor"].startswith("#")

    def test_sanity_check_fixes_implausible_percentual(self):
        from processing.acudes import enrich_reservoirs
        # scraped percentual is 110% (bad data) → should recompute from volumes
        records = [_acude("Epitácio Pessoa", 411.0, 82.2, 110.0)]
        enriched = enrich_reservoirs(records)
        assert enriched[0]["percentual"] < 105.0
        assert enriched[0]["percentual"] == pytest.approx(82.2 / 411.0 * 100, abs=0.1)

    def test_preserves_priority_order(self):
        from processing.acudes import enrich_reservoirs
        from config import RESERVOIRS_PRIORITY
        records = [
            _acude(r["nome"], r["capacidade_hm3"], r["capacidade_hm3"] * 0.3, 30.0)
            for r in reversed(RESERVOIRS_PRIORITY)
        ]
        enriched = enrich_reservoirs(records)
        expected_order = [r["nome"] for r in RESERVOIRS_PRIORITY]
        actual_order = [r["nome"] for r in enriched]
        assert actual_order == expected_order

    def test_non_mutating(self):
        from processing.acudes import enrich_reservoirs
        original = [_acude("Epitácio Pessoa", 411.0, 82.2, 20.0)]
        enrich_reservoirs(original)
        assert "classificacao" not in original[0]


class TestCriticalReservoirs:
    def test_returns_only_at_risk(self):
        from processing.acudes import enrich_reservoirs, critical_reservoirs
        records = enrich_reservoirs([
            _acude("Epitácio Pessoa",   411.0, 41.1,  10.0),   # crítico
            _acude("Coremas-Mãe D'Água", 1358.0, 679.0, 50.0), # normal
            _acude("São Gonçalo",        44.0,   13.2,  30.0),  # alerta
        ])
        at_risk = critical_reservoirs(records)
        names = [r["nome"] for r in at_risk]
        assert "Epitácio Pessoa" in names
        assert "São Gonçalo" in names
        assert "Coremas-Mãe D'Água" not in names

    def test_sorted_ascending_by_percentual(self):
        from processing.acudes import enrich_reservoirs, critical_reservoirs
        records = enrich_reservoirs([
            _acude("A", 100.0, 30.0, 30.0),  # alerta
            _acude("B", 100.0, 10.0, 10.0),  # crítico
        ])
        at_risk = critical_reservoirs(records)
        assert at_risk[0]["percentual"] < at_risk[1]["percentual"]
