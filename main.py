"""
Boletim Climático Paraíba — daily pipeline entry point.

Usage:
    python main.py                        # today's run
    python main.py --date 2026-04-20      # specific date (backfill / debug)
    python main.py --dry-run              # collect + process, skip rendering

Exit codes:
    0  — success
    1  — unrecoverable error (logged with full traceback)
"""

import argparse
import logging
import os
import sys
import time
import traceback
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Force headless matplotlib BEFORE any other import that might touch pyplot
# ---------------------------------------------------------------------------
os.environ["MPLBACKEND"] = "Agg"

from config import (
    DATA_DIR,
    DB_PATH,
    OUTPUT_DIR,
    SPONSORS_DIR,
    get_sponsor_for_date,
)
from collectors import db as database
from collectors import cemaden, aesa
from processing import chuvas as proc_chuvas
from processing import acudes as proc_acudes


def main() -> None:
    args = _parse_args()
    run_date = _resolve_date(args.date)
    _setup_logging(run_date)

    log = logging.getLogger(__name__)
    log.info("═" * 60)
    log.info("Boletim Climático PB  |  %s  |  dry-run=%s", run_date, args.dry_run)
    log.info("═" * 60)

    t_start = time.monotonic()

    # ------------------------------------------------------------------
    # 0. Setup
    # ------------------------------------------------------------------
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    database.init_db(DB_PATH)
    database.purge_old_records(DB_PATH)
    log.info("[setup] DB ready at %s", DB_PATH)

    sponsor = get_sponsor_for_date(run_date)
    log.info("[setup] Sponsor: %s", sponsor["nome"])

    # ------------------------------------------------------------------
    # 1. Collect
    # ------------------------------------------------------------------
    log.info("[collect] Starting CEMADEN…")
    t0 = time.monotonic()
    cemaden_records = cemaden.collect(run_date)
    log.info("[collect] CEMADEN: %d municipalities  (%.1fs)",
             len(cemaden_records), time.monotonic() - t0)

    log.info("[collect] Starting AESA…")
    t0 = time.monotonic()
    aesa_records = aesa.collect(run_date)
    log.info("[collect] AESA: %d reservoirs  (%.1fs)",
             len(aesa_records), time.monotonic() - t0)

    if not cemaden_records:
        log.warning("[collect] CEMADEN returned no data — video will have no rainfall info")

    # ------------------------------------------------------------------
    # 2. Process
    # ------------------------------------------------------------------
    log.info("[process] Merging sources and computing metrics…")

    merged = proc_chuvas.merge_sources(cemaden_records)
    top5 = proc_chuvas.top5_ranking(merged)
    avg_mm = proc_chuvas.state_average(merged)
    alerts = proc_chuvas.classify_alerts(merged)
    merged = proc_chuvas.enrich_with_anomaly(merged, run_date)

    log.info("[process] %d municipalities  |  state avg %.1f mm  |  %d alerts",
             len(merged), avg_mm, len(alerts))

    enriched_acudes = proc_acudes.enrich_reservoirs(aesa_records)
    semiarido_avg = proc_acudes.weighted_semiarido_average(enriched_acudes)
    semiarido_class = proc_acudes.semiarido_classification(enriched_acudes)
    critical_acudes = proc_acudes.critical_reservoirs(enriched_acudes)

    if enriched_acudes:
        log.info("[process] Semiárido: %.1f%%  (%s)  |  %d at-risk reservoirs",
                 semiarido_avg, semiarido_class, len(critical_acudes))
    else:
        log.info("[process] No reservoir data available for today")

    if args.dry_run:
        elapsed = time.monotonic() - t_start
        log.info("[dry-run] Skipping rendering  |  total %.1fs", elapsed)
        _print_summary(merged, top5, alerts, enriched_acudes, semiarido_avg)
        return

    # ------------------------------------------------------------------
    # 3. Render frames
    # ------------------------------------------------------------------
    from rendering import (
        map_frame,
        acudes_frame,
        alerta_frame,
        sponsor_frame,
        compose,
    )

    watermark = SPONSORS_DIR / "vamo.png"

    log.info("[render] Abertura…")
    frame_ab = sponsor_frame.render_abertura(sponsor, run_date)

    log.info("[render] Mapa de chuvas…")
    t0 = time.monotonic()
    frame_map = map_frame.render(merged, top5, run_date, watermark_path=watermark)
    log.info("[render] Mapa done  (%.1fs)", time.monotonic() - t0)

    log.info("[render] Açudes…")
    frame_ac = acudes_frame.render(
        enriched_acudes, semiarido_avg, semiarido_class,
        watermark_path=watermark,
    )

    log.info("[render] Alertas…")
    frame_al = alerta_frame.render(alerts, critical_acudes, watermark_path=watermark)

    log.info("[render] Encerramento…")
    frame_enc = sponsor_frame.render_encerramento(sponsor)

    # ------------------------------------------------------------------
    # 4. Compose
    # ------------------------------------------------------------------
    log.info("[compose] Assembling MP4…")
    t0 = time.monotonic()
    output_path = compose.assemble(
        frame_ab, frame_map, frame_ac, frame_al, frame_enc,
        run_date=run_date,
    )
    log.info("[compose] Done  (%.1fs)  →  %s", time.monotonic() - t0, output_path)

    elapsed = time.monotonic() - t_start
    size_mb = output_path.stat().st_size / 1_048_576
    log.info("═" * 60)
    log.info("✓ Boletim gerado  |  %.1f MB  |  total %.1fs", size_mb, elapsed)
    log.info("═" * 60)


# ---------------------------------------------------------------------------
# Summary (dry-run only)
# ---------------------------------------------------------------------------

def _print_summary(merged, top5, alerts, acudes, semiarido_avg) -> None:
    log = logging.getLogger(__name__)
    log.info("── Top 5 municípios ──")
    for r in top5:
        log.info("  %d. %-25s  %.1f mm", r["rank"], r["municipio"], r["chuva_mm"])
    if alerts:
        log.info("── Alertas ──")
        for a in alerts:
            log.info("  [%s] %-25s  %.1f mm", a["nivel"].upper(), a["municipio"], a["chuva_mm"])
    if acudes:
        log.info("── Açudes (semiárido avg %.1f%%) ──", semiarido_avg)
        for r in acudes:
            log.info("  %-30s  %.1f%%  (%s)", r.get("apelido", r["nome"]),
                     r["percentual"], r.get("classificacao", ""))


# ---------------------------------------------------------------------------
# Argument parsing and utilities
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Boletim Climático Paraíba — daily pipeline")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Override run date (default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and process data but skip rendering and MP4 export",
    )
    return parser.parse_args()


def _resolve_date(date_str: str | None) -> date:
    if date_str:
        try:
            return date.fromisoformat(date_str)
        except ValueError:
            print(f"Invalid date format: {date_str!r}. Expected YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
    # Allow CI to override via environment variable
    env_date = os.getenv("BOLETIM_DATE")
    if env_date:
        try:
            return date.fromisoformat(env_date)
        except ValueError:
            pass
    return date.today()


def _setup_logging(run_date: date) -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"boletim_{run_date.strftime('%Y%m%d')}.log"

    fmt = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).critical(
            "Pipeline failed with unhandled exception:\n%s", traceback.format_exc()
        )
        sys.exit(1)
