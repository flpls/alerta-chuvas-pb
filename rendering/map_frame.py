"""
Rainfall map frame — choropleth of PB municipalities coloured by 24h accumulation.

Rendered headlessly via matplotlib Agg backend. Must be imported AFTER
the MPLBACKEND env var is set (main.py handles this), but we also force
it here as a safety net.
"""

import io
import logging
import os
import unicodedata

os.environ.setdefault("MPLBACKEND", "Agg")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
import numpy as np

import geopandas as gpd
import pandas as pd
from PIL import Image

from config import BRAND_NAVY, BRAND_WHITE, GEOJSON_PATH, VIDEO_SIZE
from rendering.base import (
    _hex,
    centered_text,
    draw_watermark,
    font_bold,
    font_regular,
    get_draw,
    new_frame,
)
from config import ASSETS_DIR, SPONSORS_DIR

log = logging.getLogger(__name__)

W, H = VIDEO_SIZE

# Map occupies the middle band of the frame
MAP_TOP = 220        # y where the map image starts
MAP_HEIGHT = 1100    # px of vertical space for the map
MAP_BOTTOM = MAP_TOP + MAP_HEIGHT

# Colourmap: yellow (low) → deep red (high); grey for no-data
CMAP = plt.cm.YlOrRd
NO_DATA_COLOR = "#CCCCCC"
VMAX_MM = 100.0      # fixed ceiling — prevents one extreme value washing everything out


def render(
    records: list[dict],
    top5: list[dict],
    run_date=None,
    watermark_path=None,
) -> Image.Image:
    """
    Build and return the 1080×1920 map frame as a PIL Image.

    `records`  — full merged municipal rainfall list (ibge_code, chuva_mm, municipio)
    `top5`     — top-5 subset with rank field (subset of records, already sorted)
    """
    img = new_frame(BRAND_WHITE)
    draw = get_draw(img)

    _draw_header(draw, run_date)

    geo = _load_geodataframe(records)
    if geo is not None:
        map_img = _render_map(geo, top5)
        img.paste(map_img, (0, MAP_TOP))
    else:
        # GeoJSON not available — draw a placeholder
        draw.rectangle([60, MAP_TOP + 40, W - 60, MAP_BOTTOM - 40], outline=_hex("#CCCCCC"), width=3)
        centered_text(draw, "Mapa indisponível", MAP_TOP + MAP_HEIGHT // 2,
                      font_regular(36), color="#999999")

    _draw_legend(draw)
    _draw_top5_list(draw, top5)

    if watermark_path:
        draw_watermark(img, watermark_path)

    return img


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def _draw_header(draw, run_date) -> None:
    centered_text(draw, "CHUVAS NAS ÚLTIMAS 24H", 60, font_bold(52), color=BRAND_NAVY)
    if run_date:
        from datetime import date as date_cls
        if isinstance(run_date, date_cls):
            subtitle = run_date.strftime("%-d de %B de %Y").lower().capitalize() if hasattr(run_date, 'strftime') else str(run_date)
            try:
                import locale
                locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
                subtitle = run_date.strftime("%-d de %B de %Y")
            except Exception:
                # Fallback: English month names on Windows
                _MONTHS_PT = ["janeiro","fevereiro","março","abril","maio","junho",
                               "julho","agosto","setembro","outubro","novembro","dezembro"]
                subtitle = f"{run_date.day} de {_MONTHS_PT[run_date.month - 1]} de {run_date.year}"
            centered_text(draw, subtitle, 130, font_regular(36), color="#555555")


# ---------------------------------------------------------------------------
# GeoDataFrame
# ---------------------------------------------------------------------------

def _load_geodataframe(records: list[dict]):
    """Load and merge PB GeoJSON with rainfall records. Returns None if GeoJSON missing."""
    if not GEOJSON_PATH.exists():
        log.warning("GeoJSON not found at %s — map frame will show placeholder", GEOJSON_PATH)
        return None

    try:
        gdf = gpd.read_file(str(GEOJSON_PATH))
    except Exception as exc:
        log.warning("Could not load GeoJSON: %s", exc)
        return None

    # Identify the IBGE code column (IBGE GeoJSONs use CD_MUN or CD_GEOCODM)
    code_col = None
    for candidate in ("CD_MUN", "CD_GEOCODM", "CD_GEOCOD", "codmun", "code"):
        if candidate in gdf.columns:
            code_col = candidate
            break

    if code_col is None:
        log.warning("Could not find IBGE code column in GeoJSON. Columns: %s", list(gdf.columns))
        return None

    gdf[code_col] = gdf[code_col].astype(str).str[:7].astype(int)

    rain_df = pd.DataFrame([
        {"ibge_code": r["ibge_code"], "chuva_mm": r["chuva_mm"], "municipio": r["municipio"]}
        for r in records
    ])

    gdf = gdf.merge(rain_df, left_on=code_col, right_on="ibge_code", how="left")
    gdf["chuva_mm"] = gdf["chuva_mm"].fillna(-1)  # -1 = no data sentinel
    return gdf


# ---------------------------------------------------------------------------
# Map render
# ---------------------------------------------------------------------------

def _render_map(gdf, top5: list[dict]) -> Image.Image:
    """Render the choropleth to a PIL Image sized to fit the map band."""
    target_w = W
    target_h = MAP_HEIGHT
    dpi = 150
    fig_w = target_w / dpi
    fig_h = target_h / dpi

    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_facecolor("white")
    ax.set_axis_off()

    norm = Normalize(vmin=0, vmax=VMAX_MM)

    # No-data municipalities
    no_data = gdf[gdf["chuva_mm"] < 0]
    if not no_data.empty:
        no_data.plot(ax=ax, color=NO_DATA_COLOR, edgecolor="#AAAAAA", linewidth=0.4)

    # Municipalities with data
    with_data = gdf[gdf["chuva_mm"] >= 0]
    if not with_data.empty:
        with_data.plot(
            ax=ax,
            column="chuva_mm",
            cmap=CMAP,
            norm=norm,
            edgecolor="#888888",
            linewidth=0.4,
            missing_kwds={"color": NO_DATA_COLOR},
        )

    # Top-5 markers
    _draw_top5_markers(ax, gdf, top5)

    plt.tight_layout(pad=0)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight",
                facecolor="white", pad_inches=0)
    plt.close(fig)
    buf.seek(0)

    map_img = Image.open(buf).convert("RGBA")
    map_img = map_img.resize((target_w, target_h), Image.LANCZOS)
    return map_img


def _draw_top5_markers(ax, gdf, top5: list[dict]) -> None:
    """Plot rank markers on the top-5 municipality centroids."""
    if not top5:
        return

    code_col = None
    for c in ("CD_MUN", "CD_GEOCODM", "CD_GEOCOD", "codmun", "ibge_code"):
        if c in gdf.columns:
            code_col = c
            break
    if code_col is None:
        return

    for entry in top5:
        match = gdf[gdf[code_col] == entry["ibge_code"]]
        if match.empty:
            continue
        centroid = match.geometry.centroid.iloc[0]
        ax.plot(centroid.x, centroid.y, "o", color=BRAND_NAVY,
                markersize=10, markeredgecolor="white", markeredgewidth=1.5,
                zorder=5)
        ax.annotate(
            f"  {entry['rank']}. {entry['municipio']}\n  {entry['chuva_mm']:.0f}mm",
            xy=(centroid.x, centroid.y),
            fontsize=5,
            color=BRAND_NAVY,
            fontweight="bold",
            zorder=6,
        )


# ---------------------------------------------------------------------------
# Legend and top-5 list (drawn on the PIL layer, not matplotlib)
# ---------------------------------------------------------------------------

def _draw_legend(draw) -> None:
    """Draw a compact horizontal colour scale below the map."""
    legend_y = MAP_BOTTOM + 20
    legend_x = 80
    bar_w = W - 160
    bar_h = 28

    # Gradient bar using CMAP
    gradient = np.linspace(0, 1, bar_w)
    for i, val in enumerate(gradient):
        rgba = CMAP(val)
        r, g, b = int(rgba[0] * 255), int(rgba[1] * 255), int(rgba[2] * 255)
        draw.rectangle([legend_x + i, legend_y, legend_x + i + 1, legend_y + bar_h],
                       fill=(r, g, b, 255))

    f = font_regular(26)
    draw.text((legend_x, legend_y + bar_h + 8), "0 mm", font=f, fill=_hex("#555555"))
    label_100 = "100+ mm"
    bbox = draw.textbbox((0, 0), label_100, font=f)
    tw = bbox[2] - bbox[0]
    draw.text((legend_x + bar_w - tw, legend_y + bar_h + 8), label_100,
              font=f, fill=_hex("#555555"))

    # No-data swatch
    swatch_x = legend_x + bar_w // 2 - 60
    draw.rectangle([swatch_x, legend_y + bar_h + 44,
                    swatch_x + 20, legend_y + bar_h + 64],
                   fill=_hex(NO_DATA_COLOR))
    draw.text((swatch_x + 28, legend_y + bar_h + 44), "Sem dados",
              font=f, fill=_hex("#555555"))


def _draw_top5_list(draw, top5: list[dict]) -> None:
    """Draw the top-5 ranking list below the legend."""
    if not top5:
        return

    start_y = MAP_BOTTOM + 130
    label = "MAIORES ACUMULADOS"
    centered_text(draw, label, start_y, font_bold(34), color=BRAND_NAVY)

    row_h = 72
    pad_x = 80
    for entry in top5:
        y = start_y + 60 + (entry["rank"] - 1) * row_h
        # Rank circle
        r = 22
        cx, cy = pad_x + r, y + r
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=_hex(BRAND_NAVY))
        rank_f = font_bold(26)
        rank_text = str(entry["rank"])
        rb = draw.textbbox((0, 0), rank_text, font=rank_f)
        draw.text(
            (cx - (rb[2] - rb[0]) // 2, cy - (rb[3] - rb[1]) // 2),
            rank_text, font=rank_f, fill=_hex(BRAND_WHITE),
        )
        # Municipality and value
        draw.text((pad_x + r * 2 + 16, y + 4),
                  entry["municipio"], font=font_bold(30), fill=_hex(BRAND_NAVY))
        draw.text((pad_x + r * 2 + 16, y + 38),
                  f"{entry['chuva_mm']:.1f} mm", font=font_regular(26), fill=_hex("#444444"))
