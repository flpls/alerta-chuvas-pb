"""
Açudes frame — 2×3 grid of circular fill gauges, one per priority reservoir
plus a weighted semiárido summary cell.

Pure Pillow — no matplotlib dependency.
"""

import logging
import math

from PIL import Image, ImageDraw

from config import BRAND_NAVY, BRAND_WHITE, VIDEO_SIZE
from rendering.base import (
    _hex,
    centered_text,
    draw_watermark,
    font_bold,
    font_regular,
    get_draw,
    new_frame,
)

log = logging.getLogger(__name__)

W, H = VIDEO_SIZE

# Grid layout: 2 columns × 3 rows
COLS = 2
ROWS = 3
CELL_W = W // COLS          # 540
CELL_H = 480
GRID_TOP = 180              # y where the grid starts
GAUGE_RADIUS = 110          # outer radius of the gauge arc
GAUGE_THICKNESS = 22        # arc stroke width


def render(
    reservoirs: list[dict],
    semiarido_avg: float,
    semiarido_class: str,
    watermark_path=None,
) -> Image.Image:
    """
    Build and return the 1080×1920 açudes frame.

    `reservoirs`     — enriched records (from processing.acudes.enrich_reservoirs)
    `semiarido_avg`  — capacity-weighted fill % for all 5 reservoirs
    `semiarido_class`— classification string for semiarido_avg
    """
    img = new_frame(BRAND_WHITE)
    draw = get_draw(img)

    # Header
    centered_text(draw, "AÇUDES DO SEMIÁRIDO", 60, font_bold(52), color=BRAND_NAVY)
    centered_text(draw, "Nível de armazenamento atual", 130, font_regular(34), color="#555555")

    # Draw each reservoir cell
    cells = list(reservoirs[:5])  # max 5 priority reservoirs

    for i, res in enumerate(cells):
        col = i % COLS
        row = i // COLS
        cx = col * CELL_W + CELL_W // 2
        cy = GRID_TOP + row * CELL_H + CELL_H // 2
        _draw_gauge_cell(draw, cx, cy, res)

    # 6th cell: semiárido summary
    summary = {
        "apelido":       "Semiárido PB",
        "percentual":    semiarido_avg,
        "classificacao": semiarido_class,
        "cor":           _class_color(semiarido_class),
        "variacao_24h":  None,
        "is_summary":    True,
    }
    col, row = len(cells) % COLS, len(cells) // COLS
    cx = col * CELL_W + CELL_W // 2
    cy = GRID_TOP + row * CELL_H + CELL_H // 2
    _draw_gauge_cell(draw, cx, cy, summary)

    if watermark_path:
        draw_watermark(img, watermark_path)

    return img


# ---------------------------------------------------------------------------
# Gauge cell
# ---------------------------------------------------------------------------

def _draw_gauge_cell(draw: ImageDraw.ImageDraw, cx: int, cy: int, res: dict) -> None:
    """Draw a single gauge cell centred at (cx, cy)."""
    pct = max(0.0, min(100.0, res["percentual"]))
    cor = res["cor"]
    apelido = res.get("apelido") or res.get("nome", "")
    classificacao = res.get("classificacao", "")
    variacao = res.get("variacao_24h")
    is_summary = res.get("is_summary", False)

    r = GAUGE_RADIUS
    t = GAUGE_THICKNESS

    # Background arc (full circle, grey)
    _arc(draw, cx, cy, r, 0, 360, "#E0E0E0", t)

    # Fill arc: 0% = -90° (top), sweeps clockwise
    sweep = pct / 100.0 * 360.0
    if sweep > 0:
        _arc(draw, cx, cy, r, -90, -90 + sweep, cor, t)

    # Summary cell gets a dashed border around the circle
    if is_summary:
        _dashed_circle(draw, cx, cy, r + t + 6, "#AAAAAA")

    # Percentage text (large, bold)
    pct_text = f"{pct:.0f}%"
    pct_font = font_bold(56)
    _centered_in_circle(draw, pct_text, cx, cy - 16, pct_font, cor)

    # Classification label (small, coloured)
    cls_font = font_regular(26)
    _centered_in_circle(draw, classificacao.upper(), cx, cy + 44, cls_font, "#777777")

    # Variation indicator (▲▼ +X.Xpp)
    if variacao is not None:
        arrow = "▲" if variacao >= 0 else "▼"
        v_color = "#27AE60" if variacao >= 0 else "#C0392B"
        var_text = f"{arrow} {abs(variacao):.1f}pp"
        var_font = font_regular(24)
        _centered_in_circle(draw, var_text, cx, cy + 76, var_font, v_color)

    # Reservoir name below the gauge
    name_y = cy + r + t + 20
    name_font = font_bold(28) if not is_summary else font_bold(30)
    name_color = BRAND_NAVY if not is_summary else cor
    _wrap_and_center(draw, apelido, cx, name_y, name_font, name_color, max_width=CELL_W - 40)


# ---------------------------------------------------------------------------
# Drawing primitives
# ---------------------------------------------------------------------------

def _arc(
    draw: ImageDraw.ImageDraw,
    cx: int, cy: int,
    radius: int,
    start_deg: float, end_deg: float,
    color: str,
    thickness: int,
) -> None:
    """Draw an arc from start_deg to end_deg (clockwise) with given thickness."""
    bb = [cx - radius, cy - radius, cx + radius, cy + radius]
    # Pillow's arc goes counter-clockwise from 3 o'clock; we work in standard maths angles
    draw.arc(bb, start=start_deg, end=end_deg, fill=_hex(color), width=thickness)


def _dashed_circle(
    draw: ImageDraw.ImageDraw,
    cx: int, cy: int, radius: int, color: str,
    dash_deg: float = 8.0,
) -> None:
    """Draw a dashed circle outline."""
    angle = 0.0
    while angle < 360.0:
        end = min(angle + dash_deg, 360.0)
        bb = [cx - radius, cy - radius, cx + radius, cy + radius]
        draw.arc(bb, start=angle - 90, end=end - 90, fill=_hex(color), width=2)
        angle += dash_deg * 2  # gap = dash length


def _centered_in_circle(
    draw: ImageDraw.ImageDraw,
    text: str, cx: int, y: int,
    font, color: str,
) -> None:
    """Draw text horizontally centred at cx."""
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    draw.text((cx - tw // 2, y), text, font=font, fill=_hex(color))


def _wrap_and_center(
    draw: ImageDraw.ImageDraw,
    text: str, cx: int, y: int,
    font, color: str,
    max_width: int,
) -> None:
    """Draw text, wrapping to a second line if it exceeds max_width."""
    bbox = draw.textbbox((0, 0), text, font=font)
    if bbox[2] - bbox[0] <= max_width:
        _centered_in_circle(draw, text, cx, y, font, color)
        return

    # Split at first space past the midpoint
    words = text.split()
    best_split = len(words) // 2
    line1 = " ".join(words[:best_split])
    line2 = " ".join(words[best_split:])
    line_h = bbox[3] - bbox[1] + 6
    _centered_in_circle(draw, line1, cx, y, font, color)
    _centered_in_circle(draw, line2, cx, y + line_h, font, color)


def _class_color(classificacao: str) -> str:
    from config import RESERV_COLORS
    return RESERV_COLORS.get(classificacao, "#555555")
