"""
Alerts frame — two sections:

Top half:  Chuva alerts — municipalities with ≥50mm in 24h
Bottom half: Reservoir summary — at-risk açudes with mini fill bars

If no alerts exist the top section shows a green "all clear" message.
"""

import logging

from PIL import Image

from config import BRAND_NAVY, BRAND_WHITE, VIDEO_SIZE
from rendering.base import (
    _hex,
    centered_text,
    draw_watermark,
    font_bold,
    font_regular,
    get_draw,
    horizontal_rule,
    new_frame,
    pill,
)

log = logging.getLogger(__name__)

W, H = VIDEO_SIZE

NIVEL_COLORS = {
    "atenção":  "#E67E22",
    "crítico":  "#C0392B",
}
ALERT_ALL_CLEAR_COLOR = "#27AE60"

# Layout
SECTION_TOP_Y = 80
DIVIDER_Y = H // 2 - 20
SECTION_BOT_Y = DIVIDER_Y + 60

MAX_RAIN_ALERTS = 7     # rows before truncation
MAX_ACUDE_ROWS = 5


def render(
    alerts: list[dict],
    critical_acudes: list[dict],
    watermark_path=None,
) -> Image.Image:
    """
    Build and return the 1080×1920 alerts frame.

    `alerts`         — from processing.chuvas.classify_alerts()
    `critical_acudes`— from processing.acudes.critical_reservoirs() (already enriched)
    """
    img = new_frame(BRAND_WHITE)
    draw = get_draw(img)

    _draw_rain_section(draw, alerts)

    horizontal_rule(draw, DIVIDER_Y, color="#DDDDDD", x_pad=40, thickness=3)

    _draw_acude_section(draw, critical_acudes)

    if watermark_path:
        draw_watermark(img, watermark_path)

    return img


# ---------------------------------------------------------------------------
# Rain alerts section
# ---------------------------------------------------------------------------

def _draw_rain_section(draw, alerts: list[dict]) -> None:
    centered_text(draw, "ALERTAS DE CHUVA", SECTION_TOP_Y, font_bold(48), color=BRAND_NAVY)

    if not alerts:
        centered_text(
            draw,
            "Sem alertas para hoje ✓",
            SECTION_TOP_Y + 90,
            font_bold(38),
            color=ALERT_ALL_CLEAR_COLOR,
        )
        centered_text(
            draw,
            "Nenhum município atingiu 50mm",
            SECTION_TOP_Y + 148,
            font_regular(32),
            color="#888888",
        )
        return

    pad_x = 50
    row_h = 88
    y = SECTION_TOP_Y + 80

    shown = alerts[:MAX_RAIN_ALERTS]
    for alert in shown:
        _draw_alert_row(draw, alert, pad_x, y)
        y += row_h

    overflow = len(alerts) - MAX_RAIN_ALERTS
    if overflow > 0:
        centered_text(
            draw,
            f"+ {overflow} município{'s' if overflow > 1 else ''} em alerta",
            y + 8,
            font_regular(28),
            color="#888888",
        )


def _draw_alert_row(draw, alert: dict, x: int, y: int) -> None:
    nivel = alert["nivel"]
    color = NIVEL_COLORS.get(nivel, "#888888")

    # Severity pill
    pill_w, pill_h = 160, 48
    pill(draw, x, y + 12, pill_w, pill_h,
         fill=color, text=nivel.upper(),
         text_font=font_bold(24), text_color=BRAND_WHITE)

    # Municipality
    draw.text((x + pill_w + 20, y + 8),
              alert["municipio"], font=font_bold(36), fill=_hex(BRAND_NAVY))

    # Rainfall value (right-aligned)
    mm_text = f"{alert['chuva_mm']:.0f} mm"
    mm_font = font_bold(36)
    bbox = draw.textbbox((0, 0), mm_text, font=mm_font)
    tw = bbox[2] - bbox[0]
    draw.text((W - 60 - tw, y + 8), mm_text, font=mm_font, fill=_hex(color))

    # Source tag
    draw.text((x + pill_w + 20, y + 50),
              alert.get("fonte", "CEMADEN"),
              font=font_regular(24), fill=_hex("#AAAAAA"))


# ---------------------------------------------------------------------------
# Reservoir summary section
# ---------------------------------------------------------------------------

def _draw_acude_section(draw, critical_acudes: list[dict]) -> None:
    centered_text(draw, "SITUAÇÃO DOS AÇUDES", SECTION_BOT_Y, font_bold(48), color=BRAND_NAVY)

    if not critical_acudes:
        centered_text(
            draw,
            "Todos os açudes monitorados",
            SECTION_BOT_Y + 90,
            font_regular(34),
            color="#888888",
        )
        centered_text(
            draw,
            "estão em níveis normais ou cheios",
            SECTION_BOT_Y + 136,
            font_regular(34),
            color="#888888",
        )
        return

    pad_x = 50
    row_h = 100
    y = SECTION_BOT_Y + 80
    bar_max_w = W - pad_x * 2 - 200  # space for name + bar

    for res in critical_acudes[:MAX_ACUDE_ROWS]:
        _draw_acude_row(draw, res, pad_x, y, bar_max_w)
        y += row_h


def _draw_acude_row(draw, res: dict, x: int, y: int, bar_max_w: int) -> None:
    cor = res["cor"]
    apelido = res.get("apelido") or res.get("nome", "")
    pct = res["percentual"]
    classificacao = res.get("classificacao", "")

    # Classification pill
    pill_w, pill_h = 140, 44
    pill(draw, x, y + 16, pill_w, pill_h,
         fill=cor, text=classificacao.upper(),
         text_font=font_bold(22), text_color=BRAND_WHITE)

    # Reservoir name
    draw.text((x + pill_w + 18, y + 10),
              apelido, font=font_bold(34), fill=_hex(BRAND_NAVY))

    # Mini fill bar
    bar_y = y + 54
    bar_h = 18
    bar_x = x

    # Background track
    draw.rounded_rectangle([bar_x, bar_y, bar_x + bar_max_w, bar_y + bar_h],
                           radius=bar_h // 2, fill=_hex("#E0E0E0"))

    # Fill
    fill_w = max(bar_h, int(bar_max_w * pct / 100.0))
    draw.rounded_rectangle([bar_x, bar_y, bar_x + fill_w, bar_y + bar_h],
                           radius=bar_h // 2, fill=_hex(cor))

    # Percentage label
    pct_text = f"{pct:.1f}%"
    pct_font = font_bold(30)
    bbox = draw.textbbox((0, 0), pct_text, font=pct_font)
    tw = bbox[2] - bbox[0]
    draw.text((bar_x + bar_max_w + 16, y + 8),
              pct_text, font=pct_font, fill=_hex(cor))

    # Variation
    variacao = res.get("variacao_24h")
    if variacao is not None:
        arrow = "▲" if variacao >= 0 else "▼"
        v_color = "#27AE60" if variacao >= 0 else "#C0392B"
        draw.text((bar_x + bar_max_w + 16, y + 46),
                  f"{arrow}{abs(variacao):.1f}pp",
                  font=font_regular(24), fill=_hex(v_color))
