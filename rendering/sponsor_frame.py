"""
Sponsor frames — opening (abertura) and closing (encerramento).

Both use the navy brand background. The abertura shows the VAMO logo
prominently with the sponsor name and today's date. The encerramento
shows the sponsor's own logo, CTA, and the VAMO watermark.

If sponsor logo or VAMO logo files are missing the frames still render —
they'll just lack the respective image.
"""

import logging
from datetime import date
from pathlib import Path

from PIL import Image

from config import (
    ASSETS_DIR,
    BRAND_NAVY,
    BRAND_WHITE,
    SPONSORS_DIR,
    VIDEO_SIZE,
)
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

_MONTHS_PT = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
]

VAMO_LOGO_PATH = SPONSORS_DIR / "vamo.png"


def render_abertura(sponsor: dict, run_date: date | None = None) -> Image.Image:
    """
    Opening frame: navy background, VAMO logo, sponsor credit, date.
    """
    img = new_frame(BRAND_NAVY)
    draw = get_draw(img)

    # VAMO logo (centred, upper third)
    _paste_image_centred(img, VAMO_LOGO_PATH, cy=H // 3, max_w=480, max_h=220)

    # Product name
    centered_text(draw, "BOLETIM CLIMÁTICO", H // 3 + 240, font_bold(54), color=BRAND_WHITE)
    centered_text(draw, "PARAÍBA", H // 3 + 308, font_bold(64), color="#5B9BD5")

    # Divider line
    div_y = H // 3 + 400
    draw.rectangle([W // 2 - 120, div_y, W // 2 + 120, div_y + 3],
                   fill=_hex("#5B9BD5"))

    # Date
    if run_date is None:
        run_date = date.today()
    date_str = _format_date_pt(run_date)
    centered_text(draw, date_str, div_y + 28, font_regular(36), color="#AAAACC")

    # "Realização" block
    centered_text(draw, "REALIZAÇÃO", H // 3 + 540, font_regular(26), color="#888888")
    centered_text(draw, "VAMO Consultoria", H // 3 + 576, font_bold(36), color=BRAND_WHITE)

    # Sponsor block (if not VAMO itself)
    if sponsor.get("slug") != "vamo":
        sponsor_logo_path = SPONSORS_DIR / f"{sponsor['slug']}.png"
        centered_text(draw, "APOIO", H - 400, font_regular(26), color="#888888")
        _paste_image_centred(img, sponsor_logo_path, cy=H - 300, max_w=360, max_h=160,
                             bg_color=BRAND_NAVY)
        if sponsor.get("cta"):
            centered_text(draw, sponsor["cta"], H - 180, font_regular(30), color="#AAAACC")

    return img


def render_encerramento(sponsor: dict) -> Image.Image:
    """
    Closing frame: navy background, sponsor logo large, CTA, VAMO watermark.
    """
    img = new_frame(BRAND_NAVY)
    draw = get_draw(img)

    # "Apoio" header
    centered_text(draw, "APOIADO POR", H // 2 - 300, font_regular(34), color="#888888")

    # Sponsor logo (prominent, centred)
    sponsor_logo_path = SPONSORS_DIR / f"{sponsor.get('slug', 'vamo')}.png"
    _paste_image_centred(img, sponsor_logo_path, cy=H // 2 - 80, max_w=500, max_h=240,
                         bg_color=BRAND_NAVY)

    # Sponsor name (fallback if no logo)
    centered_text(draw, sponsor.get("nome", ""), H // 2 + 120,
                  font_bold(44), color=BRAND_WHITE)

    # CTA
    if sponsor.get("cta"):
        centered_text(draw, sponsor["cta"], H // 2 + 192,
                      font_regular(34), color="#5B9BD5")

    # Divider
    div_y = H // 2 + 280
    draw.rectangle([W // 2 - 100, div_y, W // 2 + 100, div_y + 2],
                   fill=_hex("#5B9BD5"))

    # VAMO credit
    centered_text(draw, "Um produto", div_y + 28, font_regular(28), color="#888888")
    centered_text(draw, "VAMO Consultoria", div_y + 66, font_bold(34), color=BRAND_WHITE)

    # VAMO watermark (slightly more visible on closing frame)
    draw_watermark(img, VAMO_LOGO_PATH, opacity=0.20)

    return img


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _paste_image_centred(
    img: Image.Image,
    path: Path,
    cy: int,
    max_w: int,
    max_h: int,
    bg_color: str = BRAND_WHITE,
) -> None:
    """Load an image, fit it within max_w×max_h, and paste it centred at cy."""
    if not path.exists():
        log.debug("Sponsor/logo image not found: %s", path)
        return

    try:
        logo = Image.open(path).convert("RGBA")
    except Exception as exc:
        log.warning("Could not open image %s: %s", path, exc)
        return

    lw, lh = logo.size
    scale = min(max_w / lw, max_h / lh, 1.0)
    lw, lh = int(lw * scale), int(lh * scale)
    logo = logo.resize((lw, lh), Image.LANCZOS)

    x = (W - lw) // 2
    y = cy - lh // 2
    img.paste(logo, (x, y), mask=logo)


def _format_date_pt(d: date) -> str:
    return f"{d.day} de {_MONTHS_PT[d.month - 1]} de {d.year}"
