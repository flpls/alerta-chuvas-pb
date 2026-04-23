"""
Shared PIL utilities for all frame renderers.

Every frame is 1080×1920 RGBA. Callers convert to RGB numpy arrays only
at compose time (MoviePy requires RGB uint8).
"""

import logging
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from config import (
    BRAND_NAVY,
    BRAND_WHITE,
    FONTS_DIR,
    FONT_BOLD,
    FONT_REGULAR,
    VIDEO_SIZE,
)

log = logging.getLogger(__name__)

W, H = VIDEO_SIZE  # 1080 × 1920


# ---------------------------------------------------------------------------
# Frame construction
# ---------------------------------------------------------------------------

def new_frame(bg_color: str = BRAND_WHITE) -> Image.Image:
    """Return a blank 1080×1920 RGBA frame filled with bg_color."""
    img = Image.new("RGBA", (W, H), _hex(bg_color))
    return img


def get_draw(img: Image.Image) -> ImageDraw.ImageDraw:
    return ImageDraw.Draw(img)


# ---------------------------------------------------------------------------
# Typography
# ---------------------------------------------------------------------------

def load_font(name: str, size: int) -> ImageFont.FreeTypeFont:
    """
    Load a TTF font from assets/fonts/. Falls back to Pillow's built-in
    bitmap font with a warning — text will look rough but the pipeline won't crash.
    """
    path = FONTS_DIR / name
    if path.exists():
        try:
            return ImageFont.truetype(str(path), size)
        except Exception as exc:
            log.warning("Could not load font %s: %s — using default", path, exc)
    else:
        log.warning("Font not found: %s — using default", path)
    return ImageFont.load_default()


def font_regular(size: int) -> ImageFont.FreeTypeFont:
    return load_font(FONT_REGULAR, size)


def font_bold(size: int) -> ImageFont.FreeTypeFont:
    return load_font(FONT_BOLD, size)


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

def centered_text(
    draw: ImageDraw.ImageDraw,
    text: str,
    y: int,
    font: ImageFont.FreeTypeFont,
    color: str = BRAND_NAVY,
    x_center: int | None = None,
) -> None:
    """Draw text horizontally centred at y."""
    cx = x_center if x_center is not None else W // 2
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    draw.text((cx - tw // 2, y), text, font=font, fill=_hex(color))


def text_block(
    draw: ImageDraw.ImageDraw,
    lines: list[str],
    x: int,
    y: int,
    font: ImageFont.FreeTypeFont,
    color: str = BRAND_NAVY,
    line_spacing: int = 8,
) -> int:
    """
    Draw a block of left-aligned text lines.
    Returns the y coordinate immediately after the last line.
    """
    cy = y
    for line in lines:
        draw.text((x, cy), line, font=font, fill=_hex(color))
        bbox = draw.textbbox((0, 0), line, font=font)
        cy += (bbox[3] - bbox[1]) + line_spacing
    return cy


def pill(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    w: int,
    h: int,
    fill: str,
    text: str,
    text_font: ImageFont.FreeTypeFont,
    text_color: str = BRAND_WHITE,
) -> None:
    """Draw a rounded-rectangle pill badge with centred text."""
    r = h // 2
    draw.rounded_rectangle([x, y, x + w, y + h], radius=r, fill=_hex(fill))
    bbox = draw.textbbox((0, 0), text, font=text_font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(
        (x + (w - tw) // 2, y + (h - th) // 2),
        text,
        font=text_font,
        fill=_hex(text_color),
    )


def horizontal_rule(
    draw: ImageDraw.ImageDraw,
    y: int,
    color: str = "#E0E0E0",
    x_pad: int = 60,
    thickness: int = 2,
) -> None:
    draw.rectangle([x_pad, y, W - x_pad, y + thickness], fill=_hex(color))


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def draw_watermark(
    img: Image.Image,
    logo_path: Path,
    opacity: float = 0.10,
    margin: int = 40,
    max_width: int = 160,
) -> None:
    """
    Composite the VAMO logo at the bottom-right corner at reduced opacity.
    Skips silently if the logo file doesn't exist.
    """
    if not logo_path.exists():
        log.debug("Watermark logo not found: %s", logo_path)
        return

    try:
        logo = Image.open(logo_path).convert("RGBA")
    except Exception as exc:
        log.warning("Could not open watermark logo: %s", exc)
        return

    # Scale to fit within max_width while preserving aspect ratio
    lw, lh = logo.size
    if lw > max_width:
        lh = int(lh * max_width / lw)
        lw = max_width
        logo = logo.resize((lw, lh), Image.LANCZOS)

    # Apply opacity by scaling the alpha channel
    r, g, b, a = logo.split()
    a = a.point(lambda p: int(p * opacity))
    logo = Image.merge("RGBA", (r, g, b, a))

    pos = (W - lw - margin, H - lh - margin)
    img.paste(logo, pos, mask=logo)


# ---------------------------------------------------------------------------
# Colour utility
# ---------------------------------------------------------------------------

def _hex(color: str) -> tuple[int, int, int, int]:
    """Convert a hex colour string to an RGBA tuple (alpha=255)."""
    color = color.lstrip("#")
    if len(color) == 6:
        r, g, b = int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)
        return (r, g, b, 255)
    if len(color) == 8:
        r, g, b, a = (int(color[i:i+2], 16) for i in (0, 2, 4, 6))
        return (r, g, b, a)
    raise ValueError(f"Unrecognised colour: #{color}")
