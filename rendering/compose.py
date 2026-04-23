"""
Video composition — assembles PIL frame images into a final MP4 via MoviePy.

Each scene is a static ImageClip. Scenes 2–5 get a 0.5s crossfade in.
Audio is disabled for the MVP.

ffmpeg must be on PATH. The pipeline checks this at startup (main.py).
"""

import logging
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
from PIL import Image

from config import (
    DUR_ABERTURA,
    DUR_ACUDES,
    DUR_ALERTAS,
    DUR_ENCERRAMENTO,
    DUR_MAPA,
    FPS,
    OUTPUT_DIR,
)

log = logging.getLogger(__name__)

CROSSFADE_S = 0.5


def assemble(
    frame_abertura: Image.Image,
    frame_mapa: Image.Image,
    frame_acudes: Image.Image,
    frame_alertas: Image.Image,
    frame_encerramento: Image.Image,
    run_date: date | None = None,
) -> Path:
    """
    Render all five frames into an MP4 and return the output path.

    Raises RuntimeError if ffmpeg is not found or MoviePy fails.
    """
    _check_ffmpeg()

    # Import here so the Agg backend is already active before moviepy touches matplotlib
    from moviepy.editor import ImageClip, concatenate_videoclips

    if run_date is None:
        run_date = date.today()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"boletim_pb_{run_date.strftime('%Y%m%d')}.mp4"

    scenes = [
        (frame_abertura,    DUR_ABERTURA),
        (frame_mapa,        DUR_MAPA),
        (frame_acudes,      DUR_ACUDES),
        (frame_alertas,     DUR_ALERTAS),
        (frame_encerramento, DUR_ENCERRAMENTO),
    ]

    clips = []
    for i, (pil_img, duration) in enumerate(scenes):
        arr = _to_rgb_array(pil_img)
        clip = ImageClip(arr).set_duration(duration)
        if i > 0:
            clip = clip.crossfadein(CROSSFADE_S)
        clips.append(clip)

    video = concatenate_videoclips(clips, method="compose", padding=-CROSSFADE_S)

    log.info("Rendering MP4 → %s", output_path)
    video.write_videofile(
        str(output_path),
        fps=FPS,
        codec="libx264",
        audio=False,
        preset="fast",
        ffmpeg_params=["-crf", "23"],
        logger=None,   # suppress moviepy's own progress bar in CI
    )
    video.close()

    size_mb = output_path.stat().st_size / 1_048_576
    log.info("MP4 written: %.1f MB", size_mb)

    if size_mb > 15:
        log.warning("Output file is %.1f MB — exceeds 15 MB target. Consider raising -crf.", size_mb)

    return output_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_rgb_array(img: Image.Image) -> np.ndarray:
    """Convert a PIL RGBA Image to a uint8 RGB numpy array for MoviePy."""
    return np.array(img.convert("RGB"), dtype=np.uint8)


def _check_ffmpeg() -> None:
    """Raise RuntimeError if ffmpeg is not on PATH."""
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            raise RuntimeError("ffmpeg returned non-zero exit code")
    except FileNotFoundError:
        raise RuntimeError(
            "ffmpeg not found on PATH. "
            "Install via: sudo apt-get install ffmpeg  (Ubuntu) "
            "or: winget install ffmpeg  (Windows)"
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("ffmpeg check timed out")
