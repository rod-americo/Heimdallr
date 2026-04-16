#!/usr/bin/env python3
"""Compose a branded PDF that explains Heimdallr metric artifacts."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate
from heimdallr.shared.paths import study_artifacts_dir, study_dir, study_id_json
from heimdallr.shared.patient_names import normalize_patient_name_display


PAGE_WIDTH = 1240
PAGE_HEIGHT = 1754
MARGIN = 72
BANNER_HEIGHT = 196
SECTION_GAP = 28
LINE_GAP = 8
CARD_RADIUS = 18
CARD_PADDING = 20
CARD_GAP = 18

BRAND_PRIMARY = "#486864"
BRAND_DARK = "#233432"
BRAND_MUTED = "#606060"
BRAND_LIGHT = "#EDEDED"
TEXT_PRIMARY = "#10201d"
TEXT_MUTED = "#475569"

REPO_ROOT = Path(__file__).resolve().parents[2]
BRANDING_FONTS_DIR = REPO_ROOT / "docs" / "branding" / "fonts"
BRANDING_WATERMARK = REPO_ROOT / "static" / "branding" / "watermarks" / "heimdallr_watermark_01.png"
BRANDING_WORDMARK = REPO_ROOT / "docs" / "branding" / "mockups" / "snap_vector2.png"
PDF_DOMAIN = "artifact_guide"


@dataclass(slots=True)
class InstructionModule:
    key: str
    title: str
    rows: list[tuple[str, str]]


def _pdf_locale(locale: str | None = None) -> str:
    return normalize_locale(locale or settings.ARTIFACTS_LOCALE)


def _t(message_id: str, *, locale: str | None = None, **kwargs: Any) -> str:
    return translate(message_id, locale=_pdf_locale(locale), domain=PDF_DOMAIN, **kwargs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True, help="Study case identifier.")
    parser.add_argument("--output", default="", help="Optional output PDF path.")
    return parser.parse_args()


def _font_cache_dir() -> Path:
    path = Path(tempfile.gettempdir()) / "heimdallr-brand-fonts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _extract_font(archive_name: str, member_name: str) -> Path:
    archive_path = BRANDING_FONTS_DIR / archive_name
    if not archive_path.exists():
        raise RuntimeError(f"Brand font archive not found: {archive_path}")

    target = _font_cache_dir() / Path(member_name).name
    if target.exists():
        return target

    with zipfile.ZipFile(archive_path) as zf:
        with zf.open(member_name) as source:
            target.write_bytes(source.read())
    return target


def _load_font(
    size: int,
    *,
    family: str = "montserrat",
    bold: bool = False,
    italic: bool = False,
) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        if family == "exo2":
            if bold and italic:
                member = "static/Exo2-SemiBoldItalic.ttf"
            elif italic:
                member = "static/Exo2-Italic.ttf"
            elif bold:
                member = "static/Exo2-SemiBold.ttf"
            else:
                member = "static/Exo2-Regular.ttf"
            font_path = _extract_font("exo_2_font_family.zip", member)
        else:
            if bold and italic:
                member = "static/Montserrat-SemiBoldItalic.ttf"
            elif italic:
                member = "static/Montserrat-Italic.ttf"
            elif bold:
                member = "static/Montserrat-SemiBold.ttf"
            else:
                member = "static/Montserrat-Regular.ttf"
            font_path = _extract_font("montserrat_font_family.zip", member)
        return ImageFont.truetype(str(font_path), size=size)
    except Exception:
        return ImageFont.load_default()


FONT_TITLE = _load_font(42, family="exo2", bold=True)
FONT_SUBTITLE = _load_font(22, family="montserrat", bold=False)
FONT_SECTION = _load_font(26, family="exo2", bold=True)
FONT_CARD_TITLE = _load_font(20, family="montserrat", bold=True)
FONT_BODY = _load_font(18, family="montserrat", bold=False)
FONT_BODY_ITALIC = _load_font(18, family="montserrat", italic=True)
FONT_BODY_BOLD = _load_font(18, family="montserrat", bold=True)
FONT_SMALL = _load_font(15, family="montserrat", bold=False)
FONT_TINY = _load_font(13, family="montserrat", bold=False)


def _safe_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _format_study_date(value: Any) -> str:
    raw = str(value or "").strip()
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[6:8]}/{raw[4:6]}/{raw[0:4]}"
    return raw or "-"


def _page() -> tuple[Image.Image, ImageDraw.ImageDraw]:
    image = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
    draw = ImageDraw.Draw(image)
    return image, draw


def _text_height(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[3] - bbox[1]


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return ["-"]

    wrapped: list[str] = []
    for paragraph in raw.splitlines() or [""]:
        words = paragraph.split()
        if not words:
            wrapped.append("")
            continue
        line = words[0]
        for word in words[1:]:
            candidate = f"{line} {word}"
            if _text_width(draw, candidate, font) <= max_width:
                line = candidate
            else:
                wrapped.append(line)
                line = word
        wrapped.append(line)
    return wrapped


def _styled_tokens(text: str) -> list[tuple[str, ImageFont.ImageFont]]:
    raw = str(text or "").strip()
    if not raw:
        return [("-", FONT_BODY)]

    parts = re.split(r"(<i>.*?</i>)", raw)
    tokens: list[tuple[str, ImageFont.ImageFont]] = []
    for part in parts:
        if not part:
            continue
        is_italic = part.startswith("<i>") and part.endswith("</i>")
        content = part[3:-4] if is_italic else part
        font = FONT_BODY_ITALIC if is_italic else FONT_BODY
        for token in re.findall(r"\S+\s*", content):
            tokens.append((token, font))
    return tokens or [("-", FONT_BODY)]


def _wrap_styled_text(draw: ImageDraw.ImageDraw, text: str, max_width: int) -> list[list[tuple[str, ImageFont.ImageFont]]]:
    lines: list[list[tuple[str, ImageFont.ImageFont]]] = []
    current: list[tuple[str, ImageFont.ImageFont]] = []
    current_width = 0

    for token, font in _styled_tokens(text):
        token_width = _text_width(draw, token, font)
        if current and current_width + token_width > max_width:
            lines.append(current)
            current = [(token, font)]
            current_width = token_width
        else:
            current.append((token, font))
            current_width += token_width

    if current:
        lines.append(current)
    return lines or [[("-", FONT_BODY)]]


def _wrapped_block_height(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> int:
    if "<i>" in str(text):
        line_height = _text_height(draw, "Ag", font)
        total = 0
        paragraphs = str(text).splitlines() or [""]
        for paragraph in paragraphs:
            if not paragraph.strip():
                total += line_height + LINE_GAP
                continue
            lines = _wrap_styled_text(draw, paragraph, max_width)
            total += sum(line_height + LINE_GAP for _ in lines)
        return total
    lines = _wrap_text(draw, text, font, max_width)
    if not lines:
        return 0
    line_height = _text_height(draw, "Ag", font)
    return sum(line_height + LINE_GAP for _ in lines)


def _draw_wrapped(
    draw: ImageDraw.ImageDraw,
    text: str,
    x: int,
    y: int,
    font: ImageFont.ImageFont,
    max_width: int,
    *,
    fill: str = TEXT_PRIMARY,
) -> int:
    if "<i>" in str(text):
        line_height = _text_height(draw, "Ag", font)
        paragraphs = str(text).splitlines() or [""]
        for paragraph in paragraphs:
            if not paragraph.strip():
                y += line_height + LINE_GAP
                continue
            for styled_line in _wrap_styled_text(draw, paragraph, max_width):
                cursor_x = x
                for token, token_font in styled_line:
                    draw.text((cursor_x, y), token, font=token_font, fill=fill)
                    cursor_x += _text_width(draw, token, token_font)
                y += line_height + LINE_GAP
        return y

    for line in _wrap_text(draw, text, font, max_width):
        draw.text((x, y), line, font=font, fill=fill)
        y += _text_height(draw, line or "A", font) + LINE_GAP
    return y


def _paste_rgba(base: Image.Image, overlay: Image.Image, x: int, y: int, *, alpha: float = 1.0) -> None:
    image = overlay.convert("RGBA")
    if alpha < 1.0:
        alpha_channel = image.getchannel("A")
        alpha_channel = alpha_channel.point(lambda value: int(value * alpha))
        image.putalpha(alpha_channel)
    base.paste(image, (x, y), image)


def _load_brand_wordmark(max_size: tuple[int, int]) -> Image.Image | None:
    if not BRANDING_WORDMARK.exists():
        return None
    image = Image.open(BRANDING_WORDMARK).convert("RGBA")
    bbox = image.getbbox()
    if bbox:
        image = image.crop(bbox)
    image.thumbnail(max_size)
    return image


def _draw_editorial_header(
    page: Image.Image,
    draw: ImageDraw.ImageDraw,
    *,
    title: str,
    subtitle: str,
    detail: str,
    wordmark_size: tuple[int, int] = (430, 136),
    title_y: int = 204,
    subtitle_y: int = 252,
    detail_y: int = 292,
    line_y: int = 346,
) -> int:
    draw.rectangle((0, 0, PAGE_WIDTH, PAGE_HEIGHT), fill="white")
    draw.rectangle((0, 0, PAGE_WIDTH, 10), fill=BRAND_PRIMARY)
    wordmark = _load_brand_wordmark(wordmark_size)
    if wordmark is not None:
        _paste_rgba(page, wordmark, MARGIN - 24, 44)
    draw.text((MARGIN, title_y), title, font=FONT_SECTION, fill=BRAND_PRIMARY)
    draw.text((MARGIN, subtitle_y), subtitle, font=FONT_SUBTITLE, fill=TEXT_PRIMARY)
    draw.text((MARGIN, detail_y), detail, font=FONT_SUBTITLE, fill=TEXT_MUTED)
    draw.line((MARGIN, line_y, PAGE_WIDTH - MARGIN, line_y), fill="#d6dfde", width=2)
    return line_y + 44


def _draw_footer(draw: ImageDraw.ImageDraw, page_number: int) -> None:
    footer_y = PAGE_HEIGHT - MARGIN + 16
    draw.line((MARGIN, footer_y - 18, PAGE_WIDTH - MARGIN, footer_y - 18), fill="#d7dee7", width=1)
    draw.text(
        (MARGIN, footer_y),
        _t("pdf.footer.page", page_number=page_number),
        font=FONT_TINY,
        fill=TEXT_MUTED,
    )


def _draw_card(draw: ImageDraw.ImageDraw, title: str, rows: list[tuple[str, str]], x: int, y: int, width: int) -> int:
    content_width = width - (2 * CARD_PADDING)
    title_height = _text_height(draw, title, FONT_CARD_TITLE)
    body_height = 0
    for label, value in rows:
        label_text = f"{label}:"
        label_width = _text_width(draw, label_text, FONT_BODY_BOLD)
        value_width = max(120, content_width - label_width - 10)
        value_height = _wrapped_block_height(draw, value, FONT_BODY, value_width)
        label_height = _text_height(draw, label_text, FONT_BODY_BOLD)
        body_height += max(label_height, value_height) + 8

    body_top_gap = 28
    card_height = CARD_PADDING * 2 + title_height + body_top_gap + body_height
    draw.rounded_rectangle(
        (x, y, x + width, y + card_height),
        radius=CARD_RADIUS,
        fill="#f8fafc",
        outline="#d7dee7",
        width=2,
    )
    draw.text((x + CARD_PADDING, y + CARD_PADDING), title, font=FONT_CARD_TITLE, fill=TEXT_PRIMARY)

    cy = y + CARD_PADDING + title_height + body_top_gap
    for label, value in rows:
        label_text = f"{label}:"
        draw.text((x + CARD_PADDING, cy), label_text, font=FONT_BODY_BOLD, fill=TEXT_MUTED)
        label_width = _text_width(draw, label_text, FONT_BODY_BOLD)
        cy = _draw_wrapped(
            draw,
            value,
            x + CARD_PADDING + label_width + 10,
            cy,
            FONT_BODY,
            content_width - label_width - 10,
        )
        cy += 4

    return y + card_height


def _metric_result_paths(case_dir: Path) -> list[Path]:
    metrics_dir = case_dir / "artifacts" / "metrics"
    if not metrics_dir.exists():
        return []
    return sorted(metrics_dir.glob("*/result.json"))


def _load_metric_payloads(case_dir: Path) -> list[dict[str, Any]]:
    payloads = []
    for path in _metric_result_paths(case_dir):
        payload = _safe_json(path)
        if payload.get("status") != "done":
            continue
        payloads.append(payload)
    return payloads


def _format_number(value: Any, digits: int = 1, suffix: str = "") -> str:
    if value in (None, "", []):
        return "-"
    try:
        text = format_decimal(float(value), digits, locale=settings.ARTIFACTS_LOCALE)
    except Exception:
        text = str(value)
    return f"{text} {suffix}".strip()


def _translate_bone_health_classification(value: Any, *, locale: str = "pt_BR") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "-"
    return _t(f"bone.classification.{raw}", locale=locale)


def _normalize_sex(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"F", "FEMALE"}:
        return "F"
    if raw in {"M", "MALE"}:
        return "M"
    return ""


def _classify_l3_smi(smi_value: Any, patient_sex: Any, *, locale: str | None = None) -> str:
    try:
        smi = float(smi_value)
    except (TypeError, ValueError):
        return _t("l3.class_value.indeterminate", locale=locale)
    sex = _normalize_sex(patient_sex)
    if sex == "M":
        return _t("l3.class_value.normal", locale=locale) if smi >= 50.0 else _t("l3.class_value.sarcopenia", locale=locale)
    if sex == "F":
        return _t("l3.class_value.normal", locale=locale) if smi >= 39.0 else _t("l3.class_value.sarcopenia", locale=locale)
    return _t("l3.class_value.indeterminate", locale=locale)


def _classify_l3_density(density_value: Any, *, locale: str | None = None) -> str:
    try:
        density = float(density_value)
    except (TypeError, ValueError):
        return _t("l3.density_value.indeterminate", locale=locale)
    if density > 40.0:
        return _t("l3.density_value.normal", locale=locale)
    if density >= 30.0:
        return _t("l3.density_value.mild", locale=locale)
    return _t("l3.density_value.severe", locale=locale)


def _classify_l3_density_bmi_adjusted(density_value: Any, bmi_value: Any, *, locale: str | None = None) -> str:
    try:
        density = float(density_value)
        bmi = float(bmi_value)
    except (TypeError, ValueError):
        return _t("l3.bmi_density_value.indeterminate", locale=locale)
    cutoff = 41.0 if bmi < 25.0 else 33.0
    return _t("l3.bmi_density_value.abnormal", locale=locale) if density < cutoff else _t("l3.bmi_density_value.normal", locale=locale)


def _build_l3_report_suggestions(
    *,
    smi_classification: str,
    density_classification: str,
    smi_value: Any,
    density_value: Any,
    locale: str | None = None,
) -> str:
    smi = _format_number(smi_value, 1, "cm²/m²")
    density = _format_number(density_value, 0, "HU")
    density_available = density_value not in (None, "")

    if smi_classification == "normal" and (not density_available or density_classification == "normal"):
        return _t("l3.report.normal", locale=locale)

    if smi_classification == "sarcopenia" and (not density_available or density_classification == "normal"):
        return _t("l3.report.sarcopenia_only", locale=locale, smi=smi)

    if smi_classification == "sarcopenia" and density_available and density_classification != "normal":
        return _t("l3.report.sarcopenia_myosteatosis", locale=locale, smi=smi, density=density)

    if smi_classification != "sarcopenia" and density_available and density_classification != "normal":
        return _t("l3.report.myosteatosis_only", locale=locale, density=density)

    return _t("l3.report.neutral", locale=locale)


def _build_l3_module(payload: dict[str, Any]) -> InstructionModule:
    locale = _pdf_locale()
    measurement = payload.get("measurement", {})
    sma = _format_number(measurement.get("skeletal_muscle_area_cm2"), 1, "cm²")
    density_value = measurement.get("skeletal_muscle_density_hu_mean")
    density = _format_number(density_value, 0, "UH")
    smi_value = measurement.get("smi_cm2_m2")
    smi = _format_number(smi_value, 1, "cm²/m²") if smi_value not in (None, "") else "-"
    height = _format_number(measurement.get("height_m"), 2, "m")
    weight = _format_number(measurement.get("weight_kg"), 0, "kg")
    bmi = _format_number(measurement.get("bmi_kg_m2"), 1, "kg/m²")
    patient_sex = payload.get("patient_sex") or payload.get("PatientSex") or measurement.get("patient_sex")
    smi_classification = _classify_l3_smi(smi_value, patient_sex, locale=locale)
    density_classification = _classify_l3_density(measurement.get("skeletal_muscle_density_hu_mean"), locale=locale)
    density_bmi_classification = _classify_l3_density_bmi_adjusted(
        measurement.get("skeletal_muscle_density_hu_mean"),
        measurement.get("bmi_kg_m2"),
        locale=locale,
    )
    rows = [
        (_t("pdf.row.measure", locale=locale), _t("l3.row.measure", locale=locale)),
        (_t("pdf.row.delivery", locale=locale), _t("l3.row.delivery", locale=locale)),
        (_t("pdf.row.reading", locale=locale), _t("l3.row.reading", locale=locale)),
        (
            _t("pdf.row.result", locale=locale),
            _t("l3.result.sma", locale=locale, value=sma)
            + (_t("l3.result.density", locale=locale, value=density) if density_value not in (None, "") else "")
            + (_t("l3.result.height", locale=locale, value=height) if height != "-" else "")
            + (_t("l3.result.weight", locale=locale, value=weight) if weight != "-" else "")
            + (_t("l3.result.smi", locale=locale, value=smi) if smi != "-" else "")
            + (_t("l3.result.bmi", locale=locale, value=bmi) if bmi != "-" else ""),
        ),
        (
            _t("pdf.row.classification", locale=locale),
            _t("l3.classification.smi", locale=locale, value=smi_classification)
            + (
                _t("l3.classification.density", locale=locale, value=density_classification)
                if density_value not in (None, "")
                else ""
            )
            + (
                _t("l3.classification.bmi_adjusted", locale=locale, value=density_bmi_classification)
                if bmi != "-" and density_value not in (None, "")
                else ""
            ),
        ),
        (
            _t("pdf.row.practical_ranges", locale=locale),
            _t("l3.row.practical_ranges_smi", locale=locale)
            + (
                " " + _t("l3.row.practical_ranges_density", locale=locale)
                if density_value not in (None, "")
                else ""
            ),
        ),
        (
            _t("l3.row.bmi_adjustment.label", locale=locale),
            (
                _t("l3.row.bmi_adjustment.value", locale=locale)
                if density_value not in (None, "")
                else _t("l3.row.bmi_adjustment.not_applicable", locale=locale)
            ),
        ),
        (
            _t("l3.row.report_suggestions", locale=locale),
            _build_l3_report_suggestions(
                smi_classification=smi_classification,
                density_classification=density_classification,
                smi_value=smi_value,
                density_value=density_value,
                locale=locale,
            ),
        ),
        (_t("pdf.row.cautions", locale=locale), _t("l3.row.cautions", locale=locale)),
        (
            _t("pdf.row.references_plural", locale=locale),
            _t("l3.references", locale=locale),
        ),
    ]
    return InstructionModule("l3_muscle_area", _t("module.l3.title", locale=locale), rows)


def _build_l1_module(payload: dict[str, Any]) -> InstructionModule:
    locale = _pdf_locale()
    measurement = payload.get("measurement", {})
    hu_mean = _format_number(measurement.get("l1_trabecular_hu_mean"), 0, "HU")
    hu_value = measurement.get("l1_trabecular_hu_mean")
    classification = _translate_bone_health_classification(
        measurement.get("classification", "-"),
        locale=locale,
    )
    report_suggestion = _t("bone.report.placeholder", locale=locale)
    try:
        numeric_hu = float(hu_value)
    except (TypeError, ValueError):
        numeric_hu = None

    if numeric_hu is not None:
        hu_report = _format_number(numeric_hu, 0, "UH")
        if numeric_hu > 160:
            report_suggestion = _t("bone.report.normal", locale=locale, value=hu_report)
        elif numeric_hu >= 110:
            report_suggestion = _t("bone.report.osteopenia", locale=locale, value=hu_report)
        elif numeric_hu >= 100:
            report_suggestion = _t("bone.report.osteoporosis_suggestive", locale=locale, value=hu_report)
        elif numeric_hu >= 91:
            report_suggestion = _t("bone.report.osteoporosis_marked", locale=locale, value=hu_report)
        else:
            report_suggestion = _t("bone.report.osteoporosis_fragility", locale=locale, value=hu_report)

    rows = [
        (_t("pdf.row.measure", locale=locale), _t("bone.row.measure", locale=locale)),
        (_t("pdf.row.delivery", locale=locale), _t("bone.row.delivery", locale=locale)),
        (_t("pdf.row.reading", locale=locale), _t("bone.row.reading", locale=locale)),
        (
            _t("pdf.row.result", locale=locale),
            _t("bone.row.result", locale=locale, hu_mean=hu_mean, classification=classification),
        ),
        (
            _t("bone.row.report_suggestion", locale=locale),
            report_suggestion,
        ),
        (
            _t("pdf.row.cautions", locale=locale),
            _t("bone.row.cautions", locale=locale),
        ),
        (
            _t("pdf.row.reference", locale=locale),
            _t("bone.references", locale=locale),
        ),
    ]
    return InstructionModule("bone_health_l1_hu", _t("module.bone.title", locale=locale), rows)


def _format_parenchymal_organs_summary(organs: dict[str, Any], *, locale: str | None = None) -> str:
    order = [
        ("liver", _t("parenchymal.organ.liver", locale=locale)),
        ("spleen", _t("parenchymal.organ.spleen", locale=locale)),
        ("pancreas", _t("parenchymal.organ.pancreas", locale=locale)),
        ("kidney_right", _t("parenchymal.organ.kidney_right", locale=locale)),
        ("kidney_left", _t("parenchymal.organ.kidney_left", locale=locale)),
    ]
    lines: list[str] = []
    for key, label in order:
        organ = organs.get(key) or {}
        if not organ:
            continue
        status = str(organ.get("analysis_status", "") or "")
        if status != "complete":
            lines.append(_t("parenchymal.organs.incomplete", locale=locale, label=label))
            continue
        volume = _format_number(organ.get("volume_cm3"), 0, "cm³")
        attenuation = _format_number(organ.get("hu_mean"), 0, "UH")
        if organ.get("hu_mean") in (None, ""):
            lines.append(_t("parenchymal.organs.volume_only", locale=locale, label=label, volume=volume))
        else:
            lines.append(_t("parenchymal.organs.volume_density", locale=locale, label=label, volume=volume, attenuation=attenuation))
    return "\n".join(lines) if lines else _t("parenchymal.organs.none", locale=locale)


def _parenchymal_steatosis_summary(measurement: dict[str, Any], *, locale: str | None = None) -> str:
    if measurement.get("density_suppressed_due_to_contrast"):
        return _t("parenchymal.steatosis.not_applicable", locale=locale)

    organs = measurement.get("organs", {}) if isinstance(measurement.get("organs"), dict) else {}
    liver = organs.get("liver") or {}
    spleen = organs.get("spleen") or {}
    liver_hu = liver.get("hu_mean")
    spleen_hu = spleen.get("hu_mean")
    try:
        liver_hu_value = float(liver_hu)
        spleen_hu_value = float(spleen_hu)
    except (TypeError, ValueError):
        return _t("parenchymal.steatosis.requires_unenhanced", locale=locale)

    l_s = liver_hu_value - spleen_hu_value
    l_s_ratio = liver_hu_value / spleen_hu_value if spleen_hu_value else None
    pdff = max(0.0, 51.0 - (0.65 * liver_hu_value))

    if liver_hu_value < 40.0 or l_s <= -10.0:
        synthesis = _t("parenchymal.steatosis.summary.moderate_severe", locale=locale)
    elif liver_hu_value <= 50.0 or l_s <= 5.0:
        synthesis = _t("parenchymal.steatosis.summary.mild_borderline", locale=locale)
    else:
        synthesis = _t("parenchymal.steatosis.summary.normal", locale=locale)

    base = _t(
        "parenchymal.steatosis.summary.base",
        locale=locale,
        liver=_format_number(liver_hu_value, 0, "UH"),
        spleen=_format_number(spleen_hu_value, 0, "UH"),
        l_s=_format_number(l_s, 1, "UH"),
        pdff=_format_number(pdff, 1, "%"),
        synthesis=synthesis,
    )
    if l_s_ratio is None:
        return base
    return _t(
        "parenchymal.steatosis.summary.with_ratio",
        locale=locale,
        base=base,
        ratio=_format_number(l_s_ratio, 2, ""),
    )


def _parenchymal_practical_ranges(measurement: dict[str, Any], *, locale: str | None = None) -> str:
    if measurement.get("density_suppressed_due_to_contrast"):
        return _t("parenchymal.practical_ranges.contrast", locale=locale)
    return _t("parenchymal.practical_ranges.unenhanced", locale=locale)


def _build_parenchymal_module(payload: dict[str, Any]) -> InstructionModule:
    locale = _pdf_locale()
    measurement = payload.get("measurement", {})
    organs = measurement.get("organs", {}) if isinstance(measurement.get("organs"), dict) else {}
    exported_slice_count = measurement.get("exported_slice_count")
    target_slice_thickness = measurement.get("target_slice_thickness_mm")
    rows = [
        (
            _t("pdf.row.measure", locale=locale),
            _t("parenchymal.row.measure", locale=locale),
        ),
        (
            _t("pdf.row.delivery", locale=locale),
            _t("parenchymal.row.delivery", locale=locale, slice_count=exported_slice_count or "-")
            if exported_slice_count is not None
            else _t("parenchymal.row.delivery_fallback", locale=locale),
        ),
        (
            _t("pdf.row.reading", locale=locale),
            _t("parenchymal.row.reading", locale=locale),
        ),
        (
            _t("pdf.row.result", locale=locale),
            _t(
                "parenchymal.row.result",
                locale=locale,
                target_slice_thickness=_format_number(target_slice_thickness, 1, "mm"),
                slice_count=exported_slice_count or "-",
            ),
        ),
        (
            _t("parenchymal.row.steatosis", locale=locale),
            _parenchymal_steatosis_summary(measurement, locale=locale),
        ),
        (
            _t("pdf.row.practical_ranges", locale=locale),
            _parenchymal_practical_ranges(measurement, locale=locale),
        ),
        (
            _t("parenchymal.row.organs", locale=locale),
            _format_parenchymal_organs_summary(organs, locale=locale),
        ),
        (
            _t("parenchymal.row.expected_values", locale=locale),
            _t("parenchymal.row.expected_values.value", locale=locale),
        ),
        (
            _t("pdf.row.cautions", locale=locale),
            _t("parenchymal.row.cautions", locale=locale),
        ),
        (
            _t("pdf.row.reference", locale=locale),
            _t("parenchymal.references", locale=locale),
        ),
    ]
    return InstructionModule("parenchymal_organ_volumetry", _t("module.parenchymal.title", locale=locale), rows)


MODULE_BUILDERS = {
    "l3_muscle_area": _build_l3_module,
    "bone_health_l1_hu": _build_l1_module,
    "parenchymal_organ_volumetry": _build_parenchymal_module,
}


def _collect_modules(case_dir: Path) -> list[InstructionModule]:
    modules: list[InstructionModule] = []
    for payload in _load_metric_payloads(case_dir):
        key = str(payload.get("metric_key", "") or "")
        builder = MODULE_BUILDERS.get(key)
        if builder is None:
            continue
        modules.append(builder(payload))
    return modules


def _render_cover(case_id: str, metadata: dict[str, Any], modules: list[InstructionModule]) -> Image.Image:
    locale = _pdf_locale()
    page, draw = _page()
    patient_name = normalize_patient_name_display(str(metadata.get("PatientName", "") or ""), settings.PATIENT_NAME_PROFILE)
    accession_number = str(metadata.get("AccessionNumber", "") or "-")
    study_date = _format_study_date(metadata.get("StudyDate", ""))
    subtitle = _t("pdf.subtitle", locale=locale, accession_number=accession_number, study_date=study_date)
    detail = _t("pdf.patient_line", locale=locale, patient_name=patient_name or "-")
    y = _draw_editorial_header(
        page,
        draw,
        title=_t("pdf.cover.title", locale=locale),
        subtitle=subtitle,
        detail=detail,
    )
    draw.text((MARGIN, y), _t("pdf.cover.subtitle", locale=locale), font=FONT_SECTION, fill=TEXT_PRIMARY)
    y += _text_height(draw, "Ag", FONT_SECTION) + 18
    y = _draw_wrapped(
        draw,
        _t("pdf.cover.body", locale=locale),
        MARGIN,
        y,
        FONT_BODY,
        PAGE_WIDTH - (2 * MARGIN),
    )
    y += 18

    y = _draw_card(
        draw,
        _t("pdf.cover.case_summary", locale=locale),
        [
            (_t("pdf.cover.patient", locale=locale), patient_name or "-"),
            (_t("pdf.cover.accession", locale=locale), accession_number),
            (_t("pdf.cover.case_id", locale=locale), case_id),
            (_t("pdf.cover.modules", locale=locale), ", ".join(module.title for module in modules) or "-"),
        ],
        MARGIN,
        y,
        PAGE_WIDTH - (2 * MARGIN),
    )
    y += CARD_GAP

    y = _draw_card(
        draw,
        _t("pdf.cover.required_reading", locale=locale),
        [
            (_t("pdf.cover.nature", locale=locale), _t("pdf.cover.nature.value", locale=locale)),
            (_t("pdf.cover.orientation", locale=locale), _t("pdf.cover.orientation.value", locale=locale)),
            (_t("pdf.cover.limits", locale=locale), _t("pdf.cover.limits.value", locale=locale)),
        ],
        MARGIN,
        y,
        PAGE_WIDTH - (2 * MARGIN),
    )
    _draw_footer(draw, 1)
    return page


def _render_module_page(
    case_id: str,
    subtitle: str,
    patient_name: str,
    module: InstructionModule,
    page_number: int,
) -> Image.Image:
    locale = _pdf_locale()
    page, draw = _page()
    y = _draw_editorial_header(
        page,
        draw,
        title=module.title,
        subtitle=subtitle,
        detail=_t("pdf.patient_line", locale=locale, patient_name=patient_name or "-"),
        wordmark_size=(360, 114),
        title_y=196,
        subtitle_y=244,
        detail_y=284,
        line_y=338,
    )
    _draw_card(draw, _t("pdf.card.instructions", locale=locale), module.rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))
    _draw_footer(draw, page_number)
    return page


def build_artifact_instructions_pdf(case_id: str, output_path: Path | None = None) -> Path:
    locale = _pdf_locale()
    case_folder = study_dir(case_id)
    metadata = _safe_json(study_id_json(case_id))
    modules = _collect_modules(case_folder)
    if not modules:
        raise RuntimeError(f"No completed metric modules found for {case_id}")

    if output_path is None:
        output_path = study_artifacts_dir(case_id) / "metrics" / "instructions" / "artifact_instructions.pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    accession_number = str(metadata.get("AccessionNumber", "") or "-")
    study_date = _format_study_date(metadata.get("StudyDate", ""))
    patient_name = normalize_patient_name_display(str(metadata.get("PatientName", "") or ""), settings.PATIENT_NAME_PROFILE)
    subtitle = _t("pdf.subtitle", locale=locale, accession_number=accession_number, study_date=study_date)

    pages = [_render_cover(case_id, metadata, modules)]
    for index, module in enumerate(modules, start=2):
        pages.append(_render_module_page(case_id, subtitle, patient_name, module, index))

    pages[0].save(
        output_path,
        "PDF",
        resolution=150.0,
        save_all=True,
        append_images=pages[1:],
    )
    return output_path


def main() -> int:
    args = parse_args()
    output_path = Path(args.output).expanduser() if args.output else None
    path = build_artifact_instructions_pdf(args.case_id, output_path=output_path)
    print(str(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
