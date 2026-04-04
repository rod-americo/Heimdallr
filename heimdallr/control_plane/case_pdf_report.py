#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from heimdallr.shared import settings
from heimdallr.shared.patient_names import normalize_patient_name_display


PAGE_WIDTH = 1240
PAGE_HEIGHT = 1754
MARGIN = 72
SECTION_GAP = 26
LINE_GAP = 8
CARD_RADIUS = 18
CARD_PADDING = 18


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = []
    if bold:
        candidates.extend([
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        ])
    else:
        candidates.extend([
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ])

    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size)

    return ImageFont.load_default()


FONT_TITLE = _load_font(34, bold=True)
FONT_HEADER = _load_font(26, bold=True)
FONT_SUBTITLE = _load_font(22, bold=True)
FONT_CARD_TITLE = _load_font(19, bold=True)
FONT_BODY = _load_font(18)
FONT_BODY_BOLD = _load_font(18, bold=True)
FONT_SMALL = _load_font(15)
FONT_TINY = _load_font(13)


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
    raw = "" if text is None else str(text)
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


def _draw_wrapped(draw: ImageDraw.ImageDraw, text: str, x: int, y: int, font: ImageFont.ImageFont, max_width: int, fill: str = "black") -> int:
    for line in _wrap_text(draw, text, font, max_width):
        draw.text((x, y), line, font=font, fill=fill)
        y += _text_height(draw, line or "A", font) + LINE_GAP
    return y


def _draw_section(draw: ImageDraw.ImageDraw, title: str, rows: list[tuple[str, str]], x: int, y: int, width: int) -> int:
    draw.text((x, y), title, font=FONT_SUBTITLE, fill="black")
    y += _text_height(draw, title, FONT_SUBTITLE) + 12

    for label, value in rows:
        label_text = f"{label}:"
        draw.text((x, y), label_text, font=FONT_BODY_BOLD, fill="black")
        label_width = _text_width(draw, label_text, FONT_BODY_BOLD)
        y = _draw_wrapped(draw, value, x + label_width + 10, y, FONT_BODY, width - label_width - 10)
        y += 4

    return y + SECTION_GAP


def _draw_card(draw: ImageDraw.ImageDraw, title: str, rows: list[tuple[str, str]], x: int, y: int, width: int, fill: str = "#f8fafc", outline: str = "#d7dee7") -> int:
    content_width = width - (2 * CARD_PADDING)
    body_height = 0
    for label, value in rows:
        label_text = f"{label}:"
        label_width = _text_width(draw, label_text, FONT_SMALL)
        wrapped = _wrap_text(draw, value, FONT_SMALL, content_width - label_width - 8)
        line_height = _text_height(draw, "Ag", FONT_SMALL)
        body_height += max(line_height, len(wrapped) * (line_height + 4)) + 6

    title_height = _text_height(draw, title, FONT_CARD_TITLE)
    card_height = CARD_PADDING * 2 + title_height + 10 + body_height
    draw.rounded_rectangle((x, y, x + width, y + card_height), radius=CARD_RADIUS, fill=fill, outline=outline, width=2)
    draw.text((x + CARD_PADDING, y + CARD_PADDING), title, font=FONT_CARD_TITLE, fill="#0f172a")

    cy = y + CARD_PADDING + title_height + 10
    for label, value in rows:
        label_text = f"{label}:"
        draw.text((x + CARD_PADDING, cy), label_text, font=FONT_SMALL, fill="#334155")
        label_width = _text_width(draw, label_text, FONT_SMALL)
        cy = _draw_wrapped(draw, value, x + CARD_PADDING + label_width + 8, cy, FONT_SMALL, content_width - label_width - 8, fill="#111827")
        cy += 2

    return y + card_height


def _draw_footer(draw: ImageDraw.ImageDraw, page_number: int) -> None:
    footer = f"Heimdallr case report  |  Page {page_number}"
    y = PAGE_HEIGHT - MARGIN + 14
    draw.line((MARGIN, y - 14, PAGE_WIDTH - MARGIN, y - 14), fill="#d7dee7", width=1)
    draw.text((MARGIN, y), footer, font=FONT_TINY, fill="#64748b")


def _fmt_number(value, unit: str | None = None, digits: int = 1) -> str:
    if value is None:
        return "-"
    try:
        rendered = f"{float(value):.{digits}f}"
    except Exception:
        return str(value)
    return f"{rendered} {unit}".strip() if unit else rendered


def _fmt_bool(value) -> str:
    if value is None:
        return "-"
    return "Yes" if value else "No"


def _fmt_components(value) -> str:
    if value is None:
        return "-"
    return f"{int(value)}"


def _safe_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r") as handle:
        return json.load(handle)


def _collect_report_images(case_folder: Path, results: dict, triage_report: dict) -> list[tuple[str, Path]]:
    images: list[tuple[str, Path]] = []
    seen: set[Path] = set()

    preferred = ["L3_overlay.png", "L1_BMD_overlay.png"]
    for filename in preferred:
        path = case_folder / filename
        if path.exists():
            images.append((filename.replace("_", " ").replace(".png", ""), path))
            seen.add(path.resolve())

    for filename in results.get("images", []):
        path = case_folder / filename
        resolved = path.resolve() if path.exists() else path
        if filename in preferred or not path.exists() or resolved in seen:
            continue
        images.append((filename.replace("_", " ").replace(".png", ""), path))
        seen.add(resolved)
        if len(images) >= 6:
            break

    for kidney in triage_report.get("kidneys", []):
        side = "Right kidney" if kidney.get("mask_name") == "kidney_right" else "Left kidney"
        for component in (kidney.get("components") or [])[:2]:
            for plane_key, plane_name in [("axial_overlay_png", "axial"), ("coronal_overlay_png", "coronal")]:
                raw_path = component.get(plane_key)
                if not raw_path:
                    continue
                path = Path(raw_path)
                if not path.is_absolute():
                    path = case_folder / path
                if not path.exists():
                    continue
                resolved = path.resolve()
                if resolved in seen:
                    continue
                label = f"{side} {component.get('component_id', '').replace('_', ' ')} {plane_name}"
                images.append((label, path))
                seen.add(resolved)
                if len(images) >= 10:
                    return images

    return images


def _render_image_page(title: str, items: list[tuple[str, Path]]) -> Image.Image:
    page, draw = _page()
    draw.text((MARGIN, MARGIN), title, font=FONT_TITLE, fill="black")
    y = MARGIN + _text_height(draw, title, FONT_TITLE) + 30

    cell_gap = 24
    cols = 2
    cell_width = (PAGE_WIDTH - (2 * MARGIN) - cell_gap) // cols
    cell_height = 600

    for index, (label, path) in enumerate(items[:4]):
        row = index // cols
        col = index % cols
        x = MARGIN + col * (cell_width + cell_gap)
        top = y + row * (cell_height + 90)
        draw.rounded_rectangle((x, top, x + cell_width, top + cell_height), radius=18, outline="#d0d7de", width=2)

        try:
            image = Image.open(path).convert("RGB")
            image.thumbnail((cell_width - 28, cell_height - 28))
            paste_x = x + ((cell_width - image.width) // 2)
            paste_y = top + ((cell_height - image.height) // 2)
            page.paste(image, (paste_x, paste_y))
        except Exception:
            draw.text((x + 20, top + 20), "Image unavailable", font=FONT_BODY, fill="#a00")

        caption_y = top + cell_height + 12
        _draw_wrapped(draw, label, x, caption_y, FONT_SMALL, cell_width)

    return page


def build_case_report(case_folder: Path, output_path: Path | None = None) -> Path:
    case_folder = Path(case_folder)
    if output_path is None:
        output_path = case_folder / "metadata" / "report.pdf"

    metadata = _safe_json(case_folder / "metadata" / "id.json")
    results = _safe_json(case_folder / "metadata" / "resultados.json")
    triage_report = _safe_json(case_folder / "artifacts" / "urology" / "kidney_stone_triage.json")
    pipeline = metadata.get("Pipeline", {})
    selected_series = metadata.get("SelectedSeries", {})
    selected_phase = selected_series.get("ContrastPhaseData", {}).get("phase", "-")

    pages: list[Image.Image] = []

    overview_page, draw = _page()
    title = "Heimdallr Case Report"
    draw.text((MARGIN, MARGIN), title, font=FONT_TITLE, fill="black")
    subtitle = metadata.get("CaseID", case_folder.name)
    draw.text((MARGIN, MARGIN + 44), subtitle, font=FONT_HEADER, fill="#334155")

    summary_top = MARGIN + 94
    summary_height = 118
    draw.rounded_rectangle(
        (MARGIN, summary_top, PAGE_WIDTH - MARGIN, summary_top + summary_height),
        radius=22,
        fill="#eef6ff",
        outline="#bfdbfe",
        width=2,
    )

    summary_items = [
        (
            "Patient",
            normalize_patient_name_display(
                metadata.get("PatientName", "-"),
                settings.PATIENT_NAME_PROFILE,
            ),
        ),
        ("Study", metadata.get("StudyDate", "-")),
        ("Accession", metadata.get("AccessionNumber", "-")),
        ("Modality", metadata.get("Modality", "-")),
        ("Series", str(selected_series.get("SeriesNumber", "-"))),
        ("Phase", str(selected_phase)),
        ("T Prep", pipeline.get("prepare_elapsed_time", "-")),
        ("T Seg", pipeline.get("segmentation_elapsed_time") or pipeline.get("elapsed_time", "-")),
    ]
    summary_cols = 4
    summary_cell = (PAGE_WIDTH - 2 * MARGIN - 24) // summary_cols
    for idx, (label, value) in enumerate(summary_items):
        row = idx // summary_cols
        col = idx % summary_cols
        sx = MARGIN + 12 + col * summary_cell
        sy = summary_top + 14 + row * 46
        draw.text((sx, sy), label, font=FONT_TINY, fill="#475569")
        draw.text((sx, sy + 16), str(value), font=FONT_BODY_BOLD, fill="#0f172a")

    alerts = []
    if (results.get("hemorrhage_vol_cm3") or 0) > 0.1:
        alerts.append(f"Hemorrhage { _fmt_number(results.get('hemorrhage_vol_cm3'), 'cm3') }")
    if results.get("L1_bmd_classification") not in (None, "-", "Normal"):
        alerts.append(f"L1 BMD {results.get('L1_bmd_classification')}")
    if (results.get("renal_stone_count") or 0) > 0:
        alerts.append(f"Segmented stones {results.get('renal_stone_count')}")
    if (results.get("kidney_stone_triage_total_components") or 0) > 0:
        alerts.append(f"HU triage components {results.get('kidney_stone_triage_total_components')}")
    alert_text = " | ".join(alerts) if alerts else "No major automated alert flagged by current modules."
    alert_y = summary_top + summary_height + 18
    draw.text((MARGIN, alert_y), "Decision Summary", font=FONT_SUBTITLE, fill="#0f172a")
    _draw_wrapped(draw, alert_text, MARGIN + 210, alert_y + 2, FONT_BODY, PAGE_WIDTH - (2 * MARGIN) - 210, fill="#991b1b" if alerts else "#166534")

    col_gap = 24
    col_width = (PAGE_WIDTH - 2 * MARGIN - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_width + col_gap
    top_y = alert_y + 44

    left_y = top_y
    right_y = top_y

    left_y = _draw_card(draw, "Overview", [
        ("Sex", metadata.get("PatientSex", "-")),
        ("Body regions", ", ".join(results.get("body_regions", [])) or "-"),
        ("Selected phase", str(selected_phase)),
        ("Selected series", str(selected_series.get("SeriesNumber", "-"))),
    ], left_x, left_y, col_width)
    left_y += 16
    left_y = _draw_card(draw, "Musculoskeletal", [
        ("Weight", _fmt_number(metadata.get("Weight"), "kg")),
        ("Height", _fmt_number(metadata.get("Height"), "m", 2)),
        ("SMA", _fmt_number(results.get("SMA_cm2"), "cm2", 2)),
        ("Muscle density", _fmt_number(results.get("muscle_HU_mean"), "HU")),
        ("L1 trabecular HU", _fmt_number(results.get("L1_trabecular_HU_mean"), "HU")),
        ("L1 BMD class", str(results.get("L1_bmd_classification", "-"))),
    ], left_x, left_y, col_width)
    left_y += 16
    left_y = _draw_card(draw, "Liver and Organs", [
        ("Liver volume", _fmt_number(results.get("liver_vol_cm3"), "cm3")),
        ("Liver mean HU", _fmt_number(results.get("liver_hu_mean"), "HU")),
        ("Estimated PDFF", _fmt_number(results.get("liver_pdff_percent"), "%")),
        ("Spleen volume", _fmt_number(results.get("spleen_vol_cm3"), "cm3")),
        ("Right kidney volume", _fmt_number(results.get("kidney_right_vol_cm3"), "cm3")),
        ("Left kidney volume", _fmt_number(results.get("kidney_left_vol_cm3"), "cm3")),
    ], left_x, left_y, col_width)

    right_y = _draw_card(draw, "Thoracic and Neuro", [
        ("Lung analysis", str(results.get("lung_analysis_status", "-"))),
        ("Total emphysema", _fmt_number(results.get("total_lung_emphysema_percent"), "%")),
        ("Emphysema burden", _fmt_number(results.get("total_lung_emphysema_vol_cm3"), "cm3")),
        ("Hemorrhage status", str(results.get("hemorrhage_analysis_status", "-"))),
        ("Hemorrhage volume", _fmt_number(results.get("hemorrhage_vol_cm3"), "cm3")),
    ], right_x, right_y, col_width)
    right_y += 16
    right_y = _draw_card(draw, "Segmented Stone Burden", [
        ("Status", str(results.get("renal_stone_analysis_status", "-"))),
        ("Stone count", str(results.get("renal_stone_count", "-"))),
        ("Total volume", _fmt_number(results.get("renal_stone_total_volume_mm3"), "mm3")),
        ("Largest axis", _fmt_number(results.get("renal_stone_largest_diameter_mm"), "mm")),
        ("Right kidney complete", _fmt_bool(results.get("renal_stone_kidney_right_complete"))),
        ("Left kidney complete", _fmt_bool(results.get("renal_stone_kidney_left_complete"))),
    ], right_x, right_y, col_width)
    right_y += 16
    right_y = _draw_card(draw, "HU Stone Triage", [
        ("Right components", _fmt_components(results.get("kidney_stone_triage_right_components"))),
        ("Right burden", _fmt_number(results.get("kidney_stone_triage_right_volume_mm3"), "mm3")),
        ("Right max axis", _fmt_number(results.get("kidney_stone_triage_right_largest_axis_mm"), "mm")),
        ("Left components", _fmt_components(results.get("kidney_stone_triage_left_components"))),
        ("Left burden", _fmt_number(results.get("kidney_stone_triage_left_volume_mm3"), "mm3")),
        ("Left max axis", _fmt_number(results.get("kidney_stone_triage_left_largest_axis_mm"), "mm")),
    ], right_x, right_y, col_width)

    bottom_y = max(left_y, right_y) + 20
    _draw_card(draw, "Technical Timing", [
        ("Prepare", pipeline.get("prepare_elapsed_time", "-")),
        ("Segmentation", pipeline.get("segmentation_elapsed_time") or pipeline.get("elapsed_time", "-")),
        ("Prepare stats", json.dumps(pipeline.get("prepare_stats", {}), ensure_ascii=True)),
        ("Prepare stage timings", json.dumps(pipeline.get("prepare_stage_timings_seconds", {}), ensure_ascii=True)),
    ], MARGIN, bottom_y, PAGE_WIDTH - 2 * MARGIN, fill="#f8fafc")
    _draw_footer(draw, 1)
    pages.append(overview_page)

    findings_page, draw = _page()
    draw.text((MARGIN, MARGIN), "Findings Summary", font=FONT_TITLE, fill="black")
    y = MARGIN + _text_height(draw, "Findings Summary", FONT_TITLE) + 24

    lung_rows = [
        ("Lung analysis status", str(results.get("lung_analysis_status", "-"))),
        ("Total emphysema", _fmt_number(results.get("total_lung_emphysema_percent"), "%")),
        ("Emphysema burden", _fmt_number(results.get("total_lung_emphysema_vol_cm3"), "cm3")),
        ("Hemorrhage status", str(results.get("hemorrhage_analysis_status", "-"))),
        ("Hemorrhage volume", _fmt_number(results.get("hemorrhage_vol_cm3"), "cm3")),
    ]
    y = _draw_section(draw, "Thoracic and Neuro", lung_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    stone_rows = [
        ("Stone burden status", str(results.get("renal_stone_analysis_status", "-"))),
        ("Stone count", str(results.get("renal_stone_count", "-"))),
        ("Stone total volume", _fmt_number(results.get("renal_stone_total_volume_mm3"), "mm3")),
        ("Largest stone axis", _fmt_number(results.get("renal_stone_largest_diameter_mm"), "mm")),
        ("Right kidney complete", _fmt_bool(results.get("renal_stone_kidney_right_complete"))),
        ("Left kidney complete", _fmt_bool(results.get("renal_stone_kidney_left_complete"))),
    ]
    y = _draw_section(draw, "Segmented Stone Burden", stone_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    triage_rows = [
        ("Triage status", str(results.get("kidney_stone_triage_status", "-"))),
        ("Right components", str(results.get("kidney_stone_triage_right_components", "-"))),
        ("Left components", str(results.get("kidney_stone_triage_left_components", "-"))),
        ("Right heuristic burden", _fmt_number(results.get("kidney_stone_triage_right_volume_mm3"), "mm3")),
        ("Left heuristic burden", _fmt_number(results.get("kidney_stone_triage_left_volume_mm3"), "mm3")),
        ("Largest component axis", _fmt_number(results.get("kidney_stone_triage_max_component_axis_mm"), "mm")),
    ]
    y = _draw_section(draw, "Kidney Stone HU Triage", triage_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    _draw_footer(draw, 2)
    pages.append(findings_page)

    image_items = _collect_report_images(case_folder, results, triage_report)
    for page_index in range(0, len(image_items), 4):
        page = _render_image_page("Selected Overlays", image_items[page_index:page_index + 4])
        _draw_footer(ImageDraw.Draw(page), len(pages) + 1)
        pages.append(page)

    appendix_page, draw = _page()
    draw.text((MARGIN, MARGIN), "Technical Appendix", font=FONT_TITLE, fill="black")
    y = MARGIN + _text_height(draw, "Technical Appendix", FONT_TITLE) + 24
    appendix_rows = [
        ("Prepare stats", json.dumps(pipeline.get("prepare_stats", {}), ensure_ascii=True)),
        ("Selected series payload", json.dumps(selected_series, ensure_ascii=True)),
        ("Triage report path", str(results.get("kidney_stone_triage_report_path", "-"))),
        ("Available images", ", ".join(results.get("images", [])) or "-"),
    ]
    _draw_section(draw, "Structured Data", appendix_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))
    _draw_footer(draw, len(pages) + 1)
    pages.append(appendix_page)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    first, *rest = pages
    first.save(output_path, "PDF", resolution=150.0, save_all=True, append_images=rest)
    return output_path
