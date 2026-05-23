#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from heimdallr.metrics.analysis.hepatic_steatosis import estimate_pdff_from_unenhanced_ct_hu
from heimdallr.shared import settings
from heimdallr.shared.i18n import normalize_locale
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

PT_BR_LABELS = {
    "Heimdallr Case Report": "Relatório do Caso Heimdallr",
    "Page": "Página",
    "Patient": "Paciente",
    "Study": "Estudo",
    "Accession": "AN",
    "Modality": "Modalidade",
    "Series": "Série",
    "Phase": "Fase",
    "T Prep": "T preparo",
    "T Seg": "T segmentação",
    "Hemorrhage": "Hemorragia",
    "Segmented stones": "Cálculos segmentados",
    "HU triage components": "Componentes da triagem UH",
    "No major automated alert flagged by current modules.": "Nenhum alerta automatizado maior sinalizado pelos módulos atuais.",
    "Decision Summary": "Resumo de Decisão",
    "Overview": "Visão Geral",
    "Sex": "Sexo",
    "Body regions": "Regiões corporais",
    "Selected phase": "Fase selecionada",
    "Selected series": "Série selecionada",
    "Musculoskeletal": "Musculoesquelético",
    "Weight": "Peso",
    "Height": "Altura",
    "Muscle density": "Densidade muscular",
    "L1 trabecular HU": "UH trabecular L1",
    "L1 BMD class": "Classe BMD L1",
    "Liver and Organs": "Fígado e Órgãos",
    "Liver volume": "Volume hepático",
    "Liver mean HU": "UH média hepática",
    "Estimated PDFF": "PDFF estimado",
    "Spleen volume": "Volume esplênico",
    "Right kidney volume": "Volume do rim direito",
    "Left kidney volume": "Volume do rim esquerdo",
    "Thoracic and Neuro": "Torácico e Neuro",
    "Lung analysis": "Análise pulmonar",
    "Total emphysema": "Enfisema total",
    "Emphysema burden": "Carga de enfisema",
    "Hemorrhage status": "Status de hemorragia",
    "Hemorrhage volume": "Volume de hemorragia",
    "Segmented Stone Burden": "Carga de Cálculos Segmentados",
    "Status": "Status",
    "Stone count": "Contagem de cálculos",
    "Total volume": "Volume total",
    "Largest axis": "Maior eixo",
    "Right kidney complete": "Rim direito completo",
    "Left kidney complete": "Rim esquerdo completo",
    "HU Stone Triage": "Triagem de Cálculos por UH",
    "Right components": "Componentes direitos",
    "Right burden": "Carga direita",
    "Right max axis": "Maior eixo direito",
    "Left components": "Componentes esquerdos",
    "Left burden": "Carga esquerda",
    "Left max axis": "Maior eixo esquerdo",
    "Technical Timing": "Tempos Técnicos",
    "Prepare": "Preparo",
    "Segmentation": "Segmentação",
    "Prepare stats": "Estatísticas de preparo",
    "Prepare stage timings": "Tempos das etapas de preparo",
    "Findings Summary": "Resumo dos Achados",
    "Lung analysis status": "Status da análise pulmonar",
    "Stone burden status": "Status da carga de cálculos",
    "Stone total volume": "Volume total dos cálculos",
    "Largest stone axis": "Maior eixo do cálculo",
    "Triage status": "Status da triagem",
    "Right heuristic burden": "Carga heurística direita",
    "Left heuristic burden": "Carga heurística esquerda",
    "Largest component axis": "Maior eixo do componente",
    "Kidney Stone HU Triage": "Triagem de Cálculos Renais por UH",
    "Selected Overlays": "Overlays Selecionados",
    "Technical Appendix": "Apêndice Técnico",
    "Selected series payload": "Payload da série selecionada",
    "Triage report path": "Caminho do relatório de triagem",
    "Available images": "Imagens disponíveis",
    "Structured Data": "Dados Estruturados",
    "Right kidney": "Rim direito",
    "Left kidney": "Rim esquerdo",
    "axial": "axial",
    "coronal": "coronal",
    "Image unavailable": "Imagem indisponível",
    "Yes": "Sim",
    "No": "Não",
}


def _t(text: str, locale: str) -> str:
    if normalize_locale(locale) == "pt_BR":
        return PT_BR_LABELS.get(text, text)
    return text


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


def _draw_footer(draw: ImageDraw.ImageDraw, page_number: int, locale: str) -> None:
    footer = f"{_t('Heimdallr Case Report', locale)}  |  {_t('Page', locale)} {page_number}"
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


def _fmt_bool(value, locale: str = "en_US") -> str:
    if value is None:
        return "-"
    return _t("Yes" if value else "No", locale)


def _fmt_components(value) -> str:
    if value is None:
        return "-"
    return f"{int(value)}"


def _safe_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r") as handle:
        return json.load(handle)


def _collect_report_images(case_folder: Path, results: dict, triage_report: dict, locale: str) -> list[tuple[str, Path]]:
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
        side = _t("Right kidney", locale) if kidney.get("mask_name") == "kidney_right" else _t("Left kidney", locale)
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
                label = f"{side} {component.get('component_id', '').replace('_', ' ')} {_t(plane_name, locale)}"
                images.append((label, path))
                seen.add(resolved)
                if len(images) >= 10:
                    return images

    return images


def _render_image_page(title: str, items: list[tuple[str, Path]], locale: str) -> Image.Image:
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
            draw.text((x + 20, top + 20), _t("Image unavailable", locale), font=FONT_BODY, fill="#a00")

        caption_y = top + cell_height + 12
        _draw_wrapped(draw, label, x, caption_y, FONT_SMALL, cell_width)

    return page


def build_case_report(case_folder: Path, output_path: Path | None = None, locale: str | None = None) -> Path:
    case_folder = Path(case_folder)
    locale = normalize_locale(locale)
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
    title = _t("Heimdallr Case Report", locale)
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
            _t("Patient", locale),
            normalize_patient_name_display(
                metadata.get("PatientName", "-"),
                settings.PATIENT_NAME_PROFILE,
            ),
        ),
        (_t("Study", locale), metadata.get("StudyDate", "-")),
        (_t("Accession", locale), metadata.get("AccessionNumber", "-")),
        (_t("Modality", locale), metadata.get("Modality", "-")),
        (_t("Series", locale), str(selected_series.get("SeriesNumber", "-"))),
        (_t("Phase", locale), str(selected_phase)),
        (_t("T Prep", locale), pipeline.get("prepare_elapsed_time", "-")),
        (
            _t("T Seg", locale),
            pipeline.get("segmentation_elapsed_time")
            or pipeline.get("processing_elapsed_time")
            or pipeline.get("elapsed_time", "-"),
        ),
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
        alerts.append(f"{_t('Hemorrhage', locale)} { _fmt_number(results.get('hemorrhage_vol_cm3'), 'cm3') }")
    if results.get("L1_bmd_classification") not in (None, "-", "Normal"):
        alerts.append(f"L1 BMD {results.get('L1_bmd_classification')}")
    if (results.get("renal_stone_count") or 0) > 0:
        alerts.append(f"{_t('Segmented stones', locale)} {results.get('renal_stone_count')}")
    if (results.get("kidney_stone_triage_total_components") or 0) > 0:
        alerts.append(f"{_t('HU triage components', locale)} {results.get('kidney_stone_triage_total_components')}")
    alert_text = " | ".join(alerts) if alerts else _t("No major automated alert flagged by current modules.", locale)
    alert_y = summary_top + summary_height + 18
    draw.text((MARGIN, alert_y), _t("Decision Summary", locale), font=FONT_SUBTITLE, fill="#0f172a")
    _draw_wrapped(draw, alert_text, MARGIN + 210, alert_y + 2, FONT_BODY, PAGE_WIDTH - (2 * MARGIN) - 210, fill="#991b1b" if alerts else "#166534")

    col_gap = 24
    col_width = (PAGE_WIDTH - 2 * MARGIN - col_gap) // 2
    left_x = MARGIN
    right_x = MARGIN + col_width + col_gap
    top_y = alert_y + 44

    left_y = top_y
    right_y = top_y

    left_y = _draw_card(draw, _t("Overview", locale), [
        (_t("Sex", locale), metadata.get("PatientSex", "-")),
        (_t("Body regions", locale), ", ".join(results.get("body_regions", [])) or "-"),
        (_t("Selected phase", locale), str(selected_phase)),
        (_t("Selected series", locale), str(selected_series.get("SeriesNumber", "-"))),
    ], left_x, left_y, col_width)
    left_y += 16
    left_y = _draw_card(draw, _t("Musculoskeletal", locale), [
        (_t("Weight", locale), _fmt_number(metadata.get("Weight"), "kg")),
        (_t("Height", locale), _fmt_number(metadata.get("Height"), "m", 2)),
        ("SMA", _fmt_number(results.get("SMA_cm2"), "cm2", 2)),
        (_t("Muscle density", locale), _fmt_number(results.get("muscle_HU_mean"), "HU")),
        (_t("L1 trabecular HU", locale), _fmt_number(results.get("L1_trabecular_HU_mean"), "HU")),
        (_t("L1 BMD class", locale), str(results.get("L1_bmd_classification", "-"))),
    ], left_x, left_y, col_width)
    left_y += 16
    liver_pdff_percent = results.get("liver_pdff_percent")
    if liver_pdff_percent in (None, ""):
        liver_pdff_percent = estimate_pdff_from_unenhanced_ct_hu(results.get("liver_hu_mean"))

    left_y = _draw_card(draw, _t("Liver and Organs", locale), [
        (_t("Liver volume", locale), _fmt_number(results.get("liver_vol_cm3"), "cm3")),
        (_t("Liver mean HU", locale), _fmt_number(results.get("liver_hu_mean"), "HU")),
        (_t("Estimated PDFF", locale), _fmt_number(liver_pdff_percent, "%")),
        (_t("Spleen volume", locale), _fmt_number(results.get("spleen_vol_cm3"), "cm3")),
        (_t("Right kidney volume", locale), _fmt_number(results.get("kidney_right_vol_cm3"), "cm3")),
        (_t("Left kidney volume", locale), _fmt_number(results.get("kidney_left_vol_cm3"), "cm3")),
    ], left_x, left_y, col_width)

    right_y = _draw_card(draw, _t("Thoracic and Neuro", locale), [
        (_t("Lung analysis", locale), str(results.get("lung_analysis_status", "-"))),
        (_t("Total emphysema", locale), _fmt_number(results.get("total_lung_emphysema_percent"), "%")),
        (_t("Emphysema burden", locale), _fmt_number(results.get("total_lung_emphysema_vol_cm3"), "cm3")),
        (_t("Hemorrhage status", locale), str(results.get("hemorrhage_analysis_status", "-"))),
        (_t("Hemorrhage volume", locale), _fmt_number(results.get("hemorrhage_vol_cm3"), "cm3")),
    ], right_x, right_y, col_width)
    right_y += 16
    right_y = _draw_card(draw, _t("Segmented Stone Burden", locale), [
        (_t("Status", locale), str(results.get("renal_stone_analysis_status", "-"))),
        (_t("Stone count", locale), str(results.get("renal_stone_count", "-"))),
        (_t("Total volume", locale), _fmt_number(results.get("renal_stone_total_volume_mm3"), "mm3")),
        (_t("Largest axis", locale), _fmt_number(results.get("renal_stone_largest_diameter_mm"), "mm")),
        (_t("Right kidney complete", locale), _fmt_bool(results.get("renal_stone_kidney_right_complete"), locale)),
        (_t("Left kidney complete", locale), _fmt_bool(results.get("renal_stone_kidney_left_complete"), locale)),
    ], right_x, right_y, col_width)
    right_y += 16
    right_y = _draw_card(draw, _t("HU Stone Triage", locale), [
        (_t("Right components", locale), _fmt_components(results.get("kidney_stone_triage_right_components"))),
        (_t("Right burden", locale), _fmt_number(results.get("kidney_stone_triage_right_volume_mm3"), "mm3")),
        (_t("Right max axis", locale), _fmt_number(results.get("kidney_stone_triage_right_largest_axis_mm"), "mm")),
        (_t("Left components", locale), _fmt_components(results.get("kidney_stone_triage_left_components"))),
        (_t("Left burden", locale), _fmt_number(results.get("kidney_stone_triage_left_volume_mm3"), "mm3")),
        (_t("Left max axis", locale), _fmt_number(results.get("kidney_stone_triage_left_largest_axis_mm"), "mm")),
    ], right_x, right_y, col_width)

    bottom_y = max(left_y, right_y) + 20
    _draw_card(draw, _t("Technical Timing", locale), [
        (_t("Prepare", locale), pipeline.get("prepare_elapsed_time", "-")),
        (
            _t("Segmentation", locale),
            pipeline.get("segmentation_elapsed_time")
            or pipeline.get("processing_elapsed_time")
            or pipeline.get("elapsed_time", "-"),
        ),
        (_t("Prepare stats", locale), json.dumps(pipeline.get("prepare_stats", {}), ensure_ascii=True)),
        (_t("Prepare stage timings", locale), json.dumps(pipeline.get("prepare_stage_timings_seconds", {}), ensure_ascii=True)),
    ], MARGIN, bottom_y, PAGE_WIDTH - 2 * MARGIN, fill="#f8fafc")
    _draw_footer(draw, 1, locale)
    pages.append(overview_page)

    findings_page, draw = _page()
    findings_title = _t("Findings Summary", locale)
    draw.text((MARGIN, MARGIN), findings_title, font=FONT_TITLE, fill="black")
    y = MARGIN + _text_height(draw, findings_title, FONT_TITLE) + 24

    lung_rows = [
        (_t("Lung analysis status", locale), str(results.get("lung_analysis_status", "-"))),
        (_t("Total emphysema", locale), _fmt_number(results.get("total_lung_emphysema_percent"), "%")),
        (_t("Emphysema burden", locale), _fmt_number(results.get("total_lung_emphysema_vol_cm3"), "cm3")),
        (_t("Hemorrhage status", locale), str(results.get("hemorrhage_analysis_status", "-"))),
        (_t("Hemorrhage volume", locale), _fmt_number(results.get("hemorrhage_vol_cm3"), "cm3")),
    ]
    y = _draw_section(draw, _t("Thoracic and Neuro", locale), lung_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    stone_rows = [
        (_t("Stone burden status", locale), str(results.get("renal_stone_analysis_status", "-"))),
        (_t("Stone count", locale), str(results.get("renal_stone_count", "-"))),
        (_t("Stone total volume", locale), _fmt_number(results.get("renal_stone_total_volume_mm3"), "mm3")),
        (_t("Largest stone axis", locale), _fmt_number(results.get("renal_stone_largest_diameter_mm"), "mm")),
        (_t("Right kidney complete", locale), _fmt_bool(results.get("renal_stone_kidney_right_complete"), locale)),
        (_t("Left kidney complete", locale), _fmt_bool(results.get("renal_stone_kidney_left_complete"), locale)),
    ]
    y = _draw_section(draw, _t("Segmented Stone Burden", locale), stone_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    triage_rows = [
        (_t("Triage status", locale), str(results.get("kidney_stone_triage_status", "-"))),
        (_t("Right components", locale), str(results.get("kidney_stone_triage_right_components", "-"))),
        (_t("Left components", locale), str(results.get("kidney_stone_triage_left_components", "-"))),
        (_t("Right heuristic burden", locale), _fmt_number(results.get("kidney_stone_triage_right_volume_mm3"), "mm3")),
        (_t("Left heuristic burden", locale), _fmt_number(results.get("kidney_stone_triage_left_volume_mm3"), "mm3")),
        (_t("Largest component axis", locale), _fmt_number(results.get("kidney_stone_triage_max_component_axis_mm"), "mm")),
    ]
    y = _draw_section(draw, _t("Kidney Stone HU Triage", locale), triage_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))

    _draw_footer(draw, 2, locale)
    pages.append(findings_page)

    image_items = _collect_report_images(case_folder, results, triage_report, locale)
    for page_index in range(0, len(image_items), 4):
        page = _render_image_page(_t("Selected Overlays", locale), image_items[page_index:page_index + 4], locale)
        _draw_footer(ImageDraw.Draw(page), len(pages) + 1, locale)
        pages.append(page)

    appendix_page, draw = _page()
    appendix_title = _t("Technical Appendix", locale)
    draw.text((MARGIN, MARGIN), appendix_title, font=FONT_TITLE, fill="black")
    y = MARGIN + _text_height(draw, appendix_title, FONT_TITLE) + 24
    appendix_rows = [
        (_t("Prepare stats", locale), json.dumps(pipeline.get("prepare_stats", {}), ensure_ascii=True)),
        (_t("Selected series payload", locale), json.dumps(selected_series, ensure_ascii=True)),
        (_t("Triage report path", locale), str(results.get("kidney_stone_triage_report_path", "-"))),
        (_t("Available images", locale), ", ".join(results.get("images", [])) or "-"),
    ]
    _draw_section(draw, _t("Structured Data", locale), appendix_rows, MARGIN, y, PAGE_WIDTH - (2 * MARGIN))
    _draw_footer(draw, len(pages) + 1, locale)
    pages.append(appendix_page)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    first, *rest = pages
    first.save(output_path, "PDF", resolution=150.0, save_all=True, append_images=rest)
    return output_path
