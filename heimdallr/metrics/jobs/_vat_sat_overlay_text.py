"""Localized text helpers for the VAT/SAT overlay."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def build_overlay_text(
    *,
    slice_idx: int,
    probable_viewer_slice_index_one_based: int,
    sat_area_cm2: float,
    vat_area_cm2: float,
    ratio: float | None,
    locale: str,
) -> tuple[str, tuple[str, str], list[str], str, str]:
    """Build localized labels for the VAT/SAT overlay."""
    title = translate("vat_sat.overlay.title", locale=locale)
    panel_titles = (
        translate("vat_sat.overlay.axial_title", locale=locale),
        translate("vat_sat.overlay.sagittal_title", locale=locale),
    )
    summary_lines = [
        translate("vat_sat.overlay.level", locale=locale),
        translate("vat_sat.overlay.nifti_slice", locale=locale, value=slice_idx),
        translate(
            "vat_sat.overlay.viewer_slice",
            locale=locale,
            value=probable_viewer_slice_index_one_based,
        ),
        translate(
            "vat_sat.overlay.sat",
            locale=locale,
            value=format_decimal(sat_area_cm2, 1, locale=locale),
        ),
        translate(
            "vat_sat.overlay.vat",
            locale=locale,
            value=format_decimal(vat_area_cm2, 1, locale=locale),
        ),
        translate(
            "vat_sat.overlay.ratio",
            locale=locale,
            value=format_decimal(ratio, 4, locale=locale) if ratio is not None else "-",
        ),
    ]
    legend = translate("vat_sat.overlay.legend", locale=locale)
    sagittal_level = translate(
        "vat_sat.overlay.sagittal_level",
        locale=locale,
        slice_idx=slice_idx,
        slab_mm=format_decimal(3.0, 0, locale=locale),
    )
    return title, panel_titles, summary_lines, legend, sagittal_level


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("vat_sat.overlay.series_description", locale=locale)


def derivation_description(
    locale: str,
    *,
    vat_area_cm2: float,
    sat_area_cm2: float,
    ratio: float | None,
) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate(
        "vat_sat.overlay.derivation_description",
        locale=locale,
        vat=format_decimal(vat_area_cm2, 2, locale=locale),
        sat=format_decimal(sat_area_cm2, 2, locale=locale),
        ratio=format_decimal(ratio, 4, locale=locale) if ratio is not None else "-",
    )
