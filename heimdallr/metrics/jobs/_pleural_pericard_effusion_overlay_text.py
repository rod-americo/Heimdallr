"""Localized text helpers for pleural and pericardial effusion overlays."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def finding_name(finding: str, locale: str) -> str:
    return translate(f"pleural_pericard_effusion.finding.{finding}", locale=locale)


def build_component_overlay_text(
    *,
    finding: str,
    component_index: int,
    component_count: int,
    slice_index: int,
    probable_viewer_slice_index_one_based: int,
    volume_cm3: float,
    locale: str,
) -> tuple[str, list[str]]:
    title = translate(
        "pleural_pericard_effusion.overlay.component_title",
        locale=locale,
        finding=finding_name(finding, locale),
        component=component_index,
        total=component_count,
    )
    lines = [
        translate(
            "pleural_pericard_effusion.overlay.nifti_slice",
            locale=locale,
            value=slice_index,
        ),
        translate(
            "pleural_pericard_effusion.overlay.viewer_slice",
            locale=locale,
            value=probable_viewer_slice_index_one_based,
        ),
        translate(
            "pleural_pericard_effusion.overlay.volume_cm3",
            locale=locale,
            value=format_decimal(volume_cm3, 2, locale=locale),
        ),
    ]
    return title, lines


def series_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.derivation_description", locale=locale)
