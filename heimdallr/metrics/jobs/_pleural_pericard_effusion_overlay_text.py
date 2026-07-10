"""Localized text helpers for pleural and pericardial effusion overlays."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def finding_name(finding: str, locale: str) -> str:
    return translate(f"pleural_pericard_effusion.finding.{finding}", locale=locale)


def build_slab_overlay_text(
    *,
    present_findings: list[str],
    slab_index: int,
    slab_count: int,
    center_mm: float,
    finding_volumes_cm3: dict[str, float],
    locale: str,
) -> tuple[str, list[str]]:
    title = translate(
        "pleural_pericard_effusion.overlay.slab_title",
        locale=locale,
        slab=slab_index,
        total=slab_count,
    )
    lines = [
        translate(
            "pleural_pericard_effusion.overlay.slab_center",
            locale=locale,
            value=format_decimal(center_mm, 1, locale=locale),
        ),
    ]
    lines.extend(
        translate(
            "pleural_pericard_effusion.overlay.finding_volume",
            locale=locale,
            finding=finding_name(finding, locale),
            value=format_decimal(finding_volumes_cm3[finding], 1, locale=locale),
        )
        for finding in present_findings
    )
    return title, lines


def series_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.derivation_description", locale=locale)
