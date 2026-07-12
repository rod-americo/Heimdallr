"""Localized presentation text for hepatic lesion overlays."""

from __future__ import annotations

from heimdallr.shared.i18n import format_decimal, format_integer, normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    return normalize_locale(str(job_config.get("locale") or ""))


def overlay_title(locale: str) -> str:
    return translate("liver_lesions.overlay.title", locale=locale)


def build_component_overlay_text(
    *,
    component_index: int,
    component_count: int,
    voxel_count: int,
    volume_cm3: float,
    locale: str,
) -> tuple[str, list[str]]:
    title = translate(
        "liver_lesions.overlay.component_title",
        locale=locale,
        index=component_index,
        count=component_count,
    )
    return title, [
        translate(
            "liver_lesions.overlay.voxel_count",
            locale=locale,
            value=format_integer(voxel_count, locale=locale),
        ),
        translate(
            "liver_lesions.overlay.volume_cm3",
            locale=locale,
            value=format_decimal(volume_cm3, 1, locale=locale),
        ),
    ]


def series_description(locale: str) -> str:
    return translate("liver_lesions.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    return translate("liver_lesions.overlay.derivation_description", locale=locale)
