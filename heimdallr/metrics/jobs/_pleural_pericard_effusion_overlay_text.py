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
    finding_volumes_cm3: dict[str, float],
    pleural_side_volumes_cm3: dict[str, float] | None = None,
    locale: str,
) -> tuple[str, list[str]]:
    title = translate(
        "pleural_pericard_effusion.overlay.slab_title",
        locale=locale,
        slab=slab_index,
        total=slab_count,
    )
    lines = []
    for finding in present_findings:
        if finding == "pleural_effusion" and pleural_side_volumes_cm3:
            for side in ("right", "left", "indeterminate"):
                volume = float(pleural_side_volumes_cm3.get(side, 0.0))
                if volume <= 0:
                    continue
                lines.append(
                    translate(
                        "pleural_pericard_effusion.overlay.pleural_side_volume",
                        locale=locale,
                        side=translate(f"pleural_pericard_effusion.side.{side}", locale=locale),
                        value=format_decimal(volume, 1, locale=locale),
                    )
                )
            continue
        lines.append(
            translate(
                "pleural_pericard_effusion.overlay.finding_volume",
                locale=locale,
                finding=finding_name(finding, locale),
                value=format_decimal(finding_volumes_cm3[finding], 1, locale=locale),
            )
        )
    return title, lines


def series_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    return translate("pleural_pericard_effusion.overlay.derivation_description", locale=locale)
