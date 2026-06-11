"""Localized text helpers for the pulmonary nodule overlay."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import normalize_locale, translate


def resolve_artifact_locale(job_config: dict) -> str:
    """Resolve artifact locale from job config with environment fallback."""
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def overlay_title(locale: str) -> str:
    """Return the localized burned-in overlay title."""
    return translate("lung_nodules.overlay.title", locale=locale)


def series_description(locale: str) -> str:
    """Return the localized DICOM SeriesDescription."""
    return translate("lung_nodules.overlay.series_description", locale=locale)


def derivation_description(locale: str) -> str:
    """Return the localized DICOM DerivationDescription."""
    return translate("lung_nodules.overlay.derivation_description", locale=locale)
