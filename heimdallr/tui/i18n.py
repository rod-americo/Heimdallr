"""Localization helpers for the Heimdallr TUI."""

from __future__ import annotations

from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate


TUI_DOMAIN = "tui"


def current_locale() -> str:
    return normalize_locale(settings.TUI_LOCALE)


def tui(message_id: str, **kwargs) -> str:
    return translate(message_id, locale=current_locale(), domain=TUI_DOMAIN, **kwargs)


def format_refresh_seconds(value: float) -> str:
    return tui("app.refresh_seconds", value=format_decimal(value, 1, locale=current_locale()))


def service_label(slug: str) -> str:
    return tui(f"service.{slug}")


def stage_label(stage_key: str) -> str:
    return tui(f"case.stage.{stage_key}")


def queue_status_label(queue_status_key: str) -> str:
    if not queue_status_key:
        return "-"
    translated = tui(f"queue_status.{queue_status_key}")
    return queue_status_key if translated == f"queue_status.{queue_status_key}" else translated


def stage_state_label(state: str) -> str:
    translated = tui(f"stage_state.{state}")
    return state.upper() if translated == f"stage_state.{state}" else translated


def no_data() -> str:
    return tui("shared.na")
