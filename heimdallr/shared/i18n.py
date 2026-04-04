"""Minimal localization helpers for presentation artifacts."""

from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path


DEFAULT_LOCALE = "en_US"
DEFAULT_DOMAIN = "artifacts"
LOCALES_DIR = Path(__file__).resolve().parents[1] / "locales"


def normalize_locale(locale: str | None) -> str:
    """Normalize locale input and fall back to the default catalog."""
    if not locale:
        return DEFAULT_LOCALE

    normalized = str(locale).strip().replace("-", "_")
    if "." in normalized:
        normalized = normalized.split(".", 1)[0]
    if "@" in normalized:
        normalized = normalized.split("@", 1)[0]

    if (LOCALES_DIR / normalized).exists():
        return normalized

    language = normalized.split("_", 1)[0]
    for candidate in sorted(LOCALES_DIR.iterdir()) if LOCALES_DIR.exists() else []:
        if candidate.is_dir() and candidate.name.split("_", 1)[0] == language:
            return candidate.name

    return DEFAULT_LOCALE


def _parse_po_quoted(text: str) -> str:
    return ast.literal_eval(text)


def _parse_po_catalog(raw_text: str) -> dict[str, str]:
    catalog: dict[str, str] = {}
    current_msgid: list[str] | None = None
    current_msgstr: list[str] | None = None
    current_field: str | None = None

    def commit() -> None:
        nonlocal current_msgid, current_msgstr, current_field
        if current_msgid is None or current_msgstr is None:
            return
        msgid = "".join(current_msgid)
        msgstr = "".join(current_msgstr)
        if msgid:
            catalog[msgid] = msgstr
        current_msgid = None
        current_msgstr = None
        current_field = None

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("msgid "):
            commit()
            current_msgid = [_parse_po_quoted(stripped[5:].strip())]
            current_msgstr = []
            current_field = "msgid"
            continue
        if stripped.startswith("msgstr "):
            if current_msgid is None:
                continue
            current_msgstr = [_parse_po_quoted(stripped[6:].strip())]
            current_field = "msgstr"
            continue
        if stripped.startswith('"'):
            if current_field == "msgid" and current_msgid is not None:
                current_msgid.append(_parse_po_quoted(stripped))
            elif current_field == "msgstr" and current_msgstr is not None:
                current_msgstr.append(_parse_po_quoted(stripped))

    commit()
    return catalog


@lru_cache(maxsize=None)
def load_catalog(locale: str, domain: str = DEFAULT_DOMAIN) -> dict[str, str]:
    """Load a `.po` catalog directly from disk."""
    normalized = normalize_locale(locale)
    po_path = LOCALES_DIR / normalized / "LC_MESSAGES" / f"{domain}.po"
    if not po_path.exists():
        if normalized == DEFAULT_LOCALE:
            return {}
        return load_catalog(DEFAULT_LOCALE, domain)
    return _parse_po_catalog(po_path.read_text(encoding="utf-8"))


def translate(message_id: str, locale: str | None = None, domain: str = DEFAULT_DOMAIN, **kwargs) -> str:
    """Translate a message id and apply simple `.format` interpolation."""
    normalized = normalize_locale(locale)
    catalog = load_catalog(normalized, domain)
    message = catalog.get(message_id)
    if message is None and normalized != DEFAULT_LOCALE:
        message = load_catalog(DEFAULT_LOCALE, domain).get(message_id)
    if message is None:
        message = message_id
    return message.format(**kwargs) if kwargs else message


def format_decimal(value: float, precision: int, locale: str | None = None) -> str:
    """Format decimals using locale-specific separators."""
    normalized = normalize_locale(locale)
    rendered = f"{float(value):.{precision}f}"
    if normalized == "pt_BR":
        return rendered.replace(".", ",")
    return rendered
