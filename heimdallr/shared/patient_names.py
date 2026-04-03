"""Helpers for patient-name normalization in user-facing views."""

from __future__ import annotations

import re


_SURNAME_PARTICLES = {
    "da",
    "das",
    "de",
    "del",
    "della",
    "di",
    "do",
    "dos",
    "du",
    "la",
    "le",
    "van",
    "von",
}
_SURNAME_SUFFIXES = {
    "filho",
    "junior",
    "jr",
    "neto",
    "sobrinho",
}


def _normalize_whitespace(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).replace("^", " ")).strip()


def _token_key(token: str) -> str:
    return re.sub(r"[^\w]+", "", token, flags=re.UNICODE).lower()


def _rotate_leading_surname(tokens: list[str]) -> list[str]:
    if len(tokens) < 2:
        return tokens

    block_size = 1
    first_key = _token_key(tokens[0])
    if first_key in _SURNAME_PARTICLES and len(tokens) >= 2:
        block_size = 2
    elif len(tokens) >= 2 and _token_key(tokens[1]) in _SURNAME_SUFFIXES:
        block_size = 2

    while block_size < len(tokens) and _token_key(tokens[block_size]) in _SURNAME_SUFFIXES:
        block_size += 1

    surname_tokens = tokens[:block_size]
    given_tokens = tokens[block_size:]
    if not given_tokens:
        return tokens
    return given_tokens + surname_tokens


def normalize_patient_name_display(name: str, profile: str = "default") -> str:
    """Return a UI-friendly patient name while preserving the raw source elsewhere."""
    cleaned = _normalize_whitespace(name)
    if not cleaned:
        return ""

    profile_key = (profile or "default").strip().lower()
    if profile_key == "medsenior":
        tokens = cleaned.split(" ")
        return " ".join(_rotate_leading_surname(tokens))

    return cleaned
