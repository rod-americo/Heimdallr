#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path


PROJECT_GATE = Path(__file__).resolve().parents[1] / "PROJECT_GATE.md"
REQUIRED_SECTION_PREFIXES = ("## 1.", "## 2.", "## 3.", "## 4.", "## 5.")
PENDING_MARKERS = ("TODO", "{{")
WEAK_EXACT_VALUES = {
    "",
    "?",
    "-",
    "n/a",
    "na",
    "not sure",
    "unknown",
    "later",
    "maybe",
    "placeholder",
    "to be defined",
}
WEAK_SUBSTRINGS = (
    "not sure",
    "to be defined",
    "unknown",
    "maybe",
    "placeholder",
    "whatever",
)
FIELD_RULES = {
    "real problem": {"min_words": 5, "min_chars": 24},
    "target user or operator": {"min_words": 3, "min_chars": 12},
    "expected result": {"min_words": 4, "min_chars": 20},
    "candidate repository that could absorb this": {"min_words": 1, "min_chars": 4},
    "why that coupling would be inadequate": {"min_words": 5, "min_chars": 24},
    "boundary that justifies a separate repository": {"min_words": 5, "min_chars": 24},
    "configuration": {"min_words": 2, "min_chars": 8},
    "logging": {"min_words": 2, "min_chars": 8},
    "runtime": {"min_words": 2, "min_chars": 8},
    "contracts": {"min_words": 2, "min_chars": 8},
    "authentication or transport": {"min_words": 2, "min_chars": 8},
    "responsibilities out of scope": {"min_words": 4, "min_chars": 20},
    "integrations that belong to another system": {"min_words": 3, "min_chars": 15},
    "data that should not live here": {"min_words": 2, "min_chars": 12},
    "primary host or environment": {"min_words": 2, "min_chars": 6},
    "most fragile external dependency": {"min_words": 2, "min_chars": 6},
    "restart need": {"min_words": 3, "min_chars": 12},
    "backup need": {"min_words": 3, "min_chars": 12},
    "operational risk": {"min_words": 4, "min_chars": 20},
}


def normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def word_count(value: str) -> int:
    return len(re.findall(r"[\w/-]+", value, flags=re.UNICODE))


def collect_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    current_required = False

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        if line.startswith("## "):
            current_required = line.startswith(REQUIRED_SECTION_PREFIXES)
            continue

        if not current_required or not line.startswith("- "):
            continue

        content = line[2:].strip()
        if ":" not in content:
            continue

        label, value = content.split(":", 1)
        fields[label.strip().lower()] = value.strip()

    return fields


def classify_fields(
    fields: dict[str, str],
) -> tuple[list[str], list[tuple[str, str]], list[tuple[str, str]]]:
    pending: list[str] = []
    weak: list[tuple[str, str]] = []
    short: list[tuple[str, str]] = []

    for label, rules in FIELD_RULES.items():
        value = fields.get(label, "").strip()
        normalized = normalize_text(value)

        if not value:
            pending.append(label)
            continue

        if any(marker.lower() in normalized for marker in PENDING_MARKERS):
            pending.append(label)
            continue

        if normalized in WEAK_EXACT_VALUES or any(marker in normalized for marker in WEAK_SUBSTRINGS):
            weak.append((label, value))
            continue

        words = word_count(value)
        chars = len(value)
        min_words = int(rules["min_words"])
        min_chars = int(rules["min_chars"])
        if words < min_words or chars < min_chars:
            short.append(
                (
                    label,
                    f"{words} word(s), {chars} character(s); minimum {min_words} word(s) and {min_chars} character(s)",
                )
            )

    return pending, weak, short


def main() -> int:
    if not PROJECT_GATE.exists():
        print(f"PROJECT_GATE.md missing: {PROJECT_GATE}", file=sys.stderr)
        return 1

    text = PROJECT_GATE.read_text(encoding="utf-8")
    fields = collect_fields(text)
    pending, weak, short = classify_fields(fields)

    if pending or weak or short:
        print("PROJECT_GATE.md failed semantic validation.", file=sys.stderr)

    if pending:
        print("", file=sys.stderr)
        print("Structural pending fields:", file=sys.stderr)
        for field in pending:
            print(f"- {field}", file=sys.stderr)

    if weak:
        print("", file=sys.stderr)
        print("Answers too vague:", file=sys.stderr)
        for field, value in weak:
            print(f"- {field}: {value}", file=sys.stderr)

    if short:
        print("", file=sys.stderr)
        print("Answers too short:", file=sys.stderr)
        for field, reason in short:
            print(f"- {field}: {reason}", file=sys.stderr)

    if pending or weak or short:
        print("", file=sys.stderr)
        print("Avoid vague placeholders and short answers without repository-specific detail.", file=sys.stderr)
        return 1

    print("PROJECT_GATE.md validated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
