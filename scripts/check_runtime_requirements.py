#!/usr/bin/env python3

from __future__ import annotations

import argparse
from importlib.metadata import distributions
from pathlib import Path
import sys

from packaging.requirements import Requirement


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REQUIREMENTS = ROOT / "requirements.txt"


def normalize_name(value: str) -> str:
    return value.lower().replace("_", "-")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare the active Python environment with requirements.txt",
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=DEFAULT_REQUIREMENTS,
        help="requirements file to audit",
    )
    parser.add_argument(
        "--show-extras",
        action="store_true",
        help="print installed distributions that are not listed in requirements",
    )
    return parser.parse_args()


def load_requirements(path: Path) -> list[Requirement]:
    requirements: list[Requirement] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            requirement = Requirement(line)
        except Exception as exc:
            raise SystemExit(f"{path}:{line_number}: cannot parse requirement {line!r}: {exc}") from exc
        if requirement.marker is not None and not requirement.marker.evaluate():
            continue
        requirements.append(requirement)
    return requirements


def installed_distributions() -> dict[str, str]:
    installed: dict[str, str] = {}
    for distribution in distributions():
        name = distribution.metadata.get("Name")
        if not name:
            continue
        installed[normalize_name(name)] = distribution.version
    return installed


def main() -> int:
    args = parse_args()
    requirements = load_requirements(args.requirements)
    installed = installed_distributions()

    missing: list[str] = []
    mismatched: list[tuple[str, str, str]] = []

    for requirement in requirements:
        name = normalize_name(requirement.name)
        installed_version = installed.get(name)
        if installed_version is None:
            missing.append(str(requirement))
            continue
        if requirement.specifier and not requirement.specifier.contains(
            installed_version,
            prereleases=True,
        ):
            mismatched.append((requirement.name, str(requirement.specifier), installed_version))

    requirement_names = {normalize_name(requirement.name) for requirement in requirements}
    extras = sorted(name for name in installed if name not in requirement_names)

    print(f"Python: {sys.executable}")
    print(f"Requirements: {args.requirements}")
    print(
        "Summary: "
        f"requirements={len(requirements)} "
        f"installed={len(installed)} "
        f"missing={len(missing)} "
        f"mismatched={len(mismatched)} "
        f"extras={len(extras)}"
    )

    if missing:
        print("")
        print("Missing requirements:")
        for item in missing:
            print(f"- {item}")

    if mismatched:
        print("")
        print("Version mismatches:")
        for name, specifier, installed_version in mismatched:
            print(f"- {name} {specifier}; installed={installed_version}")

    extras_of_interest = [name for name in extras if name in {"python-dotenv", "pip", "wheel"}]
    if extras_of_interest:
        print("")
        print("Extras of interest:")
        for name in extras_of_interest:
            print(f"- {name}=={installed[name]}")

    if args.show_extras and extras:
        print("")
        print("All extras:")
        for name in extras:
            print(f"- {name}=={installed[name]}")

    if missing or mismatched:
        return 1

    print("")
    print("Runtime requirements match the active environment.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
