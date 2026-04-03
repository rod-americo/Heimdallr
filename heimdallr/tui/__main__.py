"""CLI entrypoint for the Heimdallr Textual dashboard."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from heimdallr.shared import settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Heimdallr Textual operations dashboard")
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=2.0,
        help="Auto-refresh interval in seconds",
    )
    parser.add_argument(
        "--db-path",
        default=str(settings.DB_PATH),
        help="SQLite path used for dashboard state",
    )
    args = parser.parse_args(argv)

    try:
        from .app import HeimdallrDashboardApp
    except ImportError as exc:  # pragma: no cover - exercised only when deps are missing
        print(
            "Textual dashboard dependencies are missing. Install `textual` and `rich` from requirements.txt.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    app = HeimdallrDashboardApp(
        refresh_seconds=args.refresh_seconds,
        db_path=Path(args.db_path),
    )
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
