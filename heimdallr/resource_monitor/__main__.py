"""Module entrypoint for the resource monitor service."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
