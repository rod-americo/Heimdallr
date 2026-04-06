"""Module entrypoint for the space manager service."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
