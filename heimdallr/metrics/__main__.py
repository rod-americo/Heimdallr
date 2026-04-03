"""Module entrypoint for the metrics worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
