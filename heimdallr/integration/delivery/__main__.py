"""Module entrypoint for the integration delivery worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
