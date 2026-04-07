"""Module entrypoint for the integration dispatcher worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
