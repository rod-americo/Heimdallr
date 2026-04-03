"""Module entrypoint for the prepare worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())

