"""Module entrypoint for the processing worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
