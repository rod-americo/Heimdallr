"""Module entrypoint for the segmentation worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
