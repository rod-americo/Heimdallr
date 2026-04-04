"""Module entrypoint for the DICOM egress worker."""

from .worker import main


if __name__ == "__main__":
    raise SystemExit(main())
