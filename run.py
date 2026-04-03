"""Legacy processing entrypoint preserved as a thin wrapper."""

from heimdallr.processing.worker import main


if __name__ == "__main__":
    raise SystemExit(main())
