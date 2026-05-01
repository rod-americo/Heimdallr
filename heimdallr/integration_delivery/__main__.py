"""Compatibility entrypoint for the integration delivery worker."""

from heimdallr.integration.delivery.worker import main


if __name__ == "__main__":
    raise SystemExit(main())
