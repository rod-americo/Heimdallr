"""Compatibility entrypoint for the integration dispatch worker."""

from heimdallr.integration.dispatch.worker import main


if __name__ == "__main__":
    raise SystemExit(main())
