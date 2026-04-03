"""Operational control plane for Heimdallr."""

from __future__ import annotations

__all__ = ["app", "create_app"]


def __getattr__(name: str):
    if name in __all__:
        from .app import app, create_app

        return {"app": app, "create_app": create_app}[name]
    raise AttributeError(name)
