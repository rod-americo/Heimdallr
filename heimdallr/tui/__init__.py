"""Textual dashboard for Heimdallr operational monitoring."""

__all__ = ["HeimdallrDashboardApp"]


def __getattr__(name: str):
    if name == "HeimdallrDashboardApp":
        from .app import HeimdallrDashboardApp

        return HeimdallrDashboardApp
    raise AttributeError(name)
