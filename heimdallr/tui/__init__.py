"""TUI dashboards for Heimdallr operational monitoring."""

__all__ = ["HeimdallrDashboardApp", "SimpleQueueTui"]


def __getattr__(name: str):
    if name == "HeimdallrDashboardApp":
        from .app import HeimdallrDashboardApp

        return HeimdallrDashboardApp
    if name == "SimpleQueueTui":
        from .simple import SimpleQueueTui

        return SimpleQueueTui
    raise AttributeError(name)
