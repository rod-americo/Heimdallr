"""Outbound event dispatcher for external Heimdallr integrations."""

from .outbox import enqueue_dispatches

__all__ = ["enqueue_dispatches"]
