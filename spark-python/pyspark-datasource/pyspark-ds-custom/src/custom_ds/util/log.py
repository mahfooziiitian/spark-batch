"""Centralized logging configuration using Rich.

All ``custom_ds`` modules obtain their logger via::

    from custom_ds.util.log import get_logger
    logger = get_logger(__name__)

The root ``custom_ds`` logger uses a :class:`rich.logging.RichHandler` so
that messages render with color, timestamps, and module context in the
terminal.  The default level is **INFO** and can be overridden via the
``CUSTOM_DS_LOG_LEVEL`` environment variable (e.g. ``DEBUG``, ``WARNING``).
"""

from __future__ import annotations

import logging
import os

from rich.logging import RichHandler

_ROOT_LOGGER_NAME = "custom_ds"

_configured = False


def _configure_root() -> None:
    """Attach a RichHandler to the package root logger (once)."""
    global _configured
    if _configured:
        return

    level = os.environ.get("CUSTOM_DS_LOG_LEVEL", "INFO").upper()

    handler = RichHandler(
        rich_tracebacks=True,
        tracebacks_show_locals=False,
        show_time=True,
        show_path=False,
        markup=True,
    )
    handler.setLevel(level)

    root = logging.getLogger(_ROOT_LOGGER_NAME)
    root.setLevel(level)
    root.addHandler(handler)
    root.propagate = False

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the ``custom_ds`` namespace.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` configured with Rich output.
    """
    _configure_root()
    return logging.getLogger(name)
