"""Logging compatibility shim wrapping the canonical hb-logger sub-package.

Isolates the external ``logger`` (hb-logger) import so that every logging
call site in ``remote_iface`` goes through ``get_logger()`` here instead of
importing ``logger`` or ``logging.getLogger`` directly (issue #16, ADR 0001
Group D). Pattern mirrors ``remote_iface.hb_compat.adapter``/``factory``: a
thin wrapper isolating an external L0 dependency behind this package's own
module boundary.

Importing ``logger`` registers ``HummingbotLogger`` as the process-wide
logger class via ``logging.setLoggerClass`` (see ``logger.__init__``), so any
``logging.getLogger(name)`` call anywhere in the process — including this
one — returns a ``HummingbotLogger`` instance once this module has been
imported at least once.
"""

from __future__ import annotations

import logging

from logger import NETWORK, HummingbotLogger

__all__ = ["NETWORK", "HummingbotLogger", "get_logger"]


def get_logger(name: str) -> HummingbotLogger:
    """Return an hb-logger-backed logger for ``name``.

    Equivalent to ``logging.getLogger(name)``; the return type is
    ``HummingbotLogger`` because importing this module registers hb-logger's
    ``HummingbotLogger`` as the process-wide logger class.
    """
    return logging.getLogger(name)
