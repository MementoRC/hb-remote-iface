"""Pytest configuration for hb-remote-iface."""

from __future__ import annotations

import sys
from pathlib import Path

# Make remote_iface importable when tests run from package root or main hummingbot dir.
_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))
