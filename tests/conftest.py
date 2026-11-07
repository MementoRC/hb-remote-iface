"""Pytest configuration for hb-remote-iface."""

from __future__ import annotations

import sys
from pathlib import Path

# Make remote_iface importable when tests run from package root or main hummingbot dir.
_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_compat_gateway_singleton(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the in-tree-style ``MQTTGateway._instance`` singleton from leaking between tests."""
    from remote_iface.hb_compat.mqtt_gateway import MQTTGateway

    monkeypatch.setattr(MQTTGateway, "_instance", None, raising=False)
