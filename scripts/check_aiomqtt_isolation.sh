#!/usr/bin/env bash
# Fail if any file other than the two transport-boundary modules imports aiomqtt.
# This invariant is load-bearing: _commlib/transport.py and _commlib/node_context.py are the
# ONLY places that may import aiomqtt. Every other module goes through NodeContext, so the
# MQTT client library stays swappable behind a stable internal API.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Search the package source, including indented (lazy / TYPE_CHECKING) imports.
# Tests are excluded: they may monkeypatch aiomqtt directly.
VIOLATIONS=$(
    grep -rn -E "^\s*(from aiomqtt|import aiomqtt)" remote_iface/ \
        --include="*.py" \
        2>/dev/null \
        | grep -v -E "^remote_iface/_commlib/(transport|node_context)\.py:" \
        || true
)

if [ -n "$VIOLATIONS" ]; then
    echo "ERROR: aiomqtt imports found outside remote_iface/_commlib/{transport,node_context}.py" >&2
    echo "$VIOLATIONS" >&2
    echo "" >&2
    echo "Fix: route MQTT usage through remote_iface._commlib.NodeContext factory methods." >&2
    exit 1
fi

echo "OK: aiomqtt imports are isolated to remote_iface/_commlib/{transport,node_context}.py"
