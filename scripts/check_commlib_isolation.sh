#!/usr/bin/env bash
# Fail if any file outside remote_iface/_commlib/ imports commlib.
# This invariant is load-bearing: _commlib/ is the ONLY place that may import commlib.
# Background: hb-remote-iface wraps commlib-py 0.13.2; the wrapper layer hides version-specific
# quirks behind a stable internal API. Any commlib import outside _commlib/ leaks that coupling.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Search the package source, exclude _commlib/, exclude tests (tests may patch commlib via monkeypatch).
VIOLATIONS=$(
    grep -rn -E "^(from commlib|import commlib)" remote_iface/ \
        --include="*.py" \
        --exclude-dir="_commlib" \
        2>/dev/null || true
)

if [ -n "$VIOLATIONS" ]; then
    echo "ERROR: commlib imports found outside remote_iface/_commlib/" >&2
    echo "$VIOLATIONS" >&2
    echo "" >&2
    echo "Fix: route commlib usage through remote_iface._commlib.NodeContext factory methods." >&2
    exit 1
fi

echo "OK: commlib imports are isolated to remote_iface/_commlib/"
