# hb-remote-iface

MQTT/RPC remote interface layer for hummingbot, providing a drop-in replacement
for `hummingbot.remote_iface.*`.

## Installation

```bash
pixi install
```

Or via pip:

```bash
pip install hb-remote-iface
```

## Development

Standard pixi tasks:

```bash
pixi run test          # run all tests
pixi run lint          # ruff check
pixi run format        # ruff format
pixi run format-check  # ruff format --check
pixi run typecheck     # mypy
pixi run check         # quality + typecheck + test
```

## Status

Scaffold — no public API yet. Public API will be populated as
`hummingbot.remote_iface.*` is migrated into this package.

## License

Apache-2.0
