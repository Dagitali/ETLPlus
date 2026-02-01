# Tests Overview

ETLPlus uses three categories of tests. Each category lives in its own folder and is tagged with a
pytest marker to make selection easy.

## Unit tests (`tests/unit/`)

- Scope: single function or class in isolation
- I/O: no real file system or network I/O (use stubs/monkeypatch)
- Speed: fast, deterministic
- Marker: `unit`

## Smoke tests (`tests/smoke/`)

- Scope: minimal end-to-end sanity checks for core flows
- I/O: may touch temp files; no external network calls
- Speed: very fast; intended to catch obvious regressions
- Marker: `smoke`

## Integration tests (`tests/integration/`)

- Scope: cross-module workflows and CLI/pipeline paths
- I/O: may touch temp files; may use fakes/mocks for HTTP clients
- Speed: slower than unit/smoke
- Marker: `integration`

## Running tests

```bash
pytest tests/unit
pytest tests/smoke
pytest tests/integration

# Marker-based selection
pytest -m unit
pytest -m smoke
pytest -m integration
```
