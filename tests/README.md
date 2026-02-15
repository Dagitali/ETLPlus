# Tests Overview

ETLPlus organizes tests primarily by scope-aligned folders, with intent markers applied
orthogonally.

## Folders

### Unit tests (`tests/unit/`)

- Scope: single function or class in isolation
- I/O: no real file system or network I/O (use stubs/monkeypatch)
- Speed: fast, deterministic
- Marker: `unit`

### Integration tests (`tests/integration/`)

- Scope: cross-module workflows and CLI/pipeline paths
- I/O: may touch temp files; may use fakes/mocks for HTTP clients
- Speed: slower than unit
- Marker: `integration`

### End-to-end tests (`tests/e2e/`)

- Scope: full system-boundary validation
- I/O: real-ish boundary interactions and orchestration flows
- Speed: slowest in the regular matrix
- Marker: `e2e`

## Intent markers

- `smoke`: go/no-go viability checks (broad, shallow)
- `contract`: compatibility tests for stable interfaces and metadata

## Transitional legacy path

- `tests/smoke/` is being phased out as a top-level scope folder.
- Keep existing tests there until migrated; place new smoke tests in scope
  folders and mark them with `@pytest.mark.smoke`.

## Running tests

```bash
pytest tests/unit
pytest tests/integration
pytest tests/e2e

# Transitional legacy run
pytest tests/smoke

# Marker-based selection by scope
pytest -m unit
pytest -m integration
pytest -m e2e

# Marker-based selection by intent
pytest -m smoke
pytest -m contract
```
