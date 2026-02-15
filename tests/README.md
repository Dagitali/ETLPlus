# Tests Overview

ETLPlus organizes tests by **scope** and labels cross-cutting test **intent**
with pytest markers.

- [Tests Overview](#tests-overview)
  - [Scope Markers / Directory Layout](#scope-markers--directory-layout)
  - [Intent Markers](#intent-markers)
  - [Legacy Transitional Path](#legacy-transitional-path)
  - [Discovery and Selection](#discovery-and-selection)
  - [Common Commands](#common-commands)

## Scope Markers / Directory Layout

| Marker | Path | Scope | Meaning |
| --- | --- | --- | --- |
| `unit` | `tests/unit/` | Isolated function/class behavior | Fast, deterministic, minimal external I/O |
| `integration` | `tests/integration/` | Cross-module behavior and boundary wiring | May use temp files and fakes/mocks |
| `e2e` | `tests/e2e/` | Full workflow/system-boundary behavior | Slowest, broadest confidence checks |

## Intent Markers

Intent markers are orthogonal to scope. A test can be both `integration` and
`smoke`, for example.

| Marker | Meaning |
| --- | --- |
| `smoke` | Go/no-go viability checks (broad and shallow) |
| `contract` | Compatibility checks for stable interfaces and metadata |
| `acceptance` | User-facing behavior checks |
| `data_quality` | Output semantic invariants |
| `resilience` | Failure/retry/recovery behavior |
| `perf` | Throughput/latency performance checks |

See `pytest.ini` for the complete marker registry.

## Legacy Transitional Path

- `tests/smoke/` is a legacy location being phased out.
- New tests should be placed under scope folders (`tests/unit/`,
  `tests/integration/`, `tests/e2e/`) and labeled with `@pytest.mark.smoke`
  when appropriate.

## Discovery and Selection

Default test discovery is controlled by `pytest.ini`:

- `testpaths = tests/unit tests/integration tests/e2e`

This means plain `pytest` does **not** collect `tests/smoke/` unless you
explicitly pass that path.

## Common Commands

```bash
# Default discovery from pytest.ini (unit + integration + e2e)
pytest

# Scope folders
pytest tests/unit
pytest tests/integration
pytest tests/e2e

# Legacy smoke folder (explicit path required)
pytest tests/smoke

# Marker-based selection by scope (within configured testpaths)
pytest -m unit
pytest -m integration
pytest -m e2e

# Marker-based selection by intent
pytest -m smoke
pytest -m contract
```
