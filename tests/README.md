# Tests Overview

ETLPlus organizes tests by **scope** and labels cross-cutting test **intent**
with pytest markers.

- [Tests Overview](#tests-overview)
  - [Scope Markers / Directory Layout](#scope-markers--directory-layout)
  - [Intent Markers](#intent-markers)
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

## Discovery and Selection

Default test discovery is controlled by `pytest.ini`:

- `testpaths = tests/e2e tests/integration tests/unit`

## Common Commands

```bash
# Default discovery from pytest.ini (unit + integration + e2e)
pytest

# Scope folders
pytest tests/unit
pytest tests/integration
pytest tests/e2e

# Marker-based selection by scope (within configured testpaths)
pytest -m unit
pytest -m integration
pytest -m e2e

# Marker-based selection by intent
pytest -m smoke
pytest -m contract
```
