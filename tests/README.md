# Tests Overview

ETLPlus organizes tests by **scope** and labels cross-cutting test **intent**
with pytest markers.

- [Tests Overview](#tests-overview)
  - [Scope Markers / Directory Layout](#scope-markers--directory-layout)
  - [Intent Markers](#intent-markers)
  - [Discovery and Selection](#discovery-and-selection)
  - [Common Commands](#common-commands)
  - [Post-Move Validation Checklist](#post-move-validation-checklist)

## Scope Markers / Directory Layout

| Marker | Path | Scope | Meaning |
| --- | --- | --- | --- |
| `unit` | `tests/unit/` | Isolated function/class behavior | Fast, deterministic, minimal external I/O |
| `integration` | `tests/integration/` | Cross-module behavior and boundary wiring | May use temp files and fakes/mocks |
| `e2e` | `tests/e2e/` | Full workflow/system-boundary behavior | Slowest, broadest confidence checks |

File-format integration smoke conventions are documented in
[`tests/integration/file/README.md`](integration/file/README.md).

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

## Post-Move Validation Checklist

Use these checks after moving/renaming test modules or changing test scope
directories.

```bash
# 1) Fast import/collection sanity checks
python -m pytest --collect-only -q tests/unit tests/integration tests/e2e

# 2) Scope-layout guardrails (legacy paths, filename patterns, marker coverage)
python -m pytest -q \
  tests/unit/meta/test_u_test_layout.py \
  tests/unit/meta/test_u_test_filenames.py \
  tests/unit/meta/test_u_marker_coverage.py \
  tests/unit/meta/test_u_integration_file_conventions.py

# 3) Scope-focused smoke run (catches broken imports quickly)
python -m pytest -q tests/unit tests/integration

# 4) Conftest scope-marker grep checks
rg -n "pytest\\.mark\\.unit" tests/unit/**/conftest.py tests/unit/conftest.py
rg -n "pytest\\.mark\\.integration" tests/integration/**/conftest.py tests/integration/conftest.py
rg -n "pytest\\.mark\\.smoke" tests/integration/file/conftest.py

# 5) Filename hygiene checks (spaces / duplicate-copy suffixes)
find tests -type f -name '*.py' | awk '/ / {print}'
rg --files tests | rg " 2\\.py$| 3\\.py$"
```
