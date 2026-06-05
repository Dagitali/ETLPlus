# Performance Smoke Baselines

This document records the opt-in performance-smoke checks used to catch obvious large-file
regressions before release work. The bounds are intentionally generous and are not benchmark
targets, machine-to-machine comparisons, or published performance claims.

Performance smoke checks stay outside the default CI-parity suite because elapsed-time assertions
vary by platform and machine load. Maintainers should use this file to keep payload shapes, timing
bounds, and update criteria explicit when `make test-perf` coverage changes.

- [Performance Smoke Baselines](#performance-smoke-baselines)
  - [Scope](#scope)
  - [Current Baselines](#current-baselines)
  - [Update Criteria](#update-criteria)
  - [Suggested Local Check](#suggested-local-check)

## Scope

These checks are release-confidence smoke tests, not formal benchmarks. They should cover
stable-line workflows where an accidental slow path would create user-visible friction.

Do not use these bounds to compare hardware, publish performance claims, or gate unrelated changes
without confirming the result on a quiet machine.

## Current Baselines

| Test | Data Shape | Current Bound | Purpose |
| --- | --- | --- | --- |
| `tests/integration/file/test_i_file_csv_perf.py::test_large_csv_roundtrip_perf_smoke` | 50,000 rows, 4 columns | write under 10 seconds; read under 10 seconds | Catch obvious CSV handler regressions on larger local roundtrips. |

The current bounds are intentionally generous. They are not benchmark targets and should not be used
to compare machines. They exist to catch accidental slow paths, pathological parser changes, or
large-file regressions before release work.

## Update Criteria

Update this file when:

- A perf-smoke test changes its payload shape or elapsed-time bound;
- A new perf-smoke test is added;
- Repeated local or CI runs show the bound is too tight for maintained platforms;
- A real stable-line performance issue suggests a new opt-in smoke case.

Do not make perf-smoke tests blocking in the default suite unless maintainers explicitly decide the
runtime cost and platform variance are acceptable.

## Suggested Local Check

```bash
make test-perf
```

If `make test-perf` fails, rerun the specific failing test on a quiet machine before tightening or
relaxing a bound.
