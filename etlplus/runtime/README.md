# `etlplus.runtime` Subpackage

Documentation for the `etlplus.runtime` subpackage: shared runtime policy helpers for logging,
structured events, readiness diagnostics, and local scheduling behavior.

- Defines the stable runtime event schema metadata used by supported execution commands
- Provides runtime logging policy helpers shared by CLI execution paths
- Exposes the readiness report builder used by `etlplus check --readiness`
- Contains local scheduler support used by `etlplus schedule --run-pending`

Back to project overview: see the top-level [README](../../README.md).

- [Public API](#public-api)
- [Readiness Diagnostics](#readiness-diagnostics)
- [See Also](#see-also)

## Public API

Most callers should use the package facade:

```python
from etlplus.runtime import RuntimeEvents, RuntimeLoggingPolicy, ReadinessReportBuilder
```

Public exports include:

- `RuntimeEvents`: structured runtime event emission helpers.
- `RuntimeLoggingPolicy`: shared CLI/runtime logging policy.
- `ReadinessReportBuilder`: builder for readiness diagnostics.
- `EVENT_SCHEMA` and `EVENT_SCHEMA_VERSION`: stable event schema identifiers.

## Readiness Diagnostics

Readiness support lives in [`readiness/`](readiness/) and backs the `etlplus check --readiness`
command. Use the CLI for ordinary diagnostics; import the builder only when writing custom runtime
integration code.

## See Also

- Runtime command contracts in [RUNTIME-COMMAND-CONTRACTS.md](../../RUNTIME-COMMAND-CONTRACTS.md)
- Runtime config and logging notes in
  [RUNTIME-CONFIG-AND-LOGGING.md](../../RUNTIME-CONFIG-AND-LOGGING.md)
- Structured event guide in
  [docs/source/guides/structured-events.md](../../docs/source/guides/structured-events.md)
