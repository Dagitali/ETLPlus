# `etlplus.telemetry` Subpackage

Documentation for the `etlplus.telemetry` subpackage: optional telemetry configuration and runtime
export helpers.

- Resolves telemetry configuration from environment variables and pipeline defaults
- Forwards structured runtime events and history records to optional OpenTelemetry integrations
- Keeps telemetry opt-in so default CLI output and local behavior stay unchanged

Back to project overview: see the top-level [README](../../README.md).

- [Public API](#public-api)
- [Activation](#activation)
- [See Also](#see-also)

## Public API

Most callers should use the package facade:

```python
from etlplus.telemetry import RuntimeTelemetry, ResolvedTelemetryConfig, TelemetryConfig
```

Public exports include:

- `TelemetryConfig`: pipeline-level telemetry configuration.
- `ResolvedTelemetryConfig`: resolved runtime telemetry settings.
- `RuntimeTelemetry`: runtime bridge for events and persisted history records.

## Activation

Telemetry is optional. Install the extra and opt in through environment variables or pipeline
configuration:

```bash
pip install "etlplus[telemetry]"
```

```bash
export ETLPLUS_TELEMETRY_ENABLED=true
export ETLPLUS_TELEMETRY_EXPORTER=opentelemetry
```

## See Also

- Structured event guide in
  [docs/source/guides/structured-events.md](../../docs/source/guides/structured-events.md)
- Runtime config and logging notes in
  [RUNTIME-CONFIG-AND-LOGGING.md](../../RUNTIME-CONFIG-AND-LOGGING.md)
