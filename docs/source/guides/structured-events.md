# Structured Runtime Events

ETLPlus emits structured runtime events on STDERR when `--event-format jsonl` is selected for
supported execution commands.

The stable event schema for `v1.x` is `etlplus.event.v1`.

- [Structured Runtime Events](#structured-runtime-events)
  - [Stable Contract](#stable-contract)
  - [Shared Lifecycle Fields](#shared-lifecycle-fields)
  - [Stable Command-Context Fields](#stable-command-context-fields)
  - [Compatibility Promise](#compatibility-promise)
  - [Relationship to Run History](#relationship-to-run-history)

## Stable Contract

The stable user-facing event contract covers `extract`, `load`, `run`, `transform`, and `validate`.

Every emitted event in schema version `1` includes these required base fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `schema` | `str` | Stable schema name. Always `etlplus.event.v1`. |
| `schema_version` | `int` | Stable schema version. Always `1` for this contract. |
| `event` | `str` | `<command>.<lifecycle>`. |
| `command` | `str` | Emitting CLI command. |
| `lifecycle` | `str` | One of `started`, `completed`, or `failed`. |
| `run_id` | `str` | Stable command invocation identifier. |
| `timestamp` | `str` | ISO-8601 UTC timestamp. |

Allowed lifecycle values in `etlplus.event.v1` are exactly:

- `started`
- `completed`
- `failed`

## Shared Lifecycle Fields

In addition to the required base fields:

- `completed` events also include stable shared fields:
  - `duration_ms`
  - `status`
- `failed` events also include stable shared fields:
  - `duration_ms`
  - `status`
  - `error_type`
  - `error_message`

For the current stable line:

- `completed.status` is `ok`
- `failed.status` is `error`

## Stable Command-Context Fields

These command-context fields are part of the stable contract because they are emitted consistently
for that command across `started`, `completed`, and `failed` events.

| Command | Stable command-context fields |
| --- | --- |
| `extract` | `source`, `source_type` |
| `load` | `source`, `target`, `target_type` |
| `transform` | `source`, `target`, `target_type` |
| `validate` | `source`, `target` |
| `run` | `config_path`, `etlplus_version`, `job`, `pipeline_name`, `run_all`, `continue_on_fail` |

Other command-specific fields such as `destination`, `valid`, and `result_status` may appear
additively without a schema bump, but they are not part of the required base contract.

`run.failed` is the stable failure lifecycle for both:

- exception-driven run failures
- handled DAG summary failures returned by `etlplus run`

Consumers should rely on `lifecycle` plus `status` rather than assuming only exceptions emit failed
events.

## Compatibility Promise

The `etlplus.event.v1` compatibility promise is:

- additive optional fields are allowed without a schema bump
- removing fields, renaming fields, changing the meaning of existing fields, changing required
  field types, or changing the allowed lifecycle set requires a new schema version
- the stable contract applies to the emitted event envelope, not to incidental implementation
  details inside individual handlers

## Relationship to Run History

Only `etlplus run` currently persists local run history.

For `run`:

- `run_id` is the join key across the STDERR event stream, the STDOUT run envelope, and persisted
  history
- lifecycle events describe the command invocation
- durable run/job state lives in normalized history records
- DAG job detail is persisted in `job_runs`; it is not a one-event-per-job stream contract today

See the {doc}`run history guide <run-history>` for the stable normalized run/job shapes and the
event-to-history field mapping.

## Optional Telemetry Bridge

ETLPlus can also forward the same structured runtime events into optional
OpenTelemetry spans and metrics without changing the default CLI output.

- Activation is opt-in.
- The `telemetry` extra installs the OpenTelemetry API and SDK.
- Environment activation applies to supported execution commands.
- `etlplus run` also accepts pipeline-level defaults through a top-level
  `telemetry` block.

Current environment variables:

- `ETLPLUS_TELEMETRY_ENABLED=true|false`
- `ETLPLUS_TELEMETRY_EXPORTER=opentelemetry|none`
- `ETLPLUS_TELEMETRY_SERVICE_NAME=<name>`

Example pipeline defaults for `etlplus run`:

```yaml
telemetry:
  enabled: true
  exporter: opentelemetry
  service_name: etlplus
```
