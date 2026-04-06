# Run History

ETLPlus records local run history for `etlplus run` and exposes that history through stable
read/query commands in the CLI.

- [Run History](#run-history)
  - [What Gets Recorded](#what-gets-recorded)
  - [Commands](#commands)
    - [`etlplus history`](#etlplus-history)
    - [`etlplus log`](#etlplus-log)
    - [`etlplus status`](#etlplus-status)
    - [`etlplus report`](#etlplus-report)
  - [Output Conventions](#output-conventions)
  - [Stable Normalized Fields](#stable-normalized-fields)
  - [Event-to-history mapping for `etlplus run`](#event-to-history-mapping-for-etlplus-run)
  - [Related Documentation](#related-documentation)

## What Gets Recorded

- Each `etlplus run` invocation gets a `run_id`.
- Start and finish metadata are persisted to the configured local history backend.
- The default backend is SQLite at `${ETLPLUS_STATE_DIR:-~/.etlplus}/history.sqlite`.
- A JSONL backend is also available through `ETLPLUS_HISTORY_BACKEND=jsonl`.
- DAG-style runs persist one compact aggregate run summary on the parent run row.
- DAG-style `run --job` / `run --all` executions also persist one per-job history row for each
  executed/succeeded/failed/skipped job, including plan order, timing, terminal status, and
  per-job result summaries.

Today, the persisted history is written by `etlplus run`. The read/query commands below inspect that
local store; they do not require external services.

## Commands

### `etlplus history`

Inspect normalized persisted history.

- Scope: `--level run|job`
- Filters: `--job`, `--pipeline`, `--run-id`, `--status`, `--since`, `--until`, `--limit`
- Output: JSON by default, explicit JSON with `--json`, or a Markdown table with `--table`

Example:

```bash
etlplus history --job file_to_file_customers --status succeeded --limit 10 --table
```

Inspect per-job DAG history:

```bash
etlplus history --level job --pipeline customer_sync --status skipped --table
```

### `etlplus log`

Inspect raw persisted history events without normalization.

- Scope: `--level run|job`
- Filters: `--job`, `--pipeline`, `--run-id`, `--status`, `--since`, `--until`, `--limit`
- Streaming: `--follow` polls for newly observed matching raw records until interrupted
- Follow-mode output is compact line-oriented JSON regardless of `--pretty`

Example:

```bash
etlplus log --run-id 8e4a33d7 --follow
```

```bash
etlplus log --level job --pipeline customer_sync --status skipped --follow
```

### `etlplus status`

Show the latest normalized run or job row.

- Scope: `--level run|job`
- Filters: `--job`, `--pipeline`, `--run-id`
- Output: one normalized run/job object as JSON, or `{}` with exit code `1` when no match exists

Example:

```bash
etlplus status --job file_to_file_customers
```

```bash
etlplus status --level job --job file_to_file_customers
```

### `etlplus report`

Aggregate normalized run or job history.

- Scope: `--level run|job`
- Filters: `--job`, `--pipeline`, `--run-id`, `--status`, `--since`, `--until`
- Grouping: `--group-by job|pipeline|run|status|day`
- Output: grouped JSON by default or a Markdown table with `--table`
- Metrics include run counts, success-rate percentage, and average/minimum/maximum duration

Example:

```bash
etlplus report --group-by day --since 2026-03-01T00:00:00Z --table
```

Aggregate per-job history by pipeline:

```bash
etlplus report --level job --group-by pipeline --since 2026-03-01T00:00:00Z --table
```

## Output Conventions

- `history` returns normalized records.
- `log` returns raw backend-native records.
- `status` returns a single normalized record.
- `report` returns grouped rows plus a top-level summary in JSON mode.
- The top-level normalized run and job shapes are the stable `v1.x` contract; nested
  `result_summary` keys may grow additively over time for richer DAG summaries. DAG-aware run-level
  summaries stay compact and aggregate-oriented; detailed per-job execution data is available
  through `--level job`.

## Stable Normalized Fields

Stable normalized run fields:

- `run_id`
- `pipeline_name`
- `job_name`
- `config_path`
- `config_sha256`
- `status`
- `started_at`
- `finished_at`
- `duration_ms`
- `records_in`
- `records_out`
- `error_type`
- `error_message`
- `error_traceback`
- `result_summary`
- `host`
- `pid`
- `etlplus_version`

Stable normalized job fields:

- `run_id`
- `job_name`
- `pipeline_name`
- `sequence_index`
- `started_at`
- `finished_at`
- `duration_ms`
- `records_in`
- `records_out`
- `status`
- `result_status`
- `error_type`
- `error_message`
- `skipped_due_to`
- `result_summary`

These normalized run/job shapes are the supported read contract across both SQLite and JSONL
backends. Raw backend append records remain debug-oriented and backend-specific.

## Event-to-history mapping for `etlplus run`

| Event stream field | Persisted history field(s) | Notes |
| --- | --- | --- |
| `run_id` | `runs.run_id`, `job_runs.run_id` | Stable join key across STDERR events, STDOUT payloads, and persisted history. |
| `run.started.timestamp` | `runs.started_at` | Same command invocation start time in ISO-8601 UTC form. |
| `run.completed.duration_ms`, `run.failed.duration_ms` | `runs.duration_ms` | Semantically the same elapsed run duration. |
| `run.completed.status`, `run.failed.status` | `runs.status` | Event statuses are `ok`/`error`; persisted statuses are `succeeded`/`failed`. |
| `run.failed.error_type`, `run.failed.error_message` | `runs.error_type`, `runs.error_message` | Stable failure metadata when present. |

Event-only fields today:

- `event`
- `command`
- `lifecycle`
- `timestamp`

History-only fields today:

- `config_sha256`
- `records_in`
- `records_out`
- `error_traceback`
- `host`
- `pid`
- full run/job `result_summary`
- `job_runs.sequence_index`
- `job_runs.skipped_due_to`

DAG job detail is intentionally persisted in `job_runs`; ETLPlus does not currently promise a
one-event-per-job stream contract.

## Related Documentation

- {doc}`examples`
- {doc}`structured runtime events <structured-events>`
- {doc}`pipeline-authoring`
- {doc}`../api/operations`
