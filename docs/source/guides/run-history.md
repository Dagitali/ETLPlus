# Run History

ETLPlus records local run history for `etlplus run` and exposes that history through stable
read/query commands in the CLI.

## What gets recorded

- Each `etlplus run` invocation gets a `run_id`.
- Start and finish metadata are persisted to the configured local history backend.
- The default backend is SQLite at `${ETLPLUS_STATE_DIR:-~/.etlplus}/history.sqlite`.
- A JSONL backend is also available through `ETLPLUS_HISTORY_BACKEND=jsonl`.
- DAG-style `run --job` / `run --all` executions persist an execution summary in `result_summary`,
  including ordered jobs and succeeded/failed/skipped job lists.

Today, the persisted history is written by `etlplus run`. The read/query commands below inspect that
local store; they do not require external services.

## Commands

### `etlplus history`

Inspect normalized runs, one record per `run_id`.

- Filters: `--job`, `--status`, `--since`, `--until`, `--limit`
- Output: JSON by default, explicit JSON with `--json`, or a Markdown table with `--table`

Example:

```bash
etlplus history --job file_to_file_customers --status succeeded --limit 10 --table
```

### `etlplus log`

Inspect raw persisted history events without normalization.

- Filters: `--run-id`, `--since`, `--until`, `--limit`
- Streaming: `--follow` polls for newly observed matching raw records until interrupted
- Follow-mode output is compact line-oriented JSON regardless of `--pretty`

Example:

```bash
etlplus log --run-id 8e4a33d7 --follow
```

### `etlplus status`

Show the latest normalized run overall or for a selected job or run id.

- Filters: `--job`, `--run-id`
- Output: one normalized run object as JSON, or `{}` with exit code `1` when no match exists

Example:

```bash
etlplus status --job file_to_file_customers
```

### `etlplus report`

Aggregate normalized run history by `job`, `status`, or `day`.

- Filters: `--job`, `--since`, `--until`
- Grouping: `--group-by job|status|day`
- Output: grouped JSON by default or a Markdown table with `--table`
- Metrics include run counts, success-rate percentage, and average/minimum/maximum duration

Example:

```bash
etlplus report --group-by day --since 2026-03-01T00:00:00Z --table
```

## Output conventions

- `history` returns normalized records.
- `log` returns raw backend-native records.
- `status` returns a single normalized record.
- `report` returns grouped rows plus a top-level summary in JSON mode.
- The top-level normalized run shape is the stable `v1.x` contract; nested `result_summary` keys may
  grow additively over time for richer DAG summaries.

## Related documentation

- {doc}`examples`
- {doc}`pipeline-authoring`
- {doc}`../api/operations`
