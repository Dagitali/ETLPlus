**Run History Design and CLI Surface**
- Status: the stable `v1.x` codebase persists local `etlplus run` metadata keyed by `run_id`, and
  `etlplus history`, `etlplus log`, `etlplus status`, and `etlplus report` are now part of the
  documented stable CLI surface.
- Scope: local-first run history for the CLI with no external services; minimal
  dependencies; safe to disable.
- Goals: record start/end, status, and summary for each run; query via CLI; keep
  storage portable.

**Storage Backend**
- Default: SQLite at `${ETLPLUS_STATE_DIR:-~/.etlplus}/history.sqlite`
- Alternative: JSONL backend for ultra-lightweight setups
- Override: `ETLPLUS_HISTORY_BACKEND=sqlite|jsonl` and `ETLPLUS_STATE_DIR`
- Migrations: schema version stored in `meta` table; upgrades happen on open

**Data Model (SQLite)**
- `runs` table (one row per CLI invocation)
  - `run_id` TEXT PRIMARY KEY (uuid4)
  - `pipeline_name` TEXT NULL
  - `job_name` TEXT NULL
  - `config_path` TEXT NOT NULL
  - `config_sha256` TEXT NULL
  - `status` TEXT NOT NULL (queued|running|succeeded|failed|canceled)
  - `started_at` TEXT NOT NULL (ISO-8601 UTC)
  - `finished_at` TEXT NULL
  - `duration_ms` INTEGER NULL
  - `records_in` INTEGER NULL
  - `records_out` INTEGER NULL
  - `error_type` TEXT NULL
  - `error_message` TEXT NULL
  - `error_traceback` TEXT NULL (optional, size-capped)
  - `result_summary` TEXT NULL (JSON summary)
  - `host` TEXT NULL
  - `pid` INTEGER NULL
  - `etlplus_version` TEXT NULL
- `job_runs` table (future DAG support; optional now)
  - `run_id` TEXT NOT NULL
  - `job_name` TEXT NOT NULL
  - `status` TEXT NOT NULL
  - `started_at` TEXT NOT NULL
  - `finished_at` TEXT NULL
  - `duration_ms` INTEGER NULL
  - `records_in` INTEGER NULL
  - `records_out` INTEGER NULL
  - UNIQUE (`run_id`, `job_name`)
- `meta` table
  - `schema_version` INTEGER NOT NULL

**JSONL Backend**
- Location: `${ETLPLUS_STATE_DIR}/history.jsonl`
- Each line: one raw append event or partial run update keyed by `run_id`
- Normalized one-record-per-run views are rebuilt on read by merging records in order
- No `job_runs` table; embed any future per-job details in the raw record stream or normalized view

**Write Flow (CLI)**
- Current stable-line implementation:
  - On `etlplus run` start: write `runs` row with `status=running`.
  - On success: update `status=succeeded`, `finished_at`, `duration_ms`,
    `result_summary`.
  - On failure: update `status=failed`, set error fields.
- Remaining work:
  - capture traceback conditionally
  - publish compatibility guidance for persisted record fields and future event-schema reuse

**CLI Commands**
- `etlplus history`
  - Filters: `--job`, `--status`, `--since`, `--until`, `--limit`
  - Output: JSON by default, `--json` for explicit JSON, or `--table` for a Markdown table of normalized run records
    (one record per `run_id`)
  - Example:
    - `etlplus history --job file_to_file_customers --status succeeded --since 2026-03-01T00:00:00Z --table --limit 20`
- `etlplus log`
  - Purpose: inspect raw append events from the local history backend without normalization
  - Initial options: `--limit`, `--run-id`, `--since`, `--until`, `--follow`
  - `--follow` keeps polling for newly observed matching raw records until interrupted
    and emits compact one-record-per-line JSON regardless of `--pretty`
  - Reserved future room: backend-debug-oriented output
  - Example:
    - `etlplus log --run-id 8e4a33d7 --since 2026-03-20T00:00:00Z --until 2026-03-21T00:00:00Z --follow`
- `etlplus status`
  - Show the latest normalized run for a job or the last run overall
  - Options: `--job`, `--run-id`
  - Output: single normalized run record as JSON
- `etlplus report`
  - Aggregated stats over a time window
  - Options: `--since`, `--until`, `--job`, `--group-by job|status|day`, `--table`, `--json`
  - Output: grouped JSON report by default, or a Markdown table of grouped rows
  - Metrics: grouped rows and the top-level summary include average, minimum,
    and maximum duration plus success-rate percentage

**CLI Output Conventions**
- `etlplus run` emits a `run_id` in the output envelope:
  - `{ "status": "ok", "run_id": "...", "result": {...} }`
- `etlplus history` returns an array of normalized run objects by default.
- `etlplus history --json` explicitly requests JSON output.
- `etlplus history --table` returns a Markdown table of normalized run objects.
- `etlplus log` returns an array of raw history events.
- `etlplus log --follow` streams matching raw history records until interrupted
  using compact line-oriented JSON.
- `etlplus status` returns one normalized run object or `{}` when no match exists.
- `etlplus report` returns grouped JSON with a top-level summary and grouped rows,
  including duration extrema and success-rate metrics, or a Markdown table when
  `--table` is used.
- All commands accept `--pretty` and respect `--quiet`.

**Config Integration**
- Add optional `history` block to pipeline config:
  - `enabled` (default true)
  - `backend` (sqlite/jsonl)
  - `state_dir` override
  - `capture_tracebacks` (default false)
- CLI flags override config defaults.

**Implementation Touchpoints**
- New module: `etlplus/history` with `HistoryStore` interface and two backends.
- `etlplus/cli/_handlers.py`: wrap `run()` to record history on start/end.
- `etlplus/cli/_commands/`: add `history`, `log`, `status`, and `report` commands.
- `etlplus/run.py`: optionally return record counts for summary fields.

**Non-Goals**
- No external services or cloud dependencies.
- No daemon or scheduler integration in this phase.
