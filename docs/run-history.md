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
    - Selected job for dependency-aware `run --job` executions
    - `NULL` for `run --all`
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
- `job_runs` table (one row per executed job in DAG-aware runs)
  - `run_id` TEXT NOT NULL
  - `job_name` TEXT NOT NULL
  - `pipeline_name` TEXT NULL
  - `sequence_index` INTEGER NOT NULL
  - `status` TEXT NOT NULL
  - `result_status` TEXT NULL
  - `started_at` TEXT NULL
  - `finished_at` TEXT NULL
  - `duration_ms` INTEGER NULL
  - `records_in` INTEGER NULL
  - `records_out` INTEGER NULL
  - `error_type` TEXT NULL
  - `error_message` TEXT NULL
  - `skipped_due_to` TEXT NULL (JSON list of upstream failed jobs)
  - `result_summary` TEXT NULL (JSON summary)
  - PRIMARY KEY (`run_id`, `job_name`)
- `meta` table
  - `schema_version` INTEGER NOT NULL

**JSONL Backend**
- Location: `${ETLPLUS_STATE_DIR}/history.jsonl`
- Each line: one raw append event or partial update keyed by `run_id`
- Raw records carry `record_level=run|job` so the same append-only stream can represent both
  top-level runs and per-job DAG rows
- Normalized one-record-per-run and one-record-per-job views are rebuilt on read by merging records
  in order

**Write Flow (CLI)**
- Current stable-line implementation:
  - On `etlplus run` start: write `runs` row with `status=running`.
  - On success: update `status=succeeded`, `finished_at`, `duration_ms`,
    `result_summary`.
  - On failure: update `status=failed`, set error fields.
  - DAG-aware `run --job` and `run --all` executions persist both:
    - a top-level run summary in `runs.result_summary`
    - one per-job row in `job_runs` (SQLite) or one `record_level=job` event sequence (JSONL) for
      each executed/succeeded/failed/skipped job, including plan order and timing
- Remaining work:
  - capture traceback conditionally

**CLI Commands**
- `etlplus history`
  - Filters: `--level run|job`, `--job`, `--pipeline`, `--run-id`, `--status`, `--since`,
    `--until`, `--limit`
  - Output: JSON by default, `--json` for explicit JSON, or `--table` for a Markdown table of
    normalized run records (`--level run`) or normalized job records (`--level job`)
  - Example:
    - `etlplus history --job file_to_file_customers --status succeeded --since 2026-03-01T00:00:00Z --table --limit 20`
    - `etlplus history --level job --pipeline customer_sync --status skipped --table`
- `etlplus log`
  - Purpose: inspect raw append events from the local history backend without normalization
  - Options: `--level run|job`, `--job`, `--pipeline`, `--run-id`, `--status`, `--since`, `--until`,
    `--limit`, `--follow`
  - `--follow` keeps polling for newly observed matching raw records until interrupted
    and emits compact one-record-per-line JSON regardless of `--pretty`
  - Reserved future room: backend-debug-oriented output
  - Example:
    - `etlplus log --run-id 8e4a33d7 --since 2026-03-20T00:00:00Z --until 2026-03-21T00:00:00Z --follow`
    - `etlplus log --level job --pipeline customer_sync --status skipped --follow`
- `etlplus status`
  - Show the latest normalized run or job row
  - Options: `--level run|job`, `--job`, `--pipeline`, `--run-id`
  - Output: single normalized run/job record as JSON
- `etlplus report`
  - Aggregated stats over a time window
  - Options: `--level run|job`, `--since`, `--until`, `--job`, `--pipeline`, `--run-id`,
    `--status`, `--group-by job|pipeline|run|status|day`, `--table`, `--json`
  - Output: grouped JSON report by default, or a Markdown table of grouped rows
  - Metrics: grouped rows and the top-level summary include average, minimum,
    and maximum duration plus success-rate percentage

**CLI Output Conventions**
- `etlplus run` emits a `run_id` in the output envelope:
  - `{ "status": "ok", "run_id": "...", "result": {...} }`
  - DAG-style runs return a stable summary object in `result`, including `mode`, `ordered_jobs`,
    `executed_jobs`, and aggregate counts.
- `etlplus history` returns an array of normalized run objects by default and normalized job objects
  when `--level job` is used.
- `etlplus history --json` explicitly requests JSON output.
- `etlplus history --table` returns a Markdown table of normalized run/job objects.
- `etlplus log` returns an array of raw run/job history events.
- `etlplus log --follow` streams matching raw run/job history records until interrupted using
  compact line-oriented JSON.
- `etlplus status` returns one normalized run/job object or `{}` when no match exists.
- `etlplus report` returns grouped JSON with a top-level summary and grouped rows,
  including duration extrema and success-rate metrics, or a Markdown table when
  `--table` is used.
- All commands accept `--pretty` and respect `--quiet`.

**Compatibility Guidance**
- The normalized persisted fields in `runs` and `job_runs` are the stable local-history contract for
  `v1.x`.
- `result_summary` is extensible at both run and job levels: DAG-aware runs may add new nested keys
  without a schema-version bump as long as existing keys keep their meaning.
- Backend differences (SQLite rows vs JSONL append records) should normalize to the same stable
  run/job shapes for `history`, `status`, and `report`.
- Any breaking change to the top-level persisted run/job shapes or the meaning/type of existing
  stable fields should increment `HISTORY_SCHEMA_VERSION`.

**Config Integration**
- Add optional `history` block to pipeline config:
  - `enabled` (default true)
  - `backend` (sqlite/jsonl)
  - `state_dir` override
  - `capture_tracebacks` (default false)
- CLI flags override config defaults.

**Implementation Touchpoints**
- New module: `etlplus/history` with `HistoryStore` interface and two backends.
- `etlplus/cli/_handlers/run.py` and `etlplus/cli/_handlers/_lifecycle.py`: wrap `run()` execution
  to record history on start/end and emit lifecycle events.
- `etlplus/cli/_commands/`: add `history`, `log`, `status`, and `report` commands.
- `etlplus/ops/run.py`: produce ordered per-job execution metadata that can be persisted directly
  into `job_runs`.

**Non-Goals**
- No external services or cloud dependencies.
- No daemon or scheduler integration in this phase.
