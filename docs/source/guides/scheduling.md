# Scheduling

ETLPlus supports portable schedule definitions in pipeline config and keeps recurring invocation
separate from execution.

Use `etlplus schedule` to inspect configured schedules, emit `crontab` or `systemd` helper
snippets, or dispatch currently due schedules once with `--run-pending`.

- [Scheduling](#scheduling)
  - [Example Configuration](#example-configuration)
  - [Inspect And Emit Helpers](#inspect-and-emit-helpers)
  - [Run Due Schedules Once](#run-due-schedules-once)
  - [Observability And State](#observability-and-state)
  - [Backing-Service Posture](#backing-service-posture)

## Example Configuration

See [examples/configs/scheduling.yml][scheduling.yml] for a complete example. The relevant schedule
shape is:

```yaml
history:
  enabled: true
  backend: sqlite

schedules:
  - name: hourly_sync
    interval:
      minutes: 60
    target:
      job: sync_customers
    backfill:
      enabled: true
      max_catchup_runs: 2
      start_at: "2026-05-01T00:00:00Z"

  - name: nightly_all
    cron: "0 2 * * *"
    timezone: UTC
    target:
      run_all: true

  - name: paused_rebuild
    cron: "30 6 * * 1"
    paused: true
    target:
      job: sync_customers
```

Schedule fields:

- `cron`: five-field cron expression for calendar-based schedules.
- `interval.minutes`: fixed-minute interval schedule.
- `target.job` or `target.run_all`: select one job or the full DAG run.
- `paused`: keep the schedule defined without dispatching it.
- `backfill.enabled`: allow bounded catch-up when triggers were missed.
- `backfill.max_catchup_runs`: limit how many missed runs can dispatch in one pass.
- `backfill.start_at`: earliest timestamp eligible for bounded replay.

## Inspect And Emit Helpers

Inspect all configured schedules:

```bash
etlplus schedule --config examples/configs/scheduling.yml
```

Emit a `crontab` snippet for one schedule:

```bash
etlplus schedule \
  --config examples/configs/scheduling.yml \
  --schedule nightly_all \
  --emit crontab
```

Emit a `systemd` timer/service pair for one schedule:

```bash
etlplus schedule \
  --config examples/configs/scheduling.yml \
  --schedule hourly_sync \
  --emit systemd
```

These helper outputs keep recurring invocation delegated to OS tooling or CI while ETLPlus owns the
portable schedule model.

## Run Due Schedules Once

Dispatch all currently due schedules one time:

```bash
etlplus schedule --config examples/configs/scheduling.yml --run-pending
```

Forward structured lifecycle events from the underlying `etlplus run` invocations:

```bash
etlplus schedule \
  --config examples/configs/scheduling.yml \
  --run-pending \
  --event-format jsonl
```

This mode is intentionally one-shot. The expected operating model is to invoke it from `cron`,
`systemd`, CI, or another external trigger rather than to keep a resident ETLPlus scheduler
process running continuously.

## Observability And State

`--run-pending` reuses the existing `etlplus run` execution path.

That means scheduled runs keep the same stable contracts:

- lifecycle events still use `etlplus.event.v1`
- local history still records the run through the same SQLite-default or JSONL fallback backend
- additive scheduler metadata is attached to events and persisted under
  `result_summary.scheduler`

The local scheduler also keeps minimal trigger state under `${ETLPLUS_STATE_DIR:-~/.etlplus}`:

- `scheduler-state.json` stores the last dispatched trigger per schedule
- `scheduler-locks/` prevents overlapping dispatch for the same schedule

## Backing-Service Posture

Scheduling does not change ETLPlus' backing-service model.

The same schedule surface can target local paths, managed databases, or remote object-storage URIs.
The example config uses `s3://...` and `azure-blob://...` endpoints deliberately to show that
remote backing services remain first-class.

Local filesystem paths, Docker Compose, localhost Postgres, or Adminer are still useful for
development, but they should be treated as convenience tooling rather than the canonical operating
model.

[scheduling.yml]: https://github.com/Dagitali/ETLPlus/blob/main/examples/configs/scheduling.yml
