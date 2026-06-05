# Runtime Command Contracts

Use this note when adding or changing execution-oriented commands such as `extract`, `load`, `run`,
`schedule --run-pending`, `transform`, or `validate`.

- [Runtime Command Contracts](#runtime-command-contracts)
  - [Required Contracts](#required-contracts)
  - [Test Expectations](#test-expectations)

## Required Contracts

- Keep human-oriented command output on STDOUT.
- Keep machine-readable `etlplus.event.v1` JSONL events on STDERR.
- Emit structured events only when `--event-format jsonl` or the command's equivalent structured
  event option is enabled.
- Preserve the shared event envelope: `schema`, `schema_version`, `event`, `command`, `lifecycle`,
  `run_id`, and `timestamp`.
- Reuse the existing run/history/telemetry contracts instead of creating a second persistence or
  observability shape.
- Keep additive scheduler, retry, provider, or plugin metadata under command-specific fields.
- Return conventional exit codes and keep actionable error messages stable enough for tests and
  operator docs.
- Run `check --readiness` and strict config diagnostics through shared policies where practical.
- Treat new runtime-facing commands, output modes, event fields, history fields, or config
  precedence changes as release-affecting.

## Test Expectations

Runtime-facing command changes should include focused coverage for:

- stdout/stderr separation
- structured event envelope shape
- success and failure lifecycle events
- persisted run/job history integration when the command executes work
- readiness or strict config diagnostics for new provider or connector metadata
- stable help text for public CLI options

Tests should stay proportionate. Prefer contract tests for shared behavior and narrow unit tests for
command-specific metadata.
