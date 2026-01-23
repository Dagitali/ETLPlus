# `etlplus.cli` Subpackage

Documentation for the `etlplus.cli` subpackage: command-line interface for ETLPlus workflows.

- Provides a CLI for running ETL pipelines, jobs, and utilities
- Supports commands for running, validating, and inspecting pipelines
- Includes options for configuration, state, and output control
- Exposes handlers for custom command integration

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.cli` Subpackage](#etlpluscli-subpackage)
  - [Available Commands](#available-commands)
  - [Command Options](#command-options)
  - [Example: Running a Pipeline](#example-running-a-pipeline)
  - [See Also](#see-also)

## Available Commands

- **run**: Execute a pipeline or job
- **validate**: Validate pipeline or config files
- **inspect**: Show pipeline/job details

## Command Options

- `--config`: Path to config file
- `--state`: Path to state file
- `--output`: Output file or format

## Example: Running a Pipeline

```bash
etlplus run --config configs/pipeline.yml --output results.json
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Command handlers in [handlers.py](handlers.py)
- Command options in [options.py](options.py)
