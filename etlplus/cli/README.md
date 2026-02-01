# `etlplus.cli` Subpackage

Documentation for the `etlplus.cli` subpackage: command-line interface for ETLPlus workflows.

- Provides a CLI for running ETL pipelines, jobs, and utilities
- Supports commands for extracting, transforming, validating data, and running pipelines
- Includes options for configuration, state, and output control
- Exposes handlers for custom command integration

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.cli` Subpackage](#etlpluscli-subpackage)
  - [Available Commands](#available-commands)
  - [Command Options](#command-options)
  - [Example: Running a Pipeline](#example-running-a-pipeline)
  - [See Also](#see-also)

## Available Commands

- **check**: Inspect pipeline configuration (jobs, sources, targets)
- **extract**: Extract data from files/APIs/databases
- **transform**: Transform records
- **load**: Load data to files/APIs/databases
- **render**: Render SQL DDL from table specs
- **validate**: Validate data against rules
- **run**: Execute a pipeline or job

## Command Options

Use `etlplus <command> --help` for the exact options supported by each command.

## Example: Running a Pipeline

```bash
etlplus run --config configs/pipeline.yml --job file_to_file_customers
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Command handlers in [handlers.py](handlers.py)
- Command options in [options.py](options.py)
