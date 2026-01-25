# ETLPlus

[![PyPI](https://img.shields.io/pypi/v/etlplus.svg)][PyPI package]
[![Release](https://img.shields.io/github/v/release/Dagitali/ETLPlus)][GitHub release]
[![Python](https://img.shields.io/pypi/pyversions/etlplus)][PyPI package]
[![License](https://img.shields.io/github/license/Dagitali/ETLPlus.svg)](LICENSE)
[![CI](https://github.com/Dagitali/ETLPlus/actions/workflows/ci.yml/badge.svg?branch=main)][GitHub Actions CI workflow]
[![Coverage](https://img.shields.io/codecov/c/github/Dagitali/ETLPlus?branch=main)][Codecov project]
[![Issues](https://img.shields.io/github/issues/Dagitali/ETLPlus)][GitHub issues]
[![PRs](https://img.shields.io/github/issues-pr/Dagitali/ETLPlus)][GitHub PRs]
[![GitHub contributors](https://img.shields.io/github/contributors/Dagitali/ETLPlus)][GitHub contributors]

ETLPlus is a veritable Swiss Army knife for enabling simple ETL operations, offering both a Python
package and command-line interface for data extraction, validation, transformation, and loading.

- [ETLPlus](#etlplus)
  - [Getting Started](#getting-started)
  - [Features](#features)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
  - [Data Connectors](#data-connectors)
    - [REST APIs (`api`)](#rest-apis-api)
    - [Databases (`database`)](#databases-database)
    - [Files (`file`)](#files-file)
      - [Stubbed / Placeholder](#stubbed--placeholder)
      - [Tabular \& Delimited Text](#tabular--delimited-text)
      - [Semi-Structured Text](#semi-structured-text)
      - [Columnar / Analytics-Friendly](#columnar--analytics-friendly)
      - [Binary Serialization and Interchange](#binary-serialization-and-interchange)
      - [Databases and Embedded Storage](#databases-and-embedded-storage)
      - [Spreadsheets](#spreadsheets)
      - [Statistical / Scientific / Numeric Computing](#statistical--scientific--numeric-computing)
      - [Logs and Event Streams](#logs-and-event-streams)
      - [Data Archives](#data-archives)
      - [Templates](#templates)
  - [Usage](#usage)
    - [Command Line Interface](#command-line-interface)
      - [Argument Order and Required Options](#argument-order-and-required-options)
      - [Check Pipelines](#check-pipelines)
      - [Render SQL DDL](#render-sql-ddl)
      - [Extract Data](#extract-data)
      - [Validate Data](#validate-data)
      - [Transform Data](#transform-data)
      - [Load Data](#load-data)
    - [Python API](#python-api)
    - [Complete ETL Pipeline Example](#complete-etl-pipeline-example)
    - [Format Overrides](#format-overrides)
  - [Transformation Operations](#transformation-operations)
    - [Filter Operations](#filter-operations)
    - [Aggregation Functions](#aggregation-functions)
  - [Validation Rules](#validation-rules)
  - [Development](#development)
    - [API Client Docs](#api-client-docs)
    - [Runner Internals and Connectors](#runner-internals-and-connectors)
    - [Running Tests](#running-tests)
      - [Test Layers](#test-layers)
    - [Code Coverage](#code-coverage)
    - [Linting](#linting)
    - [Updating Demo Snippets](#updating-demo-snippets)
    - [Releasing to PyPI](#releasing-to-pypi)
  - [License](#license)
  - [Contributing](#contributing)
  - [Documentation](#documentation)
    - [Python Packages/Subpackage](#python-packagessubpackage)
    - [Community Health](#community-health)
    - [Other](#other)
  - [Acknowledgments](#acknowledgments)

## Getting Started

ETLPlus helps you extract, validate, transform, and load data from files, databases, and APIs, either
as a Python library or from the command line.

To get started:

- See [Installation](#installation) for setup instructions.
- Try the [Quickstart](#quickstart) for a minimal working example (CLI and Python).
- Explore [Usage](#usage) for more detailed options and workflows.

ETLPlus supports Python 3.13 and above.

## Features

- **Check** data pipeline definitions before running them:
  - Summarize jobs, sources, targets, and transforms
  - Confirm configuration changes by printing focused sections on demand

- **Render** SQL DDL from shared table specs:
  - Generate CREATE TABLE or view statements
  - Swap templates or direct output to files for database migrations

- **Extract** data from multiple sources:
  - Files (CSV, JSON, XML, YAML)
  - Databases (connection string support)
  - REST APIs (GET)

- **Validate** data with flexible rules:
  - Type checking
  - Required fields
  - Value ranges (min/max)
  - String length constraints
  - Pattern matching
  - Enum validation

- **Transform** data with powerful operations:
  - Filter records
  - Map/rename fields
  - Select specific fields
  - Sort data
  - Aggregate functions (avg, count, max, min, sum)

- **Load** data to multiple targets:
  - Files (CSV, JSON, XML, YAML)
  - Databases (connection string support)
  - REST APIs (PATCH, POST, PUT)

## Installation

```bash
pip install etlplus
```

For development:

```bash
pip install -e ".[dev]"
```

## Quickstart

Get up and running in under a minute.

[Command line interface](#command-line-interface):

```bash
# Inspect help and version
etlplus --help
etlplus --version

# One-liner: extract CSV, filter, select, and write JSON
etlplus extract file examples/data/sample.csv \
  | etlplus transform --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  - temp/sample_output.json
```

[Python API](#python-api):

```python
from etlplus.ops import extract, transform, validate, load

data = extract("file", "input.csv")
ops = {"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}
filtered = transform(data, ops)
rules = {"name": {"type": "string", "required": True}, "email": {"type": "string", "required": True}}
assert validate(filtered, rules)["valid"]
load(filtered, "file", "temp/sample_output.json", file_format="json")
```

## Data Connectors

Data connectors abstract sources from which to extract data and targets to which to load data. They
are differentiated by their types, each of which is represented in the subsections below.

### REST APIs (`api`)

ETLPlus can extract from REST APIs and load results via common HTTP methods. Supported operations
include GET for extract and PATCH/POST/PUT for load.

### Databases (`database`)

Database connectors use connection strings for extraction and loading, and
DDL can be rendered from table specs for migrations or schema checks.

### Files (`file`)

Recognized file formats are listed in the tables below. Support for reading to or writing from a recognized file format is marked as:

- **Y**: implemented (may require optional dependencies)
- **N**: stubbed or not yet implemented

#### Stubbed / Placeholder

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `stub` | N | Placeholder format for tests and future connectors. |

#### Tabular & Delimited Text

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `csv` | Y | Y | Comma-Separated Values |
| `dat` | N | N | Generic data file, often delimited or fixed-width |
| `fwf` | N | N | Fixed-Width Fields |
| `psv` | N | N | Pipe-Separated Values |
| `tab` | N | N | Often synonymous with TSV |
| `tsv` | Y | Y | Tab-Separated Values |
| `txt` | Y | Y | Plain text, often delimited or fixed-width |

#### Semi-Structured Text

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `cfg` | N | N | Config-style key-value pairs |
| `conf` | N | N | Config-style key-value pairs |
| `ini` | N | N | Config-style key-value pairs |
| `json` | Y | Y | JavaScript Object Notation |
| `ndjson` | Y | Y | Newline-Delimited JSON |
| `properties` | N | N | Java-style key-value pairs |
| `toml` | N | N | Tom's Obvious Minimal Language |
| `xml` | Y | Y | Extensible Markup Language |
| `yaml` | Y | Y | YAML Ain't Markup Language |

#### Columnar / Analytics-Friendly

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `arrow` | N | N | Apache Arrow IPC |
| `feather` | Y | Y | Apache Arrow Feather |
| `orc` | Y | Y | Optimized Row Columnar; common in Hadoop |
| `parquet` | Y | Y | Apache Parquet; common in Big Data |

#### Binary Serialization and Interchange

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `avro` | Y | Y | Apache Avro |
| `bson` | N | N | Binary JSON; common with MongoDB exports/dumps |
| `cbor` | N | N | Concise Binary Object Representation |
| `ion` | N | N | Amazon Ion |
| `msgpack` | N | N | MessagePack |
| `pb` | N | N | Protocol Buffers (Google Protobuf) |
| `pbf` | N | N | Protocolbuffer Binary Format; often for GIS data |
| `proto` | N | N | Protocol Buffers schema; often in .pb / .bin |

#### Databases and Embedded Storage

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `accdb` | N | N | Microsoft Access (newer format) |
| `duckdb` | N | N | DuckDB |
| `mdb` | N | N | Microsoft Access (older format) |
| `sqlite` | N | N | SQLite |

#### Spreadsheets

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `numbers` | N | N | Apple Numbers |
| `ods` | N | N | OpenDocument |
| `wks` | N | N | Lotus 1-2-3  |
| `xls` | Y | Y | Microsoft Excel (BIFF) |
| `xlsm` | N | N | Microsoft Excel Macro-Enabled (Open XML) |
| `xlsx` | Y | Y | Microsoft Excel (Open XML) |

#### Statistical / Scientific / Numeric Computing

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `dta` | N | N | Stata |
| `hdf5` | N | N | Hierarchical Data Format |
| `mat` | N | N | MATLAB |
| `nc` | N | N | NetCDF |
| `rda` | N | N | RData workspace/object |
| `rds` | N | N | R data |
| `sas7bdat` | N | N | SAS data |
| `sav` | N | N | SPSS data |
| `sylk` | N | N | Symbolic Link |
| `xpt` | N | N | SAS Transport |
| `zsav` | N | N | Compressed SPSS data |

#### Logs and Event Streams

| Format | Supported | Description |
| --- | --- | --- |
| `log` | N | N | Generic log file |

#### Data Archives

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `gz` | Y | Y | Gzip-compressed file |
| `zip` | Y | Y | ZIP archive |

#### Templates

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `hbs` | N | N | Handlebars |
| `jinja2` | N | N | Jinja2 |
| `mustache` | N | N | Mustache |
| `vm` | N | N | Apache Velocity |

## Usage

### Command Line Interface

ETLPlus provides a powerful CLI for ETL operations:

```bash
# Show help
etlplus --help

# Show version
etlplus --version
```

The CLI is implemented with Typer (Click-based). There is no argparse compatibility layer, so rely
on the documented commands/flags and run `etlplus <command> --help` for current options.

**Example error messages:**

- If you omit a required argument: `Error: Missing required argument 'SOURCE'.`
- If you place an option before its argument: `Error: Option '--source-format' must follow the 'SOURCE' argument.`

#### Argument Order and Required Options

For each command, positional arguments must precede options. Required options must follow their
associated argument:

- **extract**: `etlplus extract SOURCE [--source-format ...] [--source-type ...]`
  - `SOURCE` is required. `--source-format` and `--source-type` must follow `SOURCE`.
- **transform**: `etlplus transform [--operations ...] SOURCE [--source-format ...] [--source-type ...] TARGET [--target-format ...] [--target-type ...]`
  - `SOURCE` and `TARGET` are required. Format/type options must follow their respective argument.
- **load**: `etlplus load TARGET [--target-format ...] [--target-type ...] [--source-format ...]`
  - `TARGET` is required. `--target-format` and `--target-type` must follow `TARGET`.
- **validate**: `etlplus validate SOURCE [--rules ...] [--source-format ...] [--source-type ...]`
  - `SOURCE` is required. `--rules` and format/type options must follow `SOURCE`.

If required arguments or options are missing, or if options are placed before their associated argument, the CLI will display a clear error message.

#### Check Pipelines

Use `etlplus check` to explore pipeline YAML definitions without running them. The command can print
job names, summarize configured sources and targets, or drill into specific sections.

List jobs and show a pipeline summary:
```bash
etlplus check --config examples/configs/pipeline.yml --jobs
etlplus check --config examples/configs/pipeline.yml --summary
```

Show sources or transforms for troubleshooting:
```bash
etlplus check --config examples/configs/pipeline.yml --sources
etlplus check --config examples/configs/pipeline.yml --transforms
```

#### Render SQL DDL

Use `etlplus render` to turn table schema specs into ready-to-run SQL. Render from a pipeline config
or from a standalone schema file, and choose the built-in `ddl` or `view` templates (or provide your
own).

Render all tables defined in a pipeline:
```bash
etlplus render --config examples/configs/pipeline.yml --template ddl
```

Render a single table in that pipeline:
```bash
etlplus render --config examples/configs/pipeline.yml --table customers --template view
```

Render from a standalone table spec to a file:
```bash
etlplus render --spec schemas/customer.yml --template view -o temp/customer_view.sql
```

#### Extract Data

Note: For file sources, the format is normally inferred from the filename extension. Use
`--source-format` to override inference when a file lacks an extension or when you want to force a
specific parser.

Extract from JSON file:
```bash
etlplus extract file examples/data/sample.json
```

Extract from CSV file:
```bash
etlplus extract file examples/data/sample.csv
```

Extract from XML file:
```bash
etlplus extract file examples/data/sample.xml
```

Extract from REST API:
```bash
etlplus extract api https://api.example.com/data
```

Save extracted data to file:
```bash
etlplus extract file examples/data/sample.csv > temp/sample_output.json
```

#### Validate Data

Validate data from file or JSON string:
```bash
etlplus validate '{"name": "John", "age": 30}' --rules '{"name": {"type": "string", "required": true}, "age": {"type": "number", "min": 0, "max": 150}}'
```

Validate from file:
```bash
etlplus validate examples/data/sample.json --rules '{"email": {"type": "string", "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"}}'
```

#### Transform Data

When piping data through `etlplus transform`, use `--source-format` whenever the SOURCE argument is
`-` or a literal payload, mirroring the `etlplus extract` semantics. Use `--target-format` to
control the emitted format for STDOUT or other non-file outputs, just like `etlplus load`. File
paths continue to infer formats from their extensions. Use `--source-type` to override the inferred
source connector type and `--target-type` to override the inferred target connector type, matching
the `etlplus extract`/`etlplus load` behavior.

Transform file inputs while overriding connector types:
```bash
etlplus transform \
  --operations '{"select": ["name", "email"]}' \
  examples/data/sample.json  --source-type file \
  temp/selected_output.json --target-type file
```

Filter and select fields:
```bash
etlplus transform \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 26}, "select": ["name"]}' \
  '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]'
```

Sort data:
```bash
etlplus transform \
  --operations '{"sort": {"field": "age", "reverse": true}}' \
  examples/data/sample.json
```

Aggregate data:
```bash
etlplus transform \
  --operations '{"aggregate": {"field": "age", "func": "sum"}}' \
  examples/data/sample.json
```

Map/rename fields:
```bash
etlplus transform \
  --operations '{"map": {"name": "new_name"}}' \
  examples/data/sample.json
```

#### Load Data

`etlplus load` consumes JSON from STDIN; provide only the target argument plus optional flags.

Load to JSON file:
```bash
etlplus extract file examples/data/sample.json \
  | etlplus load temp/sample_output.json --target-type file
```

Load to CSV file:
```bash
etlplus extract file examples/data/sample.csv \
  | etlplus load temp/sample_output.csv --target-type file
```

Load to REST API:
```bash
cat examples/data/sample.json \
  | etlplus load https://api.example.com/endpoint --target-type api
```

### Python API

Use ETLPlus as a Python library:

```python
from etlplus.ops import extract, validate, transform, load

# Extract data
data = extract("file", "data.json")

# Validate data
validation_rules = {
    "name": {"type": "string", "required": True},
    "age": {"type": "number", "min": 0, "max": 150}
}
result = validate(data, validation_rules)
if result["valid"]:
    print("Data is valid!")

# Transform data
operations = {
    "filter": {"field": "age", "op": "gt", "value": 18},
    "select": ["name", "email"]
}
transformed = transform(data, operations)

# Load data
load(transformed, "file", "temp/sample_output.json", format="json")
```

For YAML-driven pipelines executed end-to-end (extract → validate → transform → load), see:

- Authoring: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Runner API and internals: [`docs/run-module.md`](docs/run-module.md)

CLI quick reference for pipelines:

```bash
# List jobs or show a pipeline summary
etlplus check --config examples/configs/pipeline.yml --jobs
etlplus check --config examples/configs/pipeline.yml --summary

# Run a job
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers
```

### Complete ETL Pipeline Example

```bash
# 1. Extract from CSV
etlplus extract file examples/data/sample.csv > temp/sample_extracted.json

# 2. Transform (filter and select fields)
etlplus transform \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  temp/sample_extracted.json \
  temp/sample_transformed.json

# 3. Validate transformed data
etlplus validate \
  --rules '{"name": {"type": "string", "required": true}, "email": {"type": "string", "required": true}}' \
  temp/sample_transformed.json

# 4. Load to CSV
cat temp/sample_transformed.json \
  | etlplus load temp/sample_output.csv
```

### Format Overrides

`--source-format` and `--target-format` override whichever format would normally be inferred from a
file extension. This is useful when an input lacks an extension (for example, `records.txt` that
actually contains CSV) or when you intentionally want to treat a file as another format.

Examples (zsh):

```zsh
# Force CSV parsing for an extension-less file
etlplus extract data.txt --source-type file --source-format csv

# Write CSV to a file without the .csv suffix
etlplus load output.bin --target-type file --target-format csv < data.json

# Leave the flags off when extensions already match the desired format
etlplus extract data.csv --source-type file
etlplus load data.json --target-type file  < data.json
```

## Transformation Operations

### Filter Operations

Supported operators:
- `eq`: Equal
- `ne`: Not equal
- `gt`: Greater than
- `gte`: Greater than or equal
- `lt`: Less than
- `lte`: Less than or equal
- `in`: Value in list
- `contains`: List/string contains value

Example:
```json
{
  "filter": {
    "field": "status",
    "op": "in",
    "value": ["active", "pending"]
  }
}
```

### Aggregation Functions

Supported functions:
- `sum`: Sum of values
- `avg`: Average of values
- `min`: Minimum value
- `max`: Maximum value
- `count`: Count of values

Example:
```json
{
  "aggregate": {
    "field": "revenue",
    "func": "sum"
  }
}
```

## Validation Rules

Supported validation rules:
- `type`: Data type (string, number, integer, boolean, array, object)
- `required`: Field is required (true/false)
- `min`: Minimum value for numbers
- `max`: Maximum value for numbers
- `minLength`: Minimum length for strings
- `maxLength`: Maximum length for strings
- `pattern`: Regex pattern for strings
- `enum`: List of allowed values

Example:
```json
{
  "email": {
    "type": "string",
    "required": true,
    "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"
  },
  "age": {
    "type": "number",
    "min": 0,
    "max": 150
  },
  "status": {
    "type": "string",
    "enum": ["active", "inactive", "pending"]
  }
}
```

## Development

### API Client Docs

Looking for the HTTP client and pagination helpers?  See the dedicated docs in
`etlplus/api/README.md` for:

- Quickstart with `EndpointClient`
- Authentication via `EndpointCredentialsBearer`
- Pagination with `PaginationConfig` (page and cursor styles)
- Tips on `records_path` and `cursor_path`

### Runner Internals and Connectors

Curious how the pipeline runner composes API requests, pagination, and load calls?

- Runner overview and helpers: [`docs/run-module.md`](docs/run-module.md)
- Unified "connector" vocabulary (API/File/DB): `etlplus/config/connector.py`
  - API/file targets reuse the same shapes as sources; API targets typically set a `method`.

### Running Tests

```bash
pytest tests/ -v
```

#### Test Layers

We split tests into two layers:

- **Unit (`tests/unit/`)**: single function or class, no real I/O, fast, uses stubs/monkeypatch
  (e.g.  `etlplus.cli.create_parser`, transform + validate helpers).
- **Integration (`tests/integration/`)**: end-to-end flows (CLI `main()`, pipeline `run()`,
  pagination + rate limit defaults, file/API connector interactions) may touch temp files and use
  fake clients.

If a test calls `etlplus.cli.main()` or `etlplus.ops.run.run()` it’s integration by default. Full
criteria: [`CONTRIBUTING.md#testing`](CONTRIBUTING.md#testing).

### Code Coverage

```bash
pytest tests/ --cov=etlplus --cov-report=html
```

### Linting

```bash
flake8 etlplus/
black etlplus/
```

### Updating Demo Snippets

`DEMO.md` shows the real output of `etlplus --version` captured from a freshly built wheel. Regenerate
the snippet (and the companion file [docs/snippets/installation_version.md](docs/snippets/installation_version.md)) after changing anything that affects the version string:

```bash
make demo-snippets
```

The helper script in [tools/update_demo_snippets.py](tools/update_demo_snippets.py) builds the wheel,
installs it into a throwaway virtual environment, runs `etlplus --version`, and rewrites the snippet
between the markers in [DEMO.md](DEMO.md).

### Releasing to PyPI

`setuptools-scm` derives the package version from Git tags, so publishing is now entirely tag
driven—no hand-editing `pyproject.toml`, `setup.py`, or `etlplus/__version__.py`.

1. Ensure `main` is green and the changelog/docs are up to date.
2. Create and push a SemVer tag matching the `v*.*.*` pattern:

```bash
git tag -a v1.4.0 -m "Release v1.4.0"
git push origin v1.4.0
```

3. GitHub Actions fetches tags, builds the sdist/wheel, and publishes to PyPI via the `publish` job
   in [.github/workflows/ci.yml](.github/workflows/ci.yml).

If you want an extra smoke-test before tagging, run `make dist && pip install dist/*.whl` locally;
this exercises the same build path the workflow uses.

## License

This project is licensed under the [MIT License](LICENSE).

## Contributing

Code and codeless contributions are welcome!  If you’d like to add a new feature, fix a bug, or
improve the documentation, please feel free to submit a pull request as follows:

1. Fork this repository.
2. Create a new feature branch for your changes (`git checkout -b feature/feature-name`).
3. Commit your changes (`git commit -m "Add feature"`).
4. Push to your branch (`git push origin feature-name`).
5. Submit a pull request with a detailed description.

If you choose to be a code contributor, please first refer these documents:

- Pipeline authoring guide: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Design notes (Mapping inputs, dict outputs):
  [`docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`](docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs)
- Typing philosophy (TypedDicts as editor hints, permissive runtime):
  [`CONTRIBUTING.md#typing-philosophy`](CONTRIBUTING.md#typing-philosophy)

## Documentation

### Python Packages/Subpackage

Navigate to detailed documentation for each subpackage:

- [etlplus.api](etlplus/api/README.md): Lightweight HTTP client and paginated REST helpers
- [etlplus.file](etlplus/file/README.md): Unified file format support and helpers
- [etlplus.cli](etlplus/cli/README.md): Command-line interface definitions for `etlplus`
- [etlplus.database](etlplus/database/README.md): Database engine, schema, and ORM helpers
- [etlplus.templates](etlplus/templates/README.md): SQL and DDL template helpers
- [etlplus.validation](etlplus/validation/README.md): Data validation utilities and helpers
- [etlplus.workflow](etlplus/workflow/README.md): Helpers for data connectors, pipelines, jobs, and
  profiles

### Community Health

- [Contributing Guidelines](CONTRIBUTING.md): How to contribute, report issues, and submit PRs
- [Code of Conduct](CODE_OF_CONDUCT.md): Community standards and expectations
- [Security Policy](SECURITY.md): Responsible disclosure and vulnerability reporting
- [Support](SUPPORT.md): Where to get help

### Other

- API client docs: [`etlplus/api/README.md`](etlplus/api/README.md)
- Examples: [`examples/README.md`](examples/README.md)
- Pipeline authoring guide: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Runner internals: [`docs/run-module.md`](docs/run-module.md)
- Design notes (Mapping inputs, dict outputs): [`docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`](docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs)
- Typing philosophy: [`CONTRIBUTING.md#typing-philosophy`](CONTRIBUTING.md#typing-philosophy)
- Demo and walkthrough: [`DEMO.md`](DEMO.md)
- Additional references: [`REFERENCES.md`](REFERENCES.md)

## Acknowledgments

ETLPlus is inspired by common work patterns in data engineering and software engineering patterns in
Python development, aiming to increase productivity and reduce boilerplate code.  Feedback and
contributions are always appreciated!

[Codecov project]: https://codecov.io/github/Dagitali/ETLPlus?branch=main
[GitHub Actions CI workflow]: https://github.com/Dagitali/ETLPlus/actions/workflows/ci.yml
[GitHub contributors]: https://github.com/Dagitali/ETLPlus/graphs/contributors
[GitHub issues]: https://github.com/Dagitali/ETLPlus/issues
[GitHub PRs]: https://github.com/Dagitali/ETLPlus/pulls
[GitHub release]: https://github.com/Dagitali/ETLPlus/releases
[PyPI package]: https://pypi.org/project/etlplus/
