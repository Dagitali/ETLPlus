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
  - [Features](#features)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
  - [Usage](#usage)
    - [Command Line Interface](#command-line-interface)
      - [Extract Data](#extract-data)
      - [Validate Data](#validate-data)
      - [Transform Data](#transform-data)
      - [Load Data](#load-data)
    - [Python API](#python-api)
    - [Complete ETL Pipeline Example](#complete-etl-pipeline-example)
    - [Environment Variables](#environment-variables)
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
  - [Links](#links)
  - [License](#license)
  - [Contributing](#contributing)
  - [Acknowledgments](#acknowledgments)

## Features

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
  | etlplus transform - --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  -o temp/sample_output.json
```

[Python API](#python-api):

```python
from etlplus import extract, transform, validate, load

data = extract("file", "input.csv")
ops = {"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}
filtered = transform(data, ops)
rules = {"name": {"type": "string", "required": True}, "email": {"type": "string", "required": True}}
assert validate(filtered, rules)["valid"]
load(filtered, "file", "temp/sample_output.json", file_format="json")
```

## Usage

### Command Line Interface

ETLPlus provides a powerful CLI for ETL operations:

```bash
# Show help
etlplus --help

# Show version
etlplus --version
```

#### Extract Data

Note: For file sources, the format is inferred from the filename extension; the `--format` option is
ignored.  To treat passing `--format` as an error for file sources, either set
`ETLPLUS_FORMAT_BEHAVIOR=error` or pass the CLI flag `--strict-format`.

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
etlplus extract file examples/data/sample.csv -o temp/sample_output.json
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

Filter and select fields:
```bash
etlplus transform '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]' \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 26}, "select": ["name"]}'
```

Sort data:
```bash
etlplus transform examples/data/sample.json --operations '{"sort": {"field": "age", "reverse": true}}'
```

Aggregate data:
```bash
etlplus transform examples/data/sample.json --operations '{"aggregate": {"field": "age", "func": "sum"}}'
```

Map/rename fields:
```bash
etlplus transform examples/data/sample.json --operations '{"map": {"name": "new_name"}}'
```

#### Load Data

Load to JSON file:
```bash
etlplus load '{"name": "John", "age": 30}' file temp/sample_output.json
```

Load to CSV file:
```bash
etlplus load '[{"name": "John", "age": 30}]' file temp/sample_output.csv
```

Load to REST API:
```bash
etlplus load examples/data/sample.json api https://api.example.com/endpoint
```

### Python API

Use ETLPlus as a Python library:

```python
from etlplus import extract, validate, transform, load

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

### Complete ETL Pipeline Example

```bash
# 1. Extract from CSV
etlplus extract file examples/data/sample.csv -o temp/sample_extracted.json

# 2. Transform (filter and select fields)
etlplus transform temp/sample_extracted.json \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  -o temp/sample_transformed.json

# 3. Validate transformed data
etlplus validate temp/sample_transformed.json \
  --rules '{"name": {"type": "string", "required": true}, "email": {"type": "string", "required": true}}'

# 4. Load to CSV
etlplus load temp/sample_transformed.json file temp/sample_output.csv
```

### Environment Variables

ETLPlus honors a small number of environment toggles to refine CLI behavior:

- `ETLPLUS_FORMAT_BEHAVIOR`: controls what happens when `--format` is provided for
  file sources or targets (extract/load) where the format is inferred from the
  filename extension.
  - `error|fail|strict`: treat as error (non-zero exit)
  - `warn` (default): print a warning to stderr
  - `ignore|silent`: no message
- Precedence: the CLI flag `--strict-format` overrides the environment.

Examples (zsh):

```zsh
# Warn (default)
etlplus extract file data.csv --format csv
etlplus load data.json file out.csv --format csv

# Enforce error via environment
ETLPLUS_FORMAT_BEHAVIOR=error \
  etlplus extract file data.csv --format csv
ETLPLUS_FORMAT_BEHAVIOR=error \
  etlplus load data.json file out.csv --format csv

# Equivalent strict behavior via flag (overrides environment)
etlplus extract file data.csv --format csv --strict-format
etlplus load data.json file out.csv --format csv --strict-format

# Recommended: rely on extension, no --format needed for files
etlplus extract file data.csv
etlplus load data.json file out.csv
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

If a test calls `etlplus.cli.main()` or `etlplus.run.run()` it’s integration by default.  Full
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

## Links

- API client docs: [`etlplus/api/README.md`](etlplus/api/README.md)
- Examples: [`examples/README.md`](examples/README.md)
- Pipeline authoring guide: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Runner internals: [`docs/run-module.md`](docs/run-module.md)
- Design notes (Mapping inputs, dict outputs): [`docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`](docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs)
- Typing philosophy: [`CONTRIBUTING.md#typing-philosophy`](CONTRIBUTING.md#typing-philosophy)
- Demo and walkthrough: [`DEMO.md`](DEMO.md)
- Additional references: [`REFERENCES.md`](`REFERENCES.md)

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
