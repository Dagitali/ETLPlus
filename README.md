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
etlplus extract file in/input.csv --format csv \
  | etlplus transform - --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  -o output.json
```

[Python API](#python-api):

```python
from etlplus import extract, transform, validate, load

data = extract("file", "input.csv", format="csv")
ops = {"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}
filtered = transform(data, ops)
rules = {"name": {"type": "string", "required": True}, "email": {"type": "string", "required": True}}
assert validate(filtered, rules)["valid"]
load(filtered, "file", "output.json", format="json")
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

Extract from JSON file:
```bash
etlplus extract file data.json
```

Extract from CSV file:
```bash
etlplus extract file data.csv --format csv
```

Extract from XML file:
```bash
etlplus extract file data.xml --format xml
```

Extract from REST API:
```bash
etlplus extract api https://api.example.com/data
```

Save extracted data to file:
```bash
etlplus extract file data.csv --format csv -o output.json
```

#### Validate Data

Validate data from file or JSON string:
```bash
etlplus validate '{"name": "John", "age": 30}' --rules '{"name": {"type": "string", "required": true}, "age": {"type": "number", "min": 0, "max": 150}}'
```

Validate from file:
```bash
etlplus validate data.json --rules '{"email": {"type": "string", "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"}}'
```

#### Transform Data

Filter and select fields:
```bash
etlplus transform '[{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]' \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 26}, "select": ["name"]}'
```

Sort data:
```bash
etlplus transform data.json --operations '{"sort": {"field": "age", "reverse": true}}'
```

Aggregate data:
```bash
etlplus transform data.json --operations '{"aggregate": {"field": "price", "func": "sum"}}'
```

Map/rename fields:
```bash
etlplus transform data.json --operations '{"map": {"old_name": "new_name"}}'
```

#### Load Data

Load to JSON file:
```bash
etlplus load '{"name": "John", "age": 30}' file output.json
```

Load to CSV file:
```bash
etlplus load '[{"name": "John", "age": 30}]' file output.csv --format csv
```

Load to REST API:
```bash
etlplus load data.json api https://api.example.com/endpoint
```

### Python API

Use ETLPlus as a Python library:

```python
from etlplus import extract, validate, transform, load

# Extract data
data = extract("file", "data.json", format="json")

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
load(transformed, "file", "output.json", format="json")
```

### Complete ETL Pipeline Example

```bash
# 1. Extract from CSV
etlplus extract file input.csv --format csv -o extracted.json

# 2. Transform (filter and select fields)
etlplus transform extracted.json \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  -o transformed.json

# 3. Validate transformed data
etlplus validate transformed.json \
  --rules '{"name": {"type": "string", "required": true}, "email": {"type": "string", "required": true}}'

# 4. Load to CSV
etlplus load transformed.json file output.csv --format csv
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

Looking for the HTTP client and pagination helpers? See the dedicated docs in `etlplus/api/README.md` for:

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

- **Unit (`tests/unit/`)**: single function or class, no real I/O, fast, uses stubs/monkeypatch (e.g. `etlplus.cli.create_parser`, transform + validate helpers).
- **Integration (`tests/integration/`)**: end-to-end flows (CLI `main()`, pipeline `run()`, pagination + rate limit defaults, file/API connector interactions) may touch temp files and use fake clients.

If a test calls `etlplus.cli.main()` or `etlplus.run.run()` it’s integration by default. Full criteria: [`CONTRIBUTING.md#testing`](CONTRIBUTING.md#testing).

### Code Coverage

```bash
pytest tests/ --cov=etlplus --cov-report=html
```

### Linting

```bash
flake8 etlplus/
black etlplus/
```

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
