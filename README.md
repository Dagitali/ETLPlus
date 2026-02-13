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
      - [Handler Matrix Guardrail](#handler-matrix-guardrail)
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
  - Databases (connection string support; extract is a placeholder today)
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
  - Databases (connection string support; load is a placeholder today)
  - REST APIs (PATCH, POST, PUT)

## Installation

```bash
pip install etlplus
```

For development:

```bash
pip install -e ".[dev]"
```

For full file-format support (optional extras):

```bash
pip install -e ".[file]"
```

## Quickstart

Get up and running in under a minute.

[Command line interface](#command-line-interface):

```bash
# Inspect help and version
etlplus --help
etlplus --version

# One-liner: extract CSV, filter, select, and write JSON
etlplus extract examples/data/sample.csv \
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

Database connectors use connection strings for extraction and loading, and DDL can be rendered from
table specs for migrations or schema checks. Database extract/load operations are currently
placeholders; plan to integrate a database client in your runner.

### Files (`file`)

Recognized file formats are listed in the tables below. Support for reading to or writing from a recognized file format is marked as:

- **Y**: implemented (may require optional dependencies)
- **N**: stubbed or not yet implemented

**Handler Architecture**

- File IO is moving to class-based handlers rooted at `etlplus/file/base.py` (`FileHandlerABC`,
  category ABCs, and `ReadOnlyFileHandlerABC`).
- `etlplus/file/registry.py` resolves handlers using an explicit `FileFormat -> handler class` map.
- Dispatch is explicit-only: unmapped formats raise `Unsupported format`.
- Placeholder handlers are split into:
  - `etlplus/file/stub.py` for generic stub behavior
  - `etlplus/file/_stub_categories.py` for category-aware internal stub ABCs
- Scientific/statistical handlers `dta`, `nc`, `rda`, `rds`, `sav`, and `xpt` now implement
  `ScientificDatasetFileHandlerABC` dataset hooks.

**Current Migration Coverage (Class-Based + Explicit Registry Mapping)**

- Delimited/text: `csv`, `dat`, `fwf`, `psv`, `tab`, `tsv`, `txt`
- Semi-structured/config: `ini`, `json`, `ndjson`, `properties`, `toml`, `xml`, `yaml`
- Columnar: `arrow`, `feather`, `orc`, `parquet`
- Binary/interchange: `avro`, `bson`, `cbor`, `msgpack`, `pb`, `proto`
- Embedded DB: `duckdb`, `sqlite`
- Spreadsheets: `ods`, `xls`, `xlsm`, `xlsx`
- Scientific/statistical: `dta`, `nc`, `rda`, `rds`, `sav`, `xpt`, `sas7bdat` (read-only), plus
  single-dataset scientific stubs `mat`, `sylk`, `zsav`
- Archive wrappers: `gz`, `zip`
- Log/event streams: `log`
- Templates: `hbs`, `jinja2`, `mustache`, `vm`
- Explicit module-owned stub handlers (via `stub.py` + `_stub_categories.py`): `stub`, `accdb`,
  `cfg`, `conf`, `ion`, `mdb`, `numbers`, `pbf`, `wks`

#### Handler Matrix Guardrail

The concise matrix below is the migration guardrail for class-based handler coverage. For
batch-by-batch maintenance notes and the same matrix in docs, see
[docs/file-handler-matrix.md](docs/file-handler-matrix.md).

| Format | Handler Class | Base ABC | Read/Write Support | Status |
| --- | --- | --- | --- | --- |
| `accdb` | `AccdbFile` | `StubEmbeddedDatabaseFileHandlerABC` | `read/write` | `stub` |
| `arrow` | `ArrowFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `avro` | `AvroFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `bson` | `BsonFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `cbor` | `CborFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `cfg` | `CfgFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `conf` | `ConfFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `csv` | `CsvFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `dat` | `DatFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `dta` | `DtaFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `duckdb` | `DuckdbFile` | `EmbeddedDatabaseFileHandlerABC` | `read/write` | `implemented` |
| `feather` | `FeatherFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `fwf` | `FwfFile` | `TextFixedWidthFileHandlerABC` | `read/write` | `implemented` |
| `gz` | `GzFile` | `ArchiveWrapperFileHandlerABC` | `read/write` | `implemented` |
| `hbs` | `HbsFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `hdf5` | `Hdf5File` | `ScientificDatasetFileHandlerABC` | `read-only` | `implemented` |
| `ini` | `IniFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `ion` | `IonFile` | `StubSemiStructuredTextFileHandlerABC` | `read/write` | `stub` |
| `jinja2` | `Jinja2File` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `json` | `JsonFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `log` | `LogFile` | `LogEventFileHandlerABC` | `read/write` | `implemented` |
| `mat` | `MatFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |
| `mdb` | `MdbFile` | `StubEmbeddedDatabaseFileHandlerABC` | `read/write` | `stub` |
| `msgpack` | `MsgpackFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `mustache` | `MustacheFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `nc` | `NcFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `ndjson` | `NdjsonFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `numbers` | `NumbersFile` | `StubSpreadsheetFileHandlerABC` | `read/write` | `stub` |
| `ods` | `OdsFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `orc` | `OrcFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `parquet` | `ParquetFile` | `ColumnarFileHandlerABC` | `read/write` | `implemented` |
| `pb` | `PbFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `pbf` | `PbfFile` | `StubBinarySerializationFileHandlerABC` | `read/write` | `stub` |
| `properties` | `PropertiesFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `proto` | `ProtoFile` | `BinarySerializationFileHandlerABC` | `read/write` | `implemented` |
| `psv` | `PsvFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `rda` | `RdaFile` | `ScientificDatasetFileHandlerABC` | `read/write` | `implemented` |
| `rds` | `RdsFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `sas7bdat` | `Sas7bdatFile` | `SingleDatasetScientificFileHandlerABC` | `read-only` | `implemented` |
| `sav` | `SavFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `sqlite` | `SqliteFile` | `EmbeddedDatabaseFileHandlerABC` | `read/write` | `implemented` |
| `stub` | `StubFile` | `StubFileHandlerABC` | `read/write` | `stub` |
| `sylk` | `SylkFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |
| `tab` | `TabFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `toml` | `TomlFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `tsv` | `TsvFile` | `DelimitedTextFileHandlerABC` | `read/write` | `implemented` |
| `txt` | `TxtFile` | `TextFixedWidthFileHandlerABC` | `read/write` | `implemented` |
| `vm` | `VmFile` | `TemplateFileHandlerABC` | `read/write` | `implemented` |
| `wks` | `WksFile` | `StubSpreadsheetFileHandlerABC` | `read/write` | `stub` |
| `xls` | `XlsFile` | `ReadOnlySpreadsheetFileHandlerABC` | `read-only` | `implemented` |
| `xlsm` | `XlsmFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `xlsx` | `XlsxFile` | `SpreadsheetFileHandlerABC` | `read/write` | `implemented` |
| `xml` | `XmlFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `xpt` | `XptFile` | `SingleDatasetScientificFileHandlerABC` | `read/write` | `implemented` |
| `yaml` | `YamlFile` | `SemiStructuredTextFileHandlerABC` | `read/write` | `implemented` |
| `zip` | `ZipFile` | `ArchiveWrapperFileHandlerABC` | `read/write` | `implemented` |
| `zsav` | `ZsavFile` | `StubSingleDatasetScientificFileHandlerABC` | `read/write` | `stub` |

#### Stubbed / Placeholder

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `stub` | N | Placeholder format for tests and future connectors. |

#### Tabular & Delimited Text

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `csv` | Y | Y | Comma-Separated Values |
| `dat` | Y | Y | Generic data file, often delimited or fixed-width |
| `fwf` | Y | Y | Fixed-Width Fields |
| `psv` | Y | Y | Pipe-Separated Values |
| `tab` | Y | Y | Often synonymous with TSV |
| `tsv` | Y | Y | Tab-Separated Values |
| `txt` | Y | Y | Plain text, often delimited or fixed-width |

#### Semi-Structured Text

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `cfg` | N | N | Config-style key-value pairs |
| `conf` | N | N | Config-style key-value pairs |
| `ini` | Y | Y | Config-style key-value pairs |
| `json` | Y | Y | JavaScript Object Notation |
| `ndjson` | Y | Y | Newline-Delimited JSON |
| `properties` | Y | Y | Java-style key-value pairs |
| `toml` | Y | Y | Tom's Obvious Minimal Language |
| `xml` | Y | Y | Extensible Markup Language |
| `yaml` | Y | Y | YAML Ain't Markup Language |

#### Columnar / Analytics-Friendly

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `arrow` | Y | Y | Apache Arrow IPC |
| `feather` | Y | Y | Apache Arrow Feather |
| `orc` | Y | Y | Optimized Row Columnar; common in Hadoop |
| `parquet` | Y | Y | Apache Parquet; common in Big Data |

#### Binary Serialization and Interchange

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `avro` | Y | Y | Apache Avro |
| `bson` | Y | Y | Binary JSON; common with MongoDB exports/dumps |
| `cbor` | Y | Y | Concise Binary Object Representation |
| `ion` | N | N | Amazon Ion |
| `msgpack` | Y | Y | MessagePack |
| `pb` | Y | Y | Protocol Buffers (Google Protobuf) |
| `pbf` | N | N | Protocolbuffer Binary Format; often for GIS data |
| `proto` | Y | Y | Protocol Buffers schema; often in .pb / .bin |

#### Databases and Embedded Storage

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `accdb` | N | N | Microsoft Access (newer format) |
| `duckdb` | Y | Y | DuckDB |
| `mdb` | N | N | Microsoft Access (older format) |
| `sqlite` | Y | Y | SQLite |

#### Spreadsheets

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `numbers` | N | N | Apple Numbers |
| `ods` | Y | Y | OpenDocument |
| `wks` | N | N | Lotus 1-2-3  |
| `xls` | Y | N | Microsoft Excel (BIFF; read-only) |
| `xlsm` | Y | Y | Microsoft Excel Macro-Enabled (Open XML) |
| `xlsx` | Y | Y | Microsoft Excel (Open XML) |

#### Statistical / Scientific / Numeric Computing

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `dta` | Y | Y | Stata |
| `hdf5` | Y | N | Hierarchical Data Format |
| `mat` | N | N | MATLAB |
| `nc` | Y | Y | NetCDF |
| `rda` | Y | Y | RData workspace/object |
| `rds` | Y | Y | R data |
| `sas7bdat` | Y | N | SAS data |
| `sav` | Y | Y | SPSS data |
| `sylk` | N | N | Symbolic Link |
| `xpt` | Y | Y | SAS Transport |
| `zsav` | N | N | Compressed SPSS data |

#### Logs and Event Streams

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `log` | Y | Y | Generic log file |

#### Data Archives

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `gz` | Y | Y | Gzip-compressed file |
| `zip` | Y | Y | ZIP archive |

#### Templates

| Format | Read | Write | Description |
| --- | --- | --- | --- |
| `hbs` | Y | Y | Handlebars |
| `jinja2` | Y | Y | Jinja2 |
| `mustache` | Y | Y | Mustache |
| `vm` | Y | Y | Apache Velocity |

## Usage

### Command Line Interface

ETLPlus provides a powerful CLI for ETL operations:

```bash
# Show help
etlplus --help

# Show version
etlplus --version
```

The CLI is implemented with Typer (Click-based). The legacy argparse parser has been removed, so
rely on the documented commands/flags and run `etlplus <command> --help` for current options.

**Example error messages:**

- If you omit a required argument: `Error: Missing required argument 'SOURCE'.`
- If you place an option before its argument: `Error: Option '--source-format' must follow the
  'SOURCE' argument.`

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
etlplus extract examples/data/sample.json
```

Extract from CSV file:
```bash
etlplus extract examples/data/sample.csv
```

Extract from XML file:
```bash
etlplus extract examples/data/sample.xml
```

Extract from REST API:
```bash
etlplus extract https://api.example.com/data
```

Save extracted data to file:
```bash
etlplus extract examples/data/sample.csv > temp/sample_output.json
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
etlplus extract examples/data/sample.json \
  | etlplus load temp/sample_output.json --target-type file
```

Load to CSV file:
```bash
etlplus extract examples/data/sample.csv \
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
load(transformed, "file", "temp/sample_output.json", file_format="json")
```

For YAML-driven pipelines executed end-to-end (extract → validate → transform → load), see:

- Authoring: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Runner API and internals: see `etlplus.ops.run` docstrings and `docs/pipeline-guide.md`.

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
etlplus extract examples/data/sample.csv > temp/sample_extracted.json

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
etlplus load output.json --target-type file < data.json
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

- Runner overview and helpers: see `etlplus.ops.run` docstrings and
  [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Unified "connector" vocabulary (API/File/DB): `etlplus/connector`
  - API/file targets reuse the same shapes as sources; API targets typically set a `method`.

### Running Tests

```bash
pytest tests/ -v
```

#### Test Layers

We split tests into three layers:

- **Unit (`tests/unit/`)**: single function or class, no real I/O, fast, uses stubs/monkeypatch
  (e.g. small helpers in `etlplus.utils`, transform + validate helpers).
- **Smoke (`tests/smoke/`)**: minimal end-to-end checks for core flows; may touch temp files but
  avoids external network calls.
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
- [etlplus.ops](etlplus/ops/README.md): Extract/validate/transform/load primitives
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
- File handler matrix guardrail: [`docs/file-handler-matrix.md`](docs/file-handler-matrix.md)
- Pipeline authoring guide: [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
- Runner internals: see `etlplus.ops.run` docstrings and [`docs/pipeline-guide.md`](docs/pipeline-guide.md)
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
