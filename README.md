# ETLPlus

A Swiss Army knife for enabling simple ETL operations - a Python package and command-line interface for data extraction, validation, transformation, and loading.

## Features

- **Extract** data from multiple sources:
  - Files (JSON, CSV, XML)
  - Databases (connection string support)
  - REST APIs
  
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
  - Aggregate functions (sum, avg, min, max, count)

- **Load** data to multiple targets:
  - Files (JSON, CSV)
  - Databases (connection string support)
  - REST APIs (POST, PUT, PATCH)

## Installation

```bash
pip install etlplus
```

For development:

```bash
pip install -e ".[dev]"
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

### Running Tests

```bash
pytest tests/ -v
```

### Code Coverage

```bash
pytest tests/ --cov=etlplus --cov-report=html
```

### Linting

```bash
flake8 etlplus/
black etlplus/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
