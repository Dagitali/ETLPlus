# ETLPlus Demo

This document demonstrates the ETLPlus package in action.

- [ETLPlus Demo](#etlplus-demo)
  - [Installation Verification](#installation-verification)
  - [Demo 1: Extract Data from Different Sources](#demo-1-extract-data-from-different-sources)
    - [Extract from JSON](#extract-from-json)
    - [Extract from CSV](#extract-from-csv)
  - [Demo 2: Validate Data](#demo-2-validate-data)
  - [Demo 3: Transform Data](#demo-3-transform-data)
    - [Filter and Select](#filter-and-select)
    - [Sort Data](#sort-data)
    - [Aggregate Data](#aggregate-data)
  - [Demo 4: Load Data](#demo-4-load-data)
    - [Load to JSON File](#load-to-json-file)
    - [Load to CSV File](#load-to-csv-file)
  - [Demo 5: Complete ETL Pipeline](#demo-5-complete-etl-pipeline)
  - [Demo 6: Using Python API](#demo-6-using-python-api)
  - [Key Features Demonstrated](#key-features-demonstrated)
  - [Performance Notes](#performance-notes)
  - [Next Steps](#next-steps)

## Installation Verification

<!-- snippet:start installation_version -->
```bash
$ etlplus --version
etlplus 0.3.4
```
<!-- snippet:end installation_version -->

```bash
$ etlplus --help
usage: etlplus [-h] [--version] {extract,validate,transform,load} ...

ETLPlus - A Swiss Army knife for enabling simple ETL operations
...
```

## Demo 1: Extract Data from Different Sources

### Extract from JSON
```bash
$ echo '{"name": "John", "age": 30}' > sample.json
$ etlplus extract file sample.json
{
  "name": "John",
  "age": 30
}
```

### Extract from CSV
```bash
$ cat > users.csv << 'CSVDATA'
name,age,city
John Doe,30,New York
Jane Smith,25,Los Angeles
CSVDATA

$ etlplus extract users.csv
[
  {
    "name": "John Doe",
    "age": "30",
    "city": "New York"
  },
  ...
]
```

## Demo 2: Validate Data

```bash
$ etlplus validate '{"email": "user@example.com", "age": 25}' \
  --rules '{"email": {"type": "string", "required": true}, "age": {"type": "number", "min": 18}}'
{
  "valid": true,
  "errors": [],
  "field_errors": {},
  "data": {
    "email": "user@example.com",
    "age": 25
  }
}
```

## Demo 3: Transform Data

### Filter and Select
```bash
$ etlplus transform --operations '{
    "filter": {"field": "age", "op": "gt", "value": 26},
    "select": ["name", "age"]
  }' '[
    {"name": "John", "age": 30, "city": "NYC"},
    {"name": "Jane", "age": 25, "city": "LA"},
    {"name": "Bob", "age": 35, "city": "Chicago"}
  ]'
[
  {
    "name": "John",
    "age": 30
  },
  {
    "name": "Bob",
    "age": 35
  }
]
```

### Sort Data
```bash
$ etlplus transform -\
  -operations '{"sort": {"field": "score", "reverse": true}}' \
  '[{"name": "Charlie", "score": 85}, {"name": "Alice", "score": 95}, {"name": "Bob", "score": 90}]'
```

### Aggregate Data
```bash
$ etlplus transform --operations '{"aggregate": {"field": "sales", "func": "sum"}}' \
  '[
    {"product": "A", "sales": 100},
    {"product": "B", "sales": 150},
    {"product": "C", "sales": 200}
  ]'
{
  "sum_sales": 450
}
```

## Demo 4: Load Data

### Load to JSON File
```bash
$ etlplus load \
  '{"name": "John", "status": "active"}' \
  output.json --target-type file
{
  "status": "success",
  "message": "Data loaded to output.json",
  "records": 1
}
```

### Load to CSV File
```bash
$ etlplus load \
  '[
    {"name": "John", "email": "john@example.com"},
    {"name": "Jane", "email": "jane@example.com"}
  ]' \
  users.csv --target-type file
{
  "status": "success",
  "message": "Data loaded to users.csv",
  "records": 2
}
```

## Demo 5: Complete ETL Pipeline

This example shows a complete ETL workflow:

1. **Extract** data from CSV
2. **Transform** data (filter and select)
3. **Validate** transformed data
4. **Load** to new CSV file

```bash
# Step 1: Extract
$ etlplus extract raw_data.csv > extracted.json

# Step 2: Transform
$ etlplus transform \
  --operations '{
    "filter": {"field": "age", "op": "gte", "value": 18},
    "select": ["name", "email", "age"]
  }' \
  extracted.json \
  transformed.json

# Step 3: Validate
$ etlplus validate \
  --rules '{
    "name": {"type": "string", "required": true},
    "email": {"type": "string", "required": true, "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"},
    "age": {"type": "number", "min": 18, "max": 120}
  }' \
  transformed.json

# Step 4: Load
$ etlplus load transformed.json file final_output.csv
```

## Demo 6: Using Python API

```python
from etlplus.ops import extract, validate, transform, load

# Extract
data = extract("file", "data.csv", format="csv")

# Validate
validation_result = validate(data, {
    "age": {"type": "number", "min": 0, "max": 150}
})

if validation_result["valid"]:
    # Transform
    transformed = transform(data, {
        "filter": {"field": "age", "op": "gt", "value": 18},
        "select": ["name", "email"]
    })

    # Load
    load(transformed, "file", "output.json", format="json")
    print("ETL pipeline completed successfully!")
else:
    print(f"Validation errors: {validation_result['errors']}")
```

## Key Features Demonstrated

✅ **Multiple Data Sources**: Files (JSON, CSV, XML), Databases, REST APIs
✅ **Flexible Validation**: Type checking, required fields, ranges, patterns, enums
✅ **Powerful Transformations**: Filter, map, select, sort, aggregate
✅ **Multiple Output Formats**: JSON, CSV
✅ **Command-Line Interface**: Easy to use CLI for all operations
✅ **Python API**: Programmatic access for integration
✅ **Chainable Operations**: Build complex ETL pipelines
✅ **Comprehensive Testing**: 66 tests covering all functionality

## Performance Notes

- Handles large datasets efficiently with streaming where possible
- Memory-efficient for file operations
- Supports batch processing for REST API operations
- Optimized for common ETL patterns

## Next Steps

- Explore more transformation operations
- Integrate with databases using connection strings
- Build custom ETL pipelines for your specific needs
- Extend with custom validation rules and transformations
