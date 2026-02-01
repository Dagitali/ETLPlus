# `etlplus.ops` Subpackage

Documentation for the `etlplus.ops` subpackage: core ETL primitives used by the CLI and pipeline
runner.

- Extract data from files, APIs, and databases (database extract is a placeholder today)
- Validate JSON-like data with schema-style rules
- Transform records (filter, map, select, sort, aggregate)
- Load data into files and APIs (database load is a placeholder today)

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.ops` Subpackage](#etlplusops-subpackage)
  - [Validation Features](#validation-features)
  - [Defining Validation Rules](#defining-validation-rules)
  - [Example: Validating Data](#example-validating-data)
  - [See Also](#see-also)

## Validation Features

- Type checking (string, number, boolean, etc.)
- Required/optional fields
- Enum and pattern validation

## Defining Validation Rules

Validation rules are defined as dictionaries specifying field types, requirements, and constraints:

```python
rules = {
    "name": {"type": "string", "required": True},
    "age": {"type": "number", "min": 0, "max": 120},
}
```

## Example: Validating Data

```python
from etlplus.ops import validate

result = validate({"name": "Alice", "age": 30}, rules)
if result["valid"]:
    print("Data is valid!")
else:
    print(result["errors"])
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Validation utilities in [validate.py](validate.py)
