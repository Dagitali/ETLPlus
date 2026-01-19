# etlplus.ops subpackage

Documentation for the `etlplus.validation` subpackage: data validation utilities and helpers.

- Provides flexible data validation for ETL pipelines
- Supports type checking, required fields, and custom rules
- Includes utilities for rule definition and validation logic

Back to project overview: see the top-level [README](../../README.md).

- [etlplus.ops subpackage](#etlplusops-subpackage)
  - [Validation Features](#validation-features)
  - [Defining Validation Rules](#defining-validation-rules)
  - [Example: Validating Data](#example-validating-data)
  - [See Also](#see-also)

## Validation Features

- Type checking (string, number, boolean, etc.)
- Required/optional fields
- Enum and pattern validation
- Custom rule support

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
from etlplus.validation import validate

result = validate({"name": "Alice", "age": 30}, rules)
if result["valid"]:
    print("Data is valid!")
else:
    print(result["errors"])
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- Validation utilities in [utils.py](utils.py)
