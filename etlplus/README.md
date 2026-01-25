# `etlplus` Package

The `etlplus` package provides a unified Python API and CLI for ETL operations: extraction,
validation, transformation, and loading of data from files, APIs, and databases.

- Top-level entry points for extract, validate, transform, and load
- Utilities for pipeline orchestration and helpers
- Exposes all subpackages for advanced usage

Back to project overview: see the top-level [README](../README.md).

## Subpackages

- [etlplus.api](api/README.md): Lightweight HTTP client and paginated REST helpers
- [etlplus.file](file/README.md): Unified file format support and helpers
- [etlplus.cli](cli/README.md): Command-line interface definitions for `etlplus`
- [etlplus.database](database/README.md): Database engine, schema, and ORM helpers
- [etlplus.templates](templates/README.md): SQL and DDL template helpers
- [etlplus.validation](validation/README.md): Data validation utilities and helpers
- [etlplus.workflow](etlplus/workflow/README.md): Helpers for data connectors, pipelines, jobs, and
  profiles

## Quickstart

```python
from etlplus.ops import extract, validate, transform, load

data = extract("file", "input.csv")
filtered = transform(data, {"filter": {"field": "age", "op": "gt", "value": 25}})
assert validate(filtered, {"age": {"type": "number", "min": 0}})["valid"]
load(filtered, "file", "output.json", file_format="json")
```

## See Also

- [Top-level project README](../README.md)
- [API reference](../docs/README.md)
