# `etlplus.ops` Subpackage

Documentation for the `etlplus.ops` subpackage: the runtime ETL primitives used by the CLI and
pipeline runner.

- Read data from files and APIs, with database extract placeholders retained for future work
- Validate JSON-like payloads with lightweight schema-style rules
- Transform records through `filter`, `map`, `select`, `sort`, and `aggregate` steps
- Load data into files and APIs, with database execution placeholders retained for future work
- Run full ETL jobs from pipeline configuration files

Back to project overview: see the top-level [README](../../README.md).

- [`etlplus.ops` Subpackage](#etlplusops-subpackage)
  - [Public Entry Points](#public-entry-points)
  - [Transform Architecture](#transform-architecture)
  - [Example: Transforming Records](#example-transforming-records)
  - [Validation Features](#validation-features)
  - [Example: Validating Data](#example-validating-data)
  - [Example: Running a Pipeline Job](#example-running-a-pipeline-job)
  - [See Also](#see-also)

## Public Entry Points

- `etlplus.ops.extract.extract`: load data from files, APIs, or placeholder database connectors
- `etlplus.ops.transform.transform`: orchestrate record transformations from a pipeline-style config
- `etlplus.ops.load.load`: write data to files, APIs, or placeholder database connectors
- `etlplus.ops.validate.validate`: validate mappings and lists of mappings with schema-style rules
- `etlplus.ops.run.run` / `etlplus.ops.run.run_pipeline`: execute named jobs from a pipeline config
- `etlplus.ops.maybe_validate`: apply validation conditionally inside custom runners or hooks

Most callers should import the coarse-grained helpers from `etlplus.ops` itself:

```python
from etlplus.ops import extract, load, run, transform, validate
```

## Transform Architecture

Transformation support now has two layers:

- `etlplus.ops.transform` is the orchestration facade. It loads the source data, normalizes step
  keys, and applies pipeline steps.
- `etlplus.ops.transformations.aggregate`, `filter`, `map`, `select`, and `sort` expose
  step-specific helpers for advanced callers that want to reuse one transformation family directly.
- Each transformation module exposes both `apply_*` helpers for direct use and `apply_*_step`
  adapters for callers that want pipeline-style step specs without calling the full orchestrator.

Important pipeline semantics:

- Step keys may be strings such as `"filter"` or `PipelineStep` enum members.
- `transform()` evaluates steps in the fixed order `aggregate`, `filter`, `map`, `select`, `sort`.
- When `aggregate` is present, the result is a single mapping containing merged aggregate outputs,
  and row-wise steps are not applied afterward. Keep aggregate-only summaries separate from row-wise
  cleanup pipelines unless that short-circuit behavior is intentional.

## Example: Transforming Records

Use the facade for ordinary ETL pipelines:

```python
from etlplus.ops import transform

rows = [
    {"CustomerId": 1, "Email": "ada@example.com", "Status": "active"},
    {"CustomerId": 2, "Email": "invalid", "Status": "inactive"},
]

ops = {
    "filter": {"field": "Email", "op": "contains", "value": "@"},
    "select": ["CustomerId", "Email"],
}

clean_rows = transform(rows, ops)
```

Import a step module directly when you need one transformation family or a pipeline-style step
adapter in custom code:

```python
from etlplus.ops.transformations.aggregate import apply_aggregate_step

summary = apply_aggregate_step(
    [{"amount": 10}, {"amount": 20}],
    {"field": "amount", "func": "sum", "alias": "total_amount"},
)
assert summary == [{"total_amount": 30.0}]
```

## Validation Features

- Type checking (string, number, boolean, etc.)
- Required/optional fields
- Enum and pattern validation

## Example: Validating Data

```python
from etlplus.ops import validate

rules = {
    "name": {"type": "string", "required": True},
    "age": {"type": "number", "min": 0, "max": 120},
}

result = validate({"name": "Alice", "age": 30}, rules)
if result["valid"]:
    print("Data is valid!")
else:
    print(result["errors"])
```

## Example: Running a Pipeline Job

```python
from etlplus.ops import run

result = run("file_to_file_customers", config_path="examples/configs/pipeline.yml")
print(result["status"])
```

## See Also

- Top-level CLI and library usage in the main [README](../../README.md)
- API reference for ETL operations in [docs/source/api/operations.rst](../../docs/source/api/operations.rst)
- Transformation facade in [transform.py](transform.py)
- Step-specific transformation modules in [transformations/](transformations)
