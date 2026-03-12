# Quickstart

ETLPlus can be used from the command line or imported as a Python package.

## Command line

```bash
# Inspect help and version
etlplus --help
etlplus --version

# Extract CSV, filter/select rows, and write JSON
etlplus extract examples/data/sample.csv \
  | etlplus transform --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  - temp/sample_output.json
```

## Python API

```python
from etlplus.ops import extract, load, transform, validate

data = extract("file", "input.csv")
operations = {
    "filter": {"field": "age", "op": "gt", "value": 25},
    "select": ["name", "email"],
}
filtered = transform(data, operations)
rules = {
    "name": {"type": "string", "required": True},
    "email": {"type": "string", "required": True},
}
assert validate(filtered, rules)["valid"]
load(filtered, "file", "temp/sample_output.json", file_format="json")
```

## Where to go next

- See the {doc}`examples guide <../guides/examples>` for runnable sample files.
- See the {doc}`pipeline authoring guide <../guides/pipeline-authoring>` for
  config-driven ETL jobs.
- See the {doc}`API reference <../api/index>` for module-level reference
  material generated from docstrings.
