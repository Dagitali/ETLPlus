# Examples

A few self-contained examples to get you started quickly.

## Python quickstart

Run a tiny ETL in Python using the sample data shipped in this repo.

```bash
python examples/quickstart_python.py
```

What it does:
- Extracts `examples/data/sample.json`
- Filters to `age > 25` and selects `name` + `email`
- Validates fields exist
- Writes `temp/sample_output.json`

## CLI quickstart

Try similar steps with the CLI:

```bash
# Show version and help
etlplus --version
etlplus --help

# Transform the sample data and write JSON
etlplus transform \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  examples/data/sample.json \
  temp/sample_output.json
```

## Pipelines

For larger workflows, author a pipeline YAML and run it with your own orchestration or helper
script.

- Authoring: see the Pipeline Authoring Guide at `docs/pipeline-guide.md` and the example
  `in/pipeline.yml` or `examples/configs/pipeline.yml`.
- Runner internals and Python entrypoint: see `docs/run-module.md` for `etlplus.ops.run.run`.

CLI examples:

```bash
# List jobs defined in a pipeline file
etlplus check --config examples/configs/pipeline.yml --jobs

# Show a pipeline summary (name, version, sources, targets, jobs)
etlplus check --config examples/configs/pipeline.yml --summary

# Run a specific job end-to-end
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers
```

Python example:

```python
from etlplus.ops.run import run as run_job

result = run_job(
    job="file_to_file_customers",
    config_path="examples/configs/pipeline.yml",
)
print(result["status"], result.get("records"))
```

Design notes on config typing and merges:
- Mapping inputs, dict outputs, and merge semantics are documented in
  `docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`.
 - Typing philosophy (TypedDicts as editor hints, permissive runtime):
   `CONTRIBUTING.md#typing-philosophy`.
