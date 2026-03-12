# Examples

ETLPlus ships with runnable examples under `examples/` for both single-command
flows and config-driven pipelines.

## Python quickstart script

Run the sample Python script:

```bash
python examples/quickstart.py
```

What it does:

- Extracts records from `examples/data/sample.json`
- Filters to `age > 25`
- Selects `name` and `email`
- Validates the transformed records
- Writes `temp/sample_output.json`

## CLI quickstart

Try the same style of flow from the command line:

```bash
etlplus --version
etlplus --help

etlplus transform \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  examples/data/sample.json \
  temp/sample_output.json
```

## Pipeline example

The repository also includes a fuller pipeline configuration at
`examples/configs/pipeline.yml`.

Useful commands:

```bash
# List jobs defined in the pipeline config
etlplus check --config examples/configs/pipeline.yml --jobs

# Show a pipeline summary
etlplus check --config examples/configs/pipeline.yml --summary

# Run one job end to end
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers
```

Python entrypoint example:

```python
from etlplus.ops.run import run as run_job

result = run_job(
    job="file_to_file_customers",
    config_path="examples/configs/pipeline.yml",
)
print(result["status"], result.get("records"))
```

Related documentation:

- {doc}`../getting-started/quickstart`
- {doc}`pipeline-authoring`
- {doc}`../api/operations`
