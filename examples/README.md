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
- Writes `examples/sample_output.json`

## CLI quickstart

Try similar steps with the CLI:

```bash
# Show version and help
etlplus --version
etlplus --help

# Transform the sample data and write JSON
etlplus transform examples/data/sample.json \
  --operations '{"filter": {"field": "age", "op": "gt", "value": 25}, "select": ["name", "email"]}' \
  -o examples/sample_output.json
```

## Pipelines

For larger workflows, author a pipeline YAML and run it with your own orchestration or helper script.
See the Pipeline Authoring Guide at `docs/pipeline-guide.md` and the example `in/pipeline.yml` for a richer configuration.

Design notes on config typing and merges:
- Mapping inputs, dict outputs, and merge semantics are documented in
  `docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`.
