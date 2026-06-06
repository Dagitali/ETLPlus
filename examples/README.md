# Examples

<!-- docs:guides-examples:start -->

A few self-contained examples to get you started quickly.

The quickstarts use local files so they run without external services. The pipeline and connector
snippets also show the intended deployable posture: the same connector shapes can point at remote
object storage and managed databases with credentials injected by the shell, CI runner, container
runtime, or scheduler.

- [Quickstarts](#quickstarts)
  - [Python](#python)
  - [CLI](#cli)
- [Pipelines](#pipelines)
  - [Python](#python-1)
  - [CLI](#cli-1)
- [Cloud Database Connector Snippets](#cloud-database-connector-snippets)
  - [BigQuery](#bigquery)
  - [Snowflake](#snowflake)
- [Remote Object Storage Snippets](#remote-object-storage-snippets)

## Quickstarts

### Python

Run a tiny ETL in Python using the sample data shipped in this repo.

```bash
python examples/quickstart.py
```

What it does:
- Extracts `examples/data/sample.json`
- Filters to `age > 25` and selects `name` + `email`
- Validates fields exist
- Writes `temp/sample_output.json`

### CLI

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

For larger workflows, author a pipeline YAML and run it with the built-in `etlplus run` command, or
integrate the same config into your own orchestration or helper script.

- Authoring: see the Pipeline Authoring Guide at `docs/pipeline-guide.md` and the example
  `examples/configs/pipeline.yml`.
- Runner internals and Python entrypoint: see `etlplus.ops.run` docstrings and
  `docs/pipeline-guide.md`.

### Python

```python
from etlplus.ops.run import run as run_job

result = run_job(
    job="file_to_file_customers",
    config_path="examples/configs/pipeline.yml",
)
print(result["status"], result.get("records"))
```

### CLI

```bash
# List jobs defined in a pipeline file
etlplus check --config examples/configs/pipeline.yml --jobs

# Show a pipeline summary (name, version, sources, targets, jobs)
etlplus check --config examples/configs/pipeline.yml --summary

# Run a specific job end-to-end
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers
```

## Cloud Database Connector Snippets

Use the same additive `type: database` connector shape for both cloud database providers. Local
Postgres, SQLite, and localhost DSNs are useful development fixtures, but managed databases with
runtime-injected credentials are the expected production path.

### BigQuery

```bash
pip install -e ".[database-bigquery]"
```

```yaml
sources:
  - name: warehouse_events_bigquery
    type: database
    provider: bigquery
    project: analytics-project
    dataset: warehouse
    table: events
```

  Typical runtime environment:

  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/etlplus.json"
  export GOOGLE_CLOUD_PROJECT="analytics-project"
  ```

### Snowflake

```bash
pip install -e ".[database-snowflake]"
```

```yaml
sources:
  - name: warehouse_events_snowflake
    type: database
    provider: snowflake
    account: acme.us-east-1
    database: ANALYTICS
    schema: PUBLIC
    warehouse: TRANSFORMING
    table: EVENTS
```

Typical runtime environment:

```bash
export SNOWFLAKE_USER="etlplus"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD}"
```

## Remote Object Storage Snippets

Use the same `type: file` connector shape for both local paths and remote object storage.
Local paths are intentionally convenient for quickstarts; remote object storage should feel like the
normal target for shared or scheduled pipelines.

```yaml
sources:
  - name: landing_customers
    type: file
    format: csv
    path: "s3://acme-landing/customers/customers.csv"

targets:
  - name: curated_customers
    type: file
    format: json
    path: "azure-blob://analytics/customers/curated/customers.json"
```

Typical runtime environment:

```bash
export AWS_PROFILE="etlplus-dev"
export AZURE_STORAGE_ACCOUNT_URL="https://analytics.blob.core.windows.net"
export AZURE_STORAGE_CREDENTIAL="${AZURE_STORAGE_CREDENTIAL}"
```

Both connector shapes can also use `connection_string` directly when you already have a
provider-specific SQLAlchemy-style DSN.

Design notes on config typing and merges:
- Mapping inputs, dict outputs, and merge semantics are documented in
  `docs/pipeline-guide.md#design-notes-mapping-inputs-dict-outputs`.
- Typing philosophy (TypedDicts as editor hints, permissive runtime):
  `CONTRIBUTING.md#typing-philosophy`.

<!-- docs:guides-examples:end -->
