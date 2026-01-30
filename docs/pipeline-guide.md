# Pipeline Authoring Guide

This guide explains how to author an ETLPlus pipeline YAML, using the example at `in/pipeline.yml`
as a reference.

ETLPlus focuses on simple, JSON-first ETL. The pipeline file is a declarative description that your
runner (a script, Makefile, CI job) can parse and execute using ETLPlus primitives: `extract`,
`validate`, `transform`, and `load`.

CLI note: ETLPlus uses Typer for command parsing and does not ship an argparse shim. Use the
documented `etlplus` commands and flags (check `etlplus --help`) when wiring your runner.

## Running a pipeline from YAML (CLI)

Use the built-in `etlplus run` command to execute jobs defined in a pipeline YAML. The command reads
your config, resolves vars and env placeholders, then runs the requested job:

```bash
# List jobs with the check command
etlplus check --config examples/configs/pipeline.yml --jobs

# Run a specific job
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers

# Run another job from the same config
etlplus run --config examples/configs/pipeline.yml --job api_to_file_github_repos
```

For scripted usage inside a larger Python project, prefer importing the Python API directly (e.g.,
`extract`, `transform`, `validate`, `load`) instead of invoking the CLI subprocess.

## Top-level structure

A pipeline file typically includes:

```yaml
name: ETLPlus Demo Pipeline
version: "1"
profile:
  default_target: local
  env:
    GITHUB_ORG: dagitali
    GITHUB_TOKEN: "${GITHUB_TOKEN}"

vars:
  data_dir: in
  out_dir: out
```

- `profile.env` is a convenient place to document expected environment variables. Resolve them in
  your runner before invoking ETLPlus functions.
- `vars` collects reusable paths/values for templating.

## APIs

Declare HTTP APIs and endpoints under `apis`. You can define headers, endpoints, and pagination:

```yaml
apis:
  github:
    base_url: "https://api.github.com"
    headers:
      Accept: application/vnd.github+json
      Authorization: "Bearer ${GITHUB_TOKEN}"
    endpoints:
      org_repos:
        path: "/orgs/${GITHUB_ORG}/repos"
        query_params:
          per_page: 100
          type: public
        pagination:
          type: page          # page | offset | cursor
          page_param: page
          size_param: per_page
          start_page: 1
          page_size: 100
        rate_limit:
          max_per_sec: 2
```

Note: Use `query_params` for URL query string pairs (e.g., `?key=value`). Older keys like `params`
or `query` are not supported to avoid ambiguity with body/form fields.

### Profiles, base_path, and auth

For per-environment settings, define named profiles under an API. Each profile can include:

- `base_url` (required): scheme + host (optionally with a path)
- `base_path` (optional): path prefix that’s composed after `base_url`
- `headers`: default headers for that profile
- `auth`: provider-specific auth block (shape is pass-through)

Example:

```yaml
apis:
  github:
    profiles:
      default:
        base_url: "https://api.github.com"
        base_path: "/v1"
        auth:
          type: bearer
          token: "${GITHUB_TOKEN}"
        headers:
          Accept: application/vnd.github+json
          Authorization: "Bearer ${GITHUB_TOKEN}"
    endpoints:
      org_repos:
        path: "/orgs/${GITHUB_ORG}/repos"
```

At runtime, the model computes an effective base URL by composing `base_url` and `base_path`. If you
build an HTTP client from the config, prefer using the composed URL. For convenience, the
`ApiConfig` model exposes:

- `effective_base_url()`: returns `base_url` + `base_path` (when present)
- `build_endpoint_url(endpoint)`: composes the full URL from `base_url`, `base_path`, and the
  endpoint’s `path`

Header precedence:

1. `profiles.<name>.defaults.headers` (lowest)
2. `profiles.<name>.headers`
3. API top-level `headers` (highest)

Pagination tips (mirrors `etlplus.api`):

- Page/offset styles: use `page_param`, `size_param`, `start_page`, and `page_size`.
- Cursor style: specify `cursor_param` and `cursor_path` (e.g., `data.nextCursor`).
- Extract records from nested payloads with `records_path` (e.g., `data.items`).
- Rate limiting: set `rate_limit.sleep_seconds` or `rate_limit.max_per_sec` on the API or endpoint
  to define default pacing. Job runners merge `jobs[].extract.options.rate_limit` over those
  defaults and forward the merged mapping into `EndpointClient.paginate(...,
  rate_limit_overrides=...)`, so you can temporarily slow down or speed up a single job without
  editing the shared API profile. The paginator enforces that effective delay via a shared
  `RateLimiter` before each page fetch.

Client helpers (``etlplus.api.EndpointClient``) now return the ``JSONRecords`` alias (a ``list`` of
``JSONDict``) so pipelines and custom runners can rely on typed payloads when aggregating paginated
responses.

See `etlplus/api/README.md` for the code-level pagination API.

### Runner behavior with `base_path` (sources and targets)

When you reference an API service and endpoint in a pipeline (whether in a source or an API target),
the runner composes the request URL using the API model’s helpers, which honor any configured
`base_path` automatically.

Example:

```yaml
apis:
  myapi:
    profiles:
      default:
        base_url: "https://api.example.com"
        base_path: "/v1"
    endpoints:
      list_items:
        path: "/items"

sources:
  - name: list_items_source
    type: api
    service: myapi
    endpoint: list_items
```

At runtime, the request is issued to:

```
https://api.example.com/v1/items
```

No extra wiring is needed — the composed base URL (including `base_path`) is used under the hood
when the job runs.

## Databases

Declare connection defaults or named connections you’ll use in sources/targets:

```yaml
databases:
  mssql:
    default:
      driver: "ODBC Driver 18 for SQL Server"
      server: "localhost,1433"
      database: "Demo"
      trusted_connection: true
      options:
        encrypt: "yes"
        trust_server_certificate: "yes"
        connection_timeout: 30
        application_name: "ETLPlus"
  sqlite:
    default:
      database: "./${data_dir}/demo.db"
      options:
        timeout: 30
```

Note: Database extract/load in ETLPlus is minimal today; consider this a placeholder for
orchestration that calls into DB clients.

## File systems

Point to local/cloud locations and logical folders:

```yaml
file_systems:
  local:
    base_path: "./${data_dir}"
    folders:
      in: "./${data_dir}"
      out: "./${out_dir}"
  s3:
    bucket: "my-etlplus-bucket"
    prefix: "data/"
    region: "us-east-1"
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

## Sources

Define where data comes from:

```yaml
sources:
  - name: customers_csv
    type: file        # file | database | api
    format: csv       # json | csv | xml | yaml
    path: "${data_dir}/customers.csv"
    options:
      header: true
      delimiter: ","
      encoding: utf-8

  - name: github_repos
    type: api
    service: github   # reference into apis
    endpoint: org_repos
```

Source-level query_params (direct form):

```yaml
sources:
  - name: users_api
    type: api
    url: "https://api.example.com/v1/users"
    headers:
      Authorization: "Bearer ${TOKEN}"
    query_params:
      active: true
      page: 1
```

Tip: You can also override query parameters per job using
`jobs[].extract.options.query_params: { ... }`.

Rate limit overrides follow the same pattern: populate `jobs[].extract.options.rate_limit` with
either `sleep_seconds` or `max_per_sec` to override an API or endpoint default for that specific
job. Those values are merged into the client configuration and forwarded to
`EndpointClient.paginate(..., rate_limit_overrides=...)`, ensuring only that job’s paginator is sped
up or slowed down.

Format override note:

When extracting from file sources, ETLPlus still infers the format from the filename extension
(`.csv`, `.json`, `.xml`, `.yaml`). However, `--source-format` and `--target-format` now override
that inference for both Typer- and argparse-based CLIs. This means you can safely point at files
without/extensions or with misleading suffixes and force the desired parser or writer without having
to rename the file first.

Note: When using a service + endpoint in a source, URL composition (including `base_path`) is
handled automatically. See “Runner behavior with base_path (sources and targets)” in the APIs
section.

## Validations

Validation rule sets map field names to rules, mirroring `etlplus.ops.validate.FieldRules`:

```yaml
validations:
  customers_basic:
    CustomerId:
      required: true
      type: integer
      min: 1
    Email:
      type: string
      maxLength: 320
```

## Transforms

Transformation pipelines follow `etlplus.ops.transform` shapes exactly:

```yaml
transforms:
  clean_customers:
    filter: { field: Email, op: contains, value: "@" }
    map:
      FirstName: first_name
      LastName: last_name
    select: [CustomerId, first_name, last_name, Email, Status]
    sort:
      - last_name
      - { field: first_name, reverse: false }

  summarize_customers:
    aggregate:
      - { field: CustomerId, func: count, alias: row_count }
      - { field: CustomerId, func: max, alias: max_id }
```

## Targets

Where your data lands:

```yaml
targets:
  - name: customers_json_out
    type: file
    format: json
    path: "${out_dir}/customers_clean.json"

  - name: webhook_out
    type: api
    url: "https://httpbin.org/post"
    method: post
    headers:
      Content-Type: application/json
```

Note: API targets that reference a service + endpoint also honor `base_path` via the same runner
behavior described in the APIs section.

Service + endpoint target example:

```yaml
apis:
  myapi:
    profiles:
      default:
        base_url: "https://api.example.com"
        base_path: "/v1"
    endpoints:
      ingest:
        path: "/ingest"

targets:
  - name: ingest_out
    type: api
    service: myapi
    endpoint: ingest
    method: post
    headers:
      Content-Type: application/json
```

## Connector parsing and extension

Under the hood, source and target entries are parsed via a single tolerant constructor that looks at
the `type` field and builds a concrete connector dataclass:

- `type: file` → `ConnectorFile`
- `type: database` → `ConnectorDb`
- `type: api` → `ConnectorApi`

Details:

- The pipeline loader uses a unified path for both `sources` and `targets`.
- Unknown or malformed entries are skipped rather than failing the whole load (keeping pipeline
  authoring permissive).
- The connector kind is also available as a type-safe literal in code as
  `etlplus.connector.ConnectorType` (values: `"file" | "database" | "api"`).

To add new connector kinds in the future, implement a new dataclass in `etlplus.connector`
and extend the internal parser to handle its `type` value.

## Jobs

Jobs orchestrate the flow end-to-end. Each job can reference a source, validations, transform, and
target:

```yaml
jobs:
  - name: file_to_file_customers
    depends_on: [seed_customers]
    extract: { source: customers_csv }
    validate: { ruleset: customers_basic }
    transform: { pipeline: clean_customers }
    load: { target: customers_json_out }
  - name: seed_customers
    extract: { source: seed_customers_csv }
    load: { target: customers_db_out }
```

Notes:

- `depends_on` is optional and can be a string or list of job names.
- Jobs without dependencies run first when ordered as a DAG.

## Running pipelines (CLI and Python)

Once you have a pipeline YAML, you can run jobs either from the
command line or directly from Python.

### CLI: `etlplus check` (inspect) and `etlplus run` (execute)

List jobs or show a summary from a pipeline file:

```bash
etlplus check --config examples/configs/pipeline.yml --jobs
etlplus check --config examples/configs/pipeline.yml --summary
```

Run a specific job end-to-end (extract → validate → transform → load):

```bash
etlplus run --config examples/configs/pipeline.yml --job file_to_file_customers
```

Notes:

- These commands read the same YAML schema described in this guide.
- Environment-variable substitution (e.g. `${GITHUB_TOKEN}`) is applied the same way as when loading
  configs via the Python API.
- For more details on the orchestration implementation, see
  [Runner internals: etlplus.ops.run](run-module.md).

### Python: `etlplus.ops.run.run`

To trigger a job programmatically, use the high-level runner function exposed by the package:

```python
from etlplus.ops.run import run as run_job

result = run_job(
    job="file_to_file_customers",
    config_path="examples/configs/pipeline.yml",
)

print(result["status"], result.get("records"))
```

The `run()` function returns the final load result as a `JSONDict` envelope, which typically
includes `status`, `message`, and implementation-specific metadata such as record counts.
```

## Minimal working example

```yaml
name: "Quickstart"
vars:
  data_dir: examples/data
  out_dir: examples
sources:
  - name: sample
    type: file
    format: json
    path: "${data_dir}/sample.json"
transforms:
  tidy:
    filter: { field: age, op: gt, value: 25 }
    select: [name, email]
validations:
  basic:
    name: { type: string, required: true }
    email: { type: string, required: true }
targets:
  - name: sample_out
    type: file
    format: json
    path: "${out_dir}/sample_output.json"
jobs:
  - name: run
    extract: { source: sample }
    validate: { ruleset: basic }
    transform: { pipeline: tidy }
    load: { target: sample_out }
```

## Tips

- Use environment variables for secrets and org-specific values; resolve them in your runner.
- Apply safety caps for API pagination (`max_pages`, `max_records`) when running in CI.
- Validation controls: set `severity: warn|error` and
  `phase: before_transform|after_transform|both`.
- Keep pipelines composable; factor common transforms into named pipelines reused across jobs.

For the HTTP client and pagination API, see `etlplus/api/README.md`.

## Design notes: Mapping inputs, dict outputs

ETLPlus config constructors (e.g., `ApiConfig.from_obj`, `PipelineConfig.from_dict`) accept
`Mapping[str, Any]` rather than `dict[str, Any]` for inputs. Why?

- Flexibility: callers can pass any mapping-like object (e.g., YAML loaders that return custom
  mappings) without copying into a `dict` first.
- Clear intent: inputs are treated as read-only; we normalize to concrete `dict` only for internal
  storage.
- Lower coupling: depending on the standard Mapping protocol avoids import cycles and keeps modules
  cohesive.

Practically, you can pass a plain `dict` everywhere and it will work.

### Merge semantics (Python 3.13)

We use the dict union operator for clarity:

- `a | b` creates a merged copy with `b` taking precedence.
- `a |= b` updates `a` in-place with `b`’s keys.

Header precedence (lowest → highest):

1. `profiles.<name>.defaults.headers`
2. `profiles.<name>.headers`
3. API top-level `headers`

### Extending config shapes

When adding new config objects or fields:

- Prefer `@dataclass(slots=True)` for models.
- Add a `@classmethod from_obj(cls, obj: Mapping[str, Any]) -> Self` that is tolerant of missing
  optional keys and performs minimal type normalization (e.g., cast header values to `str`).
- Keep inputs as `Mapping[...]` (non-mutating) and store concrete `dict` internally.
- Reuse small helpers for repeated casts (e.g., `headers: dict[str, str]`).

Contributors: for the repo-wide typing approach (TypedDicts as editor hints, `Mapping[str, Any]`
inputs, and overloads imported only under `TYPE_CHECKING`), see
[`CONTRIBUTING.md#typing-philosophy`](../CONTRIBUTING.md#typing-philosophy).
