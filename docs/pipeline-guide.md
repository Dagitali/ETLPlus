# Pipeline Authoring Guide

This guide explains how to author an ETLPlus pipeline YAML, using the example at `in/pipeline.yml`
as a reference.

ETLPlus focuses on simple, JSON-first ETL.  The pipeline file is a declarative description that your
runner (a script, Makefile, CI job) can parse and execute using ETLPlus primitives: `extract`,
`validate`, `transform`, and `load`.

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

- `profile.env` is a convenient place to document expected environment variables.  Resolve them in
  your runner before invoking ETLPlus functions.
- `vars` collects reusable paths/values for templating.

## APIs

Declare HTTP APIs and endpoints under `apis`.  You can define headers, endpoints, and pagination:

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

Note: Use `query_params` for URL query string pairs (e.g., `?key=value`).  Older keys like `params`
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

At runtime, the model computes an effective base URL by composing `base_url` and `base_path`.
If you build an HTTP client from the config, prefer using the composed URL. For convenience, the
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

See `etlplus/api/README.md` for the code-level pagination API.

### Runner behavior with `base_path` (sources and targets)

When you reference an API service and endpoint in a pipeline (whether in a
source or an API target), the runner composes the request URL using the API
model’s helpers, which honor any configured `base_path` automatically.

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

No extra wiring is needed — the composed base URL (including `base_path`) is
used under the hood when the job runs.

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

Note: When using a service + endpoint in a source, URL composition (including
`base_path`) is handled automatically. See “Runner behavior with base_path
(sources and targets)” in the APIs section.

## Validations

Validation rule sets map field names to rules, mirroring `etlplus.validate.FieldRules`:

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

Transformation pipelines follow `etlplus.transform` shapes exactly:

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

Note: API targets that reference a service + endpoint also honor `base_path`
via the same runner behavior described in the APIs section.

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

## Jobs

Jobs orchestrate the flow end-to-end.  Each job can reference a source, validations, transform, and
target:

```yaml
jobs:
  - name: file_to_file_customers
    extract: { source: customers_csv }
    validate: { ruleset: customers_basic }
    transform: { pipeline: clean_customers }
    load: { target: customers_json_out }
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
