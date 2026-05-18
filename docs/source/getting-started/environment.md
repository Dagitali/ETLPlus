# Environment Reference

ETLPlus prefers environment-injected runtime configuration over repository-local
files.

Use `profile.env` for documented defaults and placeholders, then override those values from the
invoking shell, CI job, container runtime, or scheduler.

- [Environment Reference](#environment-reference)
  - [Precedence](#precedence)
  - [Runtime Knobs](#runtime-knobs)
  - [Secret References](#secret-references)
  - [Storage and Provider Credentials](#storage-and-provider-credentials)
    - [AWS S3](#aws-s3)
    - [Azure Blob Storage and ADLS Gen2](#azure-blob-storage-and-adls-gen2)
    - [BigQuery](#bigquery)
    - [Snowflake](#snowflake)
  - [Example Shell Setup](#example-shell-setup)
  - [Related Docs](#related-docs)

## Precedence

For config substitution, ETLPlus currently resolves values in this order:

1. `vars` from the pipeline config
2. `profile.env` from the pipeline config
3. environment variables provided by the invoking process

That means external environment variables override `profile.env`.

## Runtime Knobs

| Variable | Purpose | Notes |
| --- | --- | --- |
| `ETLPLUS_LOG_LEVEL` | Override the default CLI logging level. | Applies across supported commands; `--quiet` and `--verbose` still remain available. |
| `ETLPLUS_STATE_DIR` | Change the machine-local ETLPlus state directory. | Affects local run history and scheduler state. |
| `ETLPLUS_TELEMETRY_ENABLED` | Enable or disable the optional telemetry bridge. | Use `true` or `false`. |
| `ETLPLUS_TELEMETRY_EXPORTER` | Select the telemetry exporter bridge. | Current supported values are `opentelemetry` and `none`. |
| `ETLPLUS_TELEMETRY_SERVICE_NAME` | Set the service name used by the telemetry bridge. | Useful when the same workstation or runner hosts multiple ETLPlus workloads. |

## Secret References

For pipeline config values that should not be committed, ETLPlus supports additive `secret:...`
tokens during config substitution.

- Use `secret:NAME` for the stable environment-first form. It resolves `NAME` from the effective
  runtime environment.
- Use `secret:env:NAME` when you want the provider name to be explicit. It is equivalent to
  `secret:NAME`.
- Use `secret:file:path.to.key` only for local-development compatibility. It reads a JSON or YAML
  mapping file selected by `ETLPLUS_SECRETS_FILE`.

Encrypted local secret files and cloud secret-manager backends are intentionally deferred until the
provider interface is stable. In shared, scheduled, or production-like deployments, prefer
environment injection or platform identity over repository-local secret files.

## Storage and Provider Credentials

`etlplus check --readiness --config pipeline.yml` now reports provider-specific
environment gaps for common remote-storage and cloud-database paths.

Use this page as the default runtime posture for shared or scheduled pipelines: keep provider
metadata in config, inject secrets through the process environment or platform identity, and reserve
local files or localhost services for development fixtures.

### AWS S3

Common credential-chain hints checked by readiness:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- `AWS_PROFILE`
- `AWS_DEFAULT_PROFILE`
- `AWS_ROLE_ARN`
- `AWS_WEB_IDENTITY_TOKEN_FILE`
- `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
- `AWS_CONTAINER_CREDENTIALS_FULL_URI`
- `AWS_SHARED_CREDENTIALS_FILE`
- `AWS_CONFIG_FILE`

Typical approaches:

- Explicit key pair via `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Shared config/profile via `AWS_PROFILE`
- Workload or instance credentials via the normal AWS SDK chain

### Azure Blob Storage and ADLS Gen2

Combined Azure environment hint set used by readiness reporting:

- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_STORAGE_ACCOUNT_URL`
- `AZURE_STORAGE_CREDENTIAL`

Bootstrap variables checked by readiness:

- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_STORAGE_ACCOUNT_URL`

Explicit credential variable checked by readiness:

- `AZURE_STORAGE_CREDENTIAL`

If your `azure-blob://...` or `abfs://...` path already embeds the account host, ETLPlus can use
that as bootstrap context. Readiness still evaluates Azure in two phases: bootstrap first, then
explicit credentials for non-public targets.

### BigQuery

Install support first:

```bash
pip install -e ".[database-bigquery]"
```

Common Google Cloud credential hints checked by readiness:

- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_CLOUD_PROJECT`
- `GCLOUD_PROJECT`
- `CLOUDSDK_CONFIG`

Recommended posture:

- Keep connector metadata in the pipeline config with `provider: bigquery`, `project`, and `dataset`
- Inject credentials through Application Default Credentials or other platform identity mechanisms

### Snowflake

Install support first:

```bash
pip install -e ".[database-snowflake]"
```

Common Snowflake credential hints checked by readiness:

- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_AUTHENTICATOR`
- `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SNOWFLAKE_PRIVATE_KEY`

Recommended posture:

- Keep account, database, schema, and warehouse metadata in the connector
- Inject user/password, key-based auth, or SSO-specific environment at runtime
- Use a full `connection_string` when you already have a managed DSN surface

## Example Shell Setup

```bash
export AWS_PROFILE=etlplus-dev
export AZURE_STORAGE_ACCOUNT_URL="https://acct.blob.core.windows.net"
export AZURE_STORAGE_CREDENTIAL="${AZURE_STORAGE_CREDENTIAL}"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/etlplus.json"
export SNOWFLAKE_USER="etlplus"
export SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD}"
etlplus check --readiness --config examples/configs/pipeline.yml
```

## Related Docs

- {doc}`installation`
- {doc}`compatibility`
- {doc}`../guides/pipeline-authoring`
