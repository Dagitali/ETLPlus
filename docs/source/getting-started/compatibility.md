# Compatibility

ETLPlus documents a narrow, explicit support window. The statements on this page describe the
supported release contract for the stable `v1.x` line.

## Python support

| Python | Status | Notes |
| --- | --- | --- |
| 3.13 | Supported | Covered by lint, tests, docs, packaging, and smoke-install CI. |
| 3.14 | Supported | Covered by lint and tests in CI. |
| < 3.13 | Unsupported | Outside the declared package metadata and release policy. |
| >= 3.15 | Not yet declared | Compatibility has not been validated or promised yet. |

For response targets, deprecation windows, and maintenance expectations, see the repository support
policy in `SUPPORT.md`.

## Platform coverage

| Platform | Coverage level | Notes |
| --- | --- | --- |
| Linux | Full CI path | Lint, tests, docs, build, artifact audit, wheel smoke test, and supported-installer smoke tests run on Ubuntu. |
| macOS | Smoke install | Clean package install and and CLI entrypoint/help verification run in CI: `etlplus --version`, `etlplus --help`, `etlplus check --help`, and `etlplus ui --help`. |
| Windows | Smoke install | Clean package and CLI entrypoint/help verification run in CI:  `etlplus --version`, `etlplus --help`, `etlplus check --help`, and `etlplus ui --help`. |

The Linux job remains the primary validation path. macOS and Windows coverage is intended to catch
packaging and entrypoint regressions before release.

## Dependency groups

| Install target | Command | Intended use |
| --- | --- | --- |
| Base runtime | `pip install etlplus` | Documented CLI commands, `etlplus.ops`, `etlplus.api`, and implemented built-in file handlers. |
| Isolated CLI with pipx | `pipx install etlplus` | Base ETLPlus CLI installed as an isolated command-line application. |
| Isolated CLI with uv | `uv tool install etlplus` | Base ETLPlus CLI installed as an isolated command-line application through uv's tool installer. |
| Development | `pip install -e ".[dev]"` | Local development, linting, type-checking, tests, and packaging work. |
| Docs | `pip install -e ".[docs]"` | Sphinx and Read the Docs-compatible documentation builds. |
| File extras | `pip install -e ".[file]"` | Remaining scientific and specialty format dependencies such as `netCDF4`, `pyreadr`, `pyreadstat`, and `xarray`. |
| BigQuery connector extra | `pip install -e ".[database-bigquery]"` | Optional BigQuery connector metadata/readiness support via `provider: bigquery` plus `project` and `dataset` fields when no connection string is supplied. |
| Snowflake connector extra | `pip install -e ".[database-snowflake]"` | Optional Snowflake connector metadata/readiness support via `provider: snowflake` plus `account`, `database`, and `schema` fields when no connection string is supplied. |
| Storage extras | `pip install -e ".[storage]"` | Remote storage backends for `s3://`, `azure-blob://`, `abfs://`, and `hdfs://` locations through `etlplus.storage` and `etlplus.file.File`. |

Remote object storage and managed database metadata are first-class stable-line configuration paths.
Local filesystem paths, SQLite files, localhost databases, and Docker Compose services are still
useful for development and smoke tests, but they should not be treated as the only supported
operating model.

The broad base runtime is intentional. For the `v1.x` stable line, ETLPlus treats the documented
CLI, `etlplus.ops`, `etlplus.api`, and the implemented built-in file handlers as one default
supported experience, so their dependencies stay in the base install rather than being split across
multiple extras. The `file` extra is reserved for narrower scientific and specialty workflows, and
the `storage` extra is reserved for optional remote-storage backends. The `database-bigquery` extra
is reserved for optional BigQuery connector metadata and readiness checks rather than the default
runtime surface. The `database-snowflake` extra is reserved for optional Snowflake connector
metadata and readiness checks rather than the default runtime surface.

The installer review keeps that broad base runtime unchanged for `pip`, `pipx`, and `uv tool
install`. Splitting the default install into a smaller CLI core plus larger extras would be a future
major-version packaging decision because it could change what existing users receive from the
documented base install.

## Installer policy

- Use `pip install etlplus` when ETLPlus should be available as both a Python package and CLI in the
  active environment.
- Use `pipx install etlplus` for the preferred isolated CLI installation path.
- Use `uv tool install etlplus` when uv is the local tool installer.
- Conda-forge status: tagged PyPI sdist validation has passed on Linux, macOS, and Windows. Until
  the feedstock is accepted and published, use the PyPI-based installers above for supported
  installs.

## Release interpretation

- “Supported” means the combination is covered by package metadata and some CI validation.
- “Smoke install” means install and CLI entrypoint/help confidence, not a full behavioral test
  matrix.
- File-format coverage still depends on the handler status documented in the file-handler matrix.
