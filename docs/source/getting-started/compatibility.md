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
| Linux | Full CI path | Lint, tests, docs, build, artifact audit, and wheel smoke test run on Ubuntu. |
| macOS | Smoke install | Clean package install and CLI entrypoint verification run in CI. |
| Windows | Smoke install | Clean package install and CLI entrypoint verification run in CI. |

The Linux job remains the primary validation path. macOS and Windows coverage is intended to catch
packaging and entrypoint regressions before release.

## Dependency groups

| Install target | Command | Intended use |
| --- | --- | --- |
| Base runtime | `pip install etlplus` | Documented CLI commands, `etlplus.ops`, `etlplus.api`, and implemented built-in file handlers. |
| Development | `pip install -e ".[dev]"` | Local development, linting, type-checking, tests, and packaging work. |
| Docs | `pip install -e ".[docs]"` | Sphinx and Read the Docs-compatible documentation builds. |
| File extras | `pip install -e ".[file]"` | Remaining scientific and specialty format dependencies such as `netCDF4`, `pyreadr`, `pyreadstat`, and `xarray`. |
| Storage extras | `pip install -e ".[storage]"` | Cloud storage backends for `s3://`, `azure-blob://`, and `abfs://` locations through `etlplus.storage` and `etlplus.file.File`. |

The broad base runtime is intentional. For the `v1.x` stable line, ETLPlus treats the documented
CLI, `etlplus.ops`, `etlplus.api`, and the implemented built-in file handlers as one default
supported experience, so their dependencies stay in the base install rather than being split across
multiple extras. The `file` extra is reserved for narrower scientific and specialty workflows, and
the `storage` extra is reserved for optional remote-storage backends.

## Release interpretation

- “Supported” means the combination is covered by package metadata and some CI validation.
- “Smoke install” means install and CLI entrypoint confidence, not a full behavioral test matrix.
- File-format coverage still depends on the handler status documented in the file-handler matrix.
