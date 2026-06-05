# Dependency And Extension Policy Notes

This note records maintenance guidance for dependency and extension-related changes:

- Review the broad `v1.x` dependency footprint with evidence before changing package defaults.
- Keep extension-style integration work compatible with the stable runtime contracts.

Neither item should narrow or expand the stable public surface accidentally.


- [Dependency And Extension Policy Notes](#dependency-and-extension-policy-notes)
  - [Dependency-Footprint Evidence](#dependency-footprint-evidence)
  - [Base Dependency Snapshot](#base-dependency-snapshot)
  - [Extension Compatibility Principles](#extension-compatibility-principles)

## Dependency-Footprint Evidence

The `v1.x` line intentionally keeps a broad base install so the documented CLI and built-in file
handler surface work from the base PyPI artifact. A smaller default install remains a future
major-version packaging decision unless stable-line usage shows the broad base install is creating
more support load than it prevents.

Before proposing a dependency split, gather evidence in these categories:

- Install friction: resolver failures, platform-specific build failures, install time, or wheel
  size.
- Runtime friction: imports that fail in the base artifact, optional backends that surprise users,
  or dependency conflicts reported by real projects.
- Support volume: repeated issues tied to one dependency family, one platform, or one installer.
- Stable-surface risk: documented commands, handlers, or examples that would stop working from the
  base install if a dependency moved to an extra.

Acceptable low-risk changes in `v1.x`:

- Add meta tests that prevent accidental dependency growth.
- Improve docs that explain when optional extras are needed.
- Add readiness diagnostics for missing optional backends.
- Mark a dependency as a candidate future-major split without changing installation behavior.

Avoid in `v1.x` unless maintainers explicitly approve a stable-surface change:

- Moving a dependency required by documented base CLI behavior into an extra.
- Making `pip`, `pipx`, `uv tool install`, or conda-forge expose different base behavior.
- Treating `uv.lock` or a development lockfile as canonical package metadata.

## Base Dependency Snapshot

This snapshot intentionally tracks the broad base dependency names declared in `pyproject.toml`.
Update it only when maintainers have reviewed why the base install changed and whether the change
affects `pip`, `pipx`, `uv tool install`, and conda-forge parity.

```text
cbor2
click
duckdb
fastavro
frictionless
jinja2
jsonschema
lxml
msgpack
odfpy
openpyxl
pandas
pyarrow
pydantic
pymongo
pyodbc
python-dotenv
pyyaml
requests
sqlalchemy
tomli-w
typer
xlrd
xlwt
```

## Extension Compatibility Principles

Extension-style integration work should follow the same compatibility expectations as built-in
runtime features:

- Keep configuration environment-first and compatible with strict/readiness diagnostics.
- Keep human output separate from machine-readable `etlplus.event.v1` JSONL events.
- Reuse existing run history and telemetry shapes instead of introducing parallel observability
  contracts.
- Keep optional dependencies declared, documented, and diagnosable without breaking base CLI
  startup.
- Return actionable error messages and conventional exit behavior.
- Promote any extension-facing surface to stable only with explicit documentation and public-surface
  contract tests.
