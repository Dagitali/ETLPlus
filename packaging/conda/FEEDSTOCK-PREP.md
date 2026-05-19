# Conda-Forge Feedstock Preparation

ETLPlus is preparing candidate conda-forge feedstock materials for maintainer review. These files
are not published on conda-forge yet; use the supported PyPI installation paths until a conda-forge
feedstock is accepted and maintained.

This note documents the recipe source, validation path, and remaining publication requirements for
contributors who want to review or improve the candidate feedstock.

- [Conda-Forge Feedstock Preparation](#conda-forge-feedstock-preparation)
  - [Current Result](#current-result)
  - [Recipe Source](#recipe-source)
  - [Dependency Mapping](#dependency-mapping)
  - [Validation Path](#validation-path)
  - [Support Gates](#support-gates)

## Current Result

The current base dependency set is viable for a candidate conda-forge recipe on the locally
validated platform. The preparation work has validated:

- A conda-forge solve for the base runtime dependency set with Python 3.13.
- A local source install using conda-resolved dependencies and pip build isolation disabled.
- A rendered local-source recipe build with `conda-build`.
- CLI smoke checks for `etlplus --version`, `etlplus --help`, and `etlplus check --help`.

These checks show that the packaging path is viable. Publication still requires maintainer
ownership, release-artifact validation, and cross-platform conda-forge build coverage.

## Recipe Source

The candidate recipe source is `packaging/conda/meta.yaml.j2`. It intentionally keeps
release-specific values as placeholders:

- `<release-version>`
- `<sdist-sha256>`
- `<maintainer-github-handle>`

Render the template with:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/etlplus-conda-recipe/meta.yaml \
  --version 1.2.3 \
  --sha256 <pypi-sdist-sha256> \
  --maintainer <maintainer-github-handle>
```

For local checkout validation, use `--source-path` to render a path-based recipe.

## Dependency Mapping

Most PyPI dependency names map directly to conda-forge package names. The required differences are:

| PyPI requirement | Conda-forge requirement |
| --- | --- |
| `msgpack>=1.0.8` | `msgpack-python >=1.0.8` |
| `PyYAML>=6.0.3` | `pyyaml >=6.0.3` |
| `SQLAlchemy>=2.0.45` | `sqlalchemy >=2.0.45` |

The first feedstock pass should preserve the same broad base runtime contract as the PyPI artifact.
Optional extras should remain separate follow-up outputs or variants unless maintainers explicitly
decide otherwise.

## Validation Path

The manual `Conda Recipe Validation` GitHub Actions workflow provides non-release and release-sdist
validation for the candidate recipe. It runs Linux by default and can be dispatched with
`platform_scope: all` to include Linux, macOS, and Windows.

Use `source_mode: local-source` for pre-release checks from the current checkout. Before submitting
to conda-forge, use `source_mode: tagged-sdist` with the released version and PyPI sdist SHA256, and
run with `platform_scope: all`.

Before submitting to conda-forge, maintainers should also build and test a recipe rendered from the
tagged PyPI sdist with a pinned SHA256:

```bash
conda-build /tmp/etlplus-conda-recipe --channel conda-forge
```

The recipe test phase should verify:

- `etlplus --version`
- `etlplus --help`
- `etlplus check --help`
- `python -c "import etlplus; print(etlplus.__version__)"`

## Support Gates

Before documenting conda-forge as a supported ETLPlus install channel:

- Maintainers accept feedstock ownership.
- The recipe builds from a tagged PyPI sdist with a pinned SHA256.
- Linux, macOS, and Windows recipe build/test runs pass.
- The published feedstock exposes the same base CLI/runtime contract as the PyPI artifact.
- The base recipe run requirements match `pyproject.toml` base dependencies except for documented
  conda-forge package-name mappings; optional extras remain separate follow-up outputs or variants.
