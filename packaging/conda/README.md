# Conda Feedstock Preparation

Status: feedstock-preparation draft. This directory is not a supported ETLPlus install channel and
is not published to conda-forge.

The draft recipe in this directory is intended to seed a future conda-forge staged-recipes
submission if maintainers accept feedstock ownership.

- [Conda Feedstock Preparation](#conda-feedstock-preparation)
  - [Current Scope](#current-scope)
  - [Validation Checklist](#validation-checklist)
  - [CI Validation](#ci-validation)
  - [Known Package Name Mappings](#known-package-name-mappings)

## Current Scope

- Package the same broad `v1.x` base runtime surface as the PyPI artifact.
- Keep `pyproject.toml` as the canonical packaging metadata source.
- Keep optional extras out of the first feedstock pass unless maintainers decide to add separate
  outputs or variants.
- Expose the same CLI entrypoint: `etlplus = etlplus.cli:main`.

## Validation Checklist

Before submitting to conda-forge, render `meta.yaml.j2` into a release-specific `meta.yaml` with the
released version, PyPI sdist SHA256, and maintainer handle:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/etlplus-conda-recipe/meta.yaml \
  --version 1.2.3 \
  --sha256 <pypi-sdist-sha256> \
  --maintainer <maintainer-github-handle>
```

Then run an actual recipe build/test against the rendered recipe:

```bash
conda build /tmp/etlplus-conda-recipe --channel conda-forge
```

or:

```bash
rattler-build build --recipe /tmp/etlplus-conda-recipe/meta.yaml --channel conda-forge
```

For a local pre-release validation build from the current checkout, render with `--source-path`:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/etlplus-conda-local/meta.yaml \
  --version 0.0.0 \
  --sha256 0000000000000000000000000000000000000000000000000000000000000000 \
  --maintainer <maintainer-github-handle> \
  --source-path .
```

The local validation path was exercised on `osx-arm64` with `conda-build` from a clean source
snapshot. The rendered recipe built a `.conda` artifact and passed the recipe smoke commands. That
does not replace the release-gating build from a tagged PyPI sdist with a pinned `sha256`.

Minimum validation before declaring conda-forge supported:

- Build a `.conda` artifact from the tagged PyPI sdist.
- Install the built artifact in a clean conda environment.
- Verify `etlplus --version`, `etlplus --help`, and `etlplus check --help`.
- Verify `python -c "import etlplus; print(etlplus.__version__)"`.
- Repeat the build/test path on Linux, macOS, and Windows.

## CI Validation

The manual `Conda Recipe Validation` workflow in `.github/workflows/conda-recipe.yml` renders the
draft recipe from the current checkout, builds it with `conda-build`, and runs the recipe smoke
tests. It defaults to Linux-only validation so maintainers can check the lowest-cost path first.
The initial workflow pins `conda-build=25` because the local validation environment exposed a
rattler-backed solver plugin crash in the newer tooling line unrelated to the ETLPlus recipe.

Before declaring conda-forge supported, rerun the workflow with `platform_scope: all` so Linux,
macOS, and Windows all pass, then repeat the process from a tagged PyPI sdist with a pinned
`sha256`.

## Known Package Name Mappings

Most runtime dependencies map directly from PyPI names to conda-forge names. The important
differences are:

| PyPI name | Conda-forge name |
| --- | --- |
| `msgpack` | `msgpack-python` |
| `PyYAML` | `pyyaml` |
| `SQLAlchemy` | `sqlalchemy` |

See `CONDA-FORGE-SPIKE.md` for the feasibility-spike evidence and commands.
