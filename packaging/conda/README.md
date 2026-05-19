# Conda Feedstock Preparation

This directory contains candidate conda-forge feedstock materials for maintainer review. ETLPlus is
not published on conda-forge yet; use the supported PyPI installation paths until a conda-forge
feedstock is accepted and maintained.

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
candidate recipe from the current checkout, builds it with `conda-build`, and runs the recipe smoke
tests. It defaults to Linux-only validation so maintainers can check the lowest-cost path first, and
it can also isolate macOS or Windows runs when platform-specific failures need investigation. The
initial workflow pins `micromamba` and `conda-build=25` because local and GitHub-hosted validation
exposed toolchain failures unrelated to the ETLPlus recipe.

Before declaring conda-forge supported, run the workflow with `source_mode: tagged-sdist`,
`release_version`, and `sdist_sha256` set from the released PyPI sdist. Use `platform_scope: all`
for that support-gate run so Linux, macOS, and Windows all pass from the same released artifact.

The candidate base recipe is expected to match the broad base dependency set in `pyproject.toml`.
Only documented conda-forge package-name mappings should differ from the PyPI requirement names; do
not add optional extras to the first base recipe unless maintainers choose separate conda outputs or
variants.

## Known Package Name Mappings

Most runtime dependencies map directly from PyPI names to conda-forge names. The important
differences are:

| PyPI name | Conda-forge name |
| --- | --- |
| `msgpack` | `msgpack-python` |
| `PyYAML` | `pyyaml` |
| `SQLAlchemy` | `sqlalchemy` |

See `packaging/conda/FEEDSTOCK-PREP.md` for the public feedstock-preparation status, validation
path, and support gates.
