# Conda-Forge Feedstock Preparation

ETLPlus has prepared candidate conda-forge feedstock materials for maintainer review. The recipe is
validated but not published on conda-forge yet.

- [Current Result](#current-result)
- [Recipe Source](#recipe-source)
- [Dependency Mapping](#dependency-mapping)
- [Conda CLI Validation](#conda-cli-validation)
- [GitHub Actions Validation](#github-actions-validation)
- [Publication Gates](#publication-gates)

## Current Result

The current base dependency set is viable for a candidate conda-forge recipe. Preparation has
validated:

- A conda-forge solve for the base runtime dependency set with Python 3.13.
- A local source install using conda-resolved dependencies and disabled pip build isolation.
- A rendered local-source recipe build with `conda-build`.
- A tagged PyPI sdist recipe build/test run with a pinned SHA256 across Linux, macOS, and Windows.
- CLI smoke checks for `etlplus --version`, `etlplus --help`, `etlplus check --help`, and `etlplus
  ui --help`.

Publication still requires maintainer ownership, staged-recipes acceptance, feedstock creation, and
package availability from conda-forge.

## Recipe Source

The candidate recipe source is `packaging/conda/meta.yaml.j2`. It uses placeholders for values that
change for each release:

- `<release-version>`: the PyPI version, such as `1.26.4`, without a leading `v`.
- `<sdist-sha256>`: the 64-character SHA256 for the PyPI source distribution.
- `<maintainer-github-handle>`: the GitHub username that will maintain the feedstock.

Render a release recipe from the ETLPlus repository root:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/etlplus-conda-recipe/meta.yaml \
  --version 1.2.3 \
  --sha256 <pypi-sdist-sha256> \
  --maintainer <maintainer-github-handle>
```

The rendered recipe should fetch this source:

```text
https://pypi.org/packages/source/e/etlplus/etlplus-<version>.tar.gz
```

For local checkout validation before a release exists, render a path-based recipe:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/etlplus-conda-local/meta.yaml \
  --version 0.0.0 \
  --sha256 0000000000000000000000000000000000000000000000000000000000000000 \
  --maintainer <maintainer-github-handle> \
  --source-path .
```

Use the path-based recipe only for local validation. The staged-recipes submission should use the
tagged PyPI sdist URL plus the real SHA256.

## Dependency Mapping

Most PyPI dependency names map directly to conda-forge package names. The required differences are:

| PyPI requirement | Conda-forge requirement |
| --- | --- |
| `msgpack>=1.1.2` | `msgpack-python >=1.1.2` |
| `PyYAML>=6.0.3` | `pyyaml >=6.0.3` |
| `SQLAlchemy>=2.0.49` | `sqlalchemy >=2.0.49` |

The first feedstock pass should preserve the same broad base runtime contract as the PyPI artifact.
Do not add optional extras to the base recipe. Optional extras should remain later follow-up outputs
or variants unless maintainers explicitly decide to own that added feedstock complexity.

## Conda CLI Validation

`conda build` is provided by the `conda-build` package. If your active conda environment does not
recognize `conda build`, install it from conda-forge:

```bash
conda install --channel conda-forge conda-build
```

Then build and test the release-sdist recipe by passing the directory that contains the rendered
`meta.yaml`:

```bash
conda build /tmp/etlplus-conda-recipe --channel conda-forge
```

If you rendered the local-source recipe shown above, build that matching output directory instead:

```bash
conda build /tmp/etlplus-conda-local --channel conda-forge
```

The directory passed to `conda build` must contain `meta.yaml`. During the build, conda will:

1. Read `meta.yaml`.
2. Download and verify the PyPI sdist using the pinned SHA256.
3. Create isolated build and test environments.
4. Install host and run dependencies from conda-forge.
5. Run the build script: `python -m pip install . --no-deps --no-build-isolation -vv`.
6. Run the recipe tests.

The recipe test phase must verify:

- `etlplus --version`
- `etlplus --help`
- `etlplus check --help`
- `etlplus ui --help`
- `python -c "import etlplus; print(etlplus.__version__)"`

A passing local build produces a `.conda` package in your conda build output directory. The exact
path depends on your conda installation and platform; `conda build` prints it near the end of a
successful run.

## GitHub Actions Validation

The manual `Conda Recipe Validation` workflow in `.github/workflows/conda-recipe.yml` validates the
same recipe path in CI. It can run in two modes:

- `source_mode: local-source`: validates the current checkout before a release.
- `source_mode: tagged-sdist`: validates the released PyPI sdist with the real version and SHA256.

The workflow defaults to Linux because that is the lowest-cost validation path. Use
`platform_scope: macos` or `platform_scope: windows` to isolate macOS or Windows runs when a
platform-specific failure needs investigation, and use `platform_scope: all` before staged-recipes
submission or future support-gate refreshes.

The workflow pins `micromamba` and `conda-build=25` because local and GitHub-hosted validation
exposed toolchain failures unrelated to the ETLPlus recipe. The completed support-gate run used
`source_mode: tagged-sdist`, the released version, the PyPI sdist SHA256, and
`platform_scope: all`.

## Publication Gates

Before documenting conda-forge as a supported ETLPlus install channel:

- Maintainers accept feedstock ownership.
- The recipe has passed tagged PyPI sdist validation with a pinned SHA256 across Linux, macOS, and
  Windows.
- The staged-recipes pull request is accepted.
- The generated feedstock publishes an installable package.
- The published package exposes the same base CLI/runtime contract as the PyPI artifact.
- The base recipe run requirements match `pyproject.toml` base dependencies except for documented
  conda-forge package-name mappings.

Use `STAGED-RECIPES-SUBMISSION.md` for the actual first-submission procedure.
