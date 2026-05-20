# Conda-Forge Staged-Recipes Submission

Use this note when preparing the first ETLPlus submission to `conda-forge/staged-recipes`. The
submitted recipe must stay limited to the broad base PyPI runtime contract for the first pass.

- [Conda-Forge Staged-Recipes Submission](#conda-forge-staged-recipes-submission)
  - [Submission Scope](#submission-scope)
  - [Prepare The Recipe](#prepare-the-recipe)
  - [Pre-Submission Checks](#pre-submission-checks)
  - [Pull Request Notes](#pull-request-notes)

## Submission Scope

- Submit one recipe under `recipes/etlplus/meta.yaml`.
- Render the recipe from `packaging/conda/meta.yaml.j2`; do not hand-maintain a divergent copy.
- Keep the run requirements aligned with `pyproject.toml` base dependencies.
- Do not add optional extras to the first recipe. Optional dependency groups can be evaluated later
  as separate outputs or variants after the base package is accepted and maintainers decide to own
  that extra feedstock complexity.
- Preserve the documented conda-forge package-name mappings:

| PyPI name | Conda-forge name |
| --- | --- |
| `msgpack` | `msgpack-python` |
| `PyYAML` | `pyyaml` |
| `SQLAlchemy` | `sqlalchemy` |

## Prepare The Recipe

Fork `conda-forge/staged-recipes`, create a branch from its `main`, then render the ETLPlus recipe
into the staged-recipes checkout:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /path/to/staged-recipes/recipes/etlplus/meta.yaml \
  --version <released-version-without-leading-v> \
  --sha256 <pypi-sdist-sha256> \
  --maintainer <maintainer-github-handle>
```

Use the package version string from PyPI, such as `1.26.4`, not the Git tag string `v1.26.4`.

The rendered source URL should have this shape:

```text
https://pypi.org/packages/source/e/etlplus/etlplus-<version>.tar.gz
```

## Pre-Submission Checks

Before opening the staged-recipes pull request:

- Confirm the completed ETLPlus `Conda Recipe Validation` workflow run used
  `source_mode: tagged-sdist`.
- Confirm the run used the released version and PyPI sdist SHA256.
- Confirm `platform_scope: all` passed before treating the base recipe as ready for submission.
- Confirm the rendered staged-recipes file still contains only base run requirements and the
  documented conda-forge name mappings.
- Confirm the recipe smoke commands remain:
  - `etlplus --version`
  - `etlplus --help`
  - `etlplus check --help`

## Pull Request Notes

Open the staged-recipes pull request with a short summary that states:

- ETLPlus is a pure-Python `noarch: python` package.
- The first recipe intentionally packages only the broad base PyPI runtime contract.
- Optional extras are intentionally excluded from the first submission.
- The known PyPI-to-conda name mappings are `msgpack` to `msgpack-python`, `PyYAML` to `pyyaml`,
  and `SQLAlchemy` to `sqlalchemy`.

After the staged-recipes pull request is accepted, conda-forge automation creates the package
feedstock. Future recipe updates should happen in the generated feedstock repository rather than in
`staged-recipes`.
