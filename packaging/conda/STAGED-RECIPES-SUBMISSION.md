# Conda-Forge Staged-Recipes Submission

Use this runbook to submit the first ETLPlus recipe to the
[`conda-forge/staged-recipes`](https://github.com/conda-forge/staged-recipes) GitHub repository,
where conda-forge reviews new package recipes. The submitted recipe must stay limited to the broad
base PyPI runtime contract for the first pass.

- [Conda-Forge Staged-Recipes Submission](#conda-forge-staged-recipes-submission)
  - [Submission Scope](#submission-scope)
  - [Prerequisites](#prerequisites)
  - [Prepare The Recipe](#prepare-the-recipe)
  - [Validate With The Conda CLI](#validate-with-the-conda-cli)
  - [Open The Pull Request](#open-the-pull-request)
  - [After Acceptance](#after-acceptance)

## Submission Scope

- Submit one recipe under `recipes/etlplus/meta.yaml` in your local checkout of a
  `conda-forge/staged-recipes` fork. This path is relative to the staged-recipes repository root,
  not the ETLPlus repository root.
- Render the recipe from `packaging/conda/meta.yaml.j2`; do not hand-maintain a divergent copy.
- Keep run requirements aligned with `pyproject.toml` base dependencies.
- Do not add optional extras to the first recipe.
- Preserve the dependency mappings documented in `FEEDSTOCK-PREP.md`.

The first submission should prove that the base ETLPlus CLI/package artifact is installable from
conda-forge. Optional extras can be evaluated later as separate outputs or variants.

For ETLPlus, you will fork `conda-forge/staged-recipes`, clone your fork locally, and render
ETLPlus's recipe into the cloned fork at `recipes/etlplus/meta.yaml`. That file is created during
the submission workflow; it is not expected to exist in this ETLPlus repository.

## Prerequisites

You need:

- A released ETLPlus version on PyPI.
- The PyPI sdist SHA256 for that exact release.
- A fork of `conda-forge/staged-recipes`.
- A GitHub account that should be listed as a feedstock maintainer.
- A local conda environment with `conda-build` installed.

If `conda build` is not available, install the build tool:

```bash
conda install --channel conda-forge conda-build
```

The version passed to the renderer must be the PyPI version string, such as `1.26.4`, not the Git
tag string `v1.26.4`.

## Prepare The Recipe

Clone your staged-recipes fork and create a branch from its `main` branch. The example below uses
`/tmp/staged-recipes` as the local checkout of your fork:

```bash
git clone https://github.com/<your-github-handle>/staged-recipes.git /tmp/staged-recipes
cd /tmp/staged-recipes
git checkout main
git pull
git checkout -b add-etlplus
mkdir -p recipes/etlplus
```

From the ETLPlus repository root, render the recipe into the staged-recipes checkout:

```bash
python tools/render_conda_recipe.py \
  --template packaging/conda/meta.yaml.j2 \
  --output /tmp/staged-recipes/recipes/etlplus/meta.yaml \
  --version <released-version-without-leading-v> \
  --sha256 <pypi-sdist-sha256> \
  --maintainer <maintainer-github-handle>
```

This command creates `/tmp/staged-recipes/recipes/etlplus/meta.yaml`. In the staged-recipes
repository, that same file is `recipes/etlplus/meta.yaml`. The `--output` path points outside the
ETLPlus repository because the pull request must be opened from your staged-recipes fork, not from
the ETLPlus project.

Inspect the rendered source section. It should use the PyPI sdist URL and real SHA256:

```yaml
source:
  url: https://pypi.org/packages/source/e/etlplus/etlplus-<version>.tar.gz
  sha256: <pypi-sdist-sha256>
```

Do not submit a recipe that uses `source: path`; that form is only for local checkout validation.

## Validate With The Conda CLI

Run the conda build from any directory, pointing at the staged-recipes recipe directory:

```bash
conda build /tmp/staged-recipes/recipes/etlplus --channel conda-forge
```

This command reads `/tmp/staged-recipes/recipes/etlplus/meta.yaml`, creates isolated environments,
installs dependencies from conda-forge, builds ETLPlus from the PyPI sdist, and runs the recipe test
commands. Before opening the pull request, confirm the test output includes:

- `etlplus --version`
- `etlplus --help`
- `etlplus check --help`

Also confirm the ETLPlus manual `Conda Recipe Validation` workflow has passed with:

- `source_mode: tagged-sdist`
- the released version
- the PyPI sdist SHA256
- `platform_scope: all`

## Open The Pull Request

Commit only the staged-recipes recipe:

```bash
cd /tmp/staged-recipes
git add recipes/etlplus/meta.yaml
git commit -m "Add etlplus"
git push --set-upstream origin add-etlplus
```

Open a pull request against `conda-forge/staged-recipes`. In the PR description, state:

- ETLPlus is a pure-Python `noarch: python` package.
- The first recipe intentionally packages only the broad base PyPI runtime contract.
- Optional extras are intentionally excluded from the first submission.
- The recipe was rendered from `packaging/conda/meta.yaml.j2`.
- The known PyPI-to-conda name mappings are documented in the ETLPlus conda preparation notes.

During review, prefer updating `packaging/conda/meta.yaml.j2` in ETLPlus and re-rendering the staged
recipe over hand-editing `recipes/etlplus/meta.yaml`. That keeps the repository template and the
submitted recipe aligned.

## After Acceptance

After the staged-recipes pull request is accepted, conda-forge automation creates the ETLPlus
feedstock repository. Future recipe updates should happen in the generated feedstock repository, not
in `staged-recipes`.

Only after the feedstock publishes an installable package should ETLPlus user-facing installation
docs claim conda-forge support.
