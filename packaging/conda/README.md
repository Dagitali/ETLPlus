# Conda Packaging Notes

This directory contains the draft ETLPlus conda-forge recipe and maintainer notes. ETLPlus is not
published on conda-forge yet; use the supported PyPI installation paths until a conda-forge
feedstock has been accepted and published.

- [Conda Packaging Notes](#conda-packaging-notes)
  - [File Map](#file-map)
  - [Current Status](#current-status)
  - [Beginner Workflow](#beginner-workflow)
  - [Recipe Scope](#recipe-scope)
  - [Important Terms](#important-terms)

## Files

- `meta.yaml.j2`: the source template for the first conda-forge recipe.
- `tools/render_conda_recipe.py`: the repository-root helper that renders `meta.yaml.j2` with
  release-specific values.
- `FEEDSTOCK-PREP.md`: the preparation record, dependency mapping, and validation rules.
- `STAGED-RECIPES-SUBMISSION.md`: the step-by-step submission runbook for
  `conda-forge/staged-recipes`.

Keep detailed validation history in `FEEDSTOCK-PREP.md` and submission mechanics in
`STAGED-RECIPES-SUBMISSION.md`. This README should stay short and act as the entry point.

## Current Status

The base recipe has passed the ETLPlus support gate from a tagged PyPI sdist with a pinned SHA256
across Linux, macOS, and Windows. Publication is still pending conda-forge submission, review,
feedstock creation, and package availability.

Do not add user-facing `conda install etlplus` documentation until the package is actually
available from conda-forge.

## Beginner Workflow

Use this path if you have not packaged a Python project for conda-forge before:

1. Read `FEEDSTOCK-PREP.md` to understand what is already validated and why the recipe is base-only.
2. Install or activate a conda environment that has `conda-build` available.
3. Render a release-specific `meta.yaml` from `meta.yaml.j2`; do not edit a copied recipe by hand.
4. Run `conda build` against the rendered recipe and confirm the recipe tests pass.
5. Follow `STAGED-RECIPES-SUBMISSION.md` to submit the rendered recipe to
   `conda-forge/staged-recipes`.

The local validation command shape is:

```bash
conda build /tmp/etlplus-conda-recipe --channel conda-forge
```

`conda build` reads `/tmp/etlplus-conda-recipe/meta.yaml`, creates a clean build/test environment,
installs the conda-resolved dependencies, builds the package, and runs the commands listed in the
recipe `test:` section.

## Recipe Scope

The first conda-forge recipe must:

- Package the same broad `v1.x` base runtime surface as the PyPI artifact.
- Keep `pyproject.toml` as the canonical packaging metadata source.
- Keep optional extras out of the first recipe.
- Expose the same CLI entrypoint: `etlplus = etlplus.cli:main`.
- Preserve the documented PyPI-to-conda package-name mappings in `FEEDSTOCK-PREP.md`.

Optional extras can be evaluated later as separate conda outputs or variants after the base package
is accepted and maintainers decide to own that additional feedstock complexity.

## Important Terms

- **Recipe**: a `meta.yaml` file that tells conda how to fetch, build, test, and describe a package.
- **staged-recipes**: the conda-forge intake repository for first-time package submissions.
- **Feedstock**: the generated repository that maintains a package after staged-recipes acceptance.
- **sdist**: the source distribution uploaded to PyPI; the first ETLPlus recipe builds from this
  artifact.
- **SHA256**: the checksum conda-forge uses to verify the downloaded PyPI sdist exactly matches the
  artifact maintainers validated.
