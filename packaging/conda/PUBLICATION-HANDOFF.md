# Conda-Forge Publication Handoff

Use this checklist after the `conda-forge/staged-recipes` pull request is accepted and the ETLPlus
feedstock exists. Until every publication check below passes, keep user-facing docs phrased as
support-gate validated but not yet installable from conda-forge.

- [Preconditions](#preconditions)
- [Publication Checks](#publication-checks)
- [Repository Updates After Publication](#repository-updates-after-publication)
- [Ongoing Feedstock Ownership](#ongoing-feedstock-ownership)

## Preconditions

- The staged-recipes pull request has been merged.
- The generated ETLPlus feedstock repository exists under `conda-forge`.
- Feedstock CI has produced an installable package for the released ETLPlus version.
- The published package is visible from the `conda-forge` channel.

## Publication Checks

Run these checks in a clean temporary conda environment before changing user-facing installation
docs:

```bash
conda create --name etlplus-conda-smoke --channel conda-forge etlplus
conda activate etlplus-conda-smoke
etlplus --version
etlplus --help
etlplus check --help
etlplus ui --help
python -c "import etlplus; print(etlplus.__version__)"
```

Confirm the installed version is the released version intended for the documentation update. If the
solver installs an older package, wait for channel indexing or publish a feedstock maintenance
update before advertising conda-forge support.

## Repository Updates After Publication

After the smoke checks pass:

- Update user-facing installation docs with the conda-forge command:
  `conda install --channel conda-forge etlplus`.
- Update compatibility or installer-matrix docs so conda-forge appears as a supported install
  channel rather than a prepared channel.
- Update `RELEASE-CHECKLIST.md`, `roadmap.md`, and this directory's conda notes to record the
  published feedstock status.
- Keep `pyproject.toml` as canonical package metadata and keep the feedstock recipe aligned with the
  broad `v1.x` base dependency contract.
- Keep optional extras out of the base feedstock unless maintainers explicitly choose separate
  outputs or variants later.

## Ongoing Feedstock Ownership

After publication, routine recipe updates belong in the generated feedstock repository. Changes to
the ETLPlus template in `packaging/conda/meta.yaml.j2` should still be made when the repository's
source-of-truth expectations change, but published package maintenance happens through feedstock
pull requests.
