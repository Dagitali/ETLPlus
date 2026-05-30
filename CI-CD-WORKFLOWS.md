# CI/CD Workflow Map

This document explains the public role of each GitHub Actions workflow used by ETLPlus.

- [CI/CD Workflow Map](#cicd-workflow-map)
  - [Scope](#scope)
  - [Workflow Overview](#workflow-overview)
  - [PR Gates](#pr-gates)
  - [CI](#ci)
  - [Release](#release)
  - [How They Interact](#how-they-interact)
  - [Required Checks](#required-checks)

## Scope

This file is public and describes the visible workflow structure. It does not include secrets,
credential handling, or emergency operator procedures.

## Workflow Overview

ETLPlus currently separates automation into three workflows:

- `pr.yml` for required pull-request and merge-queue gates
- `ci.yml` for heavier pre-merge validation on protected branches and merge queue
- `cd.yml` for tagged release publication

This split keeps required PR checks fast enough to use as branch-protection gates while moving
heavier validation and publication logic into a second stage that can still block protected-branch
integration when desired.

## PR Gates

Workflow file: `.github/workflows/pr.yml`

Workflow name: `PR Gates`

Primary role:

- Run the required validation used by protected-branch pull requests and merge queues

Current responsibilities:

- Ruff linting and formatting drift detection
- Committed `uv.lock` freshness against `pyproject.toml`
- Unit and integration test execution with coverage
- Docstring linting
- Type checking
- HTML docs build

This workflow runs on pull requests into protected branches and also on pushes to the working and
release-oriented branches that feed later validation.

## CI

Workflow file: `.github/workflows/ci.yml`

Workflow name: `CI`

Primary role:

- Run heavier pre-merge validation for protected-branch pull requests and merge queue entries

Current responsibilities:

- Non-PR docs builders such as `epub` and `linkcheck`
- macOS and Windows install smoke coverage
- Distribution build, verification, and wheel smoke testing

This workflow runs on pull requests into `main` and `develop`, on `merge_group` for those same
protected branches, and manually via `workflow_dispatch`.

## Release

Workflow file: `.github/workflows/cd.yml`

Workflow name: `Release`

Primary role:

- build, validate, and publish tagged releases

Current responsibilities:

- Build source and wheel distributions with `python -m build`
- Audit release artifacts and validate them with `twine check`
- Smoke-test supported installer paths against the built wheel
- Smoke-test packaged behavior against the built wheel
- Build release-time documentation targets
- Publish a GitHub Release
- Publish to PyPI through trusted publishing

This workflow is tag-driven. It runs when a `v*.*.*` tag is pushed.

## How They Interact

The workflows intentionally do not form a single linear chain.

- `PR Gates` is the required branch-protection workflow.
- `CI` runs alongside `PR Gates` when protected-branch pull requests or merge-queue entries need
  the heavier validation set.
- `Release` does not run because `CI` succeeded; it runs only when a release tag is pushed.

That means a successful `ci.yml` run is a confidence signal, not a publication trigger by itself.

## Required Checks

Protected branches must require checks from `PR Gates`. They may also require checks from `CI` when
you want the heavier docs, smoke-install, and distribution-validation workflow to block merges.

At the time of writing, the expected required checks are:

- `Lint on Python 3.13`
- `Check uv lockfile`
- `Test on Python 3.13`
- `Doclint on Python 3.13`
- `Type-check on Python 3.13`
- `Build docs (html)`

The natural next required checks, if you want the heavier protected-branch gate to block merges on
GitHub too, are:

- `Build docs (epub)`
- `Build docs (linkcheck)`
- `Smoke install on macos-latest`
- `Smoke install on windows-latest`
- `Build distributions`

The exact protected-branch settings and GitHub configuration details are maintained separately in
`.github/BRANCH-PROTECTION.md`.
