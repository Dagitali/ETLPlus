# Pre-v1.0 Release Checklist

This checklist tracks the remaining work to make ETLPlus look and behave like a stable, well-run
open source Python project before tagging `v1.0.0`.

The currently committed release-polish changes are targeted for `v0.27.0`. The sections below
therefore distinguish between work already in place for the next pre-1.0 release and additional
polish still recommended before the first stable `v1.0.0` tag.

## Must

- [x] Define the supported stable surface area for `v1.0.0`.
  - Confirm which APIs, CLI commands, config shapes, and file handlers are covered by the stability
    promise.
  - Mark placeholders and experimental areas explicitly in user-facing docs.
  - Keep `etlplus.file` as the format layer and treat `etlplus.storage` as the distinct storage
    backend/location surface.
- [x] Keep packaging metadata consistent and single-sourced.
  - Treat `pyproject.toml` as canonical.
  - Keep `setup.py` as a minimal compatibility shim only.
- [x] Enforce the documented quality bar in CI.
  - Run lint, docstring lint, type-checking, tests, docs builds, and distribution builds on pull
    requests.
  - Validate built artifacts with `twine check`.
  - Smoke-test the built wheel in a clean environment.
- [x] Fix release-blocking tooling inconsistencies.
  - Ensure `make typecheck` targets the real package path.
  - Remove or isolate stale automation and temporary backup artifacts from release-critical paths.
- [x] Refresh release-facing versioned docs before tagging.
  - Regenerate demo snippets and version examples.
  - Ensure published docs match the tagged artifact.
- [x] Decide whether the default dependency set is intentionally broad.
  - If not, move connector- or format-specific dependencies into extras before `v1.0.0`.
- [x] Confirm Python support policy.
  - Document whether `>=3.13,<3.15` is intentional.
  - Broaden support if the current floor is not a deliberate product decision.

## Should

- [x] Maintain a clear canonical release-history surface.
  - Summarize user-visible changes, deprecations, and breaking changes per release.
  - Keep the canonical release-history policy explicit in maintainer docs.
- [x] Add issue and pull request templates.
  - Bug report template.
  - Feature request template.
  - Pull request checklist.
- [x] Add at least one non-Linux CI target.
  - Run smoke coverage on macOS and Windows for CLI/package-install confidence.
- [x] Make the README more front-loaded for end users.
  - Keep the first screen focused on installation, quickstart, support policy, and stable
    capabilities.
  - Move deep migration notes and detailed handler tables further down or into docs.
- [x] Add link-checking or docs-hygiene validation.
  - Catch broken internal and external links before release.
- [x] Audit shipped files.
  - Avoid packaging defunct, backup, or scratch artifacts unless they are intentionally retained.

## Nice-to-have

- [x] Publish a documented support policy.
  - Clarify expected response times, supported Python versions, and deprecation windows.
- [x] Add release-drafter or structured release-note automation.
- [x] Add a small compatibility matrix in the docs.
  - Python versions, platforms, and optional dependency groups.
- [x] Add benchmark or performance-smoke coverage for large-file workflows.
- [x] Add provenance or supply-chain hardening beyond pinned GitHub actions.
  - For example, dependency review or SBOM generation.

## Status Notes

- The CI quality-bar item is now closed: lint, docstring lint, type-checking, tests, docs builds,
  distribution validation, artifact audits, and clean-environment wheel smoke checks are enforced
  or verified in the release path.
- The Python Package Index (PyPI) trusted publisher must point at `.github/workflows/release.yml`
  with the `pypi` GitHub environment; pointing it at `ci.yml` will cause trusted publishing to fail.
- The shipped-files item is now closed for the release path because distribution builds and artifact
  audits run in CI from a tracked checkout, so defunct, backup, and scratch files that are not part
  of the committed tree are not part of tagged release artifacts.
- The latest released version is `v0.26.1`, and the changes completed so far in this checklist are
  expected to ship first as part of `v0.27.0`, not `v1.0.0`.
- The stable-surface trimming work now treats CLI support modules and command wiring, storage
  registry/base plumbing, connector support modules, the file-handler registry, database typing
  helpers, and API request manager plumbing as protected underscore-prefixed implementation modules.

## Strongly Recommended Before `v1.0.0`

- [x] Finalize the canonical release-history policy and make docs agree.
  - Decide whether GitHub Releases is the canonical changelog or whether a root-level changelog will
    be maintained.
  - Align `README.md`, `docs/source/changelog.rst`, and maintainer release instructions with that
    choice.
- [x] Simplify or explicitly justify the contributor tooling stack.
  - Reduce overlap between Ruff, Flake8, Black, and autopep8 where practical.
  - Keep the documented maintainer workflow aligned with the actual CI/tooling path.
- [x] Revisit the default dependency surface with `v1.0.0` expectations in mind.
  - Either keep the current batteries-included install deliberately, or move more heavyweight
    connector/file dependencies into extras.
  - Document the rationale clearly either way.
- [x] Add or tighten public-surface contract tests.
  - Assert the documented CLI commands, supported import surfaces, and key stable entrypoints remain
    available across releases.
- [x] Tighten release workflow and release-doc wording drift.
  - Keep release automation comments, README release steps, and docs pages consistent with the actual
    `ci.yml` / `release.yml` split.
- [x] Clarify the `v1.x` maintenance promise beyond the initial support baseline.
  - Define the expected role of patch releases versus minor releases.
  - Clarify what kinds of fixes are expected to be backported, if any.

## Safe To Defer Until `v1.0.1`

- [ ] Ratchet docstring-style enforcement beyond the current pragmatic ignore set.
  - `D200` is intentionally tolerated for wrapped one-sentence docstrings; keep tightening the
    remaining `pydocstyle` backlog incrementally once the stable line is out.
- [ ] Further trim or rationalize dependency groups after observing real user install patterns.
- [ ] Expand performance-smoke and cross-platform coverage once `v1.0.0` usage patterns are clearer.

## Prioritized Implementation Plan

### Before `v0.27.0` Branch Cut

- [x] Finalize the canonical release-history policy and make docs agree.
  - Choose the canonical release-notes surface now so `v0.27.0` already models the intended
    maintainer workflow.
  - Align `README.md`, `docs/source/changelog.rst`, and maintainer release instructions to that
    single policy.
- [x] Tighten release workflow and release-doc wording drift.
  - Clean up comments and release instructions so the `ci.yml` / `release.yml` split is described
    consistently everywhere.
  - Keep this phase low-risk and documentation-heavy so it does not destabilize the `v0.27.0`
    release branch.

### Before `v1.0.0-rc1`

- [x] Simplify or explicitly justify the contributor tooling stack.
  - Decide whether Ruff is the primary lint/format path and demote or remove overlapping tooling as
    appropriate.
  - Ensure the documented maintainer workflow matches the actual supported toolchain.
- [x] Revisit the default dependency surface with `v1.0.0` expectations in mind.
  - Either commit to the current batteries-included install or move more heavyweight dependencies
    into extras before the stable support promise is locked.
  - Document the rationale in packaging and install docs.
- [x] Add or tighten public-surface contract tests.
  - Cover documented CLI commands, supported import surfaces, and other promised stable entrypoints.
  - Treat these as release-gating tests for the first stable line.
- [x] Clarify the `v1.x` maintenance promise beyond the initial support baseline.
  - Define patch-vs-minor expectations and any backport posture before calling the line stable.

### Safe After Stable Release

- Ratchet docstring-style enforcement incrementally once the stable line is out.
- Continue trimming dependency groups after observing actual install and support patterns.
- Expand performance-smoke and cross-platform coverage in response to real `v1.x` usage.

## Current Focus

The current release-prep posture is:

- `v0.27.x`: ship the already-completed polish and workflow improvements
- Pre-`v1.0.0`: preserve that aligned release surface, keep the tracked release docs/workflows in
  sync, and focus remaining effort on final execution hygiene rather than new policy churn
