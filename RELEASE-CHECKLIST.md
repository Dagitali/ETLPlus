# Pre-v1.0 Release Checklist

This checklist tracks the remaining work to make ETLPlus look and behave like a stable, well-run
open source Python project before tagging `v1.0.0`.

## Must

- [x] Define the supported stable surface area for `v1.0.0`.
  - Confirm which APIs, CLI commands, config shapes, and file handlers are covered by the stability
    promise.
  - Mark placeholders and experimental areas explicitly in user-facing docs.
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

- [x] Maintain an in-repository changelog.
  - Summarize user-visible changes, deprecations, and breaking changes per release.
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
- The shipped-files item is now closed for the release path because distribution builds and artifact
  audits run in CI from a tracked checkout, so defunct, backup, and scratch files that are not part
  of the committed tree are not part of tagged release artifacts.

## Current Focus

All checklist items needed to prepare `v1.0.0` are now closed.
