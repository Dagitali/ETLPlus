# Pre-v1.0 Release Checklist

This checklist tracks the remaining work to make ETLPlus look and behave like a stable, well-run
open source Python project before tagging `v1.0.0`.

## Must

- Define the supported stable surface area for `v1.0.0`.
  - Confirm which APIs, CLI commands, config shapes, and file handlers are covered by the stability
    promise.
  - Mark placeholders and experimental areas explicitly in user-facing docs.
- Keep packaging metadata consistent and single-sourced.
  - Treat `pyproject.toml` as canonical.
  - Keep `setup.py` as a minimal compatibility shim only.
- Enforce the documented quality bar in CI.
  - Run lint, docstring lint, type-checking, tests, docs builds, and distribution builds on pull
    requests.
  - Validate built artifacts with `twine check`.
  - Smoke-test the built wheel in a clean environment.
- Fix release-blocking tooling inconsistencies.
  - Ensure `make typecheck` targets the real package path.
  - Remove or isolate stale automation and temporary backup artifacts from release-critical paths.
- Refresh release-facing versioned docs before tagging.
  - Regenerate demo snippets and version examples.
  - Ensure published docs match the tagged artifact.
- Decide whether the default dependency set is intentionally broad.
  - If not, move connector- or format-specific dependencies into extras before `v1.0.0`.
- Confirm Python support policy.
  - Document whether `>=3.13,<3.15` is intentional.
  - Broaden support if the current floor is not a deliberate product decision.

## Should

- Maintain an in-repository changelog.
  - Summarize user-visible changes, deprecations, and breaking changes per release.
- Add issue and pull request templates.
  - Bug report template.
  - Feature request template.
  - Pull request checklist.
- Add at least one non-Linux CI target.
  - Run smoke coverage on macOS and Windows for CLI/package-install confidence.
- Make the README more front-loaded for end users.
  - Keep the first screen focused on installation, quickstart, support policy, and stable
    capabilities.
  - Move deep migration notes and detailed handler tables further down or into docs.
- Add link-checking or docs-hygiene validation.
  - Catch broken internal and external links before release.
- Audit shipped files.
  - Avoid packaging defunct, backup, or scratch artifacts unless they are intentionally retained.

## Nice-to-have

- Publish a documented support policy.
  - Clarify expected response times, supported Python versions, and deprecation windows.
- Add release-drafter or structured release-note automation.
- Add a small compatibility matrix in the docs.
  - Python versions, platforms, and optional dependency groups.
- Add benchmark or performance-smoke coverage for large-file workflows.
- Add provenance or supply-chain hardening beyond pinned GitHub actions.
  - For example, dependency review or SBOM generation.

## Current Focus

The first cleanup batch implemented as part of this checklist is:

- Reduce packaging metadata drift
- Enforce more of the stated quality bar in CI
- Fix local tooling inconsistencies that would weaken a `v1.0.0` release
