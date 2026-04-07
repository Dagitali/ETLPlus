# Release Checklist And Stable-Line Maintenance

This file now serves two purposes:

1. Preserve the archived pre-`v1.0.0` release-readiness record
2. Track the remaining maintenance work for the current stable `v1.x` line

The pre-1.0 sections below are retained as release-history context. They are no longer the active
checklist for ETLPlus now that the project is on the stable line.

## Archived Pre-`v1.0.0` Readiness Record

These sections capture the work that was completed or deferred while preparing the first stable
release.

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

- Checked pre-`v1.0.0` items in this file are retained as release-audit history, not as active
  tracking work.
- Active tracking in this file should now focus on stable-line follow-up, post-`v1.0.0`
  maintenance, and execution-hygiene notes that affect the supported CLI/runtime surface.
- The CI quality-bar item is now closed: lint, docstring lint, type-checking, tests, docs builds,
  distribution validation, artifact audits, and clean-environment wheel smoke checks are enforced
  or verified in the release path.
- The Python Package Index (PyPI) trusted publisher must point at `.github/workflows/release.yml`
  with the `pypi` GitHub environment; pointing it at `ci.yml` will cause trusted publishing to fail.
- The shipped-files item is now closed for the release path because distribution builds and artifact
  audits run in CI from a tracked checkout, so defunct, backup, and scratch files that are not part
  of the committed tree are not part of tagged release artifacts.
- Regenerated demo/version snippets and GitHub Releases should be treated as the canonical source
  for the latest published stable version number when preparing a tag.
- The completed items in the archived sections landed across the path to `v1.0.0` and the early
  stable releases; they are retained here as audit history rather than as pending work.
- The stable-surface trimming work now treats CLI support modules and command wiring, storage
  registry/base plumbing, connector support modules, the file-handler registry, database typing
  helpers, API request manager plumbing, and the top-level `etlplus.api` support modules (`auth`,
  `config`, `enums`, `errors`, `retry_manager`, `transport`, and `types`) as protected
  underscore-prefixed implementation modules behind package-level facades.
- Database DDL/engine/ORM/schema helpers and storage enum/location helpers plus concrete storage
  backend implementations now follow the same package-facade pattern.
- Connector config/type/enum helpers now also live behind the `etlplus.connector` package facade.
- The top-level `__version__` module and the `etlplus.cli` entrypoint module, `etlplus.file`
  core/enums helpers, and `etlplus.api` utility helpers now also follow the underscore-prefixed
  implementation plus package-facade pattern.
- `etlplus.ops` now exposes validation helpers and operation enums from its package facade while
  keeping low-level ops type aliases on an internal underscore-prefixed module.
- The documented stable ops surface now treats :mod:`etlplus.ops.transform` as the orchestration
  facade and :mod:`etlplus.ops.transformations` as the advanced step-level transform module family.
- The stable package facades now also re-export the public validation and request-shape symbols
  already used at their boundaries: `etlplus.ops` exports validation TypedDicts, `etlplus.api`
  exports `PaginationInput` and `RateLimitOverrides`, and `etlplus.history` exports
  `HISTORY_SCHEMA_VERSION`.
- The stable package facades also expose adjacent public contract types and exceptions where the
  documented surface already depends on them: `etlplus.workflow` exports `DagError`,
  `etlplus.api.rate_limiting` exports `RateLimitInput`, and `etlplus.file` now re-exports the
  curated handler-authoring layer from `etlplus.file.base`, including `BoundFileHandler`,
  `ReadOptions`, and `WriteOptions`.
- `etlplus.utils` now follows the same package-facade pattern: user-facing helpers plus the shared
  enum/mixin base abstractions are exported from `etlplus.utils`, while utility type aliases stay
  internal on an underscore-prefixed module.
- The documented stable CLI surface keeps readiness under `check --readiness`.
- The documented stable CLI surface now also includes the `init` starter-project scaffold command.
- The documented stable CLI surface now also includes DAG validation under `check --graph` plus
  DAG-aware execution via `run --all` and dependency-aware `run --job`.
- Runtime execution hygiene progress on the current branch includes:
  - Shared runtime logging policy and config precedence documentation
  - `check --readiness` runtime/config checks with required-env diagnostics, optional dependency
    detection, connector-gap detection, provider-specific environment checks, and standardized exit
    codes (`0` for `ok`/`warn`, `1` for fatal readiness errors)
  - Opt-in strict config diagnostics via `check --strict` and `check --readiness --strict` for
    malformed entries and broken references hidden by the tolerant loader
  - Stable `etlplus.event.v1` structured execution events on STDERR for supported execution
    commands: `extract`, `load`, `run`, `transform`, and `validate`
  - Local run-history persistence keyed by `run_id` for `etlplus run`, backed by SQLite by default
    with JSONL fallback support
  - DAG execution summaries for `run --all` and dependency-aware `run --job`, including skipped
    downstream jobs when an upstream dependency fails

## Current Stable-Line Maintenance Checklist

These are the active follow-up items for the `v1.x` line.

## Maintain In `v1.x`

- [ ] Continue tightening docstring-style enforcement pragmatically.
  - Keep the current ignore set intentional and reduce it only when the backlog is small enough to
    avoid noisy churn.
  - Prefer targeted cleanup in touched modules over repo-wide style-only sweeps.
- [ ] Reassess dependency grouping against real stable-line usage.
  - Confirm whether the current broad default install still matches user expectations on the `v1.x`
    line.
  - Move more dependencies into extras only if that does not break the documented stable surface.
- [ ] Expand performance-smoke and cross-platform confidence based on real `v1.x` usage.
  - Add coverage where support load or issue history shows weak spots.
  - Keep the release path proportionate rather than turning CI into a bottleneck.
- [x] Decide when the local run-history work becomes part of the documented stable CLI surface.
  - `history`, `log`, `status`, and `report` are now documented as stable `v1.x` CLI commands.
  - Contract coverage exists in the public-surface meta tests.
- [x] Review the stable event-schema contract before the next minor release.
  - `RUNTIME-CONFIG-AND-LOGGING.md` now documents the `etlplus.event.v1` compatibility rules and
    clarifies how handled DAG execution failures surface as `run.failed` lifecycle events.
  - `docs/run-history.md` now documents the persisted local-history compatibility posture for the
    stable top-level run shape: compact aggregate DAG summaries in `runs.result_summary` plus
    detailed per-job rows in `job_runs`, with additive summary growth allowed under
    `result_summary`.
  - Published docs now also include a dedicated structured-events guide and an explicit
    event-to-history mapping for `etlplus run`.

## Archived `v1.0.0` Readiness Closeout

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

## Archived Post-`v1.0.0` Follow-Up Items

- [ ] Ratchet docstring-style enforcement beyond the current pragmatic ignore set.
  - `D200` is intentionally tolerated for wrapped one-sentence docstrings; keep tightening the
    remaining `pydocstyle` backlog incrementally once the stable line is out.
- [ ] Further trim or rationalize dependency groups after observing real user install patterns.
- [ ] Expand performance-smoke and cross-platform coverage once `v1.0.0` usage patterns are clearer.

## Stable-Line Priorities

### Next Patch Releases

- Keep release docs, release automation, and the published version snippets aligned.
- Preserve the current supported CLI/runtime contract while tightening quality incrementally. This
  now includes the documented run-history surface at both run and job levels: local `job_runs`
  persistence plus the additive `history/status/report --level`, `--pipeline`, and raw `log --level`
  query affordances.
- Prefer low-risk follow-up that reduces support load without narrowing the stable surface by
  accident.

### Next Minor Release

- Treat the run/job history query surface as stable and decide which additional observability
  capabilities graduate next: traceback capture, UI affordances, and OpenTelemetry adapters.
- Review dependency-group ergonomics and installation footprint with actual user feedback.
- Expand confidence coverage where the stable line has shown friction on platforms, large-file
  workflows, or optional backends.

## Current Focus

The current stable-line posture is:

- Keep the release surface coherent and predictable across `v1.0.x`.
- Continue execution-hygiene work that supports the documented CLI/runtime contract.
- Treat the archived pre-1.0 sections above as audit history, not as the active roadmap.
