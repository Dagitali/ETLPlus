# Dependency And Plugin Design Notes

Status: Internal stable-line design note. Do not publish under `docs/source/` unless maintainers
decide this guidance should become user-facing.

- [Dependency And Plugin Design Notes](#dependency-and-plugin-design-notes)
  - [Purpose](#purpose)
  - [Dependency-Footprint Evidence](#dependency-footprint-evidence)
  - [Plugin Boundary](#plugin-boundary)
  - [Runtime Contract Requirements](#runtime-contract-requirements)
  - [Entry-Point Design Questions](#entry-point-design-questions)
  - [Suggested First Implementation Slice](#suggested-first-implementation-slice)

## Purpose

This note records the next design work implied by `RELEASE-CHECKLIST.md`,
`TWELVE-FACTOR-ASSESSMENT.md`, and `roadmap.md`:

- Review the broad `v1.x` dependency footprint with evidence before changing package defaults.
- Define the future plugin/runtime boundary before adding entry-point plugin loading.

Neither item should narrow or expand the stable public surface accidentally.

## Dependency-Footprint Evidence

The `v1.x` line intentionally keeps a broad base install so the documented CLI and built-in file
handler surface work from the base PyPI artifact. A smaller default install remains a future
major-version packaging decision unless stable-line usage shows the broad base install is creating
more support load than it prevents.

Before proposing a dependency split, gather evidence in these categories:

- Install friction: resolver failures, platform-specific build failures, install time, or wheel
  size.
- Runtime friction: imports that fail in the base artifact, optional backends that surprise users,
  or dependency conflicts reported by real projects.
- Support volume: repeated issues tied to one dependency family, one platform, or one installer.
- Stable-surface risk: documented commands, handlers, or examples that would stop working from the
  base install if a dependency moved to an extra.

Acceptable low-risk changes in `v1.x`:

- Add meta tests that prevent accidental dependency growth.
- Improve docs that explain when optional extras are needed.
- Add readiness diagnostics for missing optional backends.
- Mark a dependency as a candidate future-major split without changing installation behavior.

Avoid in `v1.x` unless maintainers explicitly approve a stable-surface change:

- Moving a dependency required by documented base CLI behavior into an extra.
- Making `pip`, `pipx`, `uv tool install`, or conda-forge expose different base behavior.
- Treating `uv.lock` or a development lockfile as canonical package metadata.

## Plugin Boundary

The roadmap's plugin phase should begin with contracts, not dynamic loading. The first design slice
should define what a third-party package can contribute and how ETLPlus behaves when plugin loading
fails.

Initial plugin scope candidates:

- Connectors: provider-specific metadata parsers, readiness diagnostics, and optional dependency
  requirements.
- Operators: reusable execution steps that can be referenced from pipeline config.
- File handlers: format handlers only if they can follow the existing `etlplus.file` handler
  authoring contract without destabilizing built-in formats.

Out of scope for the first plugin contract:

- A resident scheduler process.
- Cloud secret backends.
- Remote execution APIs.
- Replacing core CLI command registration.

## Runtime Contract Requirements

Plugin-provided execution paths must follow the same runtime contracts as built-in commands:

- Configuration must be environment-first and compatible with strict/readiness diagnostics.
- Human output must stay separate from machine-readable `etlplus.event.v1` JSONL events.
- Run history and telemetry integration must reuse the existing event/history shapes.
- Optional dependencies must be declared and diagnosable without importing unavailable providers
  during base CLI startup.
- Failures must return actionable error messages and conventional exit behavior.

## Entry-Point Design Questions

Before implementing entry-point discovery, answer these questions in an architecture decision record
or in a dedicated plugin design document:

- Which entry-point groups are supported, and what object shape does each group expose?
- How is plugin API compatibility versioned and enforced?
- Are plugin failures warnings, readiness errors, or hard startup failures?
- How are plugin-provided optional dependencies represented in `check --readiness`?
- How do plugin diagnostics reuse `ConnectorDiagnosticPolicy` or any successor policy?
- Which plugin capabilities are stable in `v1.x`, and which are experimental until a future major
  release?

## Suggested First Implementation Slice

1. Add a non-loading plugin contract module or protocol that defines connector contribution shapes.
2. Add tests for API-version validation and failure classification using in-memory fake plugins.
3. Document contributor guidance for plugin authors only after the internal contract is stable.
4. Add entry-point discovery behind an explicit experimental flag or internal feature switch.
5. Promote any plugin surface to stable only with public-surface contract tests and release notes.
