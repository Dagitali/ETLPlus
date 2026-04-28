# Release Policy And Versioning

This document describes the public release policy for ETLPlus, including versioning expectations,
release channels, deprecation posture, and release-note handling.

- [Release Policy And Versioning](#release-policy-and-versioning)
  - [Scope](#scope)
  - [Supported Release Line](#supported-release-line)
  - [Versioning Model](#versioning-model)
  - [Patch Releases](#patch-releases)
  - [Minor Releases](#minor-releases)
  - [Major Releases](#major-releases)
  - [Deprecation Policy](#deprecation-policy)
  - [Release Artifacts](#release-artifacts)
  - [Release Notes](#release-notes)

## Scope

This file is a public policy document. It explains what maintainers and contributors should expect
from ETLPlus releases, but it does not contain private operator steps, credentials, or incident
response procedures.

## Supported Release Line

ETLPlus currently treats the `v1.x` line as the stable public release line.

The supported surface is the documented CLI, the documented package facades, and the implemented
runtime capabilities explicitly described in the public documentation. Historical modules,
placeholder paths, and stubbed implementations are not part of the stable contract unless they are
promoted in the public docs.

## Versioning Model

ETLPlus follows semantic-versioning-style expectations for public releases:

- `MAJOR` releases are for intentionally breaking public changes
- `MINOR` releases add backward-compatible public capability
- `PATCH` releases are for backward-compatible fixes and release-hygiene corrections

Version tags use the format `v*.*.*` and are expected to be annotated tags.

## Patch Releases

Patch releases are appropriate for changes such as:

- bug fixes that preserve the documented public contract
- packaging or workflow fixes that do not change the supported interface
- documentation corrections that align public docs with actual stable behavior
- dependency or compatibility fixes that preserve supported usage

Patch releases should avoid widening or narrowing the stable surface unless the change is purely a
documentation correction for behavior that was already intentionally supported.

## Minor Releases

Minor releases are appropriate for changes such as:

- new documented CLI capabilities
- new documented public Python API helpers or facades
- additional supported validation or file-handling formats
- additive event, history, or configuration capabilities that remain backward compatible

Minor releases may expand the public surface, but they should not silently break existing
documented workflows.

## Major Releases

Major releases are appropriate when ETLPlus intentionally breaks the documented public contract.

Examples include:

- removing a previously documented stable command or API
- changing expected input or output shapes incompatibly
- removing support for a previously supported runtime or platform without a compatible fallback

Major releases should be accompanied by explicit migration guidance.

## Deprecation Policy

ETLPlus prefers a documented deprecation period before removing a stable public capability.

When practical:

- mark the capability as deprecated in user-facing docs
- provide a replacement path when one exists
- preserve the deprecated behavior through at least one stable minor release before removal

Immediate removal without a deprecation window should be reserved for exceptional cases such as
security, legal, or clearly broken accidental surfaces that were never intended to be stable.

## Release Artifacts

Public releases are expected to produce:

- a Git tag for the released version
- a GitHub Release for announcement and release-note publication
- PyPI artifacts for installation

Tagged release automation is handled by `.github/workflows/cd.yml`.

## Release Notes

GitHub Releases are the canonical public release-history surface for ETLPlus.

Release notes should summarize:

- user-visible features or fixes
- deprecations
- breaking changes
- notable packaging, compatibility, or workflow changes when they affect users or contributors

For release-readiness execution details, consult `RELEASE-CHECKLIST.md`. For public CI/CD workflow
roles and triggers, consult `CI-CD-WORKFLOWS.md`.
