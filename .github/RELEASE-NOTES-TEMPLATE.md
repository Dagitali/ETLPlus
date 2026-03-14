# ETLPlus Release Notes Template

Use this template when drafting GitHub Release notes for tagged ETLPlus
releases. GitHub Releases is the canonical release-history surface for the
project.

- [ETLPlus Release Notes Template](#etlplus-release-notes-template)
  - [Highlights](#highlights)
  - [Breaking Changes](#breaking-changes)
  - [Deprecations](#deprecations)
  - [Fixes](#fixes)
  - [Documentation and Maintenance](#documentation-and-maintenance)
  - [Upgrade Notes](#upgrade-notes)
  - [Support Boundary](#support-boundary)
  - [Notes for Maintainers](#notes-for-maintainers)

## Highlights

- Summarize the most important user-visible changes in 2-5 bullets.
- Focus on what users can now do, what became more reliable, or what changed in
  support expectations.

## Breaking Changes

- List any breaking change explicitly.
- If none, write `None.`

## Deprecations

- List any newly deprecated flags, APIs, config shapes, or handlers.
- If none, write `None.`

## Fixes

- Summarize the most important bug fixes.
- Prefer behavior-focused language over internal implementation detail.

## Documentation and Maintenance

- Call out important docs improvements, CI/release automation changes, or
  contributor-facing maintenance work.

## Upgrade Notes

- Mention any required user action, dependency changes, migration steps, or
  compatibility expectations.
- If nothing special is required, write `No special upgrade steps.`

## Support Boundary

- Re-state any release-specific support or stability caveats when needed.
- For ETLPlus `v1.x`, keep the message aligned with the README and SUPPORT
  policy.

## Notes for Maintainers

- Cross-check the generated GitHub release notes against this template.
- Make sure breaking changes and deprecations are never left only in the auto-
  generated PR summary.
- Keep terminology consistent with the documented stable surface in the README.
