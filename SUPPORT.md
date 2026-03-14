# Support

Thank you for using ETLPlus.

- [Support](#support)
  - [Support Policy](#support-policy)
  - [Supported Versions](#supported-versions)
  - [Maintenance Expectations](#maintenance-expectations)
  - [Where to Get Help](#where-to-get-help)
  - [Response Targets](#response-targets)
  - [Deprecation Policy](#deprecation-policy)
  - [Support the Project](#support-the-project)

## Support Policy

ETLPlus is maintained as an open source project with best-effort community support.

The intended support baseline for the `v1.0.0` line is:

- Supported Python versions: 3.13 and 3.14.
- Supported install surfaces: the published PyPI package, the documented CLI, and the documented
  Python APIs and file handlers described in the repository docs.
- Supported collaboration channels: GitHub Discussions for usage and docs questions, GitHub Issues
  for confirmed bugs and concrete feature requests, and the security reporting path for private
  vulnerabilities.

Anything marked as placeholder, stubbed, defunct, or experimental in the repository docs should be
treated as out of the stable support promise until explicitly promoted.

## Supported Versions

Until `v1.0.0` is tagged, support is focused on the latest development line and the latest published
pre-1.0 release.

After `v1.0.0`, the maintenance target is:

- The latest released minor line.
- The immediately previous minor line for critical regressions and security fixes, when practical.

Older versions may still work, but they should not be assumed to receive routine fixes.

## Maintenance Expectations

For the stable `v1.x` line, ETLPlus uses minor releases as the normal delivery vehicle for new
features, non-urgent enhancements, broader dependency updates, and deprecations.

Patch releases are intended for targeted, low-risk maintenance such as:

- Confirmed regressions in documented `v1.x` behavior
- Security fixes
- Packaging, build, or install breakages
- Docs or metadata corrections that are needed to keep the released artifact usable

Backports are not guaranteed for every fix. When practical, critical regressions and security fixes
may be applied to the latest released minor line and the immediately previous minor line. Routine
feature work, behavior changes, and larger internal refactors are expected to land only on the
latest active minor line.

## Where to Get Help

- **Questions & Usage**: Please use [GitHub Discussions][discussions] for general questions, usage
  help, and best practices.
- **Docs Feedback & Examples**: Please use [GitHub Discussions][discussions] for documentation
  gaps, unclear sections, and example-sharing.
- **Bugs & Feature Requests**: Open an issue in the [GitHub Issues][issues] tracker.
- **Security Issues**: See [SECURITY.md](SECURITY.md) for responsible disclosure.
- **Documentation**: See the [README](README.md) and [docs/](docs/) directory for guides and
  references.

In general:

- Use Discussions for questions, docs feedback, examples, and support conversations.
- Use Issues for confirmed bugs and concrete feature requests.
- See [docs/community-discussions.md](docs/community-discussions.md) for the recommended category
  structure and starter discussions.

## Response Targets

These are response targets, not guaranteed service-level agreements.

- Security reports: initial acknowledgement within 3 business days when received through the
  documented security channel.
- Confirmed bug reports and feature requests: initial triage target within 10 business days.
- Usage questions and docs discussions: response target within 10 business days when maintainer time
  allows.

Community contributions and peer support remain welcome even when maintainer response is delayed.

## Deprecation Policy

For the stable `v1.x` line, ETLPlus aims to avoid abrupt breaking changes to documented public
surfaces.

- A deprecated CLI flag, documented API entrypoint, or documented config shape should normally
  remain available for at least one minor release after deprecation notice.
- Deprecations should be called out in the changelog and nearby user-facing docs.
- Patch releases should not normally remove or materially redefine documented public behavior.
- Removal may happen sooner only for security, correctness, or ecosystem-compatibility reasons.

Internal, defunct, placeholder, or undocumented modules are excluded from this deprecation policy.

## Support the Project

If ETLPlus is useful in your work, you can support its ongoing maintenance through the repository
sponsor button.

- GitHub Sponsors: https://github.com/sponsors/Dagitali
- Buy Me a Coffee: https://buymeacoffee.com/djrlj694

Financial support helps fund maintenance, documentation, examples, compatibility work, and future
connector development.

[discussions]: https://github.com/Dagitali/ETLPlus/discussions
[issues]: https://github.com/Dagitali/ETLPlus/issues
