# `etlplus.runtime.readiness` Subpackage

Documentation for the `etlplus.runtime.readiness` subpackage: readiness diagnostics used by
`etlplus check --readiness`.

- Builds structured readiness reports for runtime and configuration checks
- Classifies missing optional dependencies, provider credential gaps, and strict config diagnostics
- Keeps provider-specific checks behind one report-building surface

Back to runtime overview: see [`etlplus.runtime`](../README.md).

- [Public API](#public-api)
- [Internal Modules](#internal-modules)
- [See Also](#see-also)

## Public API

The package facade exports:

- `ReadinessReportBuilder`: builder used by the runtime package and CLI readiness command.

Most users should run readiness checks through the CLI:

```bash
etlplus check --readiness --config pipeline.yml
```

## Internal Modules

Underscore-prefixed modules in this package are implementation details for report building,
connector checks, provider checks, strict diagnostics, and support utilities. They are not intended
as standalone public import surfaces.

## See Also

- Runtime package overview in [`../README.md`](../README.md)
- Environment reference in
  [docs/source/getting-started/environment.md](../../../docs/source/getting-started/environment.md)
