# `etlplus.utils` Subpackage

Documentation for the `etlplus.utils` subpackage: shared parsing, normalization, graph, secret, and
data helpers used across ETLPlus.

- Provides small parser classes for mappings, sequences, values, text choices, paths, and numbers
- Includes dependency-graph helpers used by DAG validation and execution planning
- Resolves environment-backed and local-file-backed secret references
- Exposes JSON/data helpers for record counting and value stringification

Back to project overview: see the top-level [README](../../README.md).

- [Public API](#public-api)
- [Compatibility Notes](#compatibility-notes)
- [See Also](#see-also)

## Public API

Most callers should import helpers from the package facade:

```python
from etlplus.utils import MappingParser, SubstitutionResolver, topological_sort_names
```

Stable helper groups include:

- Parsing helpers: `MappingParser`, `MappingFieldParser`, `SequenceParser`, `ValueParser`,
  `TextChoiceResolver`, and `TextNormalizer`.
- Numeric helpers: `FloatParser`, `IntParser`, `finite_decimal_or_none`, `is_integer_value`, and
  `is_number_value`.
- Graph helpers: `NamedDependencyGraph`, `topological_sort_named_items`, and
  `topological_sort_names`.
- Secret and substitution helpers: `EnvironmentSecretProvider`, `LocalFileSecretProvider`,
  `SecretResolver`, and `SubstitutionResolver`.

## Compatibility Notes

The package facade also retains transitional exports for `v1.x` compatibility, including
`JsonCodec`, `PathHasher`, `RecordPayloadParser`, `TokenReferenceCollector`, `BoundsWarningsMixin`,
`NonEmptyStr`, and `NonEmptyStrList`.

## See Also

- Contributor typing guidance in [CONTRIBUTING.md](../../CONTRIBUTING.md)
- Pipeline authoring guide in [docs/pipeline-guide.md](../../docs/pipeline-guide.md)
