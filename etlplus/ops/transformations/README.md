# `etlplus.ops.transformations` Subpackage

Documentation for the `etlplus.ops.transformations` subpackage: step-specific transformation helpers
used by `etlplus.ops.transform`.

- Implements aggregate, filter, map, select, and sort transformation families
- Provides direct `apply_*` helpers for one transformation family
- Provides `apply_*_step` adapters for pipeline-style step specifications

Back to ops overview: see [`etlplus.ops`](../README.md).

- [Transformation Families](#transformation-families)
- [Usage](#usage)
- [See Also](#see-also)

## Transformation Families

- `aggregate`: compute summary values such as count, sum, min, max, and average.
- `filter`: keep records matching one predicate.
- `map`: rename or derive fields.
- `select`: keep selected fields.
- `sort`: order records by one or more fields.

## Usage

Use the orchestration facade for full pipelines:

```python
from etlplus.ops import transform
```

Import a step module directly when custom code needs one transformation family:

```python
from etlplus.ops.transformations.filter import apply_filter_step
```

## See Also

- Ops package overview in [`../README.md`](../README.md)
- Pipeline authoring guide in [docs/pipeline-guide.md](../../../docs/pipeline-guide.md)
