"""
:mod:`etlplus.ops` package.

Runtime ETL primitives and orchestration helpers.

Importing :mod:`etlplus.ops` exposes the coarse-grained helpers most users care
about: ``extract``, ``transform``, ``load``, ``validate``, ``run``, and
``run_pipeline``. Each helper delegates to the richer modules under
``etlplus.ops.*`` while presenting a compact public API surface. Advanced
transform-specific imports live under :mod:`etlplus.ops.transform` and the
step modules in :mod:`etlplus.ops.transformations`. Conditional validation
orchestration is available via :func:`etlplus.ops.maybe_validate`.

Examples
--------
>>> from etlplus.ops import extract, transform
>>> raw = extract('file', 'input.json')
>>> curated = transform(raw, {'select': ['id', 'name']})

>>> from etlplus.ops import maybe_validate
>>> payload = {'name': 'Alice'}
>>> rules = {'required': ['name']}
>>> def validator(data, config):
...     missing = [field for field in config['required'] if field not in data]
...     return {'valid': not missing, 'errors': missing, 'data': data}
>>> maybe_validate(
...     payload,
...     when='both',
...     enabled=True,
...     rules=rules,
...     phase='before_transform',
...     severity='warn',
...     validate_fn=validator,
...     print_json_fn=lambda message: message,
... )
{'name': 'Alice'}

See Also
--------
:mod:`etlplus.ops.run`
:mod:`etlplus.ops.transform`
:mod:`etlplus.ops.transformations`
"""

from ._enums import AggregateName
from ._enums import OperatorName
from ._enums import PipelineStep
from ._validation import ValidationResultDict
from ._validation import ValidationSettings
from ._validation import maybe_validate
from .extract import extract
from .load import load
from .run import run
from .run import run_pipeline
from .transform import transform
from .validate import FieldRulesDict
from .validate import FieldValidationDict
from .validate import ValidationDict
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ValidationSettings',
    # Enums
    'AggregateName',
    'OperatorName',
    'PipelineStep',
    # Functions
    'extract',
    'load',
    'maybe_validate',
    'run',
    'run_pipeline',
    'transform',
    'validate',
    # Typed Dicts
    'FieldRulesDict',
    'FieldValidationDict',
    'ValidationDict',
    'ValidationResultDict',
]
