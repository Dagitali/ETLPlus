"""
:mod:`etlplus.ops` package.

Data operations helpers.

Importing :mod:`etlplus.ops` exposes the coarse-grained helpers most users care
about: ``extract``, ``transform``, ``load``, ``validate``, ``run``, and
``run_pipeline``. Each helper delegates to the richer modules under
``etlplus.ops.*`` while presenting a compact public API surface. Conditional
validation orchestration is available via
:func:`etlplus.ops.utils.maybe_validate`. The legacy compatibility module
:mod:`etlplus.ops.__init__validation` is deprecated in favor of this package.

Examples
--------
>>> from etlplus.ops import extract, transform
>>> raw = extract('file', 'input.json')
>>> curated = transform(raw, {'select': ['id', 'name']})

>>> from etlplus.ops.utils import maybe_validate
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
:mod:`etlplus.ops.utils`
"""

from .extract import extract
from .load import load
from .run import run
from .run import run_pipeline
from .transform import transform
from .validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'extract',
    'load',
    'run',
    'run_pipeline',
    'transform',
    'validate',
]
