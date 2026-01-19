"""
:mod:`etlplus.ops.__init__validation` module.

Compatibility wrapper for the conditional validation helper used by ETL ops.
See :mod:`etlplus.ops` for the package overview and
:mod:`etlplus.ops.utils` for implementation details.


Examples
--------
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
- :mod:`etlplus.ops.utils` for implementation details and helper
    utilities.
"""

from __future__ import annotations

from .utils import maybe_validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['maybe_validate']
