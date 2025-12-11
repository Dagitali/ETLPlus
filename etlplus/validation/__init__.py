"""
:mod:`etlplus.validation` package.

Lightweight helpers for running validation routines conditionally inside
pipeline jobs.

Examples
--------
>>> from etlplus.validation import maybe_validate
>>> payload = {"name": "Alice"}
>>> rules = {"required": ["name"]}
>>> def fake_validator(data, config):
...     missing = [key for key in config['required'] if key not in data]
...     return {'valid': not missing, 'errors': missing}
>>> maybe_validate(
...     payload,
...     when='both',
...     enabled=True,
...     rules=rules,
...     phase='before_transform',
...     severity='warn',
...     validate_fn=fake_validator,
...     print_json_fn=lambda msg: msg,
... )
{'name': 'Alice'}

See Also
--------
- :mod:`etlplus.validation.utils` for the implementation details and helper
    utilities.
"""
from __future__ import annotations

from .utils import maybe_validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['maybe_validate']
