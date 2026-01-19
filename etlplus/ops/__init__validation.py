"""
:mod:`etlplus.validation` package.

Conditional validation utilities used across the ETL pipeline.

The package intentionally exposes a single helper, :func:`maybe_validate`, to
keep the public API compact and predictable. Supporting logic lives in
``etlplus.validation.utils`` where validation configuration is normalized,
reducing the likelihood of phase/option mismatches.

Examples
--------
>>> from etlplus.validation import maybe_validate
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
- :mod:`etlplus.validation.utils` for implementation details and helper
    utilities.
"""

from __future__ import annotations

from .utils import maybe_validate

# SECTION: EXPORTS ========================================================== #


__all__ = ['maybe_validate']
