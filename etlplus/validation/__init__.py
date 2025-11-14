"""
etlplus.validation package.

The top-level module defining ``:mod:etlplus.validation``, a package of high-
level data validation helpers.

Summary
-------
- Data schema validation using ``jsonschema``.

Examples
--------
---------
>>> from etlplus.validation import validate_json_schema
>>> schema = {
>>>     "type": "object",
>>>     "properties": {"name": {"type": "string"}},
>>>     "required": ["name"]
>>> }
>>> payload = {"name": "Alice"}
>>> result = validate_json_schema(payload, schema)
>>> print(result)
{'valid': True, 'data': {'name': 'Alice'}}

Notes
-----
- Validation functions return a dict with ``valid: bool`` and optional ``data``
  keys.
- On validation failure, the result may include an ``errors`` key with details.

See Also
--------
- :mod:`etlplus.validation.utils` for utility functions to run validation
  conditionally based on phase and severity.
"""
from __future__ import annotations

from .utils import maybe_validate as validate_json_schema


# SECTION: PUBLIC API ======================================================= #


__all__ = ['validate_json_schema']
