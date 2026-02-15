"""
:mod:`tests.smoke` subpackage.

Smoke test suites for the :mod:`etlplus` package.

Notes
-----
This package is legacy-path smoke coverage.

- New smoke tests should live under scope folders (for example
  ``tests/unit`` or ``tests/integration``) and use ``@pytest.mark.smoke``.
- ``tests/smoke/file`` remains as transitional coverage until it is migrated.
"""

from __future__ import annotations
