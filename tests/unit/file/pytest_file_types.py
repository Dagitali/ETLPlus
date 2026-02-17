"""
:mod:`tests.unit.file.pytest_file_types` module.

Shared type aliases for unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

from collections.abc import Callable

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'OptionalModuleInstaller',
]


# SECTION: TYPE ALIASES ===================================================== #


type OptionalModuleInstaller = Callable[[dict[str, object]], None]
