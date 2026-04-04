"""
:mod:`etlplus.ops._database` module.

Shared database placeholder constants for extract/load orchestration.
"""

from __future__ import annotations

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DATABASE_DRIVER_NOTE',
    'DATABASE_EXTRACT_NOT_IMPLEMENTED',
    'DATABASE_LOAD_NOT_IMPLEMENTED',
]


# SECTION: CONSTANTS ======================================================== #


DATABASE_DRIVER_NOTE = 'Install database-specific drivers to enable this feature'
DATABASE_EXTRACT_NOT_IMPLEMENTED = 'Database extraction not yet implemented'
DATABASE_LOAD_NOT_IMPLEMENTED = 'Database loading not yet implemented'
