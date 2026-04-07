"""
:mod:`etlplus.runtime.readiness` package.

Internal readiness subsystem for runtime diagnostics.
"""

from __future__ import annotations

from ._builder import ReadinessReportBuilder

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ReadinessReportBuilder',
]
