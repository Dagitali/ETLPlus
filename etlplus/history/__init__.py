"""
:mod:`etlplus.history` package.

Local-first run history storage helpers.
"""

from __future__ import annotations

from .store import HistoryStore
from .store import JsonlHistoryStore
from .store import RunCompletion
from .store import RunRecord
from .store import RunState
from .store import SQLiteHistoryStore
from .store import build_run_record

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HistoryStore',
    'JsonlHistoryStore',
    'RunCompletion',
    'RunRecord',
    'RunState',
    'SQLiteHistoryStore',
    # Functions
    'build_run_record',
]
