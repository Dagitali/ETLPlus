"""
:mod:`etlplus.history` package.

Local-first run history storage helpers.
"""

from __future__ import annotations

from .store import HistoryStore
from .store import JsonlHistoryStore
from .store import SQLiteHistoryStore
from .store import build_run_record
from .store import iter_history_runs
from .store import open_history_store

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HistoryStore',
    'JsonlHistoryStore',
    'SQLiteHistoryStore',
    # Functions
    'build_run_record',
    'iter_history_runs',
    'open_history_store',
]
