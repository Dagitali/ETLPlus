"""
:mod:`etlplus.history` package.

Local-first run history storage helpers.
"""

from __future__ import annotations

from ._store import HISTORY_SCHEMA_VERSION
from ._store import HistoryStore
from ._store import JsonlHistoryStore
from ._store import RunCompletion
from ._store import RunRecord
from ._store import RunState
from ._store import SQLiteHistoryStore
from ._store import build_run_record

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'HistoryStore',
    'JsonlHistoryStore',
    'RunCompletion',
    'RunRecord',
    'RunState',
    'SQLiteHistoryStore',
    # Constants
    'HISTORY_SCHEMA_VERSION',
    # Functions
    'build_run_record',
]
