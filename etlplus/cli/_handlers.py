"""
:mod:`etlplus.cli._handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

from .. import Config
from ..file import File
from ..history import HistoryStore
from ..history import RunCompletion
from ..history import RunState
from ..runtime import ReadinessReportBuilder
from ..runtime import RuntimeEvents
from . import _handler_history as _history_impl
from . import _handler_lifecycle as _lifecycle
from . import _handler_output as _output
from . import _io
from . import _summary
from ._handler_check import check_handler
from ._handler_dataops import extract_handler
from ._handler_dataops import load_handler
from ._handler_dataops import transform_handler
from ._handler_dataops import validate_handler
from ._handler_history import history_handler
from ._handler_history import report_handler
from ._handler_history import status_handler
from ._handler_render import render_handler
from ._handler_run import run_handler
from ._history import HistoryReportBuilder
from ._history import HistoryView

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
    'extract_handler',
    'history_handler',
    'load_handler',
    'render_handler',
    'report_handler',
    'run_handler',
    'status_handler',
    'transform_handler',
    'validate_handler',
]


# SECTION: INTERNAL ALIASES ================================================= #


_CommandContext = _lifecycle.CommandContext
_complete_command = _lifecycle.complete_command
_fail_command = _lifecycle.fail_command
_failure_boundary = _lifecycle.failure_boundary
_check_sections = _summary.check_sections
_complete_output = _output.complete_output
_load_history_records = _history_impl.load_history_records
_pipeline_summary = _summary.pipeline_summary


# Keep these module attributes available for tests that patch shared objects.
_PATCHABLE_EXPORTS = (
    Config,
    File,
    HistoryReportBuilder,
    HistoryStore,
    HistoryView,
    ReadinessReportBuilder,
    RunCompletion,
    RunState,
    RuntimeEvents,
    _io,
    _summary,
)
