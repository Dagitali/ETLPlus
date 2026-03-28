"""
:mod:`etlplus.cli._commands` package.

Typer application and subcommands for the ``etlplus`` command-line interface.
"""

from __future__ import annotations

from .. import _handlers as handlers
from . import _root  # noqa: F401  # Register root callback.
from ._app import app
from .check import check_cmd
from .extract import extract_cmd
from .history import history_cmd
from .load import load_cmd
from .log import log_cmd
from .render import render_cmd
from .report import report_cmd
from .run import run_cmd
from .status import status_cmd
from .transform import transform_cmd
from .validate import validate_cmd

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_cmd',
    'extract_cmd',
    'handlers',
    'history_cmd',
    'load_cmd',
    'log_cmd',
    'render_cmd',
    'report_cmd',
    'run_cmd',
    'status_cmd',
    'transform_cmd',
    'validate_cmd',
    # Variables
    'app',
]
