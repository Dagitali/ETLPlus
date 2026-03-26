"""
:mod:`etlplus.cli._commands` package.

Typer application and subcommands for the ``etlplus`` command-line interface.
"""

from __future__ import annotations

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.check import check_cmd
from etlplus.cli._commands.extract import extract_cmd
from etlplus.cli._commands.helpers import _parse_json_option
from etlplus.cli._commands.helpers import parse_json_payload
from etlplus.cli._commands.history import handle_history
from etlplus.cli._commands.history import history_cmd
from etlplus.cli._commands.load import load_cmd
from etlplus.cli._commands.log import log_cmd
from etlplus.cli._commands.render import render_cmd
from etlplus.cli._commands.report import report_cmd
from etlplus.cli._commands.root import _root
from etlplus.cli._commands.run import run_cmd
from etlplus.cli._commands.status import status_cmd
from etlplus.cli._commands.transform import transform_cmd
from etlplus.cli._commands.validate import validate_cmd
from etlplus.cli._state import ResourceTypeResolver
from etlplus.cli._state import ensure_state
from etlplus.cli._state import infer_resource_type_soft
from etlplus.cli._state import log_inferred_resource
from etlplus.cli._state import optional_choice
from etlplus.cli._state import resolve_resource_type
from etlplus.cli._state import validate_choice

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    '_parse_json_option',
    '_root',
    'handlers',
    'handle_history',
    'parse_json_payload',
    'ResourceTypeResolver',
    'ensure_state',
    'infer_resource_type_soft',
    'log_inferred_resource',
    'optional_choice',
    'resolve_resource_type',
    'validate_choice',
    'check_cmd',
    'extract_cmd',
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
