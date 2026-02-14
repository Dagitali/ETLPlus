"""
:mod:`tests.conftest` module.

Top-level pytest configuration and compatibility exports for shared test types.
"""

from __future__ import annotations

from tests.pytest_shared_support import CaptureHandler
from tests.pytest_shared_support import CliInvoke
from tests.pytest_shared_support import CliRunner
from tests.pytest_shared_support import JsonFactory
from tests.pytest_shared_support import JsonFileParser
from tests.pytest_shared_support import JsonOutputParser
from tests.pytest_shared_support import RequestFactory
from tests.pytest_shared_support import coerce_cli_args
from tests.pytest_shared_support import parse_json

pytest_plugins = [
    'tests.pytest_shared_fixtures',
    'tests.unit.file.pytest_file_stubs',
    'tests.unit.pytest_unit_api',
]

__all__ = [
    'CaptureHandler',
    'CliInvoke',
    'CliRunner',
    'JsonFactory',
    'JsonFileParser',
    'JsonOutputParser',
    'RequestFactory',
    'coerce_cli_args',
    'parse_json',
]
