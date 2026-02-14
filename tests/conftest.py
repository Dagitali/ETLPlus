"""
:mod:`tests.conftest` module.

Top-level pytest configuration and compatibility exports for shared test types.
"""

from __future__ import annotations

from .pytest_shared_support import CaptureHandler
from .pytest_shared_support import CliInvoke
from .pytest_shared_support import CliRunner
from .pytest_shared_support import JsonFactory
from .pytest_shared_support import JsonFileParser
from .pytest_shared_support import JsonOutputParser
from .pytest_shared_support import RequestFactory
from .pytest_shared_support import coerce_cli_args
from .pytest_shared_support import parse_json

pytest_plugins = [
    'tests.pytest_shared_fixtures',
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
