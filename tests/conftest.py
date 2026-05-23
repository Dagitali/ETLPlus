"""
:mod:`tests.conftest` module.

Top-level pytest configuration and compatibility exports for shared test types.
"""

from __future__ import annotations

from .pytest_shared_support import REPO_ROOT
from .pytest_shared_support import STRUCTURED_EVENT_BASE_FIELDS
from .pytest_shared_support import STRUCTURED_EVENT_LIFECYCLES
from .pytest_shared_support import TESTS_ROOT
from .pytest_shared_support import CaptureHandler
from .pytest_shared_support import CliInvoke
from .pytest_shared_support import CliRunner
from .pytest_shared_support import JsonFactory
from .pytest_shared_support import JsonFileParser
from .pytest_shared_support import JsonOutputParser
from .pytest_shared_support import RequestFactory
from .pytest_shared_support import coerce_cli_args
from .pytest_shared_support import parse_json

# SECTION: PLUG-INS ========================================================= #


pytest_plugins = [
    'tests.pytest_shared_fixtures',
]


# SECTIONS: EXPORTS ========================================================= #


__all__ = [
    'CaptureHandler',
    'CliInvoke',
    'CliRunner',
    'JsonFactory',
    'JsonFileParser',
    'JsonOutputParser',
    'RequestFactory',
    'REPO_ROOT',
    'STRUCTURED_EVENT_BASE_FIELDS',
    'STRUCTURED_EVENT_LIFECYCLES',
    'TESTS_ROOT',
    'coerce_cli_args',
    'parse_json',
]
