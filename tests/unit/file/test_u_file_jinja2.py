"""
:mod:`tests.unit.file.test_u_file_jinja2` module.

Unit tests for :mod:`etlplus.file.jinja2`.
"""

from __future__ import annotations

from etlplus.file import jinja2 as mod
from tests.unit.file.conftest import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestJinja2(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.jinja2`."""

    module = mod
    handler_cls = mod.Jinja2File
    format_name = 'jinja2'
