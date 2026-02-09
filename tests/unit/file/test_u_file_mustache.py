"""
:mod:`tests.unit.file.test_u_file_mustache` module.

Unit tests for :mod:`etlplus.file.mustache`.
"""

from __future__ import annotations

from etlplus.file import mustache as mod
from tests.unit.file.conftest import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestMustache(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.mustache`."""

    module = mod
    handler_cls = mod.MustacheFile
    format_name = 'mustache'
