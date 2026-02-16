"""
:mod:`tests.unit.file.test_u_file_pbf` module.

Unit tests for :mod:`etlplus.file.pbf`.
"""

from __future__ import annotations

from etlplus.file import pbf as mod

from .pytest_file_contracts import StubModuleContract

# SECTION: TESTS ============================================================ #


class TestPbf(StubModuleContract):
    """Unit tests for :mod:`etlplus.file.pbf`."""

    module = mod
    handler_cls = mod.PbfFile
    format_name = 'pbf'
