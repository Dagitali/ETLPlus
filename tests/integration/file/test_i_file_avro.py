"""
:mod:`tests.integration.file.test_i_file_avro` module.

Integration smoke tests for :mod:`etlplus.file.avro`.
"""

from __future__ import annotations

from etlplus.file import avro as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestAvro(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.avro`."""

    module = mod
