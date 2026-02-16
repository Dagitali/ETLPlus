"""
:mod:`tests.integration.file.test_i_file_cbor` module.

Integration smoke tests for :mod:`etlplus.file.cbor`.
"""

from __future__ import annotations

from etlplus.file import cbor as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestCbor(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.cbor`."""

    module = mod
