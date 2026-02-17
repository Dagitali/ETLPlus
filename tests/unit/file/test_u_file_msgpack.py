"""
:mod:`tests.unit.file.test_u_file_msgpack` module.

Unit tests for :mod:`etlplus.file.msgpack`.
"""

from __future__ import annotations

from etlplus.file import msgpack as mod

from .pytest_file_contracts import BinaryCodecModuleContract

# SECTION: TESTS ============================================================ #


class TestMsgpack(BinaryCodecModuleContract):
    """Unit tests for :mod:`etlplus.file.msgpack`."""

    module = mod
    format_name = 'msgpack'
    dependency_name = 'msgpack'
    reader_method_name = 'unpackb'
    writer_method_name = 'packb'
    reader_kwargs = {'raw': False}
    writer_kwargs = {'use_bin_type': True}
    loaded_result = {'loaded': True}
    emitted_bytes = b'msgpack'
