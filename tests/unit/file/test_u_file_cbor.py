"""
:mod:`tests.unit.file.test_u_file_cbor` module.

Unit tests for :mod:`etlplus.file.cbor`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import cbor as mod

from .pytest_file_contracts import BinaryCodecModuleContract
from .pytest_file_types import OptionalModuleInstaller

# SECTION: TESTS ============================================================ #


class TestCbor(BinaryCodecModuleContract):
    """Unit tests for :mod:`etlplus.file.cbor`."""

    module = mod
    format_name = 'cbor'
    dependency_name = 'cbor2'
    reader_method_name = 'loads'
    writer_method_name = 'dumps'
    reader_kwargs: dict[str, object] = {}
    writer_kwargs: dict[str, object] = {}
    loaded_result = {'loaded': True}
    emitted_bytes = b'cbor'

    # pylint: disable=assignment-from-no-return
    def test_read_rejects_non_object_arrays(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """
        Test that :func:`read` raises when the CBOR payload is not an array of
        objects.
        """
        codec = self._make_codec_stub(loaded_result={'loaded': True})

        def _loads(_: bytes) -> object:  # noqa: ARG001
            return [1, 2]

        cast(Any, codec).loads = _loads
        optional_module_stub({'cbor2': codec})
        path = self.format_path(tmp_path)
        path.write_bytes(b'payload')

        with pytest.raises(TypeError, match='CBOR array must contain'):
            mod.CborFile().read(path)
