"""
:mod:`tests.unit.file.test_u_file_cbor` module.

Unit tests for :mod:`etlplus.file.cbor`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import cbor as mod

# SECTION: HELPERS ========================================================== #


class _CborStub:
    """Stub for the :mod:`cbor2` module."""

    def __init__(self) -> None:
        self.loaded: list[bytes] = []
        self.dumped: list[object] = []

    def loads(
        self,
        payload: bytes,
    ) -> object:
        """Simulate loading a CBOR payload."""
        self.loaded.append(payload)
        return {'loaded': True}

    def dumps(
        self,
        payload: object,
    ) -> bytes:
        """Simulate dumping a payload to CBOR."""
        self.dumped.append(payload)
        return b'cbor'


# SECTION: TESTS ============================================================ #


class TestCborRead:
    """Unit tests for :func:`read`."""

    def test_read_uses_cbor2(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`cbor2` module to load records.
        """
        stub = _CborStub()
        optional_module_stub({'cbor2': stub})
        path = tmp_path / 'data.cbor'
        path.write_bytes(b'payload')

        result = mod.read(path)

        assert result == {'loaded': True}
        assert stub.loaded == [b'payload']

    def test_read_rejects_non_object_arrays(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` raises when the CBOR payload is not an array of
        objects.
        """
        stub = _CborStub()

        def _loads(_: bytes) -> object:
            return [1, 2]

        stub.loads = _loads  # type: ignore[assignment]
        optional_module_stub({'cbor2': stub})
        path = tmp_path / 'data.cbor'
        path.write_bytes(b'payload')

        with pytest.raises(TypeError, match='CBOR array must contain'):
            mod.read(path)


class TestCborWrite:
    """Unit tests for :func:`etlplus.file.cbor.write`."""

    @pytest.mark.parametrize(
        ('payload', 'expected_dump'),
        [
            ([{'id': 1}], [{'id': 1}]),
            ({'id': 1}, {'id': 1}),
        ],
        ids=['list', 'dict'],
    )
    def test_write_serializes_payload(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        payload: object,
        expected_dump: object,
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`cbor2` module to dump records.
        """
        stub = _CborStub()
        optional_module_stub({'cbor2': stub})
        path = tmp_path / 'data.cbor'

        written = mod.write(path, payload)  # type: ignore[arg-type]

        assert written == 1
        assert stub.dumped == [expected_dump]
        assert path.read_bytes() == b'cbor'
