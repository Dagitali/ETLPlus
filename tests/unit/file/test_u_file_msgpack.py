"""
:mod:`tests.unit.file.test_u_file_msgpack` module.

Unit tests for :mod:`etlplus.file.msgpack`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import msgpack as mod

# SECTION: HELPERS ========================================================== #


class _MsgpackStub:
    """Stub for the ``msgpack`` module."""

    # pylint: disable=unused-argument

    def __init__(self) -> None:
        self.unpacked: list[bytes] = []
        self.packed: list[object] = []
        self.pack_kwargs: list[dict[str, object]] = []

    def unpackb(
        self,
        payload: bytes,
        *,
        raw: bool,
    ) -> object:  # noqa: ARG002
        """
        Simulate unpacking by recording the payload and returning a fixed
        value.
        """
        self.unpacked.append(payload)
        return {'loaded': True}

    def packb(
        self,
        payload: object,
        **kwargs: object,
    ) -> bytes:
        """Simulate packing by recording the payload and kwargs."""
        self.packed.append(payload)
        self.pack_kwargs.append(kwargs)
        return b'msgpack'


# SECTION: TESTS ============================================================ #


class TestMsgpackRead:
    """Unit tests for :func:`read`."""

    def test_read_uses_msgpack(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`msgpack` module to unpack the
        file.
        """
        stub = _MsgpackStub()
        optional_module_stub({'msgpack': stub})
        path = tmp_path / 'data.msgpack'
        path.write_bytes(b'payload')

        result = mod.read(path)

        assert result == {'loaded': True}
        assert stub.unpacked == [b'payload']


class TestMsgpackWrite:
    """Unit tests for :func:`write`."""

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
        Test that :func:`write` serializes the payload using the :mod:`msgpack`
        module.
        """
        stub = _MsgpackStub()
        optional_module_stub({'msgpack': stub})
        path = tmp_path / 'data.msgpack'

        written = mod.write(path, payload)  # type: ignore[arg-type]

        assert written == 1
        assert stub.packed == [expected_dump]
        assert stub.pack_kwargs == [{'use_bin_type': True}]
        assert path.read_bytes() == b'msgpack'
