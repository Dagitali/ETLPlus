"""
:mod:`tests.unit.file.test_u_file_bson` module.

Unit tests for :mod:`etlplus.file.bson`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import bson as mod

# SECTION: HELPERS ========================================================== #


class _BsonModuleStub:
    """Stub providing module-level encode/decode helpers."""

    def __init__(self) -> None:
        self.decoded: list[bytes] = []
        self.encoded: list[dict[str, object]] = []

    def decode_all(
        self,
        payload: bytes,
    ) -> list[dict[str, object]]:
        """Simulate decoding BSON payloads."""
        self.decoded.append(payload)
        return [{'decoded': True}]

    def encode(
        self,
        doc: dict[str, object],
    ) -> bytes:
        """Simulate encoding a document to BSON."""
        self.encoded.append(doc)
        return b'doc'


class _BsonClassStub:
    """Stub exposing a BSON class with encode/decode_all helpers."""

    def __init__(self) -> None:
        self.decoded: list[bytes] = []
        self.encoded: list[dict[str, object]] = []

    def decode_all(
        self,
        payload: bytes,
    ) -> list[dict[str, object]]:
        """Simulate decoding BSON payloads."""
        self.decoded.append(payload)
        return [{'decoded': True}]

    def encode(
        self,
        doc: dict[str, object],
    ) -> bytes:
        """Simulate encoding a document to BSON."""
        self.encoded.append(doc)
        return b'doc'


class _BsonModuleWithClass:
    """Stub exposing ``BSON`` class only."""

    def __init__(self) -> None:
        # pylint: disable=invalid-name
        self.BSON = _BsonClassStub()


# SECTION: TESTS ============================================================ #


class TestBsonHelpers:
    """Unit tests for BSON encode/decode helpers."""

    # pylint: disable=protected-access

    def test_decode_all_uses_module_function(self) -> None:
        """
        Test that :func:`_decode_all` uses the module-level :func:`decode_all`
        when available.
        """
        stub = _BsonModuleStub()

        assert mod._decode_all(stub, b'payload') == [{'decoded': True}]
        assert stub.decoded == [b'payload']

    def test_decode_all_uses_bson_class(self) -> None:
        """
        Test that :func:`_decode_all` uses the ``BSON`` class's
        :meth:`decode_all` method when the module-level function is not
        available.
        """
        stub = _BsonModuleWithClass()

        assert mod._decode_all(stub, b'payload') == [{'decoded': True}]
        assert stub.BSON.decoded == [b'payload']

    def test_decode_all_raises_without_support(self) -> None:
        """
        Test that :func:`_decode_all` raises when no suitable decode method is
        found.
        """
        with pytest.raises(AttributeError, match='decode_all'):
            mod._decode_all(object(), b'payload')

    def test_encode_doc_uses_module_function(self) -> None:
        """
        Test that :func:`_encode_doc` uses the module-level :func:`encode` when
        available.
        """
        stub = _BsonModuleStub()

        assert mod._encode_doc(stub, {'id': 1}) == b'doc'
        assert stub.encoded == [{'id': 1}]

    def test_encode_doc_uses_bson_class(self) -> None:
        """
        Test that :func:`_encode_doc` uses the ``BSON`` class's :meth:`encode`
        method when the module-level function is not available.
        """
        stub = _BsonModuleWithClass()

        assert mod._encode_doc(stub, {'id': 1}) == b'doc'
        assert stub.BSON.encoded == [{'id': 1}]

    def test_encode_doc_raises_without_support(self) -> None:
        """
        Test that :func:`_encode_doc` raises when no suitable encode method is
        found.
        """
        with pytest.raises(AttributeError, match='encode'):
            mod._encode_doc(object(), {'id': 1})


class TestBsonReadWrite:
    """Unit tests for :func:`etlplus.file.bson.read` and ``write``."""

    def test_read_uses_bson_module(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`bson` module to read records.
        """
        stub = _BsonModuleStub()
        optional_module_stub({'bson': stub})
        path = tmp_path / 'data.bson'
        path.write_bytes(b'payload')

        result = mod.read(path)

        assert result == [{'decoded': True}]
        assert stub.decoded == [b'payload']

    def test_read_uses_bson_class(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`bson` module to read records.
        """
        stub = _BsonModuleWithClass()
        optional_module_stub({'bson': stub})
        path = tmp_path / 'data.bson'
        path.write_bytes(b'payload')

        result = mod.read(path)

        assert result == [{'decoded': True}]
        assert stub.BSON.decoded == [b'payload']

    def test_write_uses_bson_module(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`bson` module to write records.
        """
        stub = _BsonModuleStub()
        optional_module_stub({'bson': stub})
        path = tmp_path / 'data.bson'
        payload = [{'id': 1}, {'id': 2}]

        written = mod.write(path, payload)

        assert written == 2
        assert stub.encoded == payload
        assert path.read_bytes() == b'docdoc'

    def test_write_uses_bson_class(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`bson` module to write records.
        """
        stub = _BsonModuleWithClass()
        optional_module_stub({'bson': stub})
        path = tmp_path / 'data.bson'
        payload = [{'id': 1}, {'id': 2}]

        written = mod.write(path, payload)

        assert written == 2
        assert stub.BSON.encoded == payload
        assert path.read_bytes() == b'docdoc'
