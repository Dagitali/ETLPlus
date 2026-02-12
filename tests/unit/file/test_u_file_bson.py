"""
:mod:`tests.unit.file.test_u_file_bson` module.

Unit tests for :mod:`etlplus.file.bson`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import bson as mod
from tests.unit.file.conftest import BinaryDependencyModuleContract
from tests.unit.file.conftest import OptionalModuleInstaller

# SECTION: HELPERS ========================================================== #


class _BsonCodecStub:
    """Stub providing encode/decode helpers."""

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
        self.BSON = _BsonCodecStub()


# SECTION: TESTS ============================================================ #


class TestBsonHelpers:
    """Unit tests for BSON encode/decode helpers."""

    # pylint: disable=protected-access

    def test_decode_all_raises_without_support(self) -> None:
        """
        Test that :func:`_decode_all` raises when no suitable decode method is
        found.
        """
        with pytest.raises(AttributeError, match='decode_all'):
            mod._decode_all(object(), b'payload')

    def test_decode_all_uses_bson_class(self) -> None:
        """
        Test that :func:`_decode_all` uses the ``BSON`` class's
        :meth:`decode_all` method when the module-level function is not
        available.
        """
        stub = _BsonModuleWithClass()

        assert mod._decode_all(stub, b'payload') == [{'decoded': True}]
        assert stub.BSON.decoded == [b'payload']

    def test_decode_all_uses_module_function(self) -> None:
        """
        Test that :func:`_decode_all` uses the module-level :func:`decode_all`
        when available.
        """
        stub = _BsonCodecStub()

        assert mod._decode_all(stub, b'payload') == [{'decoded': True}]
        assert stub.decoded == [b'payload']

    def test_encode_doc_uses_module_function(self) -> None:
        """
        Test that :func:`_encode_doc` uses the module-level :func:`encode` when
        available.
        """
        stub = _BsonCodecStub()

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


class TestBsonIo(BinaryDependencyModuleContract):
    """Unit tests for BSON module-level read/write dispatch."""

    module = mod
    format_name = 'bson'
    dependency_name = 'bson'
    expected_read_result = [{'decoded': True}]
    write_payload = [{'id': 1}, {'id': 2}]
    expected_written_count = 2

    def assert_dependency_after_read(
        self,
        dependency_stub: object,
        path: Path,
    ) -> None:
        """Assert bson module-level read behavior."""
        stub = dependency_stub
        assert isinstance(stub, _BsonCodecStub)
        assert stub.decoded == [b'payload']
        assert path.exists()

    def assert_dependency_after_write(
        self,
        dependency_stub: object,
        path: Path,
    ) -> None:
        """Assert bson module-level write behavior."""
        stub = dependency_stub
        assert isinstance(stub, _BsonCodecStub)
        assert stub.encoded == self.write_payload
        assert path.read_bytes() == b'docdoc'

    def make_dependency_stub(self) -> _BsonCodecStub:
        """Build a bson dependency stub exposing module-level helpers."""
        return _BsonCodecStub()

    def test_read_uses_bson_class(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`bson` module to read records.
        """
        stub = _BsonModuleWithClass()
        optional_module_stub({'bson': stub})
        path = self.format_path(tmp_path)
        path.write_bytes(b'payload')

        result = mod.BsonFile().read(path)

        assert result == [{'decoded': True}]
        assert stub.BSON.decoded == [b'payload']

    def test_write_uses_bson_class(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`bson` module to write records.
        """
        stub = _BsonModuleWithClass()
        optional_module_stub({'bson': stub})
        path = self.format_path(tmp_path)

        written = mod.BsonFile().write(path, self.write_payload)

        assert written == 2
        assert stub.BSON.encoded == self.write_payload
        assert path.read_bytes() == b'docdoc'
