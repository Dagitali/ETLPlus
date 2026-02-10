"""
:mod:`tests.unit.file.test_u_file_avro` module.

Unit tests for :mod:`etlplus.file.avro`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etlplus.file import avro as mod
from etlplus.file.enums import FileFormat
from tests.unit.file.conftest import BinaryDependencyModuleContract
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import PathMixin

# SECTION: HELPERS ========================================================== #


class _FastAvroStub:
    """Stub for the :mod:`fastavro` module."""

    # pylint: disable=unused-argument

    def __init__(self) -> None:
        self.parsed_schema: dict[str, object] | None = None
        self.writes: list[dict[str, object]] = []
        self.records: list[dict[str, object]] = [{'id': 1}, {'id': 2}]

    def reader(
        self,
        handle: object,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Simulate reading records from a file handle."""
        return list(self.records)

    def parse_schema(
        self,
        schema: dict[str, object],
    ) -> dict[str, object]:
        """Simulate parsing an Avro schema."""
        self.parsed_schema = schema
        return schema

    def writer(
        self,
        handle: object,  # noqa: ARG002
        schema: dict[str, object],
        records: list[dict[str, object]],
    ) -> None:
        """Simulate writing records to a file handle."""
        self.writes.append({'schema': schema, 'records': list(records)})


# SECTION: TESTS ============================================================ #


class TestAvroHandlerClass(PathMixin):
    """Unit tests for :class:`etlplus.file.avro.AvroFile`."""

    format_name = 'avro'

    def test_dumps_bytes_returns_empty_for_empty_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test empty payload serialization short-circuiting to bytes."""
        handler = mod.AvroFile()
        self._patch_dependency_missing(monkeypatch)

        payload = handler.dumps_bytes([])

        assert payload == b''

    def test_dumps_and_loads_bytes(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test binary helper methods delegating to :mod:`fastavro`."""
        stub = _FastAvroStub()
        optional_module_stub({'fastavro': stub})
        handler = mod.AvroFile()
        records = [{'id': 1, 'name': 'Ada'}]

        payload = handler.dumps_bytes(records)
        result = handler.loads_bytes(payload)

        assert isinstance(payload, bytes)
        assert stub.parsed_schema is not None
        assert stub.writes
        assert result == stub.records

    def test_format_constant(self) -> None:
        """Test :class:`AvroFile` exposing the expected format enum."""
        assert mod.AvroFile.format is FileFormat.AVRO

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test file writes short-circuiting on empty payloads."""
        handler = mod.AvroFile()
        self._patch_dependency_missing(monkeypatch)
        path = self.format_path(tmp_path)

        written = handler.write(path, [])

        assert written == 0

    @staticmethod
    def _patch_dependency_missing(
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Patch dependency resolution to fail if called."""
        monkeypatch.setattr(
            mod,
            'get_dependency',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )


class TestAvroHelpers:
    """Unit tests for :mod:`etlplus.file.avro` helpers."""

    # pylint: disable=protected-access

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (None, 'null'),
            (True, 'boolean'),
            (1, 'long'),
            (1.25, 'double'),
            ('text', 'string'),
            (b'bytes', 'bytes'),
            (bytearray(b'data'), 'bytes'),
        ],
        ids=[
            'null',
            'bool',
            'int',
            'float',
            'str',
            'bytes',
            'bytearray',
        ],
    )
    def test_infer_value_type_supported(
        self,
        value: object,
        expected: str,
    ) -> None:
        """
        Test that :func:`_infer_value_type` returns expected types for
        supported values.
        """
        assert mod._infer_value_type(value) == expected

    def test_infer_value_type_rejects_complex(self) -> None:
        """
        Test that :func:`_infer_value_type` raises for unsupported complex
        types.
        """
        with pytest.raises(TypeError, match='AVRO payloads must contain'):
            mod._infer_value_type({'bad': 'value'})

    def test_merge_types_orders_null_first(self) -> None:
        """
        Test that :func:`_merge_types` orders 'null' first when present.
        """
        types = ['string', 'null', 'string', 'long']

        merged = mod._merge_types(types)

        assert merged == ['null', 'long', 'string']

    def test_infer_schema_rejects_nested_values(self) -> None:
        """Test that :func:`_infer_schema` raises for nested values."""
        with pytest.raises(TypeError, match='AVRO payloads must contain'):
            mod._infer_schema([{'bad': {'nested': True}}])

    def test_infer_schema_builds_fields(self) -> None:
        """Test that :func:`_infer_schema` builds expected fields."""
        records: list[dict[str, Any]] = [
            {'b': 'text', 'a': 1},
            {'b': None},
        ]

        schema = mod._infer_schema(records)

        assert schema['type'] == 'record'
        assert schema['name'] == 'etlplus_record'
        assert schema['fields'] == [
            {'name': 'a', 'type': ['null', 'long']},
            {'name': 'b', 'type': ['null', 'string']},
        ]


class TestAvroIo(BinaryDependencyModuleContract):
    """Unit tests for AVRO module-level read/write dispatch."""

    # pylint: disable=protected-access

    module = mod
    format_name = 'avro'
    dependency_name = 'fastavro'
    expected_read_result = [{'id': 1}, {'id': 2}]
    write_payload = [{'id': 1, 'name': 'Ada'}]

    def make_dependency_stub(self) -> _FastAvroStub:
        """Build a fastavro dependency stub."""
        return _FastAvroStub()

    def assert_dependency_after_read(
        self,
        dependency_stub: object,
        _path: Path,
    ) -> None:
        """Assert fastavro read behavior."""
        stub = dependency_stub
        assert isinstance(stub, _FastAvroStub)

    def assert_dependency_after_write(
        self,
        dependency_stub: object,
        _path: Path,
    ) -> None:
        """Assert fastavro write behavior."""
        stub = dependency_stub
        assert isinstance(stub, _FastAvroStub)
        assert stub.parsed_schema is not None
        assert stub.writes
        assert stub.writes[0]['records'] == self.write_payload
