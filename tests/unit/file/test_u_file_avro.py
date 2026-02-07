"""
:mod:`tests.unit.file.test_u_file_avro` module.

Unit tests for :mod:`etlplus.file.avro`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.file import avro as mod
from etlplus.file.enums import FileFormat

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


class TestAvroHandlerClass:
    """Unit tests for :class:`etlplus.file.avro.AvroFile`."""

    def test_format_constant(self) -> None:
        """Test :class:`AvroFile` exposing the expected format enum."""
        assert mod.AvroFile.format is FileFormat.AVRO

    def test_dumps_and_loads_bytes(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
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


class TestAvroRead:
    """Unit tests for :func:`etlplus.file.avro.read`."""

    def test_read(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`read` uses the :mod:`fastavro` module to read
        records.
        """
        stub = _FastAvroStub()
        optional_module_stub({'fastavro': stub})
        path = tmp_path / 'data.avro'
        path.write_bytes(b'payload')

        result = mod.read(path)

        assert result == stub.records


class TestAvroWrite:
    """Unit tests for :func:`etlplus.file.avro.write`."""

    def test_write(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """
        Test that :func:`write` uses the :mod:`fastavro` module to write
        records.
        """
        stub = _FastAvroStub()
        optional_module_stub({'fastavro': stub})
        path = tmp_path / 'data.avro'
        records = [{'id': 1, 'name': 'Ada'}]

        written = mod.write(path, records)

        assert written == 1
        assert stub.parsed_schema is not None
        assert stub.writes
        assert stub.writes[0]['records'] == records
