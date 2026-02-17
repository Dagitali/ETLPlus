"""
:mod:`tests.unit.file.pytest_file_contracts_binary` module.

Binary/archive/stub contract suites for unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from etlplus.file import FileFormat
from etlplus.file.stub import StubFileHandlerABC
from etlplus.types import JSONData
from etlplus.types import JSONDict

from .pytest_file_contract_mixins import PathMixin
from .pytest_file_contract_utils import Operation
from .pytest_file_contract_utils import assert_stub_module_operation_raises
from .pytest_file_contract_utils import make_payload
from .pytest_file_types import OptionalModuleInstaller

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ArchiveWrapperCoreDispatchModuleContract',
    'BinaryCodecModuleContract',
    'BinaryDependencyModuleContract',
    'BinaryKeyedPayloadModuleContract',
    'StubModuleContract',
]


# SECTION: CLASSES ========================================================== #


class BinaryCodecStub:
    """
    Generic codec stub for binary serialization module tests.

    Supports configurable reader/writer method names to cover modules like
    :mod:`msgpack` and :mod:`cbor2` with one reusable implementation.
    """

    def __init__(
        self,
        *,
        reader_method_name: str,
        writer_method_name: str,
        loaded_result: object,
        emitted_bytes: bytes,
    ) -> None:
        self.reader_method_name = reader_method_name
        self.writer_method_name = writer_method_name
        self.loaded_result = loaded_result
        self.emitted_bytes = emitted_bytes
        self.reader_payloads: list[bytes] = []
        self.reader_kwargs: list[dict[str, object]] = []
        self.writer_payloads: list[object] = []
        self.writer_kwargs: list[dict[str, object]] = []

    def _reader(
        self,
        payload: bytes,
        **kwargs: object,
    ) -> object:
        self.reader_payloads.append(payload)
        self.reader_kwargs.append(dict(kwargs))
        return self.loaded_result

    def _writer(
        self,
        payload: object,
        **kwargs: object,
    ) -> bytes:
        self.writer_payloads.append(payload)
        self.writer_kwargs.append(dict(kwargs))
        return self.emitted_bytes

    def __getattr__(
        self,
        name: str,
    ) -> object:
        if name == self.reader_method_name:
            return self._reader
        if name == self.writer_method_name:
            return self._writer
        raise AttributeError(name)


class CoreDispatchFileStub:
    """
    Minimal stand-in for :class:`etlplus.file.core.File` in archive tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        path: Path,
        fmt: FileFormat,
    ) -> None:
        self.path = Path(path)
        self.fmt = fmt

    def read(self) -> dict[str, str]:
        """Return deterministic payload for archive-wrapper read tests."""
        return {'fmt': self.fmt.value, 'name': self.path.name}

    def write(
        self,
        data: object,
    ) -> int:
        """Persist deterministic content so wrapper tests can assert bytes."""
        _ = data
        self.path.write_text('payload', encoding='utf-8')
        return 1


# SECTION: CLASSES (CONTRACTS) ============================================== #


class ArchiveWrapperCoreDispatchModuleContract(PathMixin):
    """Reusable contract suite for archive wrappers using core dispatch."""

    write_payload: JSONData = make_payload('list')
    expected_written_count: int = 1
    missing_inner_error_pattern: str = 'Cannot infer file format'

    def archive_path(
        self,
        tmp_path: Path,
        *,
        stem: str,
        suffix: str | None = None,
    ) -> Path:
        """Build deterministic archive paths for ad hoc test cases."""
        extension = self.format_name if suffix is None else suffix
        return tmp_path / f'{stem}.{extension}'

    def valid_archive_path(
        self,
        tmp_path: Path,
    ) -> Path:
        """Build the canonical archive path for core-dispatch tests."""
        return self.archive_path(tmp_path, stem='payload.json')

    def missing_inner_format_path(
        self,
        tmp_path: Path,
    ) -> Path:
        """Build an archive path with no inferable inner file format."""
        return self.archive_path(tmp_path, stem='payload')

    def expected_read_result(
        self,
    ) -> JSONData:
        """Build the expected core-dispatch payload for archive reads."""
        return {'fmt': 'json', 'name': 'payload.json'}

    def seed_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Write a wrapped payload used by read tests."""
        raise NotImplementedError

    def assert_archive_payload(
        self,
        path: Path,
    ) -> None:
        """Assert wrapped payload bytes/content produced by writes."""
        raise NotImplementedError

    def install_core_file_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Patch core file dispatch for deterministic archive tests."""
        monkeypatch.setattr(
            'etlplus.file.core.File',
            CoreDispatchFileStub,
        )

    def test_read_uses_core_dispatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read delegating payload parsing through core dispatch."""
        self.install_core_file_stub(monkeypatch)
        path = self.valid_archive_path(tmp_path)
        self.seed_archive_payload(path)

        result = self.module_handler.read(path)

        assert result == self.expected_read_result()

    def test_write_creates_wrapped_payload(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write persisting wrapped payload through core dispatch."""
        self.install_core_file_stub(monkeypatch)
        path = self.valid_archive_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_archive_payload(path)

    def test_write_requires_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes requiring a resolvable inner file format."""
        path = self.missing_inner_format_path(tmp_path)

        with pytest.raises(ValueError, match=self.missing_inner_error_pattern):
            self.module_handler.write(path, self.write_payload)


class BinaryCodecModuleContract(PathMixin):
    """Reusable contract suite for binary codec wrapper modules."""

    dependency_name: str
    reader_method_name: str
    writer_method_name: str
    reader_kwargs: dict[str, object]
    writer_kwargs: dict[str, object]
    loaded_result: JSONData
    emitted_bytes: bytes
    list_payload: JSONData = make_payload('list')
    dict_payload: JSONData = make_payload('dict')
    expected_list_dump: object = make_payload('list')
    expected_dict_dump: object = make_payload('dict')

    def _make_codec_stub(
        self,
        *,
        loaded_result: object,
    ) -> BinaryCodecStub:
        """Create a codec stub configured for this binary format module."""
        return BinaryCodecStub(
            reader_method_name=self.reader_method_name,
            writer_method_name=self.writer_method_name,
            loaded_result=loaded_result,
            emitted_bytes=self.emitted_bytes,
        )

    def test_read_uses_dependency_codec(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read delegating bytes decoding to the codec dependency."""
        codec = self._make_codec_stub(loaded_result=self.loaded_result)
        optional_module_stub({self.dependency_name: codec})
        path = self.format_path(tmp_path)
        path.write_bytes(b'payload')

        result = self.module_handler.read(path)

        assert result == self.loaded_result
        assert codec.reader_payloads == [b'payload']
        assert codec.reader_kwargs == [self.reader_kwargs]

    @pytest.mark.parametrize(
        ('payload_attr', 'expected_attr'),
        [
            ('list_payload', 'expected_list_dump'),
            ('dict_payload', 'expected_dict_dump'),
        ],
        ids=['list', 'dict'],
    )
    def test_write_serializes_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        payload_attr: str,
        expected_attr: str,
    ) -> None:
        """Test write delegating payload encoding to the codec dependency."""
        codec = self._make_codec_stub(loaded_result=self.loaded_result)
        optional_module_stub({self.dependency_name: codec})
        path = self.format_path(tmp_path)
        payload = cast(JSONData, getattr(self, payload_attr))
        expected_dump = getattr(self, expected_attr)

        written = self.module_handler.write(path, payload)

        assert written == 1
        assert codec.writer_payloads == [expected_dump]
        assert codec.writer_kwargs == [self.writer_kwargs]
        assert path.read_bytes() == self.emitted_bytes


class BinaryDependencyModuleContract(PathMixin):
    """Reusable contract suite for binary modules backed by one dependency."""

    dependency_name: str
    expected_read_result: JSONData
    write_payload: JSONData
    read_payload_bytes: bytes = b'payload'
    expected_written_count: int = 1

    def make_dependency_stub(self) -> object:
        """Build dependency stub used by read/write tests."""
        raise NotImplementedError

    def assert_dependency_after_read(
        self,
        dependency_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert dependency interactions for read tests."""

    def assert_dependency_after_write(
        self,
        dependency_stub: object,
        path: Path,  # noqa: ARG002
    ) -> None:
        """Assert dependency interactions for write tests."""

    def test_read_uses_dependency(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read delegating to the configured dependency."""
        dependency = self.make_dependency_stub()
        optional_module_stub({self.dependency_name: dependency})
        path = self.format_path(tmp_path)
        path.write_bytes(self.read_payload_bytes)

        result = self.module_handler.read(path)

        assert result == self.expected_read_result
        self.assert_dependency_after_read(dependency, path)

    def test_write_uses_dependency(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test write delegating to the configured dependency."""
        dependency = self.make_dependency_stub()
        optional_module_stub({self.dependency_name: dependency})
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_dependency_after_write(dependency, path)


class BinaryKeyedPayloadModuleContract(PathMixin):
    """Reusable contract suite for keyed binary payload wrapper modules."""

    payload_key: str
    sample_payload_value: str
    expected_bytes: bytes
    invalid_payload: JSONData

    @pytest.fixture
    def sample_payload(self) -> JSONDict:
        """Create a representative keyed payload dictionary."""
        return {self.payload_key: self.sample_payload_value}

    def test_read_write_roundtrip(
        self,
        tmp_path: Path,
        sample_payload: JSONDict,
    ) -> None:
        """Test write/read round trip, preserving payload bytes."""
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, sample_payload)

        assert written == 1
        assert path.read_bytes() == self.expected_bytes
        assert self.module_handler.read(path) == sample_payload

    def test_write_rejects_missing_required_key(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes requiring the expected payload key."""
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match=self.payload_key):
            self.module_handler.write(path, self.invalid_payload)


class StubModuleContract(PathMixin):
    """Reusable contract suite for placeholder/stub format modules."""

    handler_cls: type[StubFileHandlerABC]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test handler metadata and inheritance contract."""
        assert issubclass(self.handler_cls, StubFileHandlerABC)
        assert self.handler_cls.format.value == self.format_name

    @pytest.mark.parametrize(
        ('operation', 'write_payload'),
        [
            ('read', None),
            ('write', None),
            ('write', []),
        ],
    )
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: Operation,
        write_payload: JSONData | None,
    ) -> None:
        """Test module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            self.module,
            format_name=self.format_name,
            operation=operation,
            path=self.format_path(tmp_path),
            write_payload=write_payload,
        )
