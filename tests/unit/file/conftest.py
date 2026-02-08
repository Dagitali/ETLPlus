"""
:mod:`tests.unit.file.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.file`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup across file-focused unit
    tests.
"""

from __future__ import annotations

import csv
import importlib
import inspect
import math
import numbers
import pkgutil
from collections.abc import Callable
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any
from typing import Literal
from typing import cast

import pytest

import etlplus.file._imports as import_helpers
from etlplus.file import FileFormat
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.stub import StubFileHandlerABC
from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_numeric_value(
    value: object,
) -> object:
    """Coerce numeric scalars into stable Python numeric types."""
    if isinstance(value, numbers.Real):
        try:
            numeric = float(value)
            if math.isnan(numeric):
                return None
        except (TypeError, ValueError):
            return value
        if numeric.is_integer():
            return int(numeric)
        return float(numeric)
    return value


# SECTION: FUNCTIONS ======================================================== #


def assert_single_dataset_rejects_non_default_key(
    handler: SingleDatasetScientificFileHandlerABC,
    *,
    suffix: str,
) -> None:
    """Assert single-dataset scientific handlers reject non-default keys."""
    bad_dataset = 'not_default_dataset'
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.read_dataset(
            Path(f'ignored.{suffix}'),
            dataset=bad_dataset,
        )
    with pytest.raises(ValueError, match='supports only dataset key'):
        handler.write_dataset(
            Path(f'ignored.{suffix}'),
            [],
            dataset=bad_dataset,
        )


def assert_stub_module_contract(
    module: ModuleType,
    handler_cls: type[StubFileHandlerABC],
    *,
    format_name: str,
    tmp_path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert baseline contract for a placeholder stub module."""
    if write_payload is None:
        write_payload = [{'id': 1}]

    assert issubclass(handler_cls, StubFileHandlerABC)
    assert handler_cls.format.value == format_name

    path = tmp_path / f'data.{format_name}'
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} read is not implemented yet',
    ):
        module.read(path)
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} write is not implemented yet',
    ):
        module.write(path, write_payload)


def assert_stub_module_operation_raises(
    module: ModuleType,
    *,
    format_name: str,
    operation: Literal['read', 'write'],
    path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert one stub module operation raises :class:`NotImplementedError`."""
    if write_payload is None:
        write_payload = [{'id': 1}]

    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} {operation} is not implemented yet',
    ):
        if operation == 'read':
            module.read(path)
        else:
            module.write(path, write_payload)


def make_import_error_reader_module(
    method_name: str,
) -> object:
    """
    Build a module-like object whose reader method raises ImportError.

    Parameters
    ----------
    method_name : str
        Reader method name to define (for example, ``"read_excel"``).

    Returns
    -------
    object
        Module-like object with one failing reader method.
    """

    class _FailModule:
        """Module stub exposing one reader that always raises ImportError."""

        def __getattribute__(self, name: str) -> Any:
            if name != method_name:
                return super().__getattribute__(name)

            def _fail_reader(
                *args: object,
                **kwargs: object,
            ) -> object:  # noqa: ARG001
                raise ImportError('missing')

            return _fail_reader

    return _FailModule()


def make_import_error_writer_module() -> object:
    """
    Build a pandas-like module whose DataFrame writes raise ImportError.

    Returns
    -------
    object
        Module-like object with ``DataFrame.from_records`` that returns a
        failing frame.
    """

    class _FailFrame:
        """Frame stub whose write-like attributes raise ImportError."""

        def __getattr__(self, name: str) -> object:
            if not name.startswith('to_'):
                raise AttributeError(name)

            def _fail_writer(
                *args: object,
                **kwargs: object,
            ) -> None:  # noqa: ARG001
                raise ImportError('missing')

            return _fail_writer

    class _FailModule:
        """Module stub exposing failing ``DataFrame.from_records``."""

        # pylint: disable=unused-argument

        class DataFrame:  # noqa: D106
            """Minimal DataFrame namespace for write-path tests."""

            @staticmethod
            def from_records(
                records: list[dict[str, object]],
            ) -> _FailFrame:  # noqa: ARG002
                return _FailFrame()

    return _FailModule()


def normalize_numeric_records(
    records: JSONData,
) -> JSONData:
    """
    Normalize numeric record values for deterministic comparisons.

    Parameters
    ----------
    records : JSONData
        Record payloads to normalize.

    Returns
    -------
    JSONData
        Normalized record payloads.
    """
    if isinstance(records, list):
        normalized: list[JSONDict] = []
        for row in records:
            if not isinstance(row, dict):
                normalized.append(row)
                continue
            cleaned: JSONDict = {}
            for key, value in row.items():
                cleaned[key] = _coerce_numeric_value(value)
            normalized.append(cleaned)
        return normalized
    return records


def normalize_xml_payload(payload: JSONData) -> JSONData:
    """
    Normalize XML payloads to list-based item structures when possible.

    Parameters
    ----------
    payload : JSONData
        XML payload to normalize.

    Returns
    -------
    JSONData
        Normalized XML payload.
    """
    if not isinstance(payload, dict):
        return payload
    root = payload.get('root')
    if not isinstance(root, dict):
        return payload
    items = root.get('items')
    if isinstance(items, dict):
        root = {**root, 'items': [items]}
        return {**payload, 'root': root}
    return payload


def require_optional_modules(
    *modules: str,
) -> None:
    """
    Skip the test when optional dependencies are missing.

    Parameters
    ----------
    *modules : str
        Module names to verify via ``pytest.importorskip``.
    """
    for module in modules:
        pytest.importorskip(module)


# SECTION: CLASSES (CONTRACTS) ============================================== #


class ArchiveWrapperCoreDispatchModuleContract:
    """
    Reusable contract suite for archive wrappers using core dispatch.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`valid_path_name`
    - :attr:`missing_inner_path_name`
    - :attr:`expected_read_result`
    - :attr:`seed_archive_payload`
    - :attr:`assert_archive_payload`
    """

    module: ModuleType
    format_name: str
    valid_path_name: str
    missing_inner_path_name: str
    expected_read_result: JSONData
    write_payload: JSONData = [{'id': 1}]
    expected_written_count: int = 1
    missing_inner_error_pattern: str = 'Cannot infer file format'

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

    def test_read_uses_core_dispatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read delegating payload parsing through core dispatch."""
        self.install_core_file_stub(monkeypatch)
        path = tmp_path / self.valid_path_name
        self.seed_archive_payload(path)

        result = self.module.read(path)

        assert result == self.expected_read_result

    def test_write_creates_wrapped_payload(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write persisting wrapped payload through core dispatch."""
        self.install_core_file_stub(monkeypatch)
        path = tmp_path / self.valid_path_name

        written = self.module.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_archive_payload(path)

    def test_write_requires_inner_format(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes requiring a resolvable inner file format."""
        path = tmp_path / self.missing_inner_path_name

        with pytest.raises(ValueError, match=self.missing_inner_error_pattern):
            self.module.write(path, self.write_payload)


class BaseOptionResolutionContract:
    """
    Reusable contract suite for base option helper behaviors.

    Subclasses must provide factory methods for concrete handlers.
    """

    def make_scientific_handler(self) -> ScientificDatasetFileHandlerABC:
        """Build a scientific handler used by dataset helper tests."""
        raise NotImplementedError

    def make_delimited_handler(self) -> FileHandlerABC:
        """Build a delimited handler used by delimiter helper tests."""
        raise NotImplementedError

    def make_read_only_handler(self) -> FileHandlerABC:
        """Build a generic handler used by encoding/root/extras tests."""
        raise NotImplementedError

    def make_archive_handler(self) -> FileHandlerABC:
        """Build an archive handler used by inner-name helper tests."""
        raise NotImplementedError

    def make_spreadsheet_handler(self) -> FileHandlerABC:
        """Build a spreadsheet handler used by sheet helper tests."""
        raise NotImplementedError

    def make_embedded_handler(self) -> FileHandlerABC:
        """Build an embedded-db handler used by table helper tests."""
        raise NotImplementedError

    def test_dataset_option_helpers_use_override_then_default(self) -> None:
        """Test scientific dataset helpers using explicit then default data."""
        handler = self.make_scientific_handler()

        assert cast(Any, handler).dataset_from_read_options(None) is None
        assert cast(Any, handler).dataset_from_write_options(None) is None
        assert (
            cast(
                Any,
                handler,
            ).dataset_from_read_options(ReadOptions(dataset='features'))
            == 'features'
        )
        assert (
            cast(
                Any,
                handler,
            ).dataset_from_write_options(WriteOptions(dataset='labels'))
            == 'labels'
        )
        assert (
            cast(Any, handler).resolve_read_dataset(
                None,
                options=ReadOptions(dataset='features'),
            )
            == 'features'
        )
        assert (
            cast(Any, handler).resolve_write_dataset(
                None,
                options=WriteOptions(dataset='labels'),
            )
            == 'labels'
        )
        assert (
            cast(Any, handler).resolve_read_dataset(
                'explicit',
                options=ReadOptions(dataset='ignored'),
            )
            == 'explicit'
        )
        assert (
            cast(Any, handler).resolve_write_dataset(
                'explicit',
                options=WriteOptions(dataset='ignored'),
            )
            == 'explicit'
        )
        assert (
            cast(Any, handler).resolve_read_dataset(None, default='fallback')
            == 'fallback'
        )
        assert (
            cast(Any, handler).resolve_write_dataset(None, default='fallback')
            == 'fallback'
        )

    def test_delimiter_option_helpers_use_override_then_default(self) -> None:
        """Test delimited helpers using explicit then default delimiter."""
        handler = self.make_delimited_handler()

        assert cast(Any, handler).delimiter_from_read_options(None) == ','
        assert cast(Any, handler).delimiter_from_write_options(None) == ','
        assert (
            cast(
                Any,
                handler,
            ).delimiter_from_read_options(
                ReadOptions(extras={'delimiter': '|'}),
            )
            == '|'
        )
        assert (
            cast(
                Any,
                handler,
            ).delimiter_from_write_options(
                WriteOptions(extras={'delimiter': '\t'}),
            )
            == '\t'
        )
        assert (
            cast(Any, handler).delimiter_from_read_options(None, default=';')
            == ';'
        )
        assert (
            cast(Any, handler).delimiter_from_write_options(None, default=':')
            == ':'
        )

    def test_encoding_option_helpers_use_override_then_default(self) -> None:
        """Test encoding helpers using explicit values then defaults."""
        handler = self.make_read_only_handler()

        assert cast(Any, handler).encoding_from_read_options(None) == 'utf-8'
        assert (
            cast(
                Any,
                handler,
            ).encoding_from_read_options(ReadOptions(encoding='latin-1'))
            == 'latin-1'
        )
        assert (
            cast(Any, handler).encoding_from_read_options(
                None,
                default='utf-16',
            )
            == 'utf-16'
        )

        assert cast(Any, handler).encoding_from_write_options(None) == 'utf-8'
        assert (
            cast(
                Any,
                handler,
            ).encoding_from_write_options(WriteOptions(encoding='utf-16'))
            == 'utf-16'
        )
        assert (
            cast(Any, handler).encoding_from_write_options(
                None,
                default='ascii',
            )
            == 'ascii'
        )

    def test_extra_option_helpers_use_override_then_default(self) -> None:
        """Test extras helpers using explicit values then defaults."""
        handler = self.make_read_only_handler()

        assert cast(Any, handler).read_extra_option(None, 'foo') is None
        assert cast(Any, handler).write_extra_option(None, 'foo') is None
        assert (
            cast(Any, handler).read_extra_option(None, 'foo', default='x')
            == 'x'
        )
        assert (
            cast(Any, handler).write_extra_option(None, 'foo', default='y')
            == 'y'
        )
        assert (
            cast(
                Any,
                handler,
            ).read_extra_option(ReadOptions(extras={'foo': 1}), 'foo')
            == 1
        )
        assert (
            cast(
                Any,
                handler,
            ).write_extra_option(WriteOptions(extras={'foo': 2}), 'foo')
            == 2
        )

    def test_inner_name_option_helpers_use_override_then_default(self) -> None:
        """
        Test archive option helpers using explicit then default inner name.
        """
        handler = self.make_archive_handler()

        assert cast(Any, handler).inner_name_from_read_options(None) is None
        assert cast(Any, handler).inner_name_from_write_options(None) is None
        assert (
            cast(
                Any,
                handler,
            ).inner_name_from_read_options(ReadOptions(inner_name='data.json'))
            == 'data.json'
        )
        assert (
            cast(
                Any,
                handler,
            ).inner_name_from_write_options(
                WriteOptions(inner_name='payload.csv'),
            )
            == 'payload.csv'
        )

    def test_read_options_use_independent_extras_dicts(self) -> None:
        """Test each ReadOptions instance getting its own extras dict."""
        first = ReadOptions()
        second = ReadOptions()

        assert not first.extras
        assert not second.extras
        assert first.extras is not second.extras

    def test_root_tag_option_helper_use_override_then_default(self) -> None:
        """Test root-tag helper using explicit values then defaults."""
        handler = self.make_read_only_handler()

        assert cast(Any, handler).root_tag_from_write_options(None) == 'root'
        assert (
            cast(
                Any,
                handler,
            ).root_tag_from_write_options(WriteOptions(root_tag='items'))
            == 'items'
        )
        assert (
            cast(Any, handler).root_tag_from_write_options(
                None,
                default='dataset',
            )
            == 'dataset'
        )

    def test_sheet_option_helpers_use_override_then_default(self) -> None:
        """
        Test spreadsheet option helpers using explicit then default sheet.
        """
        handler = self.make_spreadsheet_handler()

        assert cast(Any, handler).sheet_from_read_options(None) == 0
        assert cast(Any, handler).sheet_from_write_options(None) == 0
        assert (
            cast(Any, handler).sheet_from_read_options(
                ReadOptions(sheet='Sheet2'),
            )
            == 'Sheet2'
        )
        assert (
            cast(Any, handler).sheet_from_write_options(WriteOptions(sheet=3))
            == 3
        )

    def test_table_option_helpers_use_override_then_default(self) -> None:
        """
        Test embedded-db option helpers using explicit then default table.
        """
        handler = self.make_embedded_handler()

        assert cast(Any, handler).table_from_read_options(None) is None
        assert cast(Any, handler).table_from_write_options(None) is None
        assert (
            cast(Any, handler).table_from_read_options(
                ReadOptions(table='events'),
            )
            == 'events'
        )
        assert (
            cast(Any, handler).table_from_write_options(
                WriteOptions(table='staging'),
            )
            == 'staging'
        )

    def test_write_options_are_frozen(self) -> None:
        """Test WriteOptions immutability contract."""
        options = WriteOptions()

        with pytest.raises(Exception) as exc:
            options.encoding = 'latin-1'  # type: ignore[misc]
        assert exc.type.__name__ == 'FrozenInstanceError'


class BinaryCodecModuleContract:
    """
    Reusable contract suite for binary codec wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`dependency_name`
    - :attr:`reader_method_name`
    - :attr:`writer_method_name`
    - :attr:`reader_kwargs`
    - :attr:`writer_kwargs`
    - :attr:`loaded_result`
    - :attr:`emitted_bytes`
    """

    module: ModuleType
    format_name: str
    dependency_name: str
    reader_method_name: str
    writer_method_name: str
    reader_kwargs: dict[str, object]
    writer_kwargs: dict[str, object]
    loaded_result: JSONData
    emitted_bytes: bytes
    list_payload: JSONData = [{'id': 1}]
    dict_payload: JSONData = {'id': 1}
    expected_list_dump: object = [{'id': 1}]
    expected_dict_dump: object = {'id': 1}

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
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test read delegating bytes decoding to the codec dependency."""
        codec = self._make_codec_stub(loaded_result=self.loaded_result)
        optional_module_stub({self.dependency_name: codec})
        path = tmp_path / f'data.{self.format_name}'
        path.write_bytes(b'payload')

        result = self.module.read(path)

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
        optional_module_stub: Callable[[dict[str, object]], None],
        payload_attr: str,
        expected_attr: str,
    ) -> None:
        """Test write delegating payload encoding to the codec dependency."""
        codec = self._make_codec_stub(loaded_result=self.loaded_result)
        optional_module_stub({self.dependency_name: codec})
        path = tmp_path / f'data.{self.format_name}'
        payload = cast(JSONData, getattr(self, payload_attr))
        expected_dump = getattr(self, expected_attr)

        written = self.module.write(path, payload)

        assert written == 1
        assert codec.writer_payloads == [expected_dump]
        assert codec.writer_kwargs == [self.writer_kwargs]
        assert path.read_bytes() == self.emitted_bytes


class BinaryDependencyModuleContract:
    """
    Reusable contract suite for binary modules backed by one dependency.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`dependency_name`
    - :attr:`expected_read_result`
    - :attr:`write_payload`
    - :attr:`make_dependency_stub`
    """

    module: ModuleType
    format_name: str
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
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test read delegating to the configured dependency."""
        dependency = self.make_dependency_stub()
        optional_module_stub({self.dependency_name: dependency})
        path = tmp_path / f'data.{self.format_name}'
        path.write_bytes(self.read_payload_bytes)

        result = self.module.read(path)

        assert result == self.expected_read_result
        self.assert_dependency_after_read(dependency, path)

    def test_write_uses_dependency(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test write delegating to the configured dependency."""
        dependency = self.make_dependency_stub()
        optional_module_stub({self.dependency_name: dependency})
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_dependency_after_write(dependency, path)


class BinaryKeyedPayloadModuleContract:
    """
    Reusable contract suite for keyed binary payload wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`payload_key`
    - :attr:`sample_payload_value`
    - :attr:`expected_bytes`
    - :attr:`invalid_payload`
    """

    module: ModuleType
    format_name: str
    payload_key: str
    sample_payload_value: str
    expected_bytes: bytes
    invalid_payload: JSONData

    @pytest.fixture
    def sample_payload(self) -> JSONDict:
        """Create a representative keyed payload dictionary."""
        return {self.payload_key: self.sample_payload_value}

    def test_read_write_round_trip(
        self,
        tmp_path: Path,
        sample_payload: JSONDict,
    ) -> None:
        """Test write/read round-trip preserving payload bytes."""
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, sample_payload)

        assert written == 1
        assert path.read_bytes() == self.expected_bytes
        assert self.module.read(path) == sample_payload

    def test_write_rejects_missing_required_key(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes requiring the expected payload key."""
        path = tmp_path / f'data.{self.format_name}'

        with pytest.raises(TypeError, match=self.payload_key):
            self.module.write(path, self.invalid_payload)


class DelimitedModuleContract:
    """
    Reusable contract suite for delimited text wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`delimiter`
    """

    module: ModuleType
    format_name: str
    delimiter: str

    def test_read_uses_expected_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module read delegating with the expected delimiter."""
        calls: dict[str, object] = {}

        def _read_delimited(
            path: object,
            *,
            delimiter: str,
        ) -> list[dict[str, object]]:
            calls['path'] = path
            calls['delimiter'] = delimiter
            return [{'ok': True}]

        monkeypatch.setattr(self.module, 'read_delimited', _read_delimited)

        result = self.module.read(tmp_path / f'data.{self.format_name}')

        assert result == [{'ok': True}]
        assert calls['delimiter'] == self.delimiter

    def test_write_uses_expected_delimiter_and_format_name(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module write delegating with expected delimiter/format."""
        calls: dict[str, object] = {}

        def _write_delimited(
            path: object,
            data: object,
            *,
            delimiter: str,
            format_name: str,
        ) -> int:
            calls['path'] = path
            calls['data'] = data
            calls['delimiter'] = delimiter
            calls['format_name'] = format_name
            return 1

        monkeypatch.setattr(self.module, 'write_delimited', _write_delimited)

        written = self.module.write(
            tmp_path / f'data.{self.format_name}',
            [{'id': 1}],
        )

        assert written == 1
        assert calls['delimiter'] == self.delimiter
        assert calls['format_name'] == self.format_name.upper()


class EmbeddedDatabaseModuleContract:
    """
    Reusable contract suite for embedded database wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`multi_table_error_pattern`
    - :attr:`build_empty_database_path`
    - :attr:`build_multi_table_database_path`
    """

    # pylint: disable=unused-argument

    module: ModuleType
    format_name: str
    multi_table_error_pattern: str

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Create an empty database fixture path for read tests."""
        raise NotImplementedError

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> Path:
        """Create a multi-table database fixture path for read tests."""
        raise NotImplementedError

    def test_read_returns_empty_when_no_tables(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading empty embedded databases returning no records."""
        path = self.build_empty_database_path(tmp_path, optional_module_stub)
        assert self.module.read(path) == []

    def test_read_raises_on_multiple_tables(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test read rejecting ambiguous multi-table databases."""
        path = self.build_multi_table_database_path(
            tmp_path,
            optional_module_stub,
        )
        with pytest.raises(ValueError, match=self.multi_table_error_pattern):
            self.module.read(path)

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        assert (
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [],
            )
            == 0
        )


class FileCoreDispatchContract:
    """
    Reusable contract suite for class-based core dispatch in ``File``.

    Subclasses must provide:
    - :attr:`file_cls`
    - :attr:`core_module`
    """

    file_cls: type[Any]
    core_module: ModuleType
    read_format: FileFormat = FileFormat.CSV
    write_format: FileFormat = FileFormat.XML
    read_filename: str = 'sample.csv'
    write_filename: str = 'export.xml'
    read_result: JSONData = {'ok': True}
    write_payload: JSONData = [{'name': 'Ada'}]
    write_root_tag: str = 'records'
    write_result: int = 3

    def test_read_uses_class_based_handler_dispatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read dispatch through ``core.get_handler`` handlers."""
        path = tmp_path / self.read_filename
        path.write_text('name\nAda\n', encoding='utf-8')
        calls: dict[str, object] = {}

        class _StubHandler:
            def read(
                self,
                path: Path,
            ) -> JSONData:
                calls['read_path'] = path
                return cast(JSONData, self_read_result)

            def write(
                self,
                path: Path,
                data: JSONData,
                *,
                options: WriteOptions | None = None,
            ) -> int:
                _ = path
                _ = data
                _ = options
                return 0

        self_read_result = self.read_result
        stub_handler = _StubHandler()

        def _get_handler(file_format: FileFormat) -> _StubHandler:
            calls['format'] = file_format
            return stub_handler

        monkeypatch.setattr(self.core_module, 'get_handler', _get_handler)

        result = self.file_cls(path, self.read_format).read()

        assert result == self.read_result
        assert calls['format'] is self.read_format
        assert calls['read_path'] == path

    def test_write_uses_class_based_handler_and_root_tag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write dispatch preserving XML ``root_tag`` in options."""
        path = tmp_path / self.write_filename
        calls: dict[str, object] = {}

        class _StubHandler:
            def read(
                self,
                path: Path,
            ) -> JSONData:
                _ = path
                return []

            def write(
                self,
                path: Path,
                data: JSONData,
                *,
                options: WriteOptions | None = None,
            ) -> int:
                calls['write_path'] = path
                calls['write_data'] = data
                calls['write_options'] = options
                return self_write_result

        self_write_result = self.write_result
        stub_handler = _StubHandler()

        def _get_handler(file_format: FileFormat) -> _StubHandler:
            calls['format'] = file_format
            return stub_handler

        monkeypatch.setattr(self.core_module, 'get_handler', _get_handler)

        written = self.file_cls(path, self.write_format).write(
            self.write_payload,
            root_tag=self.write_root_tag,
        )

        assert written == self.write_result
        assert calls['format'] is self.write_format
        assert calls['write_path'] == path
        assert calls['write_data'] == self.write_payload
        assert isinstance(calls['write_options'], WriteOptions)
        options = cast(WriteOptions, calls['write_options'])
        assert options.root_tag == self.write_root_tag


class HandlerMethodNamingContract:
    """
    Reusable contract suite for category-level handler naming conventions.

    Subclasses must provide:
    - :attr:`delimited_handlers`
    - :attr:`spreadsheet_handlers`
    - :attr:`embedded_db_handlers`
    - :attr:`scientific_handlers`
    """

    delimited_handlers: list[type[FileHandlerABC]]
    spreadsheet_handlers: list[type[FileHandlerABC]]
    embedded_db_handlers: list[type[FileHandlerABC]]
    scientific_handlers: list[type[FileHandlerABC]]

    def test_delimited_text_handlers_expose_row_methods(self) -> None:
        """Test delimited/text handlers exposing read_rows/write_rows."""
        for handler_cls in self.delimited_handlers:
            assert callable(getattr(handler_cls, 'read', None))
            assert callable(getattr(handler_cls, 'write', None))
            assert callable(getattr(handler_cls, 'read_rows', None))
            assert callable(getattr(handler_cls, 'write_rows', None))

    def test_spreadsheet_handlers_expose_sheet_methods(self) -> None:
        """Test spreadsheet handlers exposing read_sheet/write_sheet."""
        for handler_cls in self.spreadsheet_handlers:
            assert callable(getattr(handler_cls, 'read', None))
            assert callable(getattr(handler_cls, 'write', None))
            assert callable(getattr(handler_cls, 'read_sheet', None))
            assert callable(getattr(handler_cls, 'write_sheet', None))

    def test_embedded_db_handlers_expose_table_methods(self) -> None:
        """Test embedded-db handlers exposing read_table/write_table."""
        for handler_cls in self.embedded_db_handlers:
            assert callable(getattr(handler_cls, 'read', None))
            assert callable(getattr(handler_cls, 'write', None))
            assert callable(getattr(handler_cls, 'read_table', None))
            assert callable(getattr(handler_cls, 'write_table', None))

    def test_scientific_handlers_expose_dataset_methods(self) -> None:
        """Test scientific handlers exposing read_dataset/write_dataset."""
        for handler_cls in self.scientific_handlers:
            assert callable(getattr(handler_cls, 'read', None))
            assert callable(getattr(handler_cls, 'write', None))
            assert callable(getattr(handler_cls, 'read_dataset', None))
            assert callable(getattr(handler_cls, 'write_dataset', None))


class PandasColumnarModuleContract:
    """
    Reusable contract suite for pandas-backed columnar format modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`read_method_name`
    - :attr:`write_calls_attr`
    """

    module: ModuleType
    format_name: str
    read_method_name: str
    write_calls_attr: str
    write_uses_index: bool = False
    requires_pyarrow: bool = False
    read_error_pattern: str = 'missing'
    write_error_pattern: str = 'missing'

    def _install_read_write_dependencies(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
        pandas: object,
    ) -> None:
        """Install optional module stubs used by read/write tests."""
        mapping: dict[str, object] = {'pandas': pandas}
        if self.requires_pyarrow:
            mapping['pyarrow'] = object()
        optional_module_stub(mapping)

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test read returning row records via pandas."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        self._install_read_write_dependencies(optional_module_stub, pandas)

        result = self.module.read(tmp_path / f'data.{self.format_name}')

        assert result == [{'id': 1}]
        assert pandas.read_calls

    def test_write_calls_expected_table_writer(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test write calling the expected DataFrame writer method."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        self._install_read_write_dependencies(optional_module_stub, pandas)
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        calls = cast(
            list[dict[str, object]],
            getattr(pandas.last_frame, self.write_calls_attr),
        )
        assert calls
        call = calls[-1]
        assert call.get('path') == path
        if self.write_uses_index:
            assert call.get('index') is False
        else:
            assert 'index' not in call

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        assert (
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [],
            )
            == 0
        )

    def test_read_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test read dependency failures raising :class:`ImportError`."""
        if self.requires_pyarrow:
            optional_module_stub({'pyarrow': object()})
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader(self.read_method_name),
        )

        with pytest.raises(ImportError, match=self.read_error_pattern):
            self.module.read(tmp_path / f'data.{self.format_name}')

    def test_write_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_writer: Callable[[], object],
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test write dependency failures raising :class:`ImportError`."""
        if self.requires_pyarrow:
            optional_module_stub({'pyarrow': object()})
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match=self.write_error_pattern):
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [{'id': 1}],
            )


class PyarrowGateOnlyModuleContract:
    """
    Reusable contract suite for pyarrow-gated IPC-style modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    """

    module: ModuleType
    format_name: str
    missing_dependency_pattern: str = 'missing pyarrow'

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty writes short-circuiting without file creation."""
        path = tmp_path / f'data.{self.format_name}'
        assert self.module.write(path, []) == 0
        assert not path.exists()

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_operations_raise_when_pyarrow_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        operation: str,
    ) -> None:
        """Test read/write failing when pyarrow dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError(self.missing_dependency_pattern)

        monkeypatch.setattr(self.module, 'get_dependency', _missing)
        path = tmp_path / f'data.{self.format_name}'

        with pytest.raises(ImportError, match=self.missing_dependency_pattern):
            if operation == 'read':
                self.module.read(path)
            else:
                self.module.write(path, [{'id': 1}])


class PyarrowGatedPandasColumnarModuleContract(PandasColumnarModuleContract):
    """
    Reusable suite for pandas-backed columnar modules gated by pyarrow.
    """

    requires_pyarrow = True
    missing_dependency_pattern: str = 'missing pyarrow'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_operations_raise_when_pyarrow_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        operation: str,
    ) -> None:
        """Test read/write failing when pyarrow dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError(self.missing_dependency_pattern)

        monkeypatch.setattr(self.module, 'get_dependency', _missing)
        path = tmp_path / f'data.{self.format_name}'

        with pytest.raises(ImportError, match=self.missing_dependency_pattern):
            if operation == 'read':
                self.module.read(path)
            else:
                self.module.write(path, [{'id': 1}])


class RDataModuleContract:
    """
    Reusable contract suite for R-data wrapper modules (RDA/RDS).

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`writer_missing_pattern`
    - :meth:`build_frame`
    - :meth:`build_pandas_stub`
    - :meth:`build_pyreadr_stub`
    - :meth:`build_reader_only_stub`
    """

    module: ModuleType
    format_name: str
    writer_missing_pattern: str
    write_payload: JSONData = [{'id': 1}]

    def build_frame(
        self,
        records: list[dict[str, object]],
    ) -> object:
        """Build a frame-like stub from row records."""
        raise NotImplementedError

    def build_pandas_stub(self) -> object:
        """Build pandas module stub."""
        raise NotImplementedError

    def build_pyreadr_stub(
        self,
        result: dict[str, object],
    ) -> object:
        """Build pyreadr module stub."""
        raise NotImplementedError

    def build_reader_only_stub(self) -> object:
        """Build pyreadr-like stub without writer methods."""
        raise NotImplementedError

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,
    ) -> None:
        """Assert module-specific write success behavior."""
        _ = pyreadr_stub
        _ = path

    def test_read_empty_result_returns_empty_list(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading empty R-data results returning an empty list."""
        optional_module_stub(
            {
                'pyreadr': self.build_pyreadr_stub({}),
                'pandas': self.build_pandas_stub(),
            },
        )

        assert self.module.read(tmp_path / f'data.{self.format_name}') == []

    def test_read_single_value_coerces_to_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading one R object coercing to JSON records."""
        frame = self.build_frame([{'id': 1}])
        optional_module_stub(
            {
                'pyreadr': self.build_pyreadr_stub({'data': frame}),
                'pandas': self.build_pandas_stub(),
            },
        )

        assert self.module.read(tmp_path / f'data.{self.format_name}') == [
            {'id': 1},
        ]

    def test_read_multiple_values_returns_mapping(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading multiple R objects returning key-mapped payloads."""
        result: dict[str, object] = {'one': {'id': 1}, 'two': [{'id': 2}]}
        optional_module_stub(
            {
                'pyreadr': self.build_pyreadr_stub(result),
                'pandas': self.build_pandas_stub(),
            },
        )

        assert (
            self.module.read(tmp_path / f'data.{self.format_name}') == result
        )

    def test_write_raises_when_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test writing failing when pyreadr writer methods are unavailable."""
        optional_module_stub(
            {
                'pyreadr': self.build_reader_only_stub(),
                'pandas': self.build_pandas_stub(),
            },
        )

        with pytest.raises(ImportError, match=self.writer_missing_pattern):
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                self.write_payload,
            )

    def test_write_happy_path_uses_writer(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test writing delegating to pyreadr writer methods."""
        pyreadr = self.build_pyreadr_stub({})
        optional_module_stub(
            {'pyreadr': pyreadr, 'pandas': self.build_pandas_stub()},
        )
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, self.write_payload)

        assert written == 1
        self.assert_write_success(pyreadr, path)


class ReadOnlyScientificDatasetModuleContract:
    """
    Reusable contract suite for read-only scientific dataset handlers.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`handler_cls`
    - :attr:`format_name`
    - :attr:`unknown_dataset_error_pattern`
    - :meth:`prepare_unknown_dataset_env`
    """

    module: ModuleType
    handler_cls: type[object]
    format_name: str
    unknown_dataset_error_pattern: str

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install stubs needed for unknown-dataset contract checks."""
        _ = tmp_path
        _ = monkeypatch
        _ = optional_module_stub

    @pytest.fixture
    def handler(self) -> object:
        """Create a handler instance for read-only scientific contracts."""
        return self.handler_cls()

    def test_read_dataset_rejects_unknown_dataset(
        self,
        handler: object,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test explicit unknown dataset keys being rejected."""
        self.prepare_unknown_dataset_env(
            tmp_path,
            monkeypatch,
            optional_module_stub,
        )

        with pytest.raises(
            ValueError,
            match=self.unknown_dataset_error_pattern,
        ):
            cast(Any, handler).read_dataset(
                tmp_path / f'data.{self.format_name}',
                dataset='unknown',
            )

    def test_read_rejects_unknown_dataset_from_options(
        self,
        handler: object,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test unknown dataset rejection routed via read options."""
        self.prepare_unknown_dataset_env(
            tmp_path,
            monkeypatch,
            optional_module_stub,
        )

        with pytest.raises(
            ValueError,
            match=self.unknown_dataset_error_pattern,
        ):
            cast(Any, handler).read(
                tmp_path / f'data.{self.format_name}',
                options=ReadOptions(dataset='unknown'),
            )

    def test_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read-only handlers rejecting writes."""
        with pytest.raises(RuntimeError, match='read-only'):
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [{'id': 1}],
            )


class ReadOnlySpreadsheetModuleContract:
    """
    Reusable contract suite for read-only spreadsheet wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`dependency_hint`
    """

    module: ModuleType
    format_name: str
    dependency_hint: str

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
    ) -> None:
        """Test read wrapping dependency import errors."""
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader('read_excel'),
        )

        with pytest.raises(ImportError, match=self.dependency_hint):
            self.module.read(tmp_path / f'data.{self.format_name}')

    def test_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read-only formats rejecting writes."""
        with pytest.raises(RuntimeError, match='read-only'):
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [{'id': 1}],
            )


class RegistryAbcConformanceContract:
    """
    Reusable contract suite for registry handler-ABC conformance checks.

    Subclasses must provide:
    - :attr:`registry_module`
    - :attr:`abc_cases`
    """

    registry_module: ModuleType
    # abc_cases: list[tuple[FileFormat, type[FileHandlerABC]]]
    abc_cases: list[tuple[FileFormat, type[Any]]]

    def test_mapped_handler_class_inherits_expected_abc(self) -> None:
        """Test mapped handlers inheriting each expected category ABC."""
        for file_format, expected_abc in self.abc_cases:
            handler_class = cast(
                Any,
                self.registry_module,
            ).get_handler_class(file_format)
            assert issubclass(handler_class, expected_abc)


class RegistryFallbackPolicyContract:
    """
    Reusable contract suite for registry fallback/deprecation policies.

    Subclasses must provide:
    - :attr:`registry_module`
    """

    registry_module: ModuleType
    fallback_format: FileFormat = FileFormat.GZ

    # pylint: disable=protected-access

    def test_deprecated_fallback_builds_module_adapter_and_delegates_calls(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test deprecated fallback building a module-adapter and delegating I/O.
        """
        registry = cast(Any, self.registry_module)
        calls: dict[str, object] = {}

        def _read(path: Path) -> dict[str, object]:
            calls['read_path'] = path
            return {'ok': True}

        def _write(
            path: Path,
            data: object,
            *,
            root_tag: str = 'root',
        ) -> int:
            calls['write_path'] = path
            calls['write_data'] = data
            calls['root_tag'] = root_tag
            return 7

        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )
        fake_module = SimpleNamespace(read=_read, write=_write)
        monkeypatch.setattr(
            registry,
            '_module_for_format',
            lambda _fmt: fake_module,
        )

        with pytest.warns(DeprecationWarning, match='deprecated'):
            handler_class = registry.get_handler_class(
                self.fallback_format,
                allow_module_adapter_fallback=True,
            )

        assert issubclass(handler_class, FileHandlerABC)
        assert handler_class.category == 'module_adapter'

        handler = handler_class()
        path = Path('payload.gz')
        assert handler.read(path) == {'ok': True}
        written = handler.write(
            path,
            {'row': 1},
            options=WriteOptions(root_tag='records'),
        )

        assert written == 7
        assert calls['read_path'] == path
        assert calls['write_path'] == path
        assert calls['write_data'] == {'row': 1}
        assert calls['root_tag'] == 'records'

    def test_get_handler_class_raises_without_explicit_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test strict mode rejecting unmapped formats by default."""
        registry = cast(Any, self.registry_module)
        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )

        with pytest.raises(ValueError, match='Unsupported format'):
            registry.get_handler_class(self.fallback_format)

    def test_module_adapter_builder_raises_for_missing_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test module-adapter builder raising when module import fails."""
        registry = cast(Any, self.registry_module)

        def _raise_module_not_found(_file_format: FileFormat) -> object:
            raise ModuleNotFoundError('missing test module')

        monkeypatch.delitem(
            registry._HANDLER_CLASS_SPECS,
            self.fallback_format,
            raising=False,
        )
        monkeypatch.setattr(
            registry,
            '_module_for_format',
            _raise_module_not_found,
        )

        with pytest.raises(ModuleNotFoundError, match='missing test module'):
            registry._module_adapter_class_for_format(self.fallback_format)


class RegistryMappedResolutionContract:
    """
    Reusable contract suite for explicit registry mapping resolution.

    Subclasses must provide:
    - :attr:`registry_module`
    - :attr:`file_package`
    - :attr:`mapped_class_cases`
    - :attr:`placeholder_spec_cases`
    - :attr:`singleton_format`
    - :attr:`singleton_class`
    """

    # pylint: disable=protected-access

    registry_module: ModuleType
    file_package: ModuleType
    mapped_class_cases: list[tuple[FileFormat, type[Any]]]
    placeholder_spec_cases: list[tuple[FileFormat, str]]
    singleton_format: FileFormat = FileFormat.JSON
    # singleton_class: type[FileHandlerABC]
    singleton_class: type[Any]

    def test_explicit_for_implemented_formats(self) -> None:
        """
        Test every implemented handler class format being explicitly mapped.
        """
        registry = cast(Any, self.registry_module)
        implemented_formats: set[FileFormat] = set()
        for module_info in pkgutil.iter_modules(self.file_package.__path__):
            if module_info.ispkg or module_info.name.startswith('_'):
                continue
            module_name = f'{self.file_package.__name__}.{module_info.name}'
            module = importlib.import_module(module_name)
            for _, symbol in inspect.getmembers(module, inspect.isclass):
                if symbol.__module__ != module_name:
                    continue
                if not issubclass(symbol, FileHandlerABC):
                    continue
                if not symbol.__name__.endswith('File'):
                    continue
                format_value = getattr(symbol, 'format', None)
                if isinstance(format_value, FileFormat):
                    implemented_formats.add(format_value)

        mapped_formats = set(registry._HANDLER_CLASS_SPECS.keys())
        missing = implemented_formats - mapped_formats
        assert not missing

        for file_format, spec in registry._HANDLER_CLASS_SPECS.items():
            mapped_class = registry._coerce_handler_class(
                registry._import_symbol(spec),
                file_format=file_format,
            )
            assert mapped_class.format == file_format

    def test_get_handler_class_uses_mapped_class(self) -> None:
        """Test mapped formats resolving to concrete handler classes."""
        registry = cast(Any, self.registry_module)
        for file_format, expected_class in self.mapped_class_cases:
            handler_class = registry.get_handler_class(file_format)
            assert handler_class is expected_class

    def test_get_handler_returns_singleton_instance(self) -> None:
        """Test get_handler returning a cached singleton for mapped formats."""
        registry = cast(Any, self.registry_module)
        first = registry.get_handler(self.singleton_format)
        second = registry.get_handler(self.singleton_format)

        assert first is second
        assert isinstance(first, self.singleton_class)

    def test_unstubbed_placeholder_modules_use_module_owned_classes(
        self,
    ) -> None:
        """
        Test owned placeholder modules mapping to their own class symbols.
        """
        registry = cast(Any, self.registry_module)
        for file_format, expected_spec in self.placeholder_spec_cases:
            assert registry._HANDLER_CLASS_SPECS[file_format] == expected_spec


class ScientificDatasetInheritanceContract:
    """
    Reusable contract suite for scientific dataset ABC inheritance checks.

    Subclasses must provide:
    - :attr:`scientific_handlers`
    - :attr:`single_dataset_handlers`
    """

    scientific_handlers: list[type[ScientificDatasetFileHandlerABC]]
    single_dataset_handlers: list[type[SingleDatasetScientificFileHandlerABC]]

    def test_handlers_use_scientific_dataset_abc(self) -> None:
        """Test scientific handlers inheriting ScientificDataset ABC."""
        for handler_cls in self.scientific_handlers:
            assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
            assert handler_cls.dataset_key == 'data'

    def test_single_dataset_handlers_use_single_dataset_scientific_abc(
        self,
    ) -> None:
        """Test single-dataset handlers inheriting the subtype ABC."""
        for handler_cls in self.single_dataset_handlers:
            assert issubclass(
                handler_cls,
                SingleDatasetScientificFileHandlerABC,
            )

    def test_single_dataset_handlers_reject_unknown_dataset_key(self) -> None:
        """Test single-dataset scientific handlers rejecting unknown keys."""
        suffix_by_format = {
            FileFormat.DTA: 'dta',
            FileFormat.NC: 'nc',
            FileFormat.SAV: 'sav',
            FileFormat.XPT: 'xpt',
        }
        for handler_cls in self.single_dataset_handlers:
            handler = handler_cls()
            file_format = cast(FileFormat, handler_cls.format)
            suffix = suffix_by_format.get(file_format, file_format.value)
            assert_single_dataset_rejects_non_default_key(
                handler,
                suffix=suffix,
            )


class ScientificStubDatasetKeysContract:
    """
    Reusable contract suite for stub-backed scientific dataset-key checks.

    Subclasses must provide:
    - :attr:`module_cases`
    """

    module_cases: list[
        tuple[ModuleType, type[ScientificDatasetFileHandlerABC], str]
    ]

    def _assert_stub_not_called(
        self,
        module: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        *,
        operation: Literal['read', 'write'] | None = None,
    ) -> None:
        """Patch module stub operations to fail if they are called."""
        stub_module = cast(Any, module).stub
        if operation in (None, 'read'):
            monkeypatch.setattr(
                stub_module,
                'read',
                lambda *_, **__: (_ for _ in ()).throw(AssertionError),
            )
        if operation in (None, 'write'):
            monkeypatch.setattr(
                stub_module,
                'write',
                lambda *_, **__: (_ for _ in ()).throw(AssertionError),
            )

    def test_dataset_methods_honor_options_dataset_selector(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read_dataset/write_dataset honoring options-based selectors."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(module, monkeypatch)
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.read_dataset(
                    Path('ignored.file'),
                    options=ReadOptions(dataset='unknown'),
                )
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.write_dataset(
                    Path('ignored.file'),
                    [],
                    options=WriteOptions(dataset='unknown'),
                )

    def test_list_datasets_returns_single_default_key(self) -> None:
        """Test list_datasets exposing only the default dataset key."""
        for _, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            assert handler.list_datasets(Path('ignored.file')) == ['data']

    def test_read_dataset_rejects_unknown_key_without_calling_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read_dataset rejecting unknown keys before stub reads."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(module, monkeypatch, operation='read')
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.read_dataset(Path('ignored.file'), dataset='unknown')

    def test_write_dataset_rejects_unknown_key_without_calling_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write_dataset rejecting unknown keys before stub writes."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(
                module,
                monkeypatch,
                operation='write',
            )
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.write_dataset(
                    Path('ignored.file'),
                    [],
                    dataset='unknown',
                )

    def test_read_and_write_options_route_unknown_dataset_to_validation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test option-based selectors following the same validation path."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(module, monkeypatch)
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.read(
                    Path('ignored.file'),
                    options=ReadOptions(dataset='unknown'),
                )
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.write(
                    Path('ignored.file'),
                    [],
                    options=WriteOptions(dataset='unknown'),
                )


class SemiStructuredReadModuleContract:
    """
    Reusable contract suite for semi-structured text module reads.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`sample_read_text`
    - :attr:`expected_read_payload`
    """

    # pylint: disable=unused-argument

    module: ModuleType
    format_name: str
    sample_read_text: str
    expected_read_payload: JSONData

    def setup_read_dependencies(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install optional dependencies needed for read tests."""

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        assert result == self.expected_read_payload

    def test_read_parses_expected_payload(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading expected payload from representative text content."""
        self.setup_read_dependencies(optional_module_stub)
        path = tmp_path / f'data.{self.format_name}'
        path.write_text(self.sample_read_text, encoding='utf-8')

        result = self.module.read(path)

        self.assert_read_contract_result(result)


class SemiStructuredWriteDictModuleContract:
    """
    Reusable contract suite for semi-structured text module dict writes.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`dict_payload`
    """

    # pylint: disable=unused-argument

    module: ModuleType
    format_name: str
    dict_payload: JSONData

    def setup_write_dependencies(
        self,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Install optional dependencies needed for write tests."""

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract expectations."""
        assert path.exists()

    def test_write_accepts_single_dict_payload(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test writing a single dictionary payload."""
        self.setup_write_dependencies(optional_module_stub)
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, self.dict_payload)

        assert written == 1
        self.assert_write_contract_result(path)


class SingleDatasetHandlerContract:
    """
    Reusable contract suite for single-dataset scientific handlers.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`handler_cls`
    - :attr:`format_name`
    """

    module: ModuleType
    handler_cls: type[SingleDatasetScientificFileHandlerABC]
    format_name: str

    @pytest.fixture
    def handler(self) -> SingleDatasetScientificFileHandlerABC:
        """Create a handler instance for contract tests."""
        return self.handler_cls()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test single-dataset scientific class contract."""
        assert issubclass(
            self.handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert self.handler_cls.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: SingleDatasetScientificFileHandlerABC,
    ) -> None:
        """Test non-default dataset keys are rejected."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix=self.format_name,
        )


class SingleDatasetWritableContract(SingleDatasetHandlerContract):
    """
    Reusable suite for writable single-dataset scientific handlers.
    """

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty write payloads return zero and create no file."""
        path = tmp_path / f'data.{self.format_name}'
        assert self.module.write(path, []) == 0
        assert not path.exists()


class SingleDatasetPlaceholderContract(SingleDatasetHandlerContract):
    """
    Reusable suite for placeholder single-dataset scientific handlers.
    """

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test placeholder read/write behavior for module-level wrappers."""
        path = tmp_path / f'data.{self.format_name}'
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            if operation == 'read':
                self.module.read(path)
            else:
                self.module.write(path, [{'id': 1}])


class SniffedDelimitedModuleContract:
    """
    Reusable contract suite for sniffed delimited-text modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    """

    module: ModuleType
    format_name: str

    def _patch_default_sniff(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Patch sniffer behavior to a deterministic CSV dialect."""
        monkeypatch.setattr(
            self.module,
            '_sniff',
            lambda *_args, **_kwargs: (csv.get_dialect('excel'), True),
        )

    def test_read_empty_returns_empty_list(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reading empty input returning an empty list."""
        path = tmp_path / f'empty.{self.format_name}'
        path.write_text('', encoding='utf-8')

        assert self.module.read(path) == []

    def test_write_round_trip_returns_written_count(
        self,
        tmp_path: Path,
    ) -> None:
        """Test write/read round-trip preserving the written row count."""
        sample_records: list[dict[str, object]] = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        ]
        path = tmp_path / f'out.{self.format_name}'

        written = self.module.write(path, sample_records)
        result = self.module.read(path)

        assert written == len(sample_records)
        assert len(result) == len(sample_records)

    def test_read_skips_blank_rows(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test blank rows being ignored during reads."""
        self._patch_default_sniff(monkeypatch)
        path = tmp_path / f'data.{self.format_name}'
        path.write_text(
            'a,b\n\n , \n1,2\n',
            encoding='utf-8',
        )

        assert self.module.read(path) == [{'a': '1', 'b': '2'}]


class StubModuleContract:
    """
    Reusable contract suite for placeholder/stub format modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`handler_cls`
    - :attr:`format_name`
    """

    module: ModuleType
    handler_cls: type[StubFileHandlerABC]
    format_name: str

    def test_handler_inherits_stub_abc(self) -> None:
        """Test handler metadata and inheritance contract."""
        assert issubclass(self.handler_cls, StubFileHandlerABC)
        assert self.handler_cls.format.value == self.format_name

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            self.module,
            format_name=self.format_name,
            operation=cast(Literal['read', 'write'], operation),
            path=tmp_path / f'data.{self.format_name}',
        )


class TextRowModuleContract:
    """
    Reusable contract suite for text/fixed-width row-oriented modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`write_payload`
    - :meth:`prepare_read_case`
    - :meth:`assert_write_contract_result`
    """

    module: ModuleType
    format_name: str
    write_payload: JSONData
    expected_written_count: int = 1

    def prepare_read_case(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> tuple[Path, JSONData]:
        """Prepare and return ``(path, expected_result)`` for read tests."""
        raise NotImplementedError

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract behavior."""
        assert path.exists()

    def test_read_returns_expected_rows(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
    ) -> None:
        """Test reading representative row-oriented input."""
        path, expected = self.prepare_read_case(tmp_path, optional_module_stub)

        assert self.module.read(path) == expected

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        path = tmp_path / f'data.{self.format_name}'
        assert self.module.write(path, []) == 0

    def test_write_rows_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing representative row payloads."""
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_write_contract_result(path)


class WritableSpreadsheetModuleContract:
    """
    Reusable contract suite for writable spreadsheet wrapper modules.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`dependency_hint`
    - :attr:`read_engine` (optional)
    - :attr:`write_engine` (optional)
    """

    module: ModuleType
    format_name: str
    dependency_hint: str
    read_engine: str | None = None
    write_engine: str | None = None

    # pylint: disable=unused-argument

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test read returning row records via pandas."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})
        path = tmp_path / f'data.{self.format_name}'

        result = self.module.read(path)

        assert result == [{'id': 1}]
        assert pandas.read_calls
        call = pandas.read_calls[-1]
        assert call.get('path') == path
        if self.read_engine is not None:
            assert call.get('engine') == self.read_engine

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
    ) -> None:
        """Test read wrapping dependency import errors."""
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader('read_excel'),
        )

        with pytest.raises(ImportError, match=self.dependency_hint):
            self.module.read(tmp_path / f'data.{self.format_name}')

    def test_write_calls_to_excel(
        self,
        tmp_path: Path,
        optional_module_stub: Callable[[dict[str, object]], None],
        make_records_frame: Callable[[list[dict[str, object]]], object],
        make_pandas_stub: Callable[[object], object],
    ) -> None:
        """Test write delegating to DataFrame.to_excel with expected args."""
        frame = make_records_frame([{'id': 1}])
        pandas = cast(Any, make_pandas_stub(frame))
        optional_module_stub({'pandas': pandas})
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, [{'id': 1}])

        assert written == 1
        assert pandas.last_frame is not None
        assert pandas.last_frame.to_excel_calls
        call = pandas.last_frame.to_excel_calls[-1]
        assert call.get('path') == path
        assert call.get('index') is False
        if self.write_engine is not None:
            assert call.get('engine') == self.write_engine

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        assert (
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [],
            )
            == 0
        )

    def test_write_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_writer: Callable[[], object],
    ) -> None:
        """Test write wrapping dependency import errors."""
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match=self.dependency_hint):
            self.module.write(
                tmp_path / f'data.{self.format_name}',
                [{'id': 1}],
            )


class XmlModuleContract:
    """
    Reusable contract suite for XML module read/write behavior.

    Subclasses must provide:
    - :attr:`module`
    - :attr:`format_name`
    - :attr:`root_tag`
    """

    module: ModuleType
    format_name: str
    root_tag: str = 'root'

    def test_write_uses_root_tag_and_read_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test XML write using explicit root tag and readable output."""
        path = tmp_path / f'data.{self.format_name}'

        written = self.module.write(path, [{'id': 1}], root_tag=self.root_tag)

        assert written == 1
        assert f'<{self.root_tag}>' in path.read_text(encoding='utf-8')
        result = self.module.read(path)
        assert self.root_tag in result


# SECTION: CLASSES (STUBS) ============================================== #


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


class PandasModuleStub:
    """
    Minimal pandas-module stub with read and DataFrame factory helpers.

    Parameters
    ----------
    frame : RecordsFrameStub
        Frame object returned by read operations.
    """

    # pylint: disable=invalid-name

    def __init__(
        self,
        frame: RecordsFrameStub,
    ) -> None:
        self._frame = frame
        self.read_calls: list[dict[str, object]] = []
        self.last_frame: RecordsFrameStub | None = None

        def _from_records(
            records: list[dict[str, object]],
        ) -> RecordsFrameStub:
            created = RecordsFrameStub(records)
            self.last_frame = created
            return created

        self.DataFrame = type(
            'DataFrame',
            (),
            {'from_records': staticmethod(_from_records)},
        )

    def _record_read(
        self,
        path: Path,
        **kwargs: object,
    ) -> RecordsFrameStub:
        call: dict[str, object] = {'path': path, **kwargs}
        self.read_calls.append(call)
        return self._frame

    def read_excel(
        self,
        path: Path,
        *,
        engine: str | None = None,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_excel``.

        Parameters
        ----------
        path : Path
            Input path.
        engine : str | None, optional
            Optional engine argument.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        if engine is None:
            return self._record_read(path)
        return self._record_read(path, engine=engine)

    def read_parquet(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_parquet``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)

    def read_feather(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_feather``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)

    def read_orc(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """
        Simulate ``pandas.read_orc``.

        Parameters
        ----------
        path : Path
            Input path.

        Returns
        -------
        RecordsFrameStub
            Simulated DataFrame result.
        """
        return self._record_read(path)


class RecordsFrameStub:
    """
    Minimal frame stub that mimics pandas record/table APIs.

    Parameters
    ----------
    records : list[dict[str, object]]
        In-memory records returned by :meth:`to_dict`.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = list(records)
        self.to_excel_calls: list[dict[str, object]] = []
        self.to_parquet_calls: list[dict[str, object]] = []
        self.to_feather_calls: list[dict[str, object]] = []
        self.to_orc_calls: list[dict[str, object]] = []

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """
        Return record payloads in ``records`` orientation.

        Parameters
        ----------
        orient : str
            Requested output orientation.

        Returns
        -------
        list[dict[str, object]]
            Record payloads.
        """
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
        engine: str | None = None,
    ) -> None:
        """
        Record an Excel write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        engine : str | None, optional
            Optional pandas engine argument.
        """
        call: dict[str, object] = {'path': path, 'index': index}
        if engine is not None:
            call['engine'] = engine
        self.to_excel_calls.append(call)

    def to_feather(
        self,
        path: Path,
    ) -> None:
        """
        Record a feather write call.

        Parameters
        ----------
        path : Path
            Target output path.
        """
        self.to_feather_calls.append({'path': path})

    def to_orc(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """
        Record an ORC write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        """
        self.to_orc_calls.append({'path': path, 'index': index})

    def to_parquet(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """
        Record a parquet write call.

        Parameters
        ----------
        path : Path
            Target output path.
        index : bool
            Whether index persistence was requested.
        """
        self.to_parquet_calls.append({'path': path, 'index': index})


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='make_import_error_reader')
def make_import_error_reader_fixture() -> Callable[[str], object]:
    """
    Build module-like objects with one failing reader method.

    Returns
    -------
    Callable[[str], object]
        Factory that maps a method name to a failing module-like object.
    """
    return make_import_error_reader_module


@pytest.fixture(name='make_import_error_writer')
def make_import_error_writer_fixture() -> Callable[[], object]:
    """
    Build pandas-like module objects with failing write paths.

    Returns
    -------
    Callable[[], object]
        Factory returning a failing module-like object.
    """
    return make_import_error_writer_module


@pytest.fixture(name='make_pandas_stub')
def make_pandas_stub_fixture() -> Callable[
    [RecordsFrameStub],
    PandasModuleStub,
]:
    """
    Build :class:`PandasModuleStub` instances for tests.

    Returns
    -------
    Callable[[RecordsFrameStub], PandasModuleStub]
        pandas module stub factory.
    """
    return PandasModuleStub


@pytest.fixture(name='make_records_frame')
def make_records_frame_fixture() -> Callable[
    [list[dict[str, object]]],
    RecordsFrameStub,
]:
    """
    Build :class:`RecordsFrameStub` instances for tests.

    Returns
    -------
    Callable[[list[dict[str, object]]], RecordsFrameStub]
        Frame factory.
    """
    return RecordsFrameStub


@pytest.fixture(name='optional_module_stub')
def optional_module_stub_fixture() -> Generator[
    Callable[[dict[str, object]], None]
]:
    """
    Install stub modules into the optional import cache.

    Clears the cache for deterministic tests, and restores it afterward.
    """
    cache = import_helpers._MODULE_CACHE  # pylint: disable=protected-access
    original = dict(cache)
    cache.clear()

    def _install(mapping: dict[str, object]) -> None:
        cache.update(mapping)

    try:
        yield _install
    finally:
        cache.clear()
        cache.update(original)
