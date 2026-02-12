"""
:mod:`tests.unit.file.conftest` module.

Shared fixtures and helpers for pytest-based unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

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
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.stub import StubFileHandlerABC
from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: TYPE ALIAS ======================================================= #


# Shared callable used by dependency-stubbing fixtures/contracts.
type HandlerCase = tuple[FileFormat, type[Any]]
type OptionalModuleInstaller = Callable[[dict[str, object]], None]
type Operation = Literal['read', 'write']

# SECTION: INTERNAL FUNCTIONS =============================================== #


def _call_module_operation(
    module: ModuleType,
    *,
    operation: Operation,
    path: Path,
    write_payload: JSONData | None = None,
) -> JSONData | int:
    """Invoke handler ``read``/``write`` without deprecated module wrappers."""
    handler = _module_handler(module)
    if operation == 'read':
        return cast(JSONData, handler.read(path))
    payload = make_payload('list') if write_payload is None else write_payload
    return cast(int, handler.write(path, payload))


def _call_scientific_dataset_operation(
    handler: SingleDatasetScientificFileHandlerABC,
    *,
    operation: Operation,
    path: Path,
    dataset: str,
) -> JSONData | int:
    """Invoke scientific ``read_dataset``/``write_dataset`` by operation."""
    if operation == 'read':
        return cast(JSONData, handler.read_dataset(path, dataset=dataset))
    return cast(int, handler.write_dataset(path, [], dataset=dataset))


def _module_handler(
    module: ModuleType,
) -> Any:
    """Return the singleton handler instance defined by a file module."""
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    return handlers[0]


def _raise_unexpected_dependency_call(
    *args: object,
    **kwargs: object,
) -> object:  # noqa: ARG001
    """Raise when a dependency resolver is called unexpectedly in tests."""
    raise AssertionError('dependency resolver should not be called')


# SECTION: FUNCTIONS ======================================================== #


def assert_single_dataset_rejects_non_default_key(
    handler: SingleDatasetScientificFileHandlerABC,
    *,
    suffix: str,
) -> None:
    """Assert single-dataset scientific handlers reject non-default keys."""
    bad_dataset = 'not_default_dataset'
    path = Path(f'ignored.{suffix}')
    for operation in ('read', 'write'):
        with pytest.raises(ValueError, match='supports only dataset key'):
            _call_scientific_dataset_operation(
                handler,
                operation=cast(Operation, operation),
                path=path,
                dataset=bad_dataset,
            )


def assert_stub_module_operation_raises(
    module: ModuleType,
    *,
    format_name: str,
    operation: Operation,
    path: Path,
    write_payload: JSONData | None = None,
) -> None:
    """Assert one stub module operation raises :class:`NotImplementedError`."""
    with pytest.raises(
        NotImplementedError,
        match=rf'{format_name.upper()} {operation} is not implemented yet',
    ):
        _call_module_operation(
            module,
            operation=operation,
            path=path,
            write_payload=write_payload,
        )


def make_import_error_reader_module(
    method_name: str,
) -> object:
    """Build a module-like object whose reader method raises ImportError."""

    def _fail_reader(
        *args: object,
        **kwargs: object,
    ) -> object:  # noqa: ARG001
        raise ImportError('missing')

    return SimpleNamespace(**{method_name: _fail_reader})


def make_import_error_writer_module() -> object:
    """Build a pandas-like module whose DataFrame writes raise ImportError."""

    # pylint: disable=unused-argument

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

    class _DataFrame:
        """Minimal DataFrame namespace for write-path tests."""

        @staticmethod
        def from_records(
            records: list[dict[str, object]],
        ) -> _FailFrame:  # noqa: ARG002
            return _FailFrame()

    return SimpleNamespace(DataFrame=_DataFrame)


def make_payload(
    kind: Literal['dict', 'list', 'read'],
    **kwargs: object,
) -> JSONData:
    """Build common JSON payload shapes used across test contracts."""
    if (payload := kwargs.get('payload')) is not None:
        return cast(JSONData, payload)

    match kind:
        case 'dict':
            key = cast(str, kwargs.get('key', 'id'))
            return cast(JSONData, {key: kwargs.get('value', 1)})
        case 'list':
            if (records := kwargs.get('records')) is not None:
                return cast(JSONData, records)
            if (record := kwargs.get('record')) is not None:
                return cast(JSONData, [record])
            return cast(JSONData, [make_payload('dict')])
        case _:
            if (result := kwargs.get('result')) is not None:
                return cast(JSONData, result)
            return cast(JSONData, {'ok': bool(kwargs.get('ok', True))})


def patch_dependency_resolver_unreachable(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    *,
    resolver_name: str = 'get_dependency',
) -> None:
    """Patch one dependency resolver to raise if a test triggers it."""
    monkeypatch.setattr(
        module,
        resolver_name,
        _raise_unexpected_dependency_call,
    )


def patch_dependency_resolver_value(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    *,
    resolver_name: str = 'get_dependency',
    value: object,
) -> None:
    """Patch one dependency resolver to return a deterministic value."""

    def _return_value(
        *args: object,
        **kwargs: object,
    ) -> object:  # noqa: ARG001
        return value

    monkeypatch.setattr(module, resolver_name, _return_value)


# SECTION: CLASSES (PRIMARY MIXINS) ========================================= #


class PathMixin:
    """Shared path helper for format-aligned contract classes."""

    format_name: str
    module: ModuleType

    @property
    def module_handler(self) -> Any:
        """Return the module's singleton handler instance."""
        return _module_handler(self.module)

    def format_path(
        self,
        tmp_path: Path,
        *,
        stem: str = 'data',
    ) -> Path:
        """Build a deterministic format-specific path."""
        return tmp_path / f'{stem}.{self.format_name}'


# SECTION: CLASSES (SECONDARY MIXINS) ======================================= #


class EmptyWriteReturnsZeroMixin(PathMixin):
    """
    Shared mixin for contracts where empty writes should return ``0``.
    """

    module: ModuleType
    assert_file_not_created_on_empty_write: bool = False

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing empty payloads returning zero."""
        path = self.format_path(tmp_path)
        assert self.module_handler.write(path, []) == 0
        if self.assert_file_not_created_on_empty_write:
            assert not path.exists()


class DelimitedReadWriteMixin(PathMixin):
    """
    Parametrized mixin for delimiter-forwarding read/write wrappers.
    """

    module: ModuleType
    delimiter: str
    sample_rows: JSONData

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
            return cast(
                list[dict[str, object]],
                make_payload('list', record={'ok': True}),
            )

        monkeypatch.setattr(self.module, 'read_delimited', _read_delimited)

        result = self.module_handler.read(self.format_path(tmp_path))

        assert result == make_payload('list', record={'ok': True})
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

        written = self.module_handler.write(
            self.format_path(tmp_path),
            self.sample_rows,
        )

        assert written == 1
        assert calls['delimiter'] == self.delimiter
        assert calls['format_name'] == self.format_name.upper()


class DelimitedTextRowsMixin(EmptyWriteReturnsZeroMixin):
    """
    Parametrized mixin for text/fixed-width row-oriented modules.
    """

    module: ModuleType
    write_payload: JSONData
    expected_written_count: int = 1

    def prepare_read_case(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
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
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading representative row-oriented input."""
        path, expected = self.prepare_read_case(tmp_path, optional_module_stub)

        assert self.module_handler.read(path) == expected

    def test_write_rows_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writing representative row payloads."""
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == self.expected_written_count
        self.assert_write_contract_result(path)


class ScientificReadOnlyUnknownDatasetMixin(PathMixin):
    """
    Parametrized mixin for read-only scientific unknown-dataset checks.
    """

    handler_cls: type[ScientificDatasetFileHandlerABC]
    unknown_dataset_error_pattern: str

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install stubs needed for unknown-dataset contract checks."""
        _ = tmp_path
        _ = monkeypatch
        _ = optional_module_stub

    @pytest.fixture
    def handler(self) -> ScientificDatasetFileHandlerABC:
        """Create a handler instance for read-only scientific contracts."""
        return self.handler_cls()

    def test_read_dataset_rejects_unknown_dataset(
        self,
        handler: ScientificDatasetFileHandlerABC,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
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
            handler.read_dataset(self.format_path(tmp_path), dataset='unknown')

    def test_read_rejects_unknown_dataset_from_options(
        self,
        handler: ScientificDatasetFileHandlerABC,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
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
            handler.read(
                self.format_path(tmp_path),
                options=ReadOptions(dataset='unknown'),
            )


class ReadOnlyWriteGuardMixin(PathMixin):
    """
    Shared mixin for read-only handlers rejecting module-level writes.
    """

    module: ModuleType
    read_only_error_pattern: str = 'read-only'

    def test_write_not_supported(
        self,
        tmp_path: Path,
    ) -> None:
        """Test read-only handlers rejecting writes."""
        with pytest.raises(RuntimeError, match=self.read_only_error_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                make_payload('list'),
            )


class SpreadsheetReadImportErrorMixin(PathMixin):
    """
    Shared mixin for spreadsheet read dependency error behavior.
    """

    module: ModuleType
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
            self.module_handler.read(self.format_path(tmp_path))


class SemiStructuredReadMixin(PathMixin):
    """
    Parametrized read contract mixin for semi-structured modules.
    """

    # pylint: disable=unused-argument

    module: ModuleType
    sample_read_text: str

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        raise NotImplementedError

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for read tests."""
        raise NotImplementedError

    def test_read_parses_expected_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading expected payload from representative text content."""
        self.setup_read_dependencies(optional_module_stub)
        path = self.format_path(tmp_path)
        path.write_text(self.sample_read_text, encoding='utf-8')

        result = self.module_handler.read(path)

        self.assert_read_contract_result(result)


class SemiStructuredWriteDictMixin(PathMixin):
    """
    Parametrized write contract mixin for semi-structured modules.
    """

    module: ModuleType
    dict_payload: JSONData

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for write tests."""
        raise NotImplementedError

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract behavior."""
        raise NotImplementedError

    def test_write_accepts_single_dict_payload(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing a single dictionary payload."""
        self.setup_write_dependencies(optional_module_stub)
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.dict_payload)

        assert written == 1
        self.assert_write_contract_result(path)


class ScientificSingleDatasetHandlerMixin:
    """
    Parametrized mixin for single-dataset scientific handler behavior.
    """

    handler_cls: type[SingleDatasetScientificFileHandlerABC]
    dataset_key: str
    format_name: str

    @pytest.fixture
    def handler(self) -> SingleDatasetScientificFileHandlerABC:
        """Create a handler instance for contract tests."""
        return self.handler_cls()

    def test_uses_single_dataset_scientific_abc(
        self,
    ) -> None:
        """Test single-dataset scientific class contract."""
        assert issubclass(
            self.handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert self.handler_cls.dataset_key == self.dataset_key

    def test_rejects_non_default_dataset_key(
        self,
        handler: SingleDatasetScientificFileHandlerABC,
    ) -> None:
        """Test non-default dataset keys are rejected."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix=self.format_name,
        )


class SpreadsheetWritableMixin(EmptyWriteReturnsZeroMixin):
    """
    Parametrized mixin for writable spreadsheet module contracts.
    """

    module: ModuleType
    dependency_hint: str
    read_engine: str | None
    write_engine: str | None

    # pylint: disable=unused-argument

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test read returning row records via pandas."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        optional_module_stub({'pandas': pandas})
        path = self.format_path(tmp_path)

        result = self.module_handler.read(path)

        assert result == make_payload('list')
        assert pandas.read_calls
        call = pandas.read_calls[-1]
        assert call.get('path') == path
        if self.read_engine is not None:
            assert call.get('engine') == self.read_engine

    def test_write_calls_to_excel(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test write delegating to DataFrame.to_excel with expected args."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        optional_module_stub({'pandas': pandas})
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, make_payload('list'))

        assert written == 1
        assert pandas.last_frame is not None
        frame_stub = cast(RecordsFrameStub, pandas.last_frame)
        assert frame_stub.to_excel_calls
        call = frame_stub.to_excel_calls[-1]
        assert call.get('path') == path
        assert call.get('index') is False
        if self.write_engine is not None:
            assert call.get('engine') == self.write_engine

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
            self.module_handler.write(
                self.format_path(tmp_path),
                make_payload('list'),
            )


# SECTION: CLASSES (BASES) ================================================== #


class DelimitedCategoryContractBase(PathMixin):
    """
    Shared base contract for delimited/text category modules.
    """

    module: ModuleType
    sample_rows: JSONData = make_payload('list')


class ScientificCategoryContractBase(PathMixin):
    """
    Shared base contract for scientific dataset handlers/modules.
    """

    module: ModuleType
    dataset_key: str = 'data'


class SpreadsheetCategoryContractBase(PathMixin):
    """
    Shared base contract for spreadsheet format handlers.
    """

    module: ModuleType
    dependency_hint: str
    read_engine: str | None = None
    write_engine: str | None = None


class SemiStructuredCategoryContractBase(PathMixin):
    """
    Shared base contract for semi-structured text modules.
    """

    # pylint: disable=unused-argument

    module: ModuleType
    sample_read_text: str = ''
    expected_read_payload: JSONData = make_payload('dict')
    dict_payload: JSONData = make_payload('dict')

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for read tests."""

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for write tests."""

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        assert result == self.expected_read_payload

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract expectations."""
        assert path.exists()


# SECTION: CLASSES (CONTRACTS) ============================================== #


class ArchiveWrapperCoreDispatchModuleContract(PathMixin):
    """Reusable contract suite for archive wrappers using core dispatch."""

    module: ModuleType
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

    module: ModuleType
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

    module: ModuleType
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

    module: ModuleType
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


class DelimitedModuleContract(
    DelimitedCategoryContractBase,
    DelimitedReadWriteMixin,
):
    """Reusable contract suite for standard delimited wrapper modules."""


class EmbeddedDatabaseModuleContract(EmptyWriteReturnsZeroMixin):
    """Reusable contract suite for embedded database wrapper modules."""

    # pylint: disable=unused-argument

    module: ModuleType
    multi_table_error_pattern: str

    def build_empty_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Create an empty database fixture path for read tests."""
        raise NotImplementedError

    def build_multi_table_database_path(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> Path:
        """Create a multi-table database fixture path for read tests."""
        raise NotImplementedError

    def test_read_returns_empty_when_no_tables(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading empty embedded databases returning no records."""
        path = self.build_empty_database_path(tmp_path, optional_module_stub)
        assert self.module_handler.read(path) == []

    def test_read_raises_on_multiple_tables(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read rejecting ambiguous multi-table databases."""
        path = self.build_multi_table_database_path(
            tmp_path,
            optional_module_stub,
        )
        with pytest.raises(ValueError, match=self.multi_table_error_pattern):
            self.module_handler.read(path)


class PandasColumnarModuleContract(EmptyWriteReturnsZeroMixin):
    """Reusable contract suite for pandas-backed columnar format modules."""

    module: ModuleType
    read_method_name: str
    write_calls_attr: str
    write_uses_index: bool = False
    requires_pyarrow: bool = False
    read_error_pattern: str = 'missing'
    write_error_pattern: str = 'missing'

    def _install_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
        *,
        pandas: object | None = None,
    ) -> None:
        """Install optional stubs required by columnar contract tests."""
        mapping: dict[str, object] = {}
        if pandas is not None:
            mapping['pandas'] = pandas
        if self.requires_pyarrow:
            mapping['pyarrow'] = object()
        if mapping:
            optional_module_stub(mapping)

    def test_read_returns_records(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test read returning row records via pandas."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        self._install_dependencies(optional_module_stub, pandas=pandas)

        result = self.module_handler.read(self.format_path(tmp_path))

        assert result == make_payload('list')
        assert pandas.read_calls

    def test_write_calls_expected_table_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
        make_records_frame: Callable[
            [list[dict[str, object]]],
            RecordsFrameStub,
        ],
        make_pandas_stub: Callable[[RecordsFrameStub], PandasModuleStub],
    ) -> None:
        """Test write calling the expected DataFrame writer method."""
        frame = make_records_frame([{'id': 1}])
        pandas = make_pandas_stub(frame)
        self._install_dependencies(optional_module_stub, pandas=pandas)
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, make_payload('list'))

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

    def test_read_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test read dependency failures raising :class:`ImportError`."""
        self._install_dependencies(optional_module_stub)
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_reader(self.read_method_name),
        )

        with pytest.raises(ImportError, match=self.read_error_pattern):
            self.module_handler.read(self.format_path(tmp_path))

    def test_write_import_error_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_writer: Callable[[], object],
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test write dependency failures raising :class:`ImportError`."""
        self._install_dependencies(optional_module_stub)
        monkeypatch.setattr(
            self.module,
            'get_pandas',
            lambda *_: make_import_error_writer(),
        )

        with pytest.raises(ImportError, match=self.write_error_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                make_payload('list'),
            )


class PyarrowMissingDependencyMixin(PathMixin):
    """
    Shared mixin for pyarrow-gated read/write dependency checks.
    """

    module: ModuleType
    missing_dependency_pattern: str = 'missing pyarrow'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_operations_raise_when_pyarrow_missing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        operation: Operation,
    ) -> None:
        """Test read/write failing when pyarrow dependency resolution fails."""

        def _missing(*_args: object, **_kwargs: object) -> object:
            raise ImportError(self.missing_dependency_pattern)

        monkeypatch.setattr(self.module, 'get_dependency', _missing)
        path = self.format_path(tmp_path)

        with pytest.raises(ImportError, match=self.missing_dependency_pattern):
            _call_module_operation(
                self.module,
                operation=operation,
                path=path,
            )


class PyarrowGatedPandasColumnarModuleContract(
    PyarrowMissingDependencyMixin,
    PandasColumnarModuleContract,
):
    """
    Reusable suite for pandas-backed columnar modules gated by pyarrow.
    """

    requires_pyarrow = True
    missing_dependency_pattern: str = 'missing pyarrow'


class RDataModuleContract(PathMixin):
    """Reusable contract suite for R-data wrapper modules (RDA/RDS)."""

    module: ModuleType
    writer_missing_pattern: str
    write_payload: JSONData = make_payload('list')

    def _install_optional_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
        *,
        pyreadr_stub: object,
    ) -> None:
        """Install module stubs required by R-data contract tests."""
        optional_module_stub(
            {
                'pyreadr': pyreadr_stub,
                'pandas': self.build_pandas_stub(),
            },
        )

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
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading empty R-data results returning an empty list."""
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub({}),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == []

    def test_read_single_value_coerces_to_records(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading one R object coercing to JSON records."""
        frame = self.build_frame([{'id': 1}])
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub({'data': frame}),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == [
            {'id': 1},
        ]

    def test_read_multiple_values_returns_mapping(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading multiple R objects returning key-mapped payloads."""
        result: dict[str, object] = {'one': {'id': 1}, 'two': [{'id': 2}]}
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub(result),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == result

    def test_write_raises_when_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing failing when pyreadr writer methods are unavailable."""
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_reader_only_stub(),
        )

        with pytest.raises(ImportError, match=self.writer_missing_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                self.write_payload,
            )

    def test_write_happy_path_uses_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing delegating to pyreadr writer methods."""
        pyreadr = self.build_pyreadr_stub({})
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == 1
        self.assert_write_success(pyreadr, path)


class ReadOnlyScientificDatasetModuleContract(
    ScientificCategoryContractBase,
    ScientificReadOnlyUnknownDatasetMixin,
    ReadOnlyWriteGuardMixin,
):
    """
    Reusable contract suite for read-only scientific dataset handlers.
    """


class SemiStructuredReadModuleContract(
    SemiStructuredCategoryContractBase,
    SemiStructuredReadMixin,
):
    """
    Reusable read contract suite for semi-structured text modules.
    """


class SemiStructuredWriteDictModuleContract(
    SemiStructuredCategoryContractBase,
    SemiStructuredWriteDictMixin,
):
    """
    Reusable write contract suite for semi-structured text modules.
    """


class SingleDatasetHandlerContract(
    ScientificCategoryContractBase,
    ScientificSingleDatasetHandlerMixin,
):
    """Reusable contract suite for single-dataset scientific handlers."""


class SingleDatasetWritableContract(
    EmptyWriteReturnsZeroMixin,
    SingleDatasetHandlerContract,
):
    """
    Reusable suite for writable single-dataset scientific handlers.
    """

    assert_file_not_created_on_empty_write = True


class SingleDatasetPlaceholderContract(SingleDatasetHandlerContract):
    """
    Reusable suite for placeholder single-dataset scientific handlers.
    """

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: Operation,
    ) -> None:
        """Test placeholder read/write behavior for module-level wrappers."""
        path = self.format_path(tmp_path)
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            _call_module_operation(
                self.module,
                operation=operation,
                path=path,
            )


class StubModuleContract(PathMixin):
    """Reusable contract suite for placeholder/stub format modules."""

    module: ModuleType
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


class TextRowModuleContract(
    DelimitedCategoryContractBase,
    DelimitedTextRowsMixin,
):
    """
    Reusable contract suite for text/fixed-width row-oriented modules.
    """


class WritableSpreadsheetModuleContract(
    SpreadsheetCategoryContractBase,
    SpreadsheetReadImportErrorMixin,
    SpreadsheetWritableMixin,
):
    """
    Reusable contract suite for writable spreadsheet wrapper modules.
    """


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


class DictRecordsFrameStub:
    """
    Minimal records-only frame stub shared by scientific format tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        records: list[dict[str, object]],
    ) -> None:
        self._records = list(records)
        self.columns = list(records[0].keys()) if records else []

    def __getitem__(self, key: str) -> list[object]:
        """Return one column as a list of row values."""
        return [row.get(key) for row in self._records]

    def drop(
        self,
        columns: list[str],
    ) -> DictRecordsFrameStub:
        """Return a new frame with selected columns removed."""
        return DictRecordsFrameStub(
            [
                {k: v for k, v in row.items() if k not in columns}
                for row in self._records
            ],
        )

    def reset_index(self) -> DictRecordsFrameStub:
        """Return self for simple reset-index test flows."""
        return self

    def to_dict(
        self,
        *,
        orient: str,
    ) -> list[dict[str, object]]:  # noqa: ARG002
        """Return record payloads in ``records`` orientation."""
        return list(self._records)

    @staticmethod
    def from_records(
        records: list[dict[str, object]],
    ) -> DictRecordsFrameStub:
        """Construct a frame from row records."""
        return DictRecordsFrameStub(records)


class PandasModuleStub:
    """Minimal pandas-module stub with reader and DataFrame helpers."""

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
        """Simulate ``pandas.read_excel``."""
        if engine is None:
            return self._record_read(path)
        return self._record_read(path, engine=engine)

    def _read_table(
        self,
        path: Path,
    ) -> RecordsFrameStub:
        """Simulate table-like pandas readers returning record frames."""
        return self._record_read(path)

    read_parquet = _read_table
    read_feather = _read_table
    read_orc = _read_table


class PandasReadSasStub:
    """
    Minimal pandas stub for ``read_sas``-based handlers.
    """

    def __init__(
        self,
        frame: DictRecordsFrameStub,
        *,
        fail_on_format_kwarg: bool = False,
    ) -> None:
        self._frame = frame
        self._fail_on_format_kwarg = fail_on_format_kwarg
        self.read_calls: list[dict[str, object]] = []

    def assert_fallback_read_calls(
        self,
        path: Path,
        *,
        format_name: str,
    ) -> None:
        """Assert fallback behavior after a rejected format keyword read."""
        assert self.read_calls == [
            {'path': path, 'format': format_name},
            {'path': path},
        ]

    def assert_single_read_call(
        self,
        path: Path,
        *,
        format_name: str | None = None,
    ) -> None:
        """
        Assert one pandas :meth:`read_sas` call with optional format hint.
        """
        expected: dict[str, object] = {'path': path}
        if format_name is not None:
            expected['format'] = format_name
        assert self.read_calls == [expected]

    def read_sas(
        self,
        path: Path,
        **kwargs: object,
    ) -> DictRecordsFrameStub:
        """Simulate pandas.read_sas with optional format rejection."""
        self.read_calls.append({'path': path, **kwargs})
        if self._fail_on_format_kwarg and 'format' in kwargs:
            raise TypeError('format not supported')
        return self._frame


class PyreadrStub:
    """
    Shared pyreadr-style stub for RDA/RDS tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        result: dict[str, object],
    ) -> None:
        self._result = result
        self.write_rds_calls: list[tuple[str, object]] = []
        self.write_rdata_calls: list[
            tuple[str, object, dict[str, object]]
        ] = []

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:
        """Return configured R object mapping."""
        return dict(self._result)

    def write_rds(
        self,
        path: str,
        frame: object,
    ) -> None:
        """Record one RDS write call."""
        self.write_rds_calls.append((path, frame))

    def write_rdata(
        self,
        path: str,
        frame: object,
        **kwargs: object,
    ) -> None:
        """Record one RDA write call."""
        self.write_rdata_calls.append((path, frame, dict(kwargs)))


class PyreadstatTabularStub:
    """
    Configurable pyreadstat-style stub for SAV/XPT tabular handlers.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        *,
        frame: DictRecordsFrameStub | None = None,
        read_method_name: str | None = None,
        write_method_name: str | None = None,
    ) -> None:
        self._frame = frame if frame is not None else DictRecordsFrameStub([])
        self._read_method_name = read_method_name
        self._write_method_name = write_method_name
        self.read_calls: list[str] = []
        self.write_calls: list[tuple[object, str]] = []

    def __getattr__(
        self,
        name: str,
    ) -> object:
        if name == self._read_method_name:
            return self._read_method
        if name == self._write_method_name:
            return self._write_method
        raise AttributeError(name)

    def _read_method(
        self,
        path: str,
    ) -> tuple[DictRecordsFrameStub, object]:
        self.read_calls.append(path)
        return self._frame, object()

    def _write_method(
        self,
        frame: object,
        path: str,
    ) -> None:
        self.write_calls.append((frame, path))

    def assert_single_read_path(
        self,
        path: Path,
    ) -> None:
        """Assert one pyreadstat read call using the provided path."""
        assert self.read_calls == [str(path)]

    def assert_last_write_path(
        self,
        path: Path,
    ) -> None:
        """Assert the most recent pyreadstat write call path."""
        assert self.write_calls
        _, write_path = self.write_calls[-1]
        assert write_path == str(path)


class RDataPandasStub:
    """
    Minimal pandas stub for R-data tests using record-frame conversion.
    """

    DataFrame = DictRecordsFrameStub


class ContextManagerSelfMixin:
    """
    Tiny mixin for stubs that act as context managers returning ``self``.
    """

    def __enter__(self) -> ContextManagerSelfMixin:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None


class RDataNoWriterStub:
    """
    Minimal pyreadr-like stub exposing only ``read_r``.
    """

    # pylint: disable=unused-argument

    def read_r(
        self,
        path: str,
    ) -> dict[str, object]:
        """Return an empty mapping for reader-only flows."""
        return {}


class RecordsFrameStub:
    """Minimal frame stub that mimics pandas record/table APIs."""

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
        """Return record payloads in ``records`` orientation."""
        return list(self._records)

    def to_excel(
        self,
        path: Path,
        *,
        index: bool,
        engine: str | None = None,
    ) -> None:
        """Record an Excel write call."""
        kwargs: dict[str, object] = {'index': index}
        if engine is not None:
            kwargs['engine'] = engine
        self._append_write_call(self.to_excel_calls, path, **kwargs)

    def to_feather(
        self,
        path: Path,
    ) -> None:
        """Record a feather write call."""
        self._append_write_call(self.to_feather_calls, path)

    def to_orc(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Record an ORC write call."""
        self._append_write_call(self.to_orc_calls, path, index=index)

    def to_parquet(
        self,
        path: Path,
        *,
        index: bool,
    ) -> None:
        """Record a parquet write call."""
        self._append_write_call(self.to_parquet_calls, path, index=index)

    @staticmethod
    def _append_write_call(
        calls: list[dict[str, object]],
        path: Path,
        **kwargs: object,
    ) -> None:
        """Append one writer-call record with path and keyword payload."""
        calls.append({'path': path, **kwargs})


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='make_import_error_reader')
def make_import_error_reader_fixture() -> Callable[[str], object]:
    """Return factory for module-like objects with failing reader methods."""
    return make_import_error_reader_module


@pytest.fixture(name='make_import_error_writer')
def make_import_error_writer_fixture() -> Callable[[], object]:
    """Return factory for pandas-like objects with failing write paths."""
    return make_import_error_writer_module


@pytest.fixture(name='make_pandas_stub')
def make_pandas_stub_fixture() -> Callable[
    [RecordsFrameStub],
    PandasModuleStub,
]:
    """Return factory for :class:`PandasModuleStub` test doubles."""
    return PandasModuleStub


@pytest.fixture(name='make_records_frame')
def make_records_frame_fixture() -> Callable[
    [list[dict[str, object]]],
    RecordsFrameStub,
]:
    """Return factory for :class:`RecordsFrameStub` test doubles."""
    return RecordsFrameStub


@pytest.fixture(name='optional_module_stub')
def optional_module_stub_fixture() -> Generator[OptionalModuleInstaller]:
    """Install optional dependency stubs and restore import cache afterward."""
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
