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

import math
import numbers
from collections.abc import Callable
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from typing import Any
from typing import Literal
from typing import cast

import pytest

import etlplus.file._imports as import_helpers
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
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


class StubModuleContract:
    """
    Reusable contract suite for placeholder/stub format modules.

    Subclasses must provide:
    - ``module``
    - ``handler_cls``
    - ``format_name``
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


class BinaryCodecModuleContract:
    """
    Reusable contract suite for binary codec wrapper modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
    - ``dependency_name``
    - ``reader_method_name``
    - ``writer_method_name``
    - ``reader_kwargs``
    - ``writer_kwargs``
    - ``loaded_result``
    - ``emitted_bytes``
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


class BinaryKeyedPayloadModuleContract:
    """
    Reusable contract suite for keyed binary payload wrapper modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
    - ``payload_key``
    - ``sample_payload_value``
    - ``expected_bytes``
    - ``invalid_payload``
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
    - ``module``
    - ``format_name``
    - ``delimiter``
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


class PandasColumnarModuleContract:
    """
    Reusable contract suite for pandas-backed columnar format modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
    - ``read_method_name``
    - ``write_calls_attr``
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
        assert self.module.write(
            tmp_path / f'data.{self.format_name}',
            [],
        ) == 0

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


class PyarrowGateOnlyModuleContract:
    """
    Reusable contract suite for pyarrow-gated IPC-style modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
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


class ReadOnlySpreadsheetModuleContract:
    """
    Reusable contract suite for read-only spreadsheet wrapper modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
    - ``dependency_hint``
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


class SingleDatasetHandlerContract:
    """
    Reusable contract suite for single-dataset scientific handlers.

    Subclasses must provide:
    - ``module``
    - ``handler_cls``
    - ``format_name``
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


class WritableSpreadsheetModuleContract:
    """
    Reusable contract suite for writable spreadsheet wrapper modules.

    Subclasses must provide:
    - ``module``
    - ``format_name``
    - ``dependency_hint``
    - ``read_engine`` (optional)
    - ``write_engine`` (optional)
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
        assert self.module.write(
            tmp_path / f'data.{self.format_name}',
            [],
        ) == 0

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


# SECTION: CLASSES (STUBS) ============================================== #


class BinaryCodecStub:
    """
    Generic codec stub for binary serialization module tests.

    Supports configurable reader/writer method names to cover modules like
    ``msgpack`` and ``cbor2`` with one reusable implementation.
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
