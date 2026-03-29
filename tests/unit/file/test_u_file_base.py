"""
:mod:`tests.unit.file.test_u_file_base` module.

Unit tests for :mod:`etlplus.file.base`.
"""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import NoReturn
from typing import cast

import pytest

from etlplus.file import FileFormat
from etlplus.file import _core as core_mod
from etlplus.file._stub_categories import StubSingleDatasetScientificFileHandlerABC
from etlplus.file.base import BoundFileHandler
from etlplus.file.base import DelimitedTextFileHandlerABC
from etlplus.file.base import FileHandlerABC
from etlplus.file.base import ReadOnlyFileHandlerABC
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from etlplus.file.base import TextFixedWidthFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.csv import CsvFile
from etlplus.file.dat import DatFile
from etlplus.file.dta import DtaFile
from etlplus.file.fwf import FwfFile
from etlplus.file.json import JsonFile
from etlplus.file.mat import MatFile
from etlplus.file.nc import NcFile
from etlplus.file.ods import OdsFile
from etlplus.file.psv import PsvFile
from etlplus.file.rda import RdaFile
from etlplus.file.rds import RdsFile
from etlplus.file.sav import SavFile
from etlplus.file.sqlite import SqliteFile
from etlplus.file.sylk import SylkFile
from etlplus.file.tab import TabFile
from etlplus.file.tsv import TsvFile
from etlplus.file.xls import XlsFile
from etlplus.file.xlsm import XlsmFile
from etlplus.file.xlsx import XlsxFile
from etlplus.file.xpt import XptFile
from etlplus.file.zip import ZipFile
from etlplus.file.zsav import ZsavFile
from etlplus.storage import _http as http_storage_mod
from etlplus.utils._types import JSONData
from etlplus.utils._types import JSONList

from .pytest_file_contract_utils import assert_single_dataset_rejects_non_default_key

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


_NO_DEFAULT: object = object()


class _FakeHttpResponse:
    """Minimal HTTP response double for bound-handler tests."""

    def __init__(
        self,
        *,
        status_code: int,
        payload: bytes = b'',
    ) -> None:
        self.status_code = status_code
        self.content = payload

    def close(self) -> None:
        """Close the response without side effects."""

    def raise_for_status(self) -> None:
        """Raise one error for non-successful response codes."""
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class _FakeHttpSession:
    """Minimal HTTP session double for bound-handler tests."""

    def __init__(
        self,
        *,
        payload: bytes = b'',
    ) -> None:
        self.calls: list[tuple[str, str, bool]] = []
        self.payload = payload

    def close(self) -> None:
        """Close the fake session without side effects."""

    def get(self, url: str, **kwargs: Any) -> _FakeHttpResponse:
        """Return one fake GET response and capture call metadata."""
        self.calls.append(('get', url, bool(kwargs.get('stream', False))))
        return _FakeHttpResponse(status_code=200, payload=self.payload)

    def head(self, url: str, **kwargs: Any) -> _FakeHttpResponse:
        """Return one fake HEAD response and capture call metadata."""
        self.calls.append(
            ('head', url, bool(kwargs.get('allow_redirects', False))),
        )
        return _FakeHttpResponse(status_code=200)


@dataclass(slots=True, frozen=True)
class _OptionHelperCase:
    """Parameterized case for option-helper contract assertions."""

    id: str
    handler_cls: type[object]
    method_name: str
    read_options: ReadOptions
    write_options: WriteOptions
    baseline: object
    read_expected: object
    write_expected: object
    read_default: object = _NO_DEFAULT
    write_default: object = _NO_DEFAULT


def _raise_read_only_write(format_name: FileFormat) -> NoReturn:
    """Raise the canonical read-only write error for one format."""
    raise RuntimeError(
        f'{format_name.value.upper()} is read-only and does not support '
        'write operations',
    )


_DELIMITED_HANDLER_CLASSES = (
    CsvFile,
    DatFile,
    PsvFile,
    TabFile,
    TsvFile,
    FwfFile,
)
_EMBEDDED_DB_HANDLER_CLASSES = (SqliteFile,)
_SCIENTIFIC_HANDLER_CLASSES = (
    DtaFile,
    NcFile,
    RdaFile,
    RdsFile,
    SavFile,
    XptFile,
)
_SCIENTIFIC_STUB_HANDLER_CLASSES = (
    MatFile,
    SylkFile,
    ZsavFile,
)
_SINGLE_DATASET_HANDLER_CLASSES = (
    DtaFile,
    NcFile,
    SavFile,
    XptFile,
)
_SPREADSHEET_HANDLER_CLASSES = (
    XlsFile,
    XlsxFile,
    XlsmFile,
    OdsFile,
)
_NAMING_METHOD_CASES = (
    (_DELIMITED_HANDLER_CLASSES, 'read_rows', 'write_rows'),
    (_EMBEDDED_DB_HANDLER_CLASSES, 'read_table', 'write_table'),
    (_SCIENTIFIC_HANDLER_CLASSES, 'read_dataset', 'write_dataset'),
    (_SPREADSHEET_HANDLER_CLASSES, 'read_sheet', 'write_sheet'),
)


class _RowReadWriteMixin:
    """Provide shared row-oriented read/write glue for contract stubs."""

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:
        """Provide a read implementation delegating to row-level methods."""
        handler = cast(
            DelimitedTextFileHandlerABC | TextFixedWidthFileHandlerABC,
            self,
        )
        return handler.read_rows(path, options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """Provide a write implementation delegating to row-level methods."""
        handler = cast(
            DelimitedTextFileHandlerABC | TextFixedWidthFileHandlerABC,
            self,
        )
        rows: JSONList = data if isinstance(data, list) else [data]
        return handler.write_rows(path, rows, options=options)


class _DelimitedStub(_RowReadWriteMixin, DelimitedTextFileHandlerABC):
    """Concrete delimited handler used for abstract contract tests."""

    format = FileFormat.CSV
    delimiter = ','

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:  # noqa: ARG002
        return [{'id': 1}]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        return len(rows)


class _IncompleteDelimited(_RowReadWriteMixin, DelimitedTextFileHandlerABC):
    """Incomplete delimited handler used for abstract-method checks."""

    format = FileFormat.CSV
    delimiter = ','


class _IncompleteTextFixedWidth(
    _RowReadWriteMixin,
    TextFixedWidthFileHandlerABC,
):
    """Incomplete text/fixed-width handler used for abstract checks."""

    format = FileFormat.TXT


class _TextFixedWidthStub(_RowReadWriteMixin, TextFixedWidthFileHandlerABC):
    """Concrete text/fixed-width handler used for abstract contract tests."""

    format = FileFormat.TXT

    def read_rows(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONList:  # noqa: ARG002
        return [{'text': 'ok'}]

    def write_rows(
        self,
        path: Path,
        rows: JSONList,
        *,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        return len(rows)


class _ReadOnlyScientificStub(
    ReadOnlyFileHandlerABC,
    ScientificDatasetFileHandlerABC,
):
    """Concrete read-only scientific handler for base-contract tests."""

    format = FileFormat.HDF5
    dataset_key = 'data'

    def list_datasets(
        self,
        path: Path,
    ) -> list[str]:  # noqa: ARG002
        return [self.dataset_key]

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:  # noqa: ARG002
        return []

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        _raise_read_only_write(self.format)


class _ReadOnlySingleScientificStub(
    ReadOnlyFileHandlerABC,
    SingleDatasetScientificFileHandlerABC,
):
    """Concrete read-only single-dataset handler for base-contract tests."""

    format = FileFormat.SAS7BDAT
    dataset_key = 'data'

    def read_dataset(
        self,
        path: Path,
        *,
        dataset: str | None = None,
        options: ReadOptions | None = None,
    ) -> JSONData:  # noqa: ARG002
        self.resolve_single_dataset(dataset, options=options)
        return []

    def write_dataset(
        self,
        path: Path,
        data: JSONData,
        *,
        dataset: str | None = None,
        options: WriteOptions | None = None,
    ) -> int:  # noqa: ARG002
        self.resolve_single_dataset(dataset, options=options)
        _raise_read_only_write(self.format)


_OPTION_HELPER_CASES = (
    _OptionHelperCase(
        id='delimiter',
        handler_cls=_DelimitedStub,
        method_name='delimiter_from_options',
        read_options=ReadOptions(extras={'delimiter': '|'}),
        write_options=WriteOptions(extras={'delimiter': '\t'}),
        baseline=',',
        read_expected='|',
        write_expected='\t',
        read_default=';',
        write_default=':',
    ),
    _OptionHelperCase(
        id='encoding',
        handler_cls=XlsFile,
        method_name='encoding_from_options',
        read_options=ReadOptions(encoding='latin-1'),
        write_options=WriteOptions(encoding='utf-16'),
        baseline='utf-8',
        read_expected='latin-1',
        write_expected='utf-16',
        read_default='utf-16',
        write_default='ascii',
    ),
    _OptionHelperCase(
        id='inner_name',
        handler_cls=ZipFile,
        method_name='inner_name_from_options',
        read_options=ReadOptions(inner_name='data.json'),
        write_options=WriteOptions(inner_name='payload.csv'),
        baseline=None,
        read_expected='data.json',
        write_expected='payload.csv',
    ),
    _OptionHelperCase(
        id='sheet',
        handler_cls=XlsxFile,
        method_name='sheet_from_options',
        read_options=ReadOptions(sheet='Sheet2'),
        write_options=WriteOptions(sheet=3),
        baseline=0,
        read_expected='Sheet2',
        write_expected=3,
        read_default='fallback_sheet',
        write_default=99,
    ),
    _OptionHelperCase(
        id='table',
        handler_cls=SqliteFile,
        method_name='table_from_options',
        read_options=ReadOptions(table='events'),
        write_options=WriteOptions(table='staging'),
        baseline=None,
        read_expected='events',
        write_expected='staging',
        read_default='fallback_table',
        write_default='fallback_table',
    ),
)


# SECTION: TESTS ============================================================ #


class TestBaseAbcContracts:
    """Unit tests for abstract base class contracts."""

    def test_at_returns_path_bound_facade(self) -> None:
        """Test that ``at(path)`` returns a bound callable handler facade."""
        handler = _DelimitedStub()

        bound = handler.at('ignored.csv')

        assert isinstance(bound, BoundFileHandler)
        assert bound.handler is handler
        assert bound.path == Path('ignored.csv')
        assert bound.read() == [{'id': 1}]
        assert bound.write([{'id': 1}, {'id': 2}]) == 2

    def test_at_supports_remote_uri_reads(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``at(path)`` routes remote URI reads through storage."""
        uri = 'https://example.com/files/data.json?download=1'
        session = _FakeHttpSession(payload=b'{"name": "Ada"}')
        handler = JsonFile()

        monkeypatch.setattr(http_storage_mod.requests, 'Session', lambda: session)

        bound = handler.at(uri)

        assert bound.handler is handler
        assert bound.path == uri
        assert bound.read() == {'name': 'Ada'}
        assert session.calls == [
            ('head', uri, True),
            ('get', uri, False),
        ]

    def test_at_supports_remote_uri_writes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``at(path)`` routes remote URI writes through storage."""
        uploads: list[bytes] = []

        class CaptureUpload(BytesIO):
            """Writable upload stream test double."""

            def close(self) -> None:
                """Capture uploaded bytes before closing the stream."""
                uploads.append(self.getvalue())
                super().close()

        class FakeBackend:
            """Remote backend write test double."""

            def ensure_parent_dir(self, location: object) -> None:
                """Accept parent preparation for remote storage."""
                del location

            def open(
                self,
                location: object,
                mode: str = 'r',
                **kwargs: object,
            ) -> CaptureUpload:
                """Return one writable byte stream for the remote object."""
                del location, kwargs
                assert mode == 'wb'
                return CaptureUpload()

        monkeypatch.setattr(core_mod, 'get_backend', lambda value: FakeBackend())

        handler = JsonFile()
        written = handler.at('s3://bucket/data.json').write({'name': 'Ada'})

        assert written == 1
        assert uploads == [b'{\n  "name": "Ada"\n}\n']

    @pytest.mark.parametrize(
        (
            'handler_cls',
            'path_name',
            'expected_category',
            'expected_read',
            'write_payload',
            'expected_written',
        ),
        [
            (
                _DelimitedStub,
                'ignored.csv',
                'tabular_delimited_text',
                [{'id': 1}],
                [{'id': 1}, {'id': 2}],
                2,
            ),
            (
                _TextFixedWidthStub,
                'ignored.txt',
                'text_fixed_width',
                [{'text': 'ok'}],
                [{'text': 'ok'}],
                1,
            ),
        ],
        ids=['delimited', 'text_fixed_width'],
    )
    def test_concrete_row_subclass_satisfies_contract(
        self,
        handler_cls: type[FileHandlerABC],
        path_name: str,
        expected_category: str,
        expected_read: JSONData,
        write_payload: JSONData,
        expected_written: int,
    ) -> None:
        """
        Test that concrete row-oriented subclasses being fully instantiable.
        """
        handler = handler_cls()
        path = Path(path_name)

        assert handler.category == expected_category
        assert handler.read(path) == expected_read
        assert handler.write(path, write_payload) == expected_written

    def test_file_handler_abc_declares_read_write_as_abstract(self) -> None:
        """
        Test that :class:`FileHandlerABC` preserving read/write abstract
        methods.
        """
        assert inspect.isabstract(FileHandlerABC)
        assert {'read', 'write'} <= FileHandlerABC.__abstractmethods__

    @pytest.mark.parametrize(
        (
            'handler_cls',
            'path_name',
            'dataset',
            'expected_error',
            'error_pattern',
        ),
        [
            (
                _ReadOnlyScientificStub,
                'ignored.hdf5',
                'other',
                RuntimeError,
                'read-only',
            ),
            (
                _ReadOnlySingleScientificStub,
                'ignored.sas7bdat',
                'data',
                RuntimeError,
                'read-only',
            ),
            (
                _ReadOnlySingleScientificStub,
                'ignored.sas7bdat',
                'other',
                ValueError,
                'supports only dataset key',
            ),
        ],
        ids=[
            'scientific_read_only',
            'single_default_dataset_write',
            'single_invalid_dataset_key',
        ],
    )
    def test_read_only_scientific_write_dataset_contract(
        self,
        handler_cls: type[ScientificDatasetFileHandlerABC],
        path_name: str,
        dataset: str,
        expected_error: type[Exception],
        error_pattern: str,
    ) -> None:
        """Test that read-only scientific write validation and guardrails."""
        handler = handler_cls()
        with pytest.raises(expected_error, match=error_pattern):
            handler.write_dataset(
                Path(path_name),
                [{'id': 1}],
                dataset=dataset,
            )

    @pytest.mark.parametrize(
        ('operation', 'kwargs'),
        [
            ('write', {'data': [{'a': 1}]}),
            ('write_sheet', {'rows': [{'a': 1}], 'sheet': 0}),
        ],
        ids=['module_write', 'sheet_write'],
    )
    def test_read_only_spreadsheet_handler_rejects_writes(
        self,
        operation: str,
        kwargs: dict[str, object],
    ) -> None:
        """
        Test that read-only spreadsheet handlers rejecting write operations.
        """
        handler = XlsFile()
        path = Path('ignored.xls')
        with pytest.raises(RuntimeError, match='read-only'):
            getattr(handler, operation)(path, **kwargs)

    @pytest.mark.parametrize(
        'incomplete_handler_cls',
        [_IncompleteDelimited, _IncompleteTextFixedWidth],
        ids=['delimited', 'text_fixed_width'],
    )
    def test_row_abcs_require_row_methods(
        self,
        incomplete_handler_cls: type[FileHandlerABC],
    ) -> None:
        """Test that row-oriented ABCs require row-level methods."""
        assert inspect.isabstract(incomplete_handler_cls)
        assert {'read_rows', 'write_rows'} <= (
            incomplete_handler_cls.__abstractmethods__
        )


class TestNamingConventions:
    """Unit tests for category-level internal naming conventions."""

    @pytest.mark.parametrize(
        ('handlers', 'read_method', 'write_method'),
        _NAMING_METHOD_CASES,
        ids=['delimited', 'embedded_db', 'scientific', 'spreadsheet'],
    )
    def test_handlers_expose_category_methods(
        self,
        handlers: tuple[type[FileHandlerABC], ...],
        read_method: str,
        write_method: str,
    ) -> None:
        """Test that handlers expose category-level read/write methods."""
        for handler_cls in handlers:
            for method_name in ('read', 'write', read_method, write_method):
                assert callable(getattr(handler_cls, method_name, None))


class TestOptionsContracts:
    """Unit tests for base option data classes."""

    def test_dataset_option_helpers_preserve_empty_string_values(self) -> None:
        """
        Test that dataset helpers preserving empty-string explicit/option
        values.
        """
        handler = DtaFile()
        options = ReadOptions(dataset='')

        assert handler.dataset_from_options(options) == ''
        assert handler.resolve_dataset(None, options=options) == ''
        assert (
            handler.resolve_dataset(
                '',
                options=ReadOptions(dataset='other'),
            )
            == ''
        )

    def test_dataset_option_helpers_use_override_then_default(self) -> None:
        """
        Test that scientific dataset helpers using explicit then default data.
        """
        handler = DtaFile()
        option_expectations = (
            (ReadOptions(dataset='features'), 'features'),
            (WriteOptions(dataset='labels'), 'labels'),
        )

        assert handler.dataset_from_options(None) is None
        for options, expected in option_expectations:
            assert handler.dataset_from_options(options) == expected
            assert handler.resolve_dataset(None, options=options) == expected
            assert handler.resolve_dataset('explicit', options=options) == ('explicit')
        assert handler.resolve_dataset(None, default='fallback') == 'fallback'

    @pytest.mark.parametrize(
        'case',
        _OPTION_HELPER_CASES,
        ids=[case.id for case in _OPTION_HELPER_CASES],
    )
    def test_option_helper_pairs_use_override_then_default(
        self,
        case: _OptionHelperCase,
    ) -> None:
        """Test that paired read/write option helpers with override/default."""
        helper = getattr(case.handler_cls(), case.method_name)

        assert helper(None) == case.baseline
        assert helper(case.read_options) == case.read_expected
        assert helper(case.write_options) == case.write_expected
        if case.read_default is not _NO_DEFAULT:
            assert helper(None, default=case.read_default) == case.read_default
        if case.write_default is not _NO_DEFAULT:
            assert helper(None, default=case.write_default) == case.write_default

    def test_read_options_use_independent_extras_dicts(self) -> None:
        """
        Test that each :class:`ReadOptions` instance gets its own extras dict.
        """
        first = ReadOptions()
        second = ReadOptions()

        assert not first.extras
        assert not second.extras
        assert first.extras is not second.extras

    def test_root_tag_option_helper_use_override_then_default(self) -> None:
        """
        Test that :class:`XlsFile` root-tag helper uses explicit values, then
        defaults.
        """
        handler = XlsFile()
        assert handler.root_tag_from_write_options(None) == 'root'
        assert (
            handler.root_tag_from_write_options(WriteOptions(root_tag='items'))
            == 'items'
        )
        assert handler.root_tag_from_write_options(None, default='dataset') == 'dataset'

    def test_write_options_are_frozen(self) -> None:
        """Test :class:`WriteOptions` immutability contract."""
        options = WriteOptions()

        with pytest.raises(FrozenInstanceError):
            options.encoding = 'latin-1'  # type: ignore[misc]


class TestScientificDatasetContracts:
    """Unit tests for scientific dataset handler contracts."""

    @pytest.mark.parametrize(
        'handler_cls',
        _SCIENTIFIC_HANDLER_CLASSES,
    )
    def test_handlers_use_scientific_dataset_abc(
        self,
        handler_cls: type[ScientificDatasetFileHandlerABC],
    ) -> None:
        """Test that scientific handlers inherit ScientificDataset ABC."""
        assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
        assert handler_cls.dataset_key == 'data'

    @pytest.mark.parametrize(
        'handler_cls',
        _SINGLE_DATASET_HANDLER_CLASSES,
    )
    def test_single_dataset_handlers_reject_unknown_dataset_key(
        self,
        handler_cls: type[SingleDatasetScientificFileHandlerABC],
    ) -> None:
        """
        Test that single-dataset scientific handlers reject unknown keys.
        """
        assert issubclass(
            handler_cls,
            SingleDatasetScientificFileHandlerABC,
        )
        assert_single_dataset_rejects_non_default_key(
            handler_cls(),
            suffix=handler_cls.format.value,
        )


class TestScientificStubConventions:
    """Unit tests for scientific placeholder handler conventions."""

    @pytest.mark.parametrize(
        'handler_cls',
        _SCIENTIFIC_STUB_HANDLER_CLASSES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_scientific_stubs_inherit_stub_single_dataset_abc(
        self,
        handler_cls: type[StubSingleDatasetScientificFileHandlerABC],
    ) -> None:
        """Test that scientific stubs inheriting the unified stub base."""
        assert issubclass(
            handler_cls,
            StubSingleDatasetScientificFileHandlerABC,
        )
